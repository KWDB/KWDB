// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package ddl

import (
	"context"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

var scheduledBackupOptionExpectValues = map[string]sqlconst.KVStringOptValidate{
	sqlconst.OptFirstRun:          sqlconst.KVStringOptRequireValue,
	sqlconst.OptOnExecFailure:     sqlconst.KVStringOptRequireValue,
	sqlconst.OptOnPreviousRunning: sqlconst.KVStringOptRequireValue,
}

var _ sql.PlanNode = &alterScheduleNode{}

type alterScheduleNode struct {
	n *tree.AlterSchedule
	p *GenericPlanner
}

// NewAlterScheduleNode creates a new alterScheduleNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterScheduleNode(n *tree.AlterSchedule, p *GenericPlanner) *alterScheduleNode {
	return &alterScheduleNode{
		n: n,
		p: p,
	}
}

// AlterSchedule creates a AlterSchedule PlanNode.
func AlterSchedule(
	ctx context.Context, p *GenericPlanner, n *tree.AlterSchedule,
) (sql.PlanNode, error) {
	return &alterScheduleNode{
		n: n,
		p: p,
	}, nil
}

func (n *alterScheduleNode) StartExec(params RunParams) error {
	if isAdmin, err := n.p.HasAdminRole(params.Ctx); !isAdmin {
		if err != nil {
			return err
		}
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is not superuser or membership of admin, has no privilege to ALTER SCHEDULE",
			n.p.User())
	}

	schedule, err := sql.LoadSchedule(params, n.n.ScheduleName)
	if err != nil {
		return err
	}

	if schedule == nil {
		if n.n.IfExists {
			return nil
		}
		return pgerror.Newf(pgcode.UndefinedObject, "schedule %s does not exist", n.n.ScheduleName)
	}

	if n.n.Recurrence == nil {
		return nil
	}

	scheduleExpr := strings.Trim(n.n.Recurrence.String(), "'")
	expr, err := cronexpr.Parse(scheduleExpr)
	if err != nil {
		return err
	}
	schedule.SetNextRun(expr.Next(scheduledjobs.ProdJobSchedulerEnv.Now()))
	if err := schedule.SetSchedule(scheduleExpr); err != nil {
		return err
	}

	optsFn, err := n.p.TypeAsStringOpts(n.n.ScheduleOptions, scheduledBackupOptionExpectValues)
	if err != nil {
		return err
	}
	opts, err := optsFn()
	if err != nil {
		return err
	}

	if value, ok := opts[sqlconst.OptFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(&n.p.ExtendedEvalContext().EvalContext, value, time.Microsecond)
		if err != nil {
			return err
		}
		schedule.SetNextRun(firstRun.Time)
	}

	scheduleDetails := jobspb.ScheduleDetails{Wait: jobspb.ScheduleDetails_SKIP, OnError: jobspb.ScheduleDetails_RETRY_SCHED}
	if value, ok := opts[sqlconst.OptOnExecFailure]; ok {
		switch value {
		case "retry":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SOON
		case "reschedule":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SCHED
		case "pause":
			scheduleDetails.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
		default:
			return errors.Errorf("%s is not valid on_execution_failure parameter", value)
		}
	}

	if value, ok := opts[sqlconst.OptOnPreviousRunning]; ok {
		switch value {
		case "start":
			scheduleDetails.Wait = jobspb.ScheduleDetails_NO_WAIT
		case "skip":
			scheduleDetails.Wait = jobspb.ScheduleDetails_SKIP
		case "wait":
			scheduleDetails.Wait = jobspb.ScheduleDetails_WAIT
		default:
			return errors.Errorf("%s is not valid on_previous_running parameter", value)
		}
	}
	schedule.SetScheduleDetails(scheduleDetails)
	err = sql.UpdateSchedule(params, schedule)
	if err != nil {
		return err
	}
	params.GetPlanner().SetAuditTarget(0, string(n.n.ScheduleName), nil)
	return nil
}

func (*alterScheduleNode) Next(RunParams) (bool, error) { return false, nil }
func (*alterScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterScheduleNode) Close(context.Context)        {}
