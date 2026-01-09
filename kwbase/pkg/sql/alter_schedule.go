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

package sql

import (
	"context"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
	"github.com/gorhill/cronexpr"
)

const (
	// states and behaviors of scheduled jobs
	optFirstRun          = "first_run"
	optOnExecFailure     = "on_execution_failure"
	optOnPreviousRunning = "on_previous_running"
)

const (
	// ScheduleRetention is the name of scheduled_table_retention
	ScheduleRetention = "scheduled_table_retention"
	// ScheduleAutonomy is the name of scheduled_table_autonomy
	ScheduleAutonomy = "scheduled_table_autonomy"
	// ScheduleVacuum is the name of scheduled_table_vacuum
	ScheduleVacuum = "scheduled_table_vacuum"
	// ScheduleCount is the name of scheduled_table_count
	ScheduleCount = "scheduled_table_count"
)

var scheduledBackupOptionExpectValues = map[string]KVStringOptValidate{
	optFirstRun:          KVStringOptRequireValue,
	optOnExecFailure:     KVStringOptRequireValue,
	optOnPreviousRunning: KVStringOptRequireValue,
}

type alterScheduleNode struct {
	n *tree.AlterSchedule
	p *planner
}

// AlterSchedule creates a AlterSchedule planNode.
func (p *planner) AlterSchedule(ctx context.Context, n *tree.AlterSchedule) (planNode, error) {
	return &alterScheduleNode{
		n: n,
		p: p,
	}, nil
}

func (n *alterScheduleNode) startExec(params runParams) error {
	if isAdmin, err := n.p.HasAdminRole(params.ctx); !isAdmin {
		if err != nil {
			return err
		}
		return pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is not superuser or membership of admin, has no privilege to ALTER SCHEDULE",
			n.p.User())
	}

	schedule, err := loadSchedule(params, n.n.ScheduleName)
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

	if value, ok := opts[optFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(&n.p.ExtendedEvalContext().EvalContext, value, time.Microsecond)
		if err != nil {
			return err
		}
		schedule.SetNextRun(firstRun.Time)
	}

	scheduleDetails := jobspb.ScheduleDetails{Wait: jobspb.ScheduleDetails_SKIP, OnError: jobspb.ScheduleDetails_RETRY_SCHED}
	if value, ok := opts[optOnExecFailure]; ok {
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

	if value, ok := opts[optOnPreviousRunning]; ok {
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
	err = updateSchedule(params, schedule)
	if err != nil {
		return err
	}
	params.p.SetAuditTarget(0, string(n.n.ScheduleName), nil)
	return nil
}

func (*alterScheduleNode) Next(runParams) (bool, error) { return false, nil }
func (*alterScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*alterScheduleNode) Close(context.Context)        {}
