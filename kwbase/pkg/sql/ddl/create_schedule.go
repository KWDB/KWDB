// Copyright 2017 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
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
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlconst"
	"github.com/gogo/protobuf/types"
	"github.com/gorhill/cronexpr"
)

var _ sql.PlanNode = &createScheduleNode{}

type createScheduleNode struct {
	n *tree.CreateSchedule
	// schedule specific properties that get evaluated.
	scheduleName func() (string, error)
	recurrence   func() (string, error)
	scheduleOpts func() (map[string]string, error)
}

// NewCreateScheduleNode creates a new createScheduleNode. This func is added only for testing.
// nolint:unexportedreturn
func NewCreateScheduleNode(
	n *tree.CreateSchedule,
	scheduleName func() (string, error),
	recurrence func() (string, error),
	scheduleOpts func() (map[string]string, error),
) *createScheduleNode {
	return &createScheduleNode{
		n:            n,
		scheduleName: scheduleName,
		recurrence:   recurrence,
		scheduleOpts: scheduleOpts,
	}
}

const scheduleExecSQLOp = "CREATE SCHEDULE FOR SQL"

// CreateSchedule creates a Schedule.
func CreateSchedule(
	ctx context.Context, p *GenericPlanner, n *tree.CreateSchedule,
) (sql.PlanNode, error) {
	node := &createScheduleNode{n: n}
	var err error
	if n.ScheduleName != nil {
		node.scheduleName, err = sql.TypeAsString(p, n.ScheduleName, scheduleExecSQLOp)
		if err != nil {
			return nil, err
		}
	}

	if n.Recurrence == nil {
		// sanity check: recurrence must be specified.
		return nil, pgerror.New(pgcode.InvalidParameterValue, "RECURRING clause required")
	}
	node.recurrence, err = sql.TypeAsString(p, n.Recurrence, scheduleExecSQLOp)
	if err != nil {
		return nil, err
	}

	node.scheduleOpts, err = p.TypeAsStringOpts(n.ScheduleOptions, scheduledBackupOptionExpectValues)
	if err != nil {
		return nil, err
	}
	if isAdmin, err := p.HasAdminRole(ctx); !isAdmin {
		if err != nil {
			return nil, err
		}
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is not superuser or membership of admin, has no privilege to CREATE SCHEDULE",
			p.User())
	}

	return node, nil
}

func (n *createScheduleNode) StartExec(params RunParams) error {
	env := scheduledjobs.ProdJobSchedulerEnv
	if knobs, ok := params.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			env = knobs.JobSchedulerEnv
		}
	}

	var fullScheduleName string
	if n.scheduleName != nil {
		scheduleName, err := n.scheduleName()
		if err != nil {
			return err
		}
		fullScheduleName = scheduleName
	} else {
		fullScheduleName = fmt.Sprintf("EXEC SQL %d", env.Now().Unix())
	}
	// check if the schedule already exists.
	schedule, err := sql.LoadSchedule(params, tree.Name(fullScheduleName))
	if err != nil {
		return err
	}
	if schedule != nil {
		if n.n.IfNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject, "schedule %q already exists", fullScheduleName)
	}
	// check if the target is correct
	err = sql.CheckScheduledSQL(params.Ctx, params.GetPlanner(), n.n.SQL)
	if err != nil {
		return err
	}

	// parse cron expr
	cron, err := n.recurrence()
	if err != nil {
		return err
	}
	expr, err := cronexpr.Parse(cron)
	if err != nil {
		return err
	}
	scheduleOptions, err := n.scheduleOpts()
	if err != nil {
		return err
	}

	scheduleDetails := jobspb.ScheduleDetails{Wait: jobspb.ScheduleDetails_SKIP, OnError: jobspb.ScheduleDetails_RETRY_SCHED}
	if value, ok := scheduleOptions[sqlconst.OptOnExecFailure]; ok {
		switch value {
		case "retry":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SOON
		case "reschedule":
			scheduleDetails.OnError = jobspb.ScheduleDetails_RETRY_SCHED
		case "pause":
			scheduleDetails.OnError = jobspb.ScheduleDetails_PAUSE_SCHED
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "%s is not valid on_execution_failure parameter", value)
		}
	}

	if value, ok := scheduleOptions[sqlconst.OptOnPreviousRunning]; ok {
		switch value {
		case "start":
			scheduleDetails.Wait = jobspb.ScheduleDetails_NO_WAIT
		case "skip":
			scheduleDetails.Wait = jobspb.ScheduleDetails_SKIP
		case "wait":
			scheduleDetails.Wait = jobspb.ScheduleDetails_WAIT
		default:
			return pgerror.Newf(pgcode.InvalidParameterValue, "%s is not valid on_previous_running parameter", value)
		}
	}
	// make new schedule job
	schedule = jobs.NewScheduledJob(env)
	schedule.SetScheduleLabel(fullScheduleName)
	schedule.SetOwner(params.GetPlanner().User())
	schedule.SetNextRun(expr.Next(env.Now()))
	schedule.SetScheduleDetails(scheduleDetails)
	if err := schedule.SetSchedule(cron); err != nil {
		return err
	}

	any, err := types.MarshalAny(
		&jobspb.SqlStatementExecutionArg{Statement: n.n.SQL})
	if err != nil {
		return err
	}
	schedule.SetExecutionDetails(tree.ScheduledExecSQLExecutor.InternalName(), jobspb.ExecutionArguments{Args: any})
	if value, ok := scheduleOptions[sqlconst.OptFirstRun]; ok {
		firstRun, err := tree.ParseDTimestampTZ(params.ExtEvalContext(), value, time.Microsecond)
		if err != nil {
			return err
		}
		schedule.SetNextRun(firstRun.Time)
	}

	if err := schedule.Create(params.Ctx, params.PlannerExecCfg().InternalExecutor, nil); err != nil {
		return err
	}
	return nil
}

func (*createScheduleNode) Next(RunParams) (bool, error) { return false, nil }
func (*createScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*createScheduleNode) Close(context.Context)        {}
