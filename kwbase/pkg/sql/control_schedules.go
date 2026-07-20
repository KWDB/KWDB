// Copyright 2020 The Cockroach Authors.
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

package sql

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/scheduledjobs"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// jobSchedulerEnv returns JobSchedulerEnv.
func jobSchedulerEnv(params RunParams) scheduledjobs.JobSchedulerEnv {
	if knobs, ok := params.ExecCfg().DistSQLSrv.TestingKnobs.JobsTestingKnobs.(*jobs.TestingKnobs); ok {
		if knobs.JobSchedulerEnv != nil {
			return knobs.JobSchedulerEnv
		}
	}
	return scheduledjobs.ProdJobSchedulerEnv
}

// LoadSchedule loads schedule information.
func LoadSchedule(params RunParams, scheduleName tree.Name) (*jobs.ScheduledJob, error) {
	env := jobSchedulerEnv(params)
	schedule := jobs.NewScheduledJob(env)

	// Load schedule expression.  This is needed for resume command, but we
	// also use this query to check for the schedule existence.
	datums, cols, err := params.ExecCfg().InternalExecutor.QueryWithCols(
		params.Ctx,
		"load-schedule",
		params.EvalContext().Txn, sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"SELECT schedule_id, schedule_expr FROM %s WHERE schedule_name = $1",
			env.ScheduledJobsTableName(),
		),
		string(scheduleName))
	if err != nil {
		return nil, err
	}

	// Not an error if schedule does not exist.
	if len(datums) != 1 {
		return nil, nil
	}

	if err := schedule.InitFromDatums(datums[0], cols); err != nil {
		return nil, err
	}
	return schedule, nil
}

// UpdateSchedule executes update for the schedule.
func UpdateSchedule(params RunParams, schedule *jobs.ScheduledJob) error {
	return schedule.Update(
		params.Ctx,
		params.ExecCfg().InternalExecutor,
		params.EvalContext().Txn,
	)
}

// DeleteSchedule deletes specified schedule.
func DeleteSchedule(params RunParams, scheduleID int64) error {
	env := jobSchedulerEnv(params)
	_, err := params.ExecCfg().InternalExecutor.ExecEx(
		params.Ctx,
		"delete-schedule",
		params.EvalContext().Txn,
		sqlbase.InternalExecutorSessionDataOverride{User: security.RootUser},
		fmt.Sprintf(
			"DELETE FROM %s WHERE schedule_id = $1",
			env.ScheduledJobsTableName(),
		),
		scheduleID,
	)
	return err
}

// CheckScheduledSQL checks whether the sql statement supports to be scheduled.
func CheckScheduledSQL(ctx context.Context, p *GenericPlanner, sql string) error {
	stmt, err := parser.ParseOne(sql)
	if err != nil {
		return err
	}
	// check if the sql statement is supported by schedule
	switch stmt.AST.(type) {
	// only support some dml statement for now
	case *tree.Insert, *tree.Update, *tree.Delete:
	default:
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s does not support to be scheduled", stmt.AST.StatementTag())
	}
	if stmt.NumPlaceholders != 0 {
		return pgerror.New(pgcode.FeatureNotSupported, "scheduled sql does not support placeholder")
	}
	// make a new local planner to check the sql if correct
	plan, cleanup := newInternalPlanner("sqlSchedule", p.txn, p.User(), &MemoryMetrics{}, p.execCfg)
	defer cleanup()
	localPlanner := plan
	localPlanner.stmt = &Statement{Statement: stmt}
	//localPlanner.SessionData().Database = p.CurrentDatabase()
	//localPlanner.SessionData().SearchPath = p.CurrentSearchPath()

	localPlanner.optPlanningCtx.init(localPlanner)

	localPlanner.RunWithOptions(ResolveFlags{SkipCache: true}, func() {
		err = localPlanner.makeOptimizerPlan(ctx)
	})
	if err != nil {
		return err
	}
	defer localPlanner.curPlan.close(ctx)
	return nil
}
