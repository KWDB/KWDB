// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

type dropScheduleNode struct {
	scheduleNames []tree.Name
	numRow        int
	ifExists      bool
}

// DropSchedule resumes a Schedule.
func (p *planner) DropSchedule(ctx context.Context, n *tree.DropSchedule) (planNode, error) {
	if isAdmin, err := p.HasAdminRole(ctx); !isAdmin {
		if err != nil {
			return nil, err
		}
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s is not superuser or membership of admin, has no privilege to DROP SCHEDULE", p.User())
	}
	var scheduleNames []tree.Name

	if n.ScheduleName != nil {
		if *n.ScheduleName == ScheduleCompress || *n.ScheduleName == ScheduleRetention ||
			*n.ScheduleName == ScheduleAutonomy || *n.ScheduleName == ScheduleVacuum || *n.ScheduleName == ScheduleCount {
			return nil, pgerror.Newf(pgcode.DataException, "can not drop system schedule %s", *n.ScheduleName)
		}
		scheduleNames = append(scheduleNames, *n.ScheduleName)
	}

	if n.Schedules != nil {
		rows, err := p.EvalContext().InternalExecutor.Query(ctx, "select schedules", p.txn, n.Schedules.String())
		if err != nil {
			return nil, err
		}
		for _, row := range rows {
			if len(row) != 1 {
				return nil, pgerror.Newf(pgcode.DataException, "expects a single column source, got %d columns", len(row))
			}
			name, ok := tree.AsDString(row[0])
			if !ok {
				return nil, pgerror.New(pgcode.DataException, "expects a string column source")
			}
			if name == ScheduleCompress || name == ScheduleRetention || name == ScheduleAutonomy || name == ScheduleVacuum || name == ScheduleCount {
				return nil, pgerror.Newf(pgcode.DataException, "can not drop system schedule %s", name)
			}
			scheduleNames = append(scheduleNames, tree.Name(name))
		}
	}

	return &dropScheduleNode{scheduleNames: scheduleNames, ifExists: n.IfExists}, nil
}

func (n *dropScheduleNode) startExec(params runParams) error {
	for _, name := range n.scheduleNames {
		schedule, err := loadSchedule(params, name)
		if err != nil {
			return err
		}
		if schedule == nil {
			if n.ifExists {
				continue
			}
			return pgerror.Newf(pgcode.UndefinedObject, "schedule %s does not exist", name)
		}
		err = deleteSchedule(params, schedule.ScheduleID())
		if err != nil {
			return err
		}
		n.numRow++
	}
	return nil
}

// FastPathResults implements the planNodeFastPath interface.
func (n *dropScheduleNode) FastPathResults() (int, bool) {
	return n.numRow, true
}

func (*dropScheduleNode) Next(runParams) (bool, error) { return false, nil }
func (*dropScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*dropScheduleNode) Close(context.Context)        {}
