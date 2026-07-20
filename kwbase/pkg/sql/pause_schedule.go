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

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

var _ PlanNode = &pauseScheduleNode{}

type pauseScheduleNode struct {
	n *tree.PauseSchedule
	p *GenericPlanner
}

// PauseSchedule pauses a Schedule.
func (p *GenericPlanner) PauseSchedule(
	ctx context.Context, n *tree.PauseSchedule,
) (PlanNode, error) {
	return &pauseScheduleNode{
		n: n,
		p: p,
	}, nil
}

func (n *pauseScheduleNode) StartExec(params RunParams) error {
	if isAdmin, err := n.p.HasAdminRole(params.Ctx); !isAdmin {
		if err != nil {
			return err
		}
		return errors.Errorf("%s is not superuser or membership of admin, has no privilege to PAUSE SCHEDULE",
			n.p.User())
	}
	schedule, err := LoadSchedule(params, n.n.ScheduleName)
	if err != nil {
		return err
	}

	if schedule == nil {
		if n.n.IfExists {
			return nil
		}
		return pgerror.Newf(pgcode.UndefinedObject, "schedule %s does not exist", n.n.ScheduleName)
	}
	schedule.Pause()
	err = UpdateSchedule(params, schedule)
	if err != nil {
		return err
	}
	params.GetPlanner().SetAuditTarget(0, string(n.n.ScheduleName), nil)
	return nil
}

func (*pauseScheduleNode) Next(RunParams) (bool, error) { return false, nil }
func (*pauseScheduleNode) Values() tree.Datums          { return tree.Datums{} }
func (*pauseScheduleNode) Close(context.Context)        {}
