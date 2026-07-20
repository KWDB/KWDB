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

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/audit"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

var _ sql.PlanNode = &alterAuditNode{}

type alterAuditNode struct {
	name     string
	newName  string
	enable   bool
	ifExists bool
}

// NewAlterAuditNode creates a new alterAuditNode. This func is added only for testing.
// nolint:unexportedreturn
func NewAlterAuditNode(name, newName string, enable, ifExists bool) *alterAuditNode {
	return &alterAuditNode{
		name:     name,
		newName:  newName,
		enable:   enable,
		ifExists: ifExists,
	}
}

// AlterAudit handles ALTER AUDIT statements to modify audit configurations
func AlterAudit(ctx context.Context, p *GenericPlanner, n *tree.AlterAudit) (sql.PlanNode, error) {
	return &alterAuditNode{
		name:     string(n.Name),
		newName:  string(n.NewName),
		enable:   n.Enable,
		ifExists: n.IfExists,
	}, nil
}

func (n *alterAuditNode) StartExec(params RunParams) error {
	row, err := params.ExecCfg().InternalExecutor.QueryRow(
		params.Ctx,
		"select-audit",
		params.PlannerTxn(),
		`SELECT 1 FROM system.audits WHERE audit_name = $1`,
		n.name,
	)
	if err != nil {
		return errors.Wrapf(err, "error looking up audit")
	}
	if row == nil {
		if n.ifExists {
			return nil
		}
		return errors.Errorf("audit %s does not exists", n.name)
	}

	stmt := fmt.Sprintf(`UPDATE system.audits SET enable = %t WHERE audit_name = '%s'`, n.enable, n.name)
	if len(n.newName) != 0 {
		stmt = fmt.Sprintf(`UPDATE system.audits SET audit_name = '%s' WHERE audit_name = '%s'`, n.newName, n.name)
	}
	_, err = params.ExecCfg().InternalExecutor.Exec(
		params.Ctx,
		"alter-audit",
		params.PlannerTxn(),
		stmt,
	)
	if err != nil {
		return err
	}

	if err := params.GetPlanner().BumpTableVersion(params.Ctx, &audit.AuditTableName); err != nil {
		return err
	}
	params.GetPlanner().SetAuditTarget(0, n.name, nil)
	return nil
}

func (*alterAuditNode) Next(params RunParams) (bool, error) { return false, nil }

func (*alterAuditNode) Values() tree.Datums { return nil }

func (*alterAuditNode) Close(ctx context.Context) {}

func (*alterAuditNode) SetUpsert() error { return nil }
