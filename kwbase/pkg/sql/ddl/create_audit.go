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
	"sort"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql"
	"gitee.com/kwbasedb/kwbase/pkg/sql/audit"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/pkg/errors"
)

var _ sql.PlanNode = &createAuditNode{}

type createAuditNode struct {
	ifNotExists bool
	name        string
	eventType   string
	id          uint32
	operations  []string
	operators   tree.NameList
	whenever    string
	condition   int32
	action      int32
	level       int32
}

// NewCreateAuditNode for testing
// nolint:unexportedreturn
func NewCreateAuditNode(
	ifNotExists bool,
	name string,
	eventType string,
	id uint32,
	operations []string,
	operators tree.NameList,
	whenever string,
	condition int32,
	action int32,
	level int32,
) *createAuditNode {
	return &createAuditNode{
		ifNotExists: ifNotExists,
		name:        name,
		eventType:   eventType,
		id:          id,
		operations:  operations,
		operators:   operators,
		whenever:    whenever,
		condition:   condition,
		action:      action,
		level:       level,
	}
}

// CreateAudit handles CREATE AUDIT statements to create new audit configurations
func CreateAudit(
	ctx context.Context, p *GenericPlanner, n *tree.CreateAudit,
) (sql.PlanNode, error) {
	var id uint32
	var operations []string
	targetType := n.Target.Type
	if len(n.Target.Name.TableName) != 0 {
		// object audit
		var requiredType sql.ResolveRequiredType
		if targetType == "TABLE" {
			requiredType = sql.ResolveRequireTableDesc
		} else if targetType == "VIEW" {
			requiredType = sql.ResolveRequireViewDesc
		}
		desc, err := p.ResolveMutableTableDescriptor(ctx, &n.Target.Name, true, requiredType)
		if err != nil {
			return nil, err
		}
		if sqlbase.IsReservedID(desc.ID) {
			return nil, errors.Errorf("unsupport audit on system object")
		}
		id = uint32(desc.ID)

		targetTypes := auditObjectType[targetType]
		if n.Operations[0] != "ALL" {
			for _, op := range n.Operations {
				op := strings.ToUpper(string(op))
				if index := sort.SearchStrings(targetTypes, op); index >= len(targetTypes) || op != targetTypes[index] {
					return nil, errors.Errorf("invalid audit operation %s for type %s of object audit", op, targetType)
				}
				operations = append(operations, op)
			}
		} else {
			operations = append(operations, "ALL")
		}
	} else {
		// statement audit
		if targetTypes, ok := auditStmtType[targetType]; ok || targetType == "ALL" {
			if n.Operations[0] != "ALL" {
				for _, op := range n.Operations {
					op := strings.ToUpper(string(op))
					if index := sort.SearchStrings(targetTypes, op); index >= len(targetTypes) || op != targetTypes[index] {
						return nil, errors.Errorf("invalid audit operation %s for type %s of statement audit", op, targetType)
					}
					operations = append(operations, op)
				}
			} else {
				operations = append(operations, "ALL")
			}
		} else {
			return nil, errors.Errorf("invalid audit type: %s", targetType)
		}
	}

	whenever := "ALL"
	if len(n.Whenever) != 0 {
		whenever = strings.ToUpper(n.Whenever)
	}

	var condition, action, level int32
	var err error
	if n.Condition != nil {
		v, ok := n.Condition.(*tree.NumVal)
		if !ok {
			return nil, errors.Errorf("failed to cast %T to int", n.Condition)
		}
		condition, err = v.AsInt32()
		if err != nil {
			return nil, err
		}
	}

	if n.Action != nil {
		v, ok := n.Action.(*tree.NumVal)
		if !ok {
			return nil, errors.Errorf("failed to cast %T to int", n.Action)
		}
		action, err = v.AsInt32()
		if err != nil {
			return nil, err
		}
	}

	if n.Level != nil {
		v, ok := n.Level.(*tree.NumVal)
		if !ok {
			return nil, errors.Errorf("failed to cast %T to int", n.Level)
		}
		level, err = v.AsInt32()
		if err != nil {
			return nil, err
		}
	}

	return &createAuditNode{
		ifNotExists: n.IfNotExists,
		name:        n.Name.String(),
		eventType:   targetType,
		id:          id,
		operations:  operations,
		operators:   n.Operators,
		whenever:    whenever,
		condition:   condition,
		action:      action,
		level:       level,
	}, nil
}

func (n *createAuditNode) StartExec(params RunParams) error {

	operators := tree.NewDArray(types.String)
	operations := tree.NewDArray(types.String)
	if n.operators[0] != "ALL" {
		for _, user := range n.operators {
			if err := operators.Append(tree.NewDString(string(user))); err != nil {
				return err
			}
		}
	} else {
		if err := operators.Append(tree.NewDString("ALL")); err != nil {
			return err
		}
	}

	for _, op := range n.operations {
		if err := operations.Append(tree.NewDString(op)); err != nil {
			return err
		}
	}

	row, err := params.ExecCfg().InternalExecutor.QueryRow(
		params.Ctx,
		"select-audit",
		params.PlannerTxn(),
		`SELECT 1 FROM system.audits WHERE audit_name = $1`,
		n.name,
	)
	if err != nil {
		return pgerror.New(pgcode.Warning, "error looking up audit")
	}
	if row != nil {
		if n.ifNotExists {
			return nil
		}
		return pgerror.Newf(pgcode.DuplicateObject, "audit %s has already exists", n.name)
	}

	_, err = params.ExecCfg().InternalExecutor.Exec(
		params.Ctx,
		"create-audit",
		params.PlannerTxn(),
		`INSERT INTO system.audits VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)`,
		n.name,
		n.eventType,
		n.id,
		operations,
		operators,
		n.condition,
		n.whenever,
		n.action,
		n.level,
		false,
	)
	if err != nil {
		return err
	}

	if err := params.GetPlanner().BumpTableVersion(params.Ctx, &audit.AuditTableName); err != nil {
		return err
	}

	params.GetPlanner().SetAuditTarget(n.id, n.name, nil)
	return nil
}

func (*createAuditNode) Next(params RunParams) (bool, error) { return false, nil }

func (*createAuditNode) Values() tree.Datums { return nil }

func (*createAuditNode) Close(ctx context.Context) {}

func (*createAuditNode) SetUpsert() error { return nil }

var auditStmtType = map[string][]string{
	"DATABASE":   {"ALTER", "CREATE", "DROP", "EXPORT", "IMPORT"},
	"SCHEMA":     {"ALTER", "CREATE", "DROP", "DUMP", "LOAD"},
	"TABLE":      {"ALTER", "CREATE", "DROP", "DUMP", "EXPORT", "FLASHBACK", "IMPORT", "LOAD", "TRUNCATE"},
	"INDEX":      {"ALTER", "CREATE", "DROP"},
	"VIEW":       {"ALTER", "CREATE", "DROP"},
	"SEQUENCE":   {"ALTER", "CREATE", "DROP"},
	"JOB":        {"CANCEL", "PAUSE", "RESUME"},
	"SCHEDULE":   {"ALTER", "PAUSE", "RESUME"},
	"USER":       {"ALTER", "CREATE", "DROP"},
	"ROLE":       {"ALTER", "CREATE", "DROP", "GRANT", "REVOKE"},
	"PRIVILEGE":  {"GRANT", "REVOKE"},
	"QUERY":      {"CANCEL", "EXPLAIN"},
	"RANGE":      {"ALTER"},
	"STATISTICS": {"CREATE"},
	"SESSION":    {"CANCEL", "RESET", "SET"},
	"AUDIT":      {"ALTER", "CREATE", "DROP"},
}

var auditObjectType = map[string][]string{
	"TABLE": {"DELETE", "INSERT", "SELECT", "UPDATE"},
	"VIEW":  {"DELETE", "INSERT", "SELECT", "UPDATE"},
}
