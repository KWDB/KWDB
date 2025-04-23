// Copyright 2019 The Cockroach Authors.
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

package delegate

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/errorutil/unimplemented"
)

// Certain statements (most SHOW variants) are just syntactic sugar for a more
// complicated underlying query.
//
// This package contains the logic to convert the AST of such a statement to the
// AST of the equivalent query to be planned.

// TryDelegate takes a statement and checks if it is one of the statements that
// can be rewritten as a lower level query. If it can, returns a new AST which
// is equivalent to the original statement. Otherwise, returns nil.
func TryDelegate(
	ctx context.Context, catalog cat.Catalog, evalCtx *tree.EvalContext, stmt tree.Statement,
) (tree.Statement, error) {
	d := delegator{
		ctx:     ctx,
		catalog: catalog,
		evalCtx: evalCtx,
	}
	switch t := stmt.(type) {
	case *tree.ShowApplications:
		return d.delegateShowApplications()

	case *tree.ShowTags:
		return d.delegateShowAttributes(t)

	case *tree.ShowTagValues:
		return d.delegateShowTagValues(t)

	case *tree.ShowClusterSettingList:
		return d.delegateShowClusterSettingList(t)

	case *tree.ShowDatabases:
		return d.delegateShowDatabases(t)

	case *tree.ShowFunction:
		return d.delegateShowFunction(t)

	case *tree.ShowSchedule:
		return d.delegateShowSchedules(t)

	case *tree.ShowCreate:
		return d.delegateShowCreate(t)

	case *tree.ShowCreateDatabase:
		return d.delegateShowCreateDatabase(t)

	case *tree.ShowDatabaseIndexes:
		return d.delegateShowDatabaseIndexes(t)

	case *tree.ShowIndexes:
		return d.delegateShowIndexes(t)

	case *tree.ShowColumns:
		return d.delegateShowColumns(t)

	case *tree.ShowConstraints:
		return d.delegateShowConstraints(t)

	case *tree.ShowPartitions:
		return d.delegateShowPartitions(t)

	case *tree.ShowGrants:
		return d.delegateShowGrants(t)

	case *tree.ShowJobs:
		return d.delegateShowJobs(t)

	case *tree.ShowQueries:
		return d.delegateShowQueries(t)

	case *tree.ShowRanges:
		return d.delegateShowRanges(t)

	case *tree.ShowRangeForRow:
		return d.delegateShowRangeForRow(t)

	case *tree.ShowRetentions:
		return d.delegateShowRetentions(t)

	case *tree.ShowRoleGrants:
		return d.delegateShowRoleGrants(t)

	case *tree.ShowRoles:
		return d.delegateShowRoles()

	case *tree.ShowSchemas:
		return d.delegateShowSchemas(t)

	case *tree.ShowSequences:
		return d.delegateShowSequences(t)

	case *tree.ShowSessions:
		return d.delegateShowSessions(t)

	case *tree.ShowSyntax:
		return d.delegateShowSyntax(t)

	case *tree.ShowTables:
		return d.delegateShowTables(t)

	case *tree.ShowUsers:
		return d.delegateShowRoles()

	case *tree.ShowVar:
		return d.delegateShowVar(t)

	case *tree.ShowUdvVar:
		return d.delegateShowUdvVar(t)

	case *tree.ShowZoneConfig:
		return d.delegateShowZoneConfig(t)

	case *tree.ShowTransactionStatus:
		return d.delegateShowVar(&tree.ShowVar{Name: "transaction_status"})

	case *tree.ShowAudits:
		return d.delegateShowAudits(t)

	case *tree.ShowSavepointStatus:
		return nil, unimplemented.NewWithIssue(47333, "cannot use SHOW SAVEPOINT STATUS as a statement source")

	default:
		return nil, nil
	}
}

type delegator struct {
	ctx     context.Context
	catalog cat.Catalog
	evalCtx *tree.EvalContext
}

func parse(sql string) (tree.Statement, error) {
	s, err := parser.ParseOne(sql)
	return s.AST, err
}
