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

package delegate

import (
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

// ValidVars contains the set of variable names; initialized from the SQL
// package.
var ValidVars = make(map[string]struct{})

// Show a session-local variable name.
func (d *delegator) delegateShowVar(n *tree.ShowVar) (tree.Statement, error) {
	origName := n.Name
	name := strings.ToLower(n.Name)

	if name == "locality" {
		sqltelemetry.IncrementShowCounter(sqltelemetry.Locality)
	}

	if name == "all" {
		return parse(
			"SELECT variable, value FROM kwdb_internal.session_variables WHERE hidden = FALSE",
		)
	}

	if _, ok := ValidVars[name]; !ok {
		return nil, pgerror.Newf(pgcode.UndefinedObject,
			"unrecognized configuration parameter %q", origName)
	}

	varName := lex.EscapeSQLString(name)
	nm := tree.Name(name)
	return parse(fmt.Sprintf(
		`SELECT value AS %[1]s FROM kwdb_internal.session_variables WHERE variable = %[2]s`,
		nm.String(), varName,
	))
}

// Show a user defined variable name and type.
func (d *delegator) delegateShowUdvVar(n *tree.ShowUdvVar) (tree.Statement, error) {
	var name, typ string
	varName := strings.ToLower(n.Name)
	if varValue, ok := d.evalCtx.SessionData.UserDefinedVars[varName]; ok {
		name = n.Name
		typ = varValue.(tree.Datum).ResolvedType().SQLString()
	} else {
		return nil, pgerror.Newf(pgcode.UndefinedObject, "%s is not defined", varName)
	}
	return parse(fmt.Sprintf(
		`SELECT '%[1]s' AS var_name, '%[2]s' AS var_type `,
		name, typ,
	))
}
