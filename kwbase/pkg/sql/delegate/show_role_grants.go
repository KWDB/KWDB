// Copyright 2018 The Cockroach Authors.
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
	"bytes"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// ShowRoleGrants returns role membership details for the specified roles and grantees.
// Privileges: SELECT on system.role_members.
//
//	Notes: postgres does not have a SHOW GRANTS ON ROLES statement.
func (d *delegator) delegateShowRoleGrants(n *tree.ShowRoleGrants) (tree.Statement, error) {
	const selectQuery = `
SELECT role AS role_name,
       member,
       "isAdmin" AS is_admin
 FROM system.role_members`

	var query bytes.Buffer
	query.WriteString(selectQuery)

	if n.Roles != nil {
		var roles []string
		for _, r := range n.Roles.ToStrings() {
			roles = append(roles, lex.EscapeSQLString(r))
		}
		fmt.Fprintf(&query, ` WHERE "role" IN (%s)`, strings.Join(roles, ","))
	}

	if n.Grantees != nil {
		if n.Roles == nil {
			// No roles specified: we need a WHERE clause.
			query.WriteString(" WHERE ")
		} else {
			// We have a WHERE clause for roles.
			query.WriteString(" AND ")
		}

		var grantees []string
		for _, g := range n.Grantees.ToStrings() {
			grantees = append(grantees, lex.EscapeSQLString(g))
		}
		fmt.Fprintf(&query, ` member IN (%s)`, strings.Join(grantees, ","))

	}

	return parse(query.String())
}
