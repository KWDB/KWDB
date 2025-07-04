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
	"bytes"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowGrants implements SHOW GRANTS which returns grant details for the
// specified objects and users.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW GRANTS statement.
//	       mysql only returns the user's privileges.
func (d *delegator) delegateShowGrants(n *tree.ShowGrants) (tree.Statement, error) {
	var params []string

	const dbPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type
  FROM "".information_schema.schema_privileges`
	const schemaPrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       grantee,
       privilege_type
  FROM "".information_schema.schema_privileges`
	const tablePrivQuery = `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name,
       grantee,
       privilege_type
FROM "".information_schema.table_privileges`
	const procPrivQuery = `
SELECT procedure_catalog AS database_name,
       procedure_schema AS schema_name,
       procedure_name,
       grantee,
       privilege_type
FROM "".information_schema.procedure_privileges`

	var source bytes.Buffer
	var cond bytes.Buffer
	var orderBy string

	if n.Targets != nil && n.Targets.Databases != nil {
		// Get grants of database from information_schema.schema_privileges
		// if the type of target is database.
		dbNames := n.Targets.Databases.ToStrings()

		for _, db := range dbNames {
			name := cat.SchemaName{
				CatalogName:     tree.Name(db),
				SchemaName:      tree.Name(tree.PublicSchema),
				ExplicitCatalog: true,
				ExplicitSchema:  true,
			}
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
			if err != nil {
				return nil, err
			}
			params = append(params, lex.EscapeSQLString(db))
		}

		fmt.Fprint(&source, dbPrivQuery)
		orderBy = "1,2,3,4"
		if len(params) == 0 {
			// There are no rows, but we can't simply return emptyNode{} because
			// the result columns must still be defined.
			cond.WriteString(`WHERE false`)
		} else {
			fmt.Fprintf(&cond, `WHERE database_name IN (%s)`, strings.Join(params, ","))
		}
	} else if n.Targets != nil && len(n.Targets.Schemas) > 0 {
		schemaNames := n.Targets.Schemas.ToStrings()
		for _, schema := range schemaNames {
			name := cat.SchemaName{
				SchemaName:     tree.Name(schema),
				ExplicitSchema: true,
			}
			_, _, err := d.catalog.ResolveSchema(d.ctx, cat.Flags{AvoidDescriptorCaches: true}, &name)
			if err != nil {
				return nil, err
			}
			params = append(params, lex.EscapeSQLString(schema))
		}
		dbNameClause := "true"
		// If the current database is set, restrict the command to it.
		if currDB := d.evalCtx.SessionData.Database; currDB != "" {
			dbNameClause = fmt.Sprintf("database_name = %s", lex.EscapeSQLString(currDB))
		}
		fmt.Fprint(&source, schemaPrivQuery)
		orderBy = "1,2,3,4"
		if len(params) == 0 {
			cond.WriteString(fmt.Sprintf(`WHERE %s`, dbNameClause))
		} else {
			fmt.Fprintf(
				&cond,
				`WHERE %s AND schema_name IN (%s)`,
				dbNameClause,
				strings.Join(params, ","),
			)
		}
	} else if n.Targets != nil && len(n.Targets.Procedures) > 0 {
		fmt.Fprint(&source, procPrivQuery)
		orderBy = "1,2,3,4,5"
		for _, proc := range n.Targets.Procedures {
			found, _, err := d.catalog.ResolveProcCatalog(d.ctx, &proc, false)
			if err != nil {
				return nil, err
			}
			if !found {
				return nil, sqlbase.NewUndefinedProcedureError(&proc)
			}
			catalog := proc.Catalog()
			schema := proc.Schema()
			if catalog == "" {
				catalog = d.catalog.GetCurrentDatabase(d.ctx)
			}
			if schema == "" {
				schema = tree.PublicSchema
			}
			params = append(params, fmt.Sprintf("(%s,%s,%s)",
				lex.EscapeSQLString(catalog),
				lex.EscapeSQLString(schema),
				lex.EscapeSQLString(proc.Table())))
		}
		fmt.Fprintf(&cond, `WHERE (database_name, schema_name, procedure_name) IN (%s)`, strings.Join(params, ","))
	} else {

		if n.Targets != nil {
			fmt.Fprint(&source, tablePrivQuery)
			orderBy = "1,2,3,4,5"
			// Get grants of table from information_schema.table_privileges
			// if the type of target is table.
			var allTables tree.TableNames

			for _, tableTarget := range n.Targets.Tables {
				tableGlob, err := tableTarget.NormalizeTablePattern()
				if err != nil {
					return nil, err
				}
				// We avoid the cache so that we can observe the grants taking
				// a lease, like other SHOW commands.
				tables, err := cat.ExpandDataSourceGlob(
					d.ctx, d.catalog, cat.Flags{AvoidDescriptorCaches: true}, tableGlob,
				)
				if err != nil {
					return nil, err
				}
				allTables = append(allTables, tables...)
			}

			for i := range allTables {
				params = append(params, fmt.Sprintf("(%s,%s,%s)",
					lex.EscapeSQLString(allTables[i].Catalog()),
					lex.EscapeSQLString(allTables[i].Schema()),
					lex.EscapeSQLString(allTables[i].Table())))
			}

			if len(params) == 0 {
				// The glob pattern has expanded to zero matching tables.
				// There are no rows, but we can't simply return emptyNode{} because
				// the result columns must still be defined.
				cond.WriteString(`WHERE false`)
			} else {
				fmt.Fprintf(&cond, `WHERE (database_name, schema_name, table_name) IN (%s)`, strings.Join(params, ","))
			}
		} else {
			fmt.Fprint(&source, `
SELECT table_catalog AS database_name,
       table_schema AS schema_name,
       table_name,
       NULL::STRING AS procedure_name,
       grantee,
       privilege_type
FROM "".information_schema.table_privileges`)
			orderBy = "1,2,3,4,5,6"
			// No target: only look at tables and schemas in the current database.
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, schema_name, NULL::STRING AS table_name, NULL::STRING AS procedure_name, grantee, privilege_type FROM (`)
			source.WriteString(dbPrivQuery)
			source.WriteByte(')')
			source.WriteString(` UNION ALL ` +
				`SELECT database_name, schema_name, NULL::STRING AS table_name, procedure_name, grantee, privilege_type FROM (`)
			source.WriteString(procPrivQuery)
			source.WriteByte(')')
			// If the current database is set, restrict the command to it.
			if currDB := d.evalCtx.SessionData.Database; currDB != "" {
				fmt.Fprintf(&cond, ` WHERE database_name = %s`, lex.EscapeSQLString(currDB))
			} else {
				cond.WriteString(`WHERE true`)
			}
		}
	}

	if n.Grantees != nil {
		params = params[:0]
		for _, grantee := range n.Grantees.ToStrings() {
			params = append(params, lex.EscapeSQLString(grantee))
		}
		fmt.Fprintf(&cond, ` AND grantee IN (%s)`, strings.Join(params, ","))
	}
	query := fmt.Sprintf("SELECT * FROM (%s) %s ORDER BY %s", source.String(), cond.String(), orderBy)
	return parse(query)
}
