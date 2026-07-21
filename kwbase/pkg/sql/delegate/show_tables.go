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
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
)

// ErrNoDatabase is returned when no database has been specified in the current session
var ErrNoDatabase = pgerror.New(pgcode.InvalidName, "no database specified")

// delegateShowTables implements SHOW TABLES which returns all the tables.
// Privileges: None.
//
//	Notes: postgres does not have a SHOW TABLES statement.
//	       mysql only returns tables you have privileges on.
func (d *delegator) delegateShowTables(n *tree.ShowTables) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, name, err := d.catalog.ResolveSchema(d.ctx, flags, &n.TableNamePrefix)
	if err != nil {
		if d.catalog.GetCurrentDatabase(d.ctx) == "" && !n.ExplicitSchema {
			return nil, ErrNoDatabase
		}
		if !n.TableNamePrefix.ExplicitCatalog && !n.TableNamePrefix.ExplicitSchema {
			return nil, pgerror.New(
				pgcode.InvalidCatalogName,
				"current search_path does not match any valid schema")
		}
		return nil, err
	}

	var query string
	schema := lex.EscapeSQLString(name.Schema())
	if name.Schema() == sessiondata.PgTempSchemaName {
		schema = lex.EscapeSQLString(d.evalCtx.SessionData.SearchPath.GetTemporarySchemaName())
	}

	if n.WithComment {
		const getTablesQuery = `
SELECT
	inf.table_name AS table_name, inf.table_type AS table_type,
  COALESCE(pd.description, '') AS comment
 FROM %[1]s.pg_catalog.pg_class       AS pc
LEFT JOIN %[1]s.pg_catalog.pg_description AS pd ON (pc.oid = pd.objoid AND pd.objsubid = 0)
RIGHT JOIN %[1]s.information_schema.tables AS inf ON (pc.relname = inf.table_name AND pc.relnamespace = inf.namespace_oid)
WHERE inf.TABLE_SCHEMA = %[2]s`

		query = fmt.Sprintf(
			getTablesQuery,
			&name.CatalogName,
			schema,
		)

	} else {
		getTablesQuery := `
  SELECT table_name, table_type
    FROM %[1]s.information_schema.tables
   WHERE table_schema = %[2]s
ORDER BY table_name`

		if n.IsTemplate {
			getTablesQuery = `
  SELECT table_name
    FROM %[1]s.information_schema.tables
   WHERE table_schema = %[2]s AND table_type = 'TEMPLATE TABLE'
ORDER BY table_name`
		}

		query = fmt.Sprintf(
			getTablesQuery,
			&name.CatalogName,
			schema,
		)
	}

	return parse(query)
}
