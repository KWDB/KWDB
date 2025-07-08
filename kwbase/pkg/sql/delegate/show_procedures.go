// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowProcedures rewrites ShowProcedures statement to select statement which returns
// procedure_name from kwdb_internal.kwdb_procedures
func (d *delegator) delegateShowProcedures(n *tree.ShowProcedures) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true}
	_, name, err := d.catalog.ResolveSchema(d.ctx, flags, &n.TableNamePrefix)
	if err != nil {
		if d.catalog.GetCurrentDatabase(d.ctx) == "" && !n.ExplicitSchema {
			return nil, errNoDatabase
		}
		if !n.TableNamePrefix.ExplicitCatalog && !n.TableNamePrefix.ExplicitSchema {
			return nil, pgerror.New(
				pgcode.InvalidCatalogName,
				"current search_path does not match any valid schema")
		}
		return nil, err
	}

	schema := lex.EscapeSQLString(name.Schema())
	if name.Schema() == sessiondata.PgTempSchemaName {
		schema = lex.EscapeSQLString(d.evalCtx.SessionData.SearchPath.GetTemporarySchemaName())
	}

	var query string
	query = `SELECT procedure_name `
	if n.WithComment {
		query += `, description as COMMENT `
	}
	query += `from %[1]s.kwdb_internal.kwdb_procedures `
	if n.WithComment {
		query += ` left join pg_catalog.pg_description on oid(kwdb_procedures.procedure_id) = pg_description.objoid`
	}
	query += ` WHERE schema_name = %[2]s ORDER BY procedure_id`
	query = fmt.Sprintf(
		query,
		&name.CatalogName,
		schema,
	)

	return parse(query)
}

// delegateShowCreateProcedure rewrites ShowCreateProcedure statement to select statement which returns
// procedure_name, procedure_body from kwdb_internal.kwdb_procedures
func (d *delegator) delegateShowCreateProcedure(
	stmt *tree.ShowCreateProcedure,
) (tree.Statement, error) {
	var query string
	tn := stmt.Name
	found, objMeta, err := d.catalog.ResolveProcCatalog(d.ctx, &tn, false)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, sqlbase.NewUndefinedProcedureError(&stmt.Name)
	}
	query = fmt.Sprintf(`SELECT procedure_name, procedure_body from %[1]s.kwdb_internal.kwdb_procedures 
	where procedure_id = %[2]d`, objMeta.Name.CatalogName, objMeta.ProcID)

	return parse(query)
}
