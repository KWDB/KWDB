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
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

type showType string

const (
	showCreate showType = "create"
	showColumn          = "column"
	showOther           = ""
)

// delegateShowCreate rewrites ShowCreate statement to select statement which returns
// table_name, create_statement from kwdb_internal.create_statements
func (d *delegator) delegateShowCreate(n *tree.ShowCreate) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)
	const showCreateQuery = `
    SELECT
			%[3]s AS table_name,
			array_to_string(
				array_cat(
					ARRAY[create_statement],
					zone_configuration_statements
				),
				e';\n'
			)
				AS create_statement
		FROM
			%[4]s.kwdb_internal.create_statements
		WHERE
			(database_name IS NULL OR database_name = %[1]s)
			AND schema_name = %[5]s
			AND descriptor_name = %[2]s
	`

	return d.showTableDetails(n.Name, showCreateQuery, showCreate)
}

// delegateShowIndexes rewrites ShowIndexes statement to select statement which returns
// table_name, index_name... from information_schema.statistics and pg_catalog.pg_indexes
func (d *delegator) delegateShowIndexes(n *tree.ShowIndexes) (tree.Statement, error) {
	//flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	//tn := n.Table.ToTableName()
	//dataSource, _, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	//if err != nil {
	//	return nil, err
	//}
	//if ds, ok := dataSource.(cat.Table); ok {
	//	if ds.GetTableType() != tree.RelationalTable {
	//		return nil, sqlbase.TSUnsupportedError("show indexes")
	//	}
	//}

	sqltelemetry.IncrementShowCounter(sqltelemetry.Indexes)
	getIndexesQuery := `
SELECT
	s.table_name,
	s.index_name,
	non_unique::BOOL,
	seq_in_index,
	column_name,
	direction,
	storing::BOOL,
	implicit::BOOL`

	if n.WithComment {
		getIndexesQuery += `,
	obj_description(%[4]s.pg_catalog.pg_indexes.kwdb_oid) AS comment`
	}

	getIndexesQuery += `
FROM
	%[4]s.information_schema.statistics AS s`

	if n.WithComment {
		getIndexesQuery += `
	LEFT JOIN %[4]s.pg_catalog.pg_indexes ON
		pg_indexes.tablename = s.table_name AND
		pg_indexes.indexname = s.index_name
	`
	}

	getIndexesQuery += `
WHERE
	table_catalog=%[1]s
	AND table_schema=%[5]s
	AND table_name=%[2]s`

	return d.showTableDetails(n.Table, getIndexesQuery, showOther)
}

// delegateShowColumns rewrites ShowColumns statement to select statement which returns
// column_name, data_type... from information_schema.columns and information_schema.statistics
func (d *delegator) delegateShowColumns(n *tree.ShowColumns) (tree.Statement, error) {
	getColumnsQuery := `
SELECT
  column_name AS column_name,
  kwdb_sql_type AS data_type,
  is_nullable::BOOL,
  column_default,
  generation_expression,
  IF(inames[1] IS NULL, ARRAY[]:::STRING[], inames) AS indices,
  is_hidden::BOOL,
	is_tag::BOOL`

	if n.WithComment {
		getColumnsQuery += `,
  col_description(%[6]d, attnum) AS comment`
	}

	getColumnsQuery += `
FROM
  (SELECT column_name, kwdb_sql_type, is_nullable, column_default, generation_expression,
	        ordinal_position, is_hidden, is_tag, array_agg(index_name) AS inames
     FROM
         (SELECT column_name, kwdb_sql_type, is_nullable, column_default, generation_expression,
				         ordinal_position, is_hidden, is_tag
            FROM %[4]s.information_schema.columns
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         LEFT OUTER JOIN
         (SELECT column_name, index_name
            FROM %[4]s.information_schema.statistics
           WHERE (length(%[1]s)=0 OR table_catalog=%[1]s) AND table_schema=%[5]s AND table_name=%[2]s)
         USING(column_name)
    GROUP BY column_name, kwdb_sql_type, is_nullable, column_default, generation_expression,
		         ordinal_position, is_hidden, is_tag
   )`

	if n.WithComment {
		getColumnsQuery += `
         LEFT OUTER JOIN %[4]s.pg_catalog.pg_attribute 
           ON column_name = %[4]s.pg_catalog.pg_attribute.attname
           AND attrelid = %[6]d`
	}

	getColumnsQuery += `
ORDER BY is_tag, ordinal_position`

	return d.showTableDetails(n.Table, getColumnsQuery, showColumn)
}

// delegateShowConstraints rewrites ShowConstraints statement to select statement which returns
// table_name, constraint_name... from pg_catalog.pg_class, pg_catalog.pg_namespace and pg_catalog.pg_constraint
func (d *delegator) delegateShowConstraints(n *tree.ShowConstraints) (tree.Statement, error) {
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Table.ToTableName()
	dataSource, _, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}
	if ds, ok := dataSource.(cat.Table); ok {
		if ds.GetTableType() != tree.RelationalTable {
			return nil, sqlbase.TSUnsupportedError("show constraints")
		}
	}
	sqltelemetry.IncrementShowCounter(sqltelemetry.Constraints)
	const getConstraintsQuery = `
    SELECT
        t.relname AS table_name,
        c.conname AS constraint_name,
        CASE c.contype
           WHEN 'p' THEN 'PRIMARY KEY'
           WHEN 'u' THEN 'UNIQUE'
           WHEN 'c' THEN 'CHECK'
           WHEN 'f' THEN 'FOREIGN KEY'
           ELSE c.contype
        END AS constraint_type,
        c.condef AS details,
        c.convalidated AS validated
    FROM
       %[4]s.pg_catalog.pg_class t,
       %[4]s.pg_catalog.pg_namespace n,
       %[4]s.pg_catalog.pg_constraint c
    WHERE t.relname = %[2]s
      AND n.nspname = %[5]s AND t.relnamespace = n.oid
      AND t.oid = c.conrelid
    ORDER BY 1, 2`

	return d.showTableDetails(n.Table, getConstraintsQuery, showOther)
}

// showTableDetails returns the AST of a query which extracts information about
// the given table using the given query patterns in SQL. The query pattern must
// accept the following formatting parameters:
//
//	%[1]s the database name as SQL string literal.
//	%[2]s the unqualified table name as SQL string literal.
//	%[3]s the given table name as SQL string literal.
//	%[4]s the database name as SQL identifier.
//	%[5]s the schema name as SQL string literal.
func (d *delegator) showTableDetails(
	name *tree.UnresolvedObjectName, query string, typ showType,
) (tree.Statement, error) {
	// We avoid the cache so that we can observe the details without
	// taking a lease, like other SHOW commands.
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := name.ToTableName()
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}
	tblName := resName.Table()
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	if typ == showColumn || typ == showCreate {
		// resolve instance table through db name and table name.
		instNameSpace, found, err := sqlbase.ResolveInstanceName(d.ctx, d.evalCtx.Txn, resName.Catalog(), resName.Table())
		if err != nil {
			return nil, err
		}
		if found {
			tblName = instNameSpace.STableName
			if typ == showCreate {
				desc, err := sqlbase.GetTableDescriptorWithErr(d.evalCtx.DB, instNameSpace.DBName, instNameSpace.STableName)
				if err != nil {
					return nil, err
				}
				return d.delegateInstanceShowCreate(instNameSpace, desc)
			}
		}
	}

	fullQuery := fmt.Sprintf(query,
		lex.EscapeSQLString(resName.Catalog()),
		lex.EscapeSQLString(tblName),
		lex.EscapeSQLString(resName.String()),
		resName.CatalogName.String(), // note: CatalogName.String() != Catalog()
		lex.EscapeSQLString(resName.Schema()),
		dataSource.ID(),
	)

	return parse(fullQuery)
}

// delegateInstanceShowCreate returns the AST of a query which extracts information about
// the given instance table using the given query patterns in SQL. The query pattern must
// accept the following formatting parameters:
// cTbNameSpace: instance table name
// desc: template table descriptor
func (d *delegator) delegateInstanceShowCreate(
	cTbNameSpace sqlbase.InstNameSpace, desc *sqlbase.TableDescriptor,
) (tree.Statement, error) {
	var name []string
	var values []string
	var typ []types.T
	for _, attribute := range desc.Columns {
		if attribute.IsTagCol() && !attribute.IsPrimaryTagCol() {
			name = append(name, attribute.Name)
			typ = append(typ, attribute.Type)
		}
	}
	tagQuery := sqlbase.BuildTagHintQuery(name, cTbNameSpace.DBName, cTbNameSpace.InstName)
	row, err := d.evalCtx.InternalExecutor.QueryRow(d.ctx, "queryTag", d.evalCtx.Txn, tagQuery)
	if err != nil {
		return nil, err
	}
	for i := range row {
		value := ConvertTagValToString(row[i])
		values = append(values, value)
	}
	stmt := ShowCreateInstanceTable(tree.Name(cTbNameSpace.STableName), cTbNameSpace.InstName, name, values, desc.TsTable.Sde, typ)
	query := `SELECT '%[1]s' AS table_name, e'%[2]s ' AS create_statement`
	query = fmt.Sprintf(query, cTbNameSpace.InstName, stmt)
	return parse(query)
}

// ConvertTagValToString Converts tag value to string.
func ConvertTagValToString(d tree.Datum) string {
	var res string
	switch val := d.(type) {
	// Add single quotes to values of string type
	// eg: d = "blue" -> res = "'blue'"
	case *tree.DString:
		res = fmt.Sprintf("\\'%s\\'", *val)
	case *tree.DBytes:
		if sqlbase.NeedConvert(string(*val)) {
			res = strings.Trim(val.String(), "'")
		} else {
			res = strings.Trim(tree.NewDBytes(*val).String(), "'")
		}
		res = fmt.Sprintf("b\\'\\%s\\'\\", res)
	case *tree.DTimestamp:
		res = val.String()
		if len(res) > 0 && res[0] == '\'' {
			res = res[1:]
		}
		if len(res) > 0 && res[len(res)-1] == '\'' {
			res = res[:len(res)-1]
		}
	default:
		res = val.String()
	}
	return res
}

// ShowCreateInstanceTable returns a valid SQL representation of the CREATE
// INSTANCE TABLE statement used to create the given table.
func ShowCreateInstanceTable(
	sTable tree.Name,
	cTable string,
	attributeName []string,
	attributeValue []string,
	sde bool,
	typ []types.T,
) string {
	f := tree.NewFmtCtx(tree.FmtSimple)
	f.WriteString("CREATE ")
	f.WriteString("TABLE ")
	f.WriteString(cTable)
	f.WriteString(" USING ")
	f.FormatNode(&sTable)
	f.WriteString(" (")
	for i := range attributeName {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")
		f.WriteString(attributeName[i])
	}
	f.WriteString(" )")
	f.WriteString(" TAGS")
	f.WriteString(" (")
	for i := range attributeValue {
		if i > 0 {
			f.WriteString(", ")
		}
		f.WriteString("\n\t")
		f.WriteString(attributeValue[i])
	}
	f.WriteString(" )")
	if sde {
		f.WriteString(" DICT ENCODING")
	}
	return f.CloseAndGetString()
}

// delegateShowCreate rewrites ShowCreate statement to select statement which returns
// database_name, create_statement from pg_catalog.pg_database
func (d *delegator) delegateShowCreateDatabase(n *tree.ShowCreateDatabase) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Create)
	const showCreateQuery = `select %[1]s as database_name, datstatement as create_statement from pg_catalog.pg_database where datname = %[2]s`

	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	database, err := d.catalog.ResolveDatabase(d.ctx, flags, string(n.Database))
	if err != nil {
		return nil, err
	}
	if err := d.catalog.CheckAnyPrivilege(d.ctx, database); err != nil {
		return nil, err
	}
	query := fmt.Sprintf(showCreateQuery, lex.EscapeSQLString(n.Database.String()), lex.EscapeSQLString(string(n.Database)))

	return parse(query)
}
