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

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowAttributes rewrites [SHOW TAGS FROM timeseries_table] statement
// to select_stmt which returns the tags.
func (d *delegator) delegateShowAttributes(n *tree.ShowTags) (tree.Statement, error) {
	var query string
	var getAttributesQuery string

	var columnName string
	var privateFilter string
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Table.ToTableName()
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}
	// check privilege
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	if !resName.ExplicitCatalog {
		resName.CatalogName = tree.Name(d.catalog.GetCurrentDatabase(d.ctx))
		resName.ExplicitCatalog = true
	}
	_, found, err := sqlbase.ResolveInstanceName(d.ctx, d.evalCtx.Txn, resName.Catalog(), resName.Table())
	if err != nil {
		return nil, err
	}
	if found {
		columnName = "table_name"
		privateFilter = " AND NOT is_primary"
	} else {
		desc, err := sqlbase.GetTableDescriptorWithErr(d.evalCtx.DB, resName.Catalog(), resName.Table())
		if err != nil {
			return nil, err
		}
		switch desc.TableType {
		case tree.TemplateTable:
			columnName = "stable_name"
			privateFilter = " AND table_name IS NULL AND NOT is_primary"
		case tree.TimeseriesTable, tree.SparseTable:
			columnName = "table_name"
		case tree.RelationalTable:
			return nil, pgerror.Newf(pgcode.WrongObjectType, "relational table %s does not have tag", resName.Table())
		}
	}

	// generate select sql
	getAttributesQuery = `
				SELECT tag_name AS tag, tag_type AS type , is_primary, nullable
				FROM %[1]s.kwdb_internal.kwdb_attributes
				WHERE db_name = '%[2]s' AND %[3]s = '%[4]s'` + privateFilter

	query = fmt.Sprintf(
		getAttributesQuery,
		&resName.CatalogName,
		resName.Catalog(),
		columnName,
		resName.TableName,
	)

	s, err := parser.ParseOne(query)
	return s.AST, err
}

// delegateShowTagValues rewrites [SHOW TAG VALUES FROM timeseries_table] statement
// to select_stmt which returns the tags and values.
func (d *delegator) delegateShowTagValues(n *tree.ShowTagValues) (tree.Statement, error) {
	// resolve table name and get table metadata
	flags := cat.Flags{AvoidDescriptorCaches: false, NoTableStats: true}
	tn := n.Table.ToTableName()
	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}
	tblName := resName.TableName.String()

	// check privilege
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	// check table type
	tab, ok := dataSource.(cat.Table)
	if !ok || tab.GetTableType() == tree.RelationalTable {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "\"%s\" is not a timeseries table", resName.Table())
	}

	var notShowPrimary bool
	if tab.GetTableType() == tree.TemplateTable || tab.GetTableType() == tree.InstanceTable {
		notShowPrimary = true
	}
	var name []string

	var privateFilter string
	sortColList := ""
	// check if child table ,if it is and will get child table name
	if tab.GetTableType() == tree.InstanceTable {
		tblName = tn.TableName.String()
		privateFilter = fmt.Sprintf(" WHERE \"pTag\" = '%v' ", tblName)
	}

	// get tag name
	for i := 0; i < tab.ColumnCount(); i++ {
		tag := tab.Column(i)
		if notShowPrimary && tag.IsPrimaryTagCol() {
			tagName := tag.ColName()
			tagNameStr := fmt.Sprintf("%v AS instance_table", tagName.String())
			name = append(name, tagNameStr)
			if len(sortColList) != 0 {
				sortColList += ","
			}
			sortColList += "instance_table"
		}
		if (notShowPrimary && tag.IsTagCol() && !tag.IsPrimaryTagCol()) || (!notShowPrimary && tag.IsTagCol()) {
			tagName := tag.ColName()
			name = append(name, tagName.String())
			if len(sortColList) != 0 {
				sortColList += ","
			}
			sortColList += tagName.String()
		}
	}

	if len(sortColList) != 0 {
		sortColList = " ORDER BY " + sortColList
	}

	query := sqlbase.BuildTagHintQuery(name, resName.CatalogName.String(), tblName)
	query = "select distinct * from (" + query + privateFilter + ") " + sortColList
	s, err := parser.ParseOne(query)
	return s.AST, err
}
