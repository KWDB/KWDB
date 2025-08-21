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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowCreateTrigger rewrites ShowCreateTrigger statement to select statement which returns
// procedure_name, procedure_body from kwdb_internal.kwdb_procedures
func (d *delegator) delegateShowCreateTrigger(
	stmt *tree.ShowCreateTrigger,
) (tree.Statement, error) {
	var query string
	tn := stmt.TabName
	// check if the given table exists
	currentDatabase := d.catalog.GetCurrentDatabase(d.ctx)
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}

	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}

	// privilege checking
	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}
	if !resName.ExplicitCatalog {
		resName.CatalogName = tree.Name(currentDatabase)
		resName.ExplicitCatalog = true
	}

	// table type checking
	desc, err := sqlbase.GetTableDescriptorWithErr(d.evalCtx.DB, resName.Catalog(), string(dataSource.Name()))
	if err != nil {
		return nil, err
	}
	triDesc := desc.GetTriggerByName(string(stmt.Name))
	if triDesc == nil {
		return nil, sqlbase.NewUndefinedTriggerError(&stmt.Name)
	}

	query = fmt.Sprintf(`SELECT trigger_name, create_statement from %[1]s.kwdb_internal.kwdb_triggers 
	where trigger_name = '%[2]s' and table_id = %[3]d`, resName.Catalog(), stmt.Name, desc.ID)

	return parse(query)
}
