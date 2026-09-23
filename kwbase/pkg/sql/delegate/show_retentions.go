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
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// delegateShowRetentions rewrites ShowRetentions statement to select statement which returns
// table_name, retentions, sample of kwdb_internal.kwdb_retention
func (d *delegator) delegateShowRetentions(n *tree.ShowRetentions) (tree.Statement, error) {
	var query string
	var tableID uint32

	// check if the given table exists
	currentDatabase := d.catalog.GetCurrentDatabase(d.ctx)
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Table.ToTableName()
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
	if desc.IsTSTable() {
		tableID = uint32(desc.ID)
	} else {
		return nil, pgerror.Newf(pgcode.WrongObjectType, "table %s does not support downsampling", resName.Table())
	}

	const getRetention = `
  	SELECT %[1]s AS name, retentions, "sample"
    FROM %[2]s.kwdb_internal.kwdb_retention
   	WHERE table_id = %[3]d`

	query = fmt.Sprintf(
		getRetention,
		fmt.Sprintf("'%s'", resName.Table()),
		resName.CatalogName.String(),
		tableID,
	)
	return parse(query)
}
