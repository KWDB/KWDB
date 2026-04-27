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
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// CheckDBSupportShow checks whether the database supports show.
func CheckDBSupportShow(d *delegator, dbName tree.Name, n tree.Statement) error {
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	var name cat.SchemaName
	name.SchemaName = dbName
	name.ExplicitSchema = true
	schema, _, err := d.catalog.ResolveSchema(d.ctx, flags, &name)
	if err != nil {
		return err
	}
	if schema == nil {
		return pgerror.Newf(pgcode.Internal, "schema is nil")
	}
	if schema.GetDatabaseType() == tree.EngineTypeTimeseries {
		return TSUnsupportedShowError(n)
	}
	return nil
}

// CheckTsDBSupportShow checks whether the database supports show.
func CheckTsDBSupportShow(d *delegator, dbName tree.Name, n tree.Statement) error {
	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	var name cat.SchemaName
	name.SchemaName = dbName
	name.ExplicitSchema = true
	schema, _, err := d.catalog.ResolveSchema(d.ctx, flags, &name)
	if err != nil {
		return err
	}
	if schema.GetDatabaseType() == tree.EngineTypeRelational {
		return pgerror.Newf(pgcode.FeatureNotSupported, "%s is not supported in relation database", "show ts partitions")
	}
	return nil
}

// TSUnsupportedShowError returns error that ts object does not support show.
func TSUnsupportedShowError(n tree.Statement) error {
	var op string
	switch n.(type) {
	case *tree.ShowDatabaseIndexes:
		op = "show database indexes"

	case *tree.ShowIndexes:
		op = "show indexes"

	case *tree.ShowPartitions:
		op = "show partitions"

	case *tree.ShowRanges:
		op = "show ranges"

	case *tree.ShowZoneConfig:
		op = "show zone config"

	default:
	}

	return sqlbase.TSUnsupportedError(op)
}
