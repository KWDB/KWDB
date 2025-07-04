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
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/errors"
)

// delegateShowCreate rewrites ShowPartitions statement to select statement which returns
// table_name, partition_name from kwdb_internal.partitions, kwdb_internal.tables...
func (d *delegator) delegateShowPartitions(n *tree.ShowPartitions) (tree.Statement, error) {
	sqltelemetry.IncrementShowCounter(sqltelemetry.Partitions)
	if n.IsTable {
		flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
		tn := n.Table.ToTableName()

		dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
		if err != nil {
			return nil, err
		}
		if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
			return nil, err
		}

		// We use the raw_config_sql from the partition_lookup result to get the
		// official zone config for the partition, and use the full_config_sql from the zones table
		// which is the result of looking up the partition's inherited zone configuraion.
		const showTablePartitionsQuery = `
		SELECT
			tables.database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value,partitions.hash_value) as partition_value,
			replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
			replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
		FROM
			%[3]s.kwdb_internal.partitions
			JOIN %[3]s.kwdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN %[3]s.kwdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN %[3]s.kwdb_internal.zones ON
					partitions.zone_id = zones.zone_id
					AND partitions.subzone_id = zones.subzone_id
			LEFT JOIN %[3]s.kwdb_internal.zones AS partition_lookup ON
				partition_lookup.database_name = tables.database_name
				AND partition_lookup.table_name = tables.name
				AND partition_lookup.index_name = table_indexes.index_name
				AND partition_lookup.partition_name = partitions.name
		WHERE
			tables.name = %[1]s AND tables.database_name = %[2]s;
		`
		return parse(fmt.Sprintf(showTablePartitionsQuery,
			lex.EscapeSQLString(resName.Table()),
			lex.EscapeSQLString(resName.Catalog()),
			resName.CatalogName.String()))
	} else if n.IsDB {
		if err1 := CheckDBSupportShow(d, n.Database, n); err1 != nil {
			return nil, err1
		}
		const showDatabasePartitionsQuery = `
		SELECT
			tables.database_name,
			tables.name AS table_name,
			partitions.name AS partition_name,
			partitions.parent_name AS parent_partition,
			partitions.column_names,
			concat(tables.name, '@', table_indexes.index_name) AS index_name,
			coalesce(partitions.list_value, partitions.range_value,partitions.hash_value) as partition_value,
			replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
			replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
		FROM
			%[1]s.kwdb_internal.partitions
			JOIN %[1]s.kwdb_internal.tables ON partitions.table_id = tables.table_id
			JOIN %[1]s.kwdb_internal.table_indexes ON
					table_indexes.descriptor_id = tables.table_id
					AND table_indexes.index_id = partitions.index_id
			LEFT JOIN %[1]s.kwdb_internal.zones ON
					partitions.zone_id = zones.zone_id
					AND partitions.subzone_id = zones.subzone_id
			LEFT JOIN %[1]s.kwdb_internal.zones AS partition_lookup ON
				partition_lookup.database_name = tables.database_name
				AND partition_lookup.table_name = tables.name
				AND partition_lookup.index_name = table_indexes.index_name
				AND partition_lookup.partition_name = partitions.name
		WHERE
			tables.database_name = %[2]s
		ORDER BY
			tables.name, partitions.name;
		`
		// Note: n.Database.String() != string(n.Database)
		return parse(fmt.Sprintf(showDatabasePartitionsQuery, n.Database.String(), lex.EscapeSQLString(string(n.Database))))
	}

	flags := cat.Flags{AvoidDescriptorCaches: true, NoTableStats: true}
	tn := n.Index.Table

	// Throw a more descriptive error if the user did not use the index hint syntax.
	if tn.TableName == "" {
		err := errors.New("no table specified")
		err = pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
		err = errors.WithHint(err, "Specify a table using the hint syntax of table@index")
		return nil, err
	}

	dataSource, resName, err := d.catalog.ResolveDataSource(d.ctx, flags, &tn)
	if err != nil {
		return nil, err
	}

	if err := d.catalog.CheckAnyPrivilege(d.ctx, dataSource); err != nil {
		return nil, err
	}

	// Force resolution of the index.
	_, _, err = cat.ResolveTableIndex(d.ctx, d.catalog, flags, &n.Index)
	if err != nil {
		return nil, err
	}

	const showIndexPartitionsQuery = `
	SELECT
		tables.database_name,
		tables.name AS table_name,
		partitions.name AS partition_name,
		partitions.parent_name AS parent_partition,
		partitions.column_names,
		concat(tables.name, '@', table_indexes.index_name) AS index_name,
		coalesce(partitions.list_value, partitions.range_value,partitions.hash_value) as partition_value,
		replace(regexp_extract(partition_lookup.raw_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as zone_config,
		replace(regexp_extract(zones.full_config_sql, 'CONFIGURE ZONE USING\n((?s:.)*)'), e'\t', '') as full_zone_config
	FROM
		%[5]s.kwdb_internal.partitions
		JOIN %[5]s.kwdb_internal.table_indexes ON
				partitions.index_id = table_indexes.index_id
				AND partitions.table_id = table_indexes.descriptor_id
		JOIN %[5]s.kwdb_internal.tables ON table_indexes.descriptor_id = tables.table_id
		LEFT JOIN %[5]s.kwdb_internal.zones ON
			partitions.zone_id = zones.zone_id
			AND partitions.subzone_id = zones.subzone_id
		LEFT JOIN %[5]s.kwdb_internal.zones AS partition_lookup ON
			partition_lookup.database_name = tables.database_name
			AND partition_lookup.table_name = tables.name
			AND partition_lookup.index_name = table_indexes.index_name
			AND partition_lookup.partition_name = partitions.name
	WHERE
		table_indexes.index_name = %[1]s AND tables.name = %[2]s;
	`
	return parse(fmt.Sprintf(showIndexPartitionsQuery,
		lex.EscapeSQLString(n.Index.Index.String()),
		lex.EscapeSQLString(resName.Table()),
		resName.Table(),
		n.Index.Index.String(),
		// note: CatalogName.String() != Catalog()
		resName.CatalogName.String()))
}
