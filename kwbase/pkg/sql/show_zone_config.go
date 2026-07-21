// Copyright 2017 The Cockroach Authors.
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

package sql

import (
	"bytes"
	"context"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

// These must match kwdb_internal.zones.
var showZoneConfigColumns = sqlbase.ResultColumns{
	{Name: "zone_id", Typ: types.Int, Hidden: true},
	{Name: "subzone_id", Typ: types.Int, Hidden: true},
	{Name: "target", Typ: types.String},
	{Name: "range_name", Typ: types.String, Hidden: true},
	{Name: "database_name", Typ: types.String, Hidden: true},
	{Name: "table_name", Typ: types.String, Hidden: true},
	{Name: "index_name", Typ: types.String, Hidden: true},
	{Name: "partition_name", Typ: types.String, Hidden: true},
	{Name: "raw_config_yaml", Typ: types.String, Hidden: true},
	{Name: "raw_config_sql", Typ: types.String},
	{Name: "raw_config_protobuf", Typ: types.Bytes, Hidden: true},
	{Name: "full_config_yaml", Typ: types.String, Hidden: true},
	{Name: "full_config_sql", Typ: types.String, Hidden: true},
}

// These must match showZoneConfigColumns.
const (
	zoneIDCol int = iota
	subZoneIDCol
	targetCol
	rangeNameCol
	databaseNameCol
	tableNameCol
	indexNameCol
	partitionNameCol
	rawConfigYAMLCol
	rawConfigSQLCol
	rawConfigProtobufCol
	fullConfigYamlCol
	fullConfigSQLCol
)

// ShowZoneConfig displays the zone configuration for a table or database
func (p *GenericPlanner) ShowZoneConfig(
	ctx context.Context, n *tree.ShowZoneConfig,
) (PlanNode, error) {
	return &DelayedNode{
		name:    n.String(),
		columns: showZoneConfigColumns,
		constructor: func(ctx context.Context, p *GenericPlanner) (PlanNode, error) {
			v := p.NewContainerValuesNode(showZoneConfigColumns, 0)

			// This signifies SHOW ALL.
			// However, SHOW ALL should be handled by the delegate.
			if n.ZoneSpecifier == (tree.ZoneSpecifier{}) {
				return nil, errors.AssertionFailedf("zone must be specified")
			}

			row, err := getShowZoneConfigRow(ctx, p, n.ZoneSpecifier)
			if err != nil {
				v.Close(ctx)
				return nil, err
			}
			if _, err := v.rows.AddRow(ctx, row); err != nil {
				v.Close(ctx)
				return nil, err
			}
			return v, nil
		},
	}, nil
}

func getShowZoneConfigRow(
	ctx context.Context, p *GenericPlanner, zoneSpecifier tree.ZoneSpecifier,
) (tree.Datums, error) {
	tblDesc, err := p.resolveTableForZone(ctx, &zoneSpecifier)
	if err != nil {
		return nil, err
	}

	if zoneSpecifier.TableOrIndex.Table.TableName != "" {
		//if tblDesc.IsTSTable() {
		//	return nil, sqlbase.TSUnsupportedError("show zone config for table")
		//}
		if err = p.CheckAnyPrivilege(ctx, tblDesc); err != nil {
			return nil, err
		}
	} else if zoneSpecifier.Database != "" {
		database, err := p.ResolveUncachedDatabaseByName(
			ctx,
			string(zoneSpecifier.Database),
			true, /* required */
		)
		if err != nil {
			return nil, err
		}
		//if database.EngineType == tree.EngineTypeTimeseries {
		//	return nil, sqlbase.TSUnsupportedError("show zone config for database")
		//}
		if err = p.CheckAnyPrivilege(ctx, database); err != nil {
			return nil, err
		}
	}

	targetID, err := resolveZone(ctx, p.execCfg.Settings, p.txn, &zoneSpecifier)
	if err != nil {
		return nil, err
	}

	index, partition, err := resolveSubzone(&zoneSpecifier, tblDesc)
	if err != nil {
		return nil, err
	}

	subZoneIdx := uint32(0)
	zoneID, zone, subzone, err := GetZoneConfigInTxn(ctx, p.txn,
		uint32(targetID), index, partition, false /* getInheritedDefault */)
	if err == errNoZoneConfigApplies {
		// TODO(benesch): This shouldn't be the caller's responsibility;
		// GetZoneConfigInTxn should just return the default zone config if no zone
		// config applies.
		zone = p.execCfg.DefaultZoneConfig
		zoneID = keys.RootNamespaceID
	} else if err != nil {
		return nil, err
	} else if subzone != nil {
		for i := range zone.Subzones {
			subZoneIdx++
			if subzone == &zone.Subzones[i] {
				break
			}
		}
		zone = &subzone.Config
	}

	// Determine the zone specifier for the zone config that actually applies
	// without performing another KV lookup.
	zs := ascendZoneSpecifier(zoneSpecifier, uint32(targetID), zoneID, subzone)

	// Ensure subzone configs don't infect the output of config_bytes.
	zone.Subzones = nil
	zone.SubzoneSpans = nil

	vals := make(tree.Datums, len(showZoneConfigColumns))
	if err := generateZoneConfigIntrospectionValues(
		vals, tree.NewDInt(tree.DInt(zoneID)), tree.NewDInt(tree.DInt(subZoneIdx)), &zs, zone, nil,
	); err != nil {
		return nil, err
	}
	return vals, nil
}

// zoneConfigToSQL pretty prints a zone configuration as a SQL string.
func zoneConfigToSQL(zs *tree.ZoneSpecifier, zone *zonepb.ZoneConfig) (string, error) {
	constraints, err := yamlMarshalFlow(zonepb.ConstraintsList{
		Constraints: zone.Constraints,
		Inherited:   zone.InheritedConstraints})
	if err != nil {
		return "", err
	}
	constraints = strings.TrimSpace(constraints)
	prefs, err := yamlMarshalFlow(zone.LeasePreferences)
	if err != nil {
		return "", err
	}
	prefs = strings.TrimSpace(prefs)

	useComma := false
	f := tree.NewFmtCtx(tree.FmtParsable)
	f.WriteString("ALTER ")
	f.FormatNode(zs)
	f.WriteString(" CONFIGURE ZONE USING\n")
	if zone.RangeMinBytes != nil {
		f.Printf("\trange_min_bytes = %d", *zone.RangeMinBytes)
		useComma = true
	}
	if zone.RangeMaxBytes != nil {
		writeComma(f, useComma)
		f.Printf("\trange_max_bytes = %d", *zone.RangeMaxBytes)
		useComma = true
	}
	if zone.GC != nil {
		writeComma(f, useComma)
		f.Printf("\tgc.ttlseconds = %d", zone.GC.TTLSeconds)
		useComma = true
	}
	if zone.NumReplicas != nil {
		writeComma(f, useComma)
		f.Printf("\tnum_replicas = %d", *zone.NumReplicas)
		useComma = true
	}
	if zone.TimeSeriesMergeDuration != 0 {
		writeComma(f, useComma)
		f.Printf("\tts_merge.days = %s", zone.TimeSeriesMergeDuration)
		useComma = true
	}
	if !zone.InheritedConstraints {
		writeComma(f, useComma)
		f.Printf("\tconstraints = %s", lex.EscapeSQLString(constraints))
		useComma = true
	}
	if !zone.InheritedLeasePreferences {
		writeComma(f, useComma)
		f.Printf("\tlease_preferences = %s", lex.EscapeSQLString(prefs))
	}
	return f.String(), nil
}

// generateZoneConfigIntrospectionValues creates a result row
// suitable for populating kwdb_internal.zones or SHOW ZONE CONFIG.
// The values are populated into the `values` first argument.
// The caller is responsible for creating the DInt for the ID and
// provide it as 2nd argument. The function will compute
// the remaining values based on the zone specifier and configuration.
// The fullZoneConfig argument is a zone config populated with all
// inherited zone configuration information. If this argument is nil,
// then the zone argument is used to populate the full_config_sql and
// full_config_yaml columns.
func generateZoneConfigIntrospectionValues(
	values tree.Datums,
	zoneID tree.Datum,
	subZoneID tree.Datum,
	zs *tree.ZoneSpecifier,
	zone *zonepb.ZoneConfig,
	fullZoneConfig *zonepb.ZoneConfig,
) error {
	// Populate the ID column.
	values[zoneIDCol] = zoneID
	values[subZoneIDCol] = subZoneID

	// Populate the zone specifier columns.
	values[targetCol] = tree.DNull
	values[rangeNameCol] = tree.DNull
	values[databaseNameCol] = tree.DNull
	values[tableNameCol] = tree.DNull
	values[indexNameCol] = tree.DNull
	values[partitionNameCol] = tree.DNull
	if zs != nil {
		values[targetCol] = tree.NewDString(zs.String())
		if zs.NamedZone != "" {
			values[rangeNameCol] = tree.NewDString(string(zs.NamedZone))
		}
		if zs.Database != "" {
			values[databaseNameCol] = tree.NewDString(string(zs.Database))
		}
		if zs.TableOrIndex.Table.TableName != "" {
			values[databaseNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.CatalogName))
			values[tableNameCol] = tree.NewDString(string(zs.TableOrIndex.Table.TableName))
		}
		if zs.TableOrIndex.Index != "" {
			values[indexNameCol] = tree.NewDString(string(zs.TableOrIndex.Index))
		}
		if zs.Partition != "" {
			values[partitionNameCol] = tree.NewDString(string(zs.Partition))
		}
	}

	// Populate the YAML column.
	yamlConfig, err := yaml.Marshal(zone)
	if err != nil {
		return err
	}
	values[rawConfigYAMLCol] = tree.NewDString(string(yamlConfig))

	// Populate the SQL column.
	if zs == nil {
		values[rawConfigSQLCol] = tree.DNull
	} else {
		sqlStr, err := zoneConfigToSQL(zs, zone)
		if err != nil {
			return err
		}
		values[rawConfigSQLCol] = tree.NewDString(sqlStr)
	}

	// Populate the protobuf column.
	protoConfig, err := protoutil.Marshal(zone)
	if err != nil {
		return err
	}
	values[rawConfigProtobufCol] = tree.NewDBytes(tree.DBytes(protoConfig))

	// Populate the full_config_yaml and full_config_sql columns.
	inheritedConfig := fullZoneConfig
	if inheritedConfig == nil {
		inheritedConfig = zone
	}

	yamlConfig, err = yaml.Marshal(inheritedConfig)
	if err != nil {
		return err
	}
	values[fullConfigYamlCol] = tree.NewDString(string(yamlConfig))

	if zs == nil {
		values[fullConfigSQLCol] = tree.DNull
	} else {
		sqlStr, err := zoneConfigToSQL(zs, inheritedConfig)
		if err != nil {
			return err
		}
		values[fullConfigSQLCol] = tree.NewDString(sqlStr)
	}
	return nil
}

// Writes a comma followed by a newline if useComma is true.
func writeComma(f *tree.FmtCtx, useComma bool) {
	if useComma {
		f.Printf(",\n")
	}
}

func yamlMarshalFlow(v interface{}) (string, error) {
	var buf bytes.Buffer
	e := yaml.NewEncoder(&buf)
	e.UseStyle(yaml.FlowStyle)
	if err := e.Encode(v); err != nil {
		return "", err
	}
	if err := e.Close(); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ascendZoneSpecifier logically ascends the zone hierarchy for the zone
// specified by (zs, resolvedID) until the zone matching actualID is found, and
// returns that zone's specifier. Results are undefined if actualID is not in
// the hierarchy for (zs, resolvedID).
//
// Under the hood, this function encodes knowledge about the zone lookup
// hierarchy to avoid performing any KV lookups, and so must be kept in sync
// with GetZoneConfig.
//
// TODO(benesch): Teach GetZoneConfig to return the specifier of the zone it
// finds without impacting performance.
func ascendZoneSpecifier(
	zs tree.ZoneSpecifier, resolvedID, actualID uint32, actualSubzone *zonepb.Subzone,
) tree.ZoneSpecifier {
	if actualID == keys.RootNamespaceID {
		// We had to traverse to the top of the hierarchy, so we're showing the
		// default zone config.
		zs.NamedZone = zonepb.DefaultZoneName
		zs.Database = ""
		zs.TableOrIndex = tree.TableIndexName{}
		// Since the default zone has no partition, we can erase the
		// partition name field.
		zs.Partition = ""
	} else if resolvedID != actualID {
		// We traversed at least one level up, and we're not at the top of the
		// hierarchy, so we're showing the database zone config.
		zs.Database = zs.TableOrIndex.Table.CatalogName
		zs.TableOrIndex = tree.TableIndexName{}
		// Since databases don't have partition, we can erase the
		// partition name field.
		zs.Partition = ""
	} else if actualSubzone == nil {
		// We didn't find a subzone, so no index or partition zone config exists.
		zs.TableOrIndex.Index = ""
		zs.Partition = ""
	} else if actualSubzone.PartitionName == "" {
		// The resolved subzone did not name a partition, just an index.
		zs.Partition = ""
	}
	return zs
}
