// Copyright 2015 The Cockroach Authors.
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

package sqlbase

import (
	"context"
	"fmt"
	"sort"

	"gitee.com/kwbasedb/kwbase/pkg/clusterversion"
	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
)

var _ DescriptorProto = &DatabaseDescriptor{}
var _ DescriptorProto = &TableDescriptor{}
var _ DescriptorProto = &SchemaDescriptor{}

// DescriptorKey is the interface implemented by both
// databaseKey and tableKey. It is used to easily get the
// descriptor key and plain name.
type DescriptorKey interface {
	Key() roachpb.Key
	Name() string
}

// DescriptorProto is the interface implemented by both DatabaseDescriptor
// and TableDescriptor.
// TODO(marc): this is getting rather large.
type DescriptorProto interface {
	protoutil.Message
	GetPrivileges() *PrivilegeDescriptor
	GetID() ID
	SetID(ID)
	TypeName() string
	GetName() string
	SetName(string)
	GetAuditMode() TableDescriptor_AuditMode
}

// WrapDescriptor fills in a Descriptor.
func WrapDescriptor(descriptor DescriptorProto) *Descriptor {
	desc := &Descriptor{}
	switch t := descriptor.(type) {
	case *MutableTableDescriptor:
		desc.Union = &Descriptor_Table{Table: &t.TableDescriptor}
	case *TableDescriptor:
		desc.Union = &Descriptor_Table{Table: t}
	case *DatabaseDescriptor:
		desc.Union = &Descriptor_Database{Database: t}
	case *SchemaDescriptor:
		desc.Union = &Descriptor_Schema{Schema: t}
	default:
		panic(fmt.Sprintf("unknown descriptor type: %s", descriptor.TypeName()))
	}
	return desc
}

// MetadataSchema is used to construct the initial sql schema for a new
// CockroachDB cluster being bootstrapped. Tables and databases must be
// installed on the underlying persistent storage before a kwbase store can
// start running correctly, thus requiring this special initialization.
type MetadataSchema struct {
	descs         []metadataDescriptor
	otherSplitIDs []uint32
	otherKV       []roachpb.KeyValue
}

type metadataDescriptor struct {
	parentID ID
	desc     DescriptorProto
}

// MakeMetadataSchema constructs a new MetadataSchema value which constructs
// the "system" database.
func MakeMetadataSchema(
	defaultZoneConfig *zonepb.ZoneConfig, defaultSystemZoneConfig *zonepb.ZoneConfig,
) MetadataSchema {
	ms := MetadataSchema{}
	addSystemDatabaseToSchema(&ms, defaultZoneConfig, defaultSystemZoneConfig)
	return ms
}

// AddDescriptor adds a new non-config descriptor to the system schema.
func (ms *MetadataSchema) AddDescriptor(parentID ID, desc DescriptorProto) {
	if id := desc.GetID(); id > keys.MaxReservedDescID {
		panic(fmt.Sprintf("invalid reserved table ID: %d > %d", id, keys.MaxReservedDescID))
	}
	for _, d := range ms.descs {
		if d.desc.GetID() == desc.GetID() {
			log.Errorf(context.TODO(), "adding descriptor failed for %d with duplicate ID: %v", desc.GetID(), desc)
			return
		}
	}
	ms.descs = append(ms.descs, metadataDescriptor{parentID, desc})
}

// AddSplitIDs adds some "table ids" to the MetadataSchema such that
// corresponding keys are returned as split points by GetInitialValues().
// AddDescriptor() has the same effect for the table descriptors that are passed
// to it, but we also have a couple of "fake tables" that don't have descriptors
// but need splits just the same.
func (ms *MetadataSchema) AddSplitIDs(id ...uint32) {
	ms.otherSplitIDs = append(ms.otherSplitIDs, id...)
}

// SystemDescriptorCount returns the number of descriptors that will be created by
// this schema. This value is needed to automate certain tests.
func (ms MetadataSchema) SystemDescriptorCount() int {
	return len(ms.descs)
}

// GetInitialValues returns the set of initial K/V values which should be added to
// a bootstrapping cluster in order to create the tables contained
// in the schema. Also returns a list of split points (a split for each SQL
// table descriptor part of the initial values). Both returned sets are sorted.
func (ms MetadataSchema) GetInitialValues(
	bootstrapVersion clusterversion.ClusterVersion,
) ([]roachpb.KeyValue, []roachpb.RKey) {
	var ret []roachpb.KeyValue
	var splits []roachpb.RKey

	// Save the ID generator value, which will generate descriptor IDs for user
	// objects.
	value := roachpb.Value{}
	value.SetInt(int64(keys.MinUserDescID))
	ret = append(ret, roachpb.KeyValue{
		Key:   keys.DescIDGenerator,
		Value: value,
	})

	// addDescriptor generates the needed KeyValue objects to install a
	// descriptor on a new cluster.
	addDescriptor := func(parentID ID, desc DescriptorProto) {
		// Create name metadata key.
		value := roachpb.Value{}
		value.SetInt(int64(desc.GetID()))

		// TODO(solon): This if/else can be removed in 20.2, as there will be no
		// need to support the deprecated namespace table. Note that we only
		// bootstrap the initial data in code in 20.1 without 20.1 as the active
		// version for testing.
		if bootstrapVersion.IsActive(clusterversion.VersionNamespaceTableWithSchemas) {
			if parentID != keys.RootNamespaceID {
				ret = append(ret, roachpb.KeyValue{
					Key:   NewPublicTableKey(parentID, desc.GetName()).Key(),
					Value: value,
				})
			} else {
				// Initializing a database. Databases must be initialized with
				// the public schema, as all tables are scoped under the public schema.
				publicSchemaValue := roachpb.Value{}
				publicSchemaValue.SetInt(int64(keys.PublicSchemaID))
				ret = append(
					ret,
					roachpb.KeyValue{
						Key:   NewDatabaseKey(desc.GetName()).Key(),
						Value: value,
					},
					roachpb.KeyValue{
						Key:   NewPublicSchemaKey(desc.GetID()).Key(),
						Value: publicSchemaValue,
					})
			}
		} else {
			// Don't add the new namespace table to a cluster which is being
			// bootstrapped before the migration to add the new namespace table has
			// been run.
			//
			// TODO(ajwerner): Providing a mapping from system tables to the version
			// at which they were added to avoid adding table descriptors to namespace
			// prior to the migration which should add them.
			if desc.GetID() != keys.NamespaceTableID {
				ret = append(ret, roachpb.KeyValue{
					Key:   NewDeprecatedTableKey(parentID, desc.GetName()).Key(),
					Value: value,
				})
			}
		}

		// Create descriptor metadata key.
		value = roachpb.Value{}
		wrappedDesc := WrapDescriptor(desc)
		if err := value.SetProto(wrappedDesc); err != nil {
			log.Fatalf(context.TODO(), "could not marshal %v", desc)
		}
		ret = append(ret, roachpb.KeyValue{
			Key:   MakeDescMetadataKey(desc.GetID()),
			Value: value,
		})
		if desc.GetID() > keys.MaxSystemConfigDescID {
			splits = append(splits, roachpb.RKey(keys.MakeTablePrefix(uint32(desc.GetID()))))
		}
	}

	// Generate initial values for system databases and tables, which have
	// static descriptors that were generated elsewhere.
	for _, sysObj := range ms.descs {
		addDescriptor(sysObj.parentID, sysObj.desc)
	}

	for _, id := range ms.otherSplitIDs {
		splits = append(splits, roachpb.RKey(keys.MakeTablePrefix(id)))
	}

	// Other key/value generation that doesn't fit into databases and
	// tables. This can be used to add initial entries to a table.
	ret = append(ret, ms.otherKV...)

	// Sort returned key values; this is valuable because it matches the way the
	// objects would be sorted if read from the engine.
	sort.Sort(roachpb.KeyValueByKey(ret))
	sort.Slice(splits, func(i, j int) bool {
		return splits[i].Less(splits[j])
	})

	return ret, splits
}

// DescriptorIDs returns the descriptor IDs present in the metadata schema in
// sorted order.
func (ms MetadataSchema) DescriptorIDs() IDs {
	descriptorIDs := IDs{}
	for _, md := range ms.descs {
		descriptorIDs = append(descriptorIDs, md.desc.GetID())
	}
	sort.Sort(descriptorIDs)
	return descriptorIDs
}

// systemTableIDCache is used to accelerate name lookups
// on table descriptors. It relies on the fact that
// table IDs under MaxReservedDescID are fixed.
var systemTableIDCache = func() map[string]ID {
	cache := make(map[string]ID)

	ms := MetadataSchema{}
	addSystemDescriptorsToSchema(&ms)
	for _, d := range ms.descs {
		t, ok := d.desc.(*TableDescriptor)
		if !ok || t.ParentID != SystemDB.ID || t.ID > keys.MaxReservedDescID {
			// We only cache table descriptors under 'system' with a reserved table ID.
			continue
		}
		cache[t.Name] = t.ID
	}

	// This special case exists so that we resolve "namespace" to the new
	// namespace table ID (30) in 20.1, while the Name in the "namespace"
	// descriptor is still set to "namespace2" during the
	// 20.1 cycle. We couldn't set the new namespace table's Name to "namespace"
	// in 20.1, because it had to co-exist with the old namespace table, whose
	// name must *remain* "namespace" - and you can't have duplicate descriptor
	// Name fields.
	//
	// This can be removed in 20.2, when we add a migration to change the new
	// namespace table's Name to "namespace" again.
	// TODO(solon): remove this in 20.2.
	cache[NamespaceTableName] = keys.NamespaceTableID

	return cache
}()

// LookupSystemTableDescriptorID uses the lookup cache above
// to bypass a KV lookup when resolving the name of system tables.
func LookupSystemTableDescriptorID(
	ctx context.Context, settings *cluster.Settings, dbID ID, tableName string,
) ID {
	if dbID != SystemDB.ID {
		return InvalidID
	}

	if settings != nil &&
		!settings.Version.IsActive(ctx, clusterversion.VersionNamespaceTableWithSchemas) &&
		tableName == NamespaceTableName {
		return DeprecatedNamespaceTable.ID
	}
	dbID, ok := systemTableIDCache[tableName]
	if !ok {
		return InvalidID
	}
	return dbID
}

// FilterInfo contain expr can use position and can use type
type FilterInfo struct {
	Pos uint32 // expr can use
	Typ uint32 // expr limit type byte 1 - const byte 2 - column
}

// WhiteListMap whitelist map for check expr can exec in ts engine
type WhiteListMap struct {
	Map map[uint32]FilterInfo
	Mu  syncutil.Mutex
}

// getValue get key value from map, need lock
func (b *WhiteListMap) getValue(key uint32) (ret FilterInfo, find bool) {
	b.Mu.Lock()
	if v, ok := b.Map[key]; ok {
		ret = v
		find = true
	}
	b.Mu.Unlock()
	return ret, find
}

// getBitIsTrue get index bit is 1 return true, 0 return false
func getBitIsTrue(value uint32, index uint32) bool {
	return (value>>index)&0x01 == 1
}

// CanPushByPos judge expr can push by position
func (b *WhiteListMap) CanPushByPos(key uint32, pos uint32) bool {
	val, find := b.getValue(key)
	if !find {
		return true
	}
	return !getBitIsTrue(val.Pos, pos)
}

// CheckWhiteListParam check if operation exists on the whitelist through key and pos,
// and if it exists, it can be executed in ts engine
func (b *WhiteListMap) CheckWhiteListParam(key uint32, pos uint32) bool {
	if b == nil {
		return false
	}
	val, find := b.getValue(key)
	if !find {
		return false
	}
	return getBitIsTrue(val.Pos, pos)
}

// CheckWhiteListAll judge white list expr can push by position and param type
func (b *WhiteListMap) CheckWhiteListAll(key uint32, pos uint32, tp uint32) bool {
	val, find := b.getValue(key)
	if !find {
		return false
	}
	return getBitIsTrue(val.Pos, pos) && getBitIsTrue(val.Typ, tp)
}
