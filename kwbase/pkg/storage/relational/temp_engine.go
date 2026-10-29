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

package relational

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/diskmap"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage/fs"
	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
)

// NewTempEngine creates a new engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewTempEngine(
	ctx context.Context,
	engine enginepb.EngineType,
	tempStorage base.TempStorageConfig,
	storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	switch engine {
	case enginepb.EngineTypeDefault, enginepb.EngineTypeRocksDB:
		return NewRocksDBTempEngine(tempStorage, storeSpec)
	default:
		return nil, nil, ErrRemovedStorageEngine(engine)
	}
}

type rocksDBTempEngine struct {
	db *RocksDB
}

// Close implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) Close() {
	r.db.Close()
}

// NewSortedDiskMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, false /* allowDuplications */)
}

// NewSortedDiskMultiMap implements the diskmap.Factory interface.
func (r *rocksDBTempEngine) NewSortedDiskMultiMap() diskmap.SortedDiskMap {
	return newRocksDBMap(r.db, true /* allowDuplicates */)
}

// storageConfigFromTempStorageConfigAndStoreSpec creates a base.StorageConfig
// used by the RocksDB temp engine from the given arguments.
func storageConfigFromTempStorageConfigAndStoreSpec(
	config base.TempStorageConfig, spec base.StoreSpec,
) base.StorageConfig {
	return base.StorageConfig{
		Attrs: roachpb.Attributes{},
		Dir:   config.Path,
		// MaxSize doesn't matter for temp storage - it's not enforced in any way.
		MaxSize:         0,
		UseFileRegistry: spec.UseFileRegistry,
		ExtraOptions:    spec.ExtraOptions,
	}
}

// NewRocksDBTempEngine creates a new RocksDB engine for DistSQL processors to use when
// the working set is larger than can be stored in memory.
func NewRocksDBTempEngine(
	tempStorage base.TempStorageConfig, storeSpec base.StoreSpec,
) (diskmap.Factory, fs.FS, error) {
	if tempStorage.InMemory {
		// TODO(arjun): Limit the size of the store once #16750 is addressed.
		// Technically we do not pass any attributes to temporary store.
		db := newRocksDBInMem(roachpb.Attributes{} /* attrs */, 0 /* cacheSize */)
		return &rocksDBTempEngine{db: db}, db, nil
	}

	cfg := RocksDBConfig{
		StorageConfig: storageConfigFromTempStorageConfigAndStoreSpec(tempStorage, storeSpec),
		MaxOpenFiles:  128, // TODO(arjun): Revisit this.
	}
	rocksDBCache := NewRocksDBCache(0)
	defer rocksDBCache.Release()
	db, err := NewRocksDB(cfg, rocksDBCache)
	if err != nil {
		return nil, nil, err
	}

	return &rocksDBTempEngine{db: db}, db, nil
}
