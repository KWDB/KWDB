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

package relational

import (
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"github.com/cockroachdb/pebble/vfs"
)

// EncryptionStatsHandler provides encryption related stats.
type EncryptionStatsHandler interface {
	// Returns a serialized enginepbccl.EncryptionStatus.
	GetEncryptionStatus() ([]byte, error)
	// Returns a serialized enginepbccl.DataKeysRegistry, scrubbed of key contents.
	GetDataKeysRegistry() ([]byte, error)
	// Returns the ID of the active data key, or "plain" if none.
	GetActiveDataKeyID() (string, error)
	// Returns the enum value of the encryption type.
	GetActiveStoreKeyType() int32
	// Returns the KeyID embedded in the serialized EncryptionSettings.
	GetKeyIDFromSettings(settings []byte) (string, error)
}

// EncryptedEnvConfig holds inputs for resolving encryption-at-rest on a store
// directory. Used when probing store metadata (e.g. sticky engine resolution)
// without opening a full storage engine.
type EncryptedEnvConfig struct {
	Dir             string
	UseFileRegistry bool
	ExtraOptions    []byte
	FS              vfs.FS
	ReadOnly        bool
}

// NewEncryptedEnvFunc creates an encrypted environment and returns the vfs.FS to
// use for reading and writing data. Initialized by engineccl.Init(). The
// optionBytes is a binary serialized baseccl.EncryptionOptions.
var NewEncryptedEnvFunc func(
	fs vfs.FS, fr *PebbleFileRegistry, dbDir string, readOnly bool, optionBytes []byte,
) (vfs.FS, EncryptionStatsHandler, error)

// ResolveEncryptedEnvOptions configures FS for encryption-at-rest when enabled.
// Returns the file registry and encryption stats handler when applicable.
func ResolveEncryptedEnvOptions(
	cfg *EncryptedEnvConfig,
) (*PebbleFileRegistry, EncryptionStatsHandler, error) {
	fileRegistry := &PebbleFileRegistry{FS: cfg.FS, DBDir: cfg.Dir, ReadOnly: cfg.ReadOnly}
	if cfg.UseFileRegistry {
		if err := fileRegistry.Load(); err != nil {
			return nil, nil, err
		}
	} else {
		if err := fileRegistry.checkNoRegistryFile(); err != nil {
			return nil, nil, fmt.Errorf("encryption was used on this store before, but no encryption flags " +
				"specified. You need a CCL build and must fully specify the --enterprise-encryption flag")
		}
		fileRegistry = nil
	}

	var statsHandler EncryptionStatsHandler
	if len(cfg.ExtraOptions) > 0 {
		if !cfg.UseFileRegistry {
			return nil, nil, fmt.Errorf("file registry is needed to support encryption")
		}
		if NewEncryptedEnvFunc == nil {
			return nil, nil, fmt.Errorf("encryption is enabled but no function to create the encrypted env")
		}
		var err error
		cfg.FS, statsHandler, err =
			NewEncryptedEnvFunc(cfg.FS, fileRegistry, cfg.Dir, cfg.ReadOnly, cfg.ExtraOptions)
		if err != nil {
			return nil, nil, err
		}
	}
	return fileRegistry, statsHandler, nil
}

// ErrRemovedStorageEngine reports that a removed storage engine type was requested.
func ErrRemovedStorageEngine(engine enginepb.EngineType) error {
	switch engine {
	default:
		return fmt.Errorf("unknown storage engine: %d", engine)
	}
}
