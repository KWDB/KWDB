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
	"io/ioutil"
	"os"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/gogo/protobuf/proto"
)

// PebbleFileRegistry keeps track of files for the data-FS and store-FS for Pebble (see encrypted_fs.go
// for high-level comment).
//
// It is created even when file registry is disabled, so that it can be used to ensure that
// a registry file did not exist previously, since that would indicate that disabling the registry
// can cause data loss.
type PebbleFileRegistry struct {
	// Initialize the following before calling Load().

	// The FS to write the file registry file.
	FS vfs.FS

	// The directory used by the DB. It is used to construct the name of the file registry file and
	// to turn absolute path names of files in this directory into relative path names. The latter
	// is done for compatibility with the file registry implemented for RocksDB, even though it
	// currently requires some potentially non-portable filepath manipulation.
	DBDir string

	// Is the DB read only.
	ReadOnly bool

	// Implementation.
	registryFilename string

	mu struct {
		syncutil.Mutex
		currProto *enginepb.FileRegistry
	}
}

const (
	fileRegistryFilename = "KWBASEDB_REGISTRY"
)

func (r *PebbleFileRegistry) checkNoRegistryFile() error {
	r.registryFilename = r.FS.PathJoin(r.DBDir, fileRegistryFilename)
	if f, err := r.FS.Open(r.registryFilename); err == nil {
		f.Close()
		return os.ErrExist
	}
	return nil
}

// Load loads the contents of the file registry from a file, if the file exists, else it is a noop.
// It must be called at most once, before the other functions.
func (r *PebbleFileRegistry) Load() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.currProto = &enginepb.FileRegistry{}
	r.registryFilename = r.FS.PathJoin(r.DBDir, fileRegistryFilename)
	f, err := r.FS.Open(r.registryFilename)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	var b []byte
	if b, err = ioutil.ReadAll(f); err != nil {
		return err
	}
	if err = protoutil.Unmarshal(b, r.mu.currProto); err != nil {
		return err
	}
	return nil
}

// GetFileEntry gets the file entry corresponding to filename, if there is one, else returns nil.
func (r *PebbleFileRegistry) GetFileEntry(filename string) *enginepb.FileEntry {
	filename = r.tryMakeRelativePath(filename)
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.mu.currProto.Files[filename]
}

// SetFileEntry sets filename => entry in the registry map and persists the registry.
func (r *PebbleFileRegistry) SetFileEntry(filename string, entry *enginepb.FileEntry) error {
	filename = r.tryMakeRelativePath(filename)
	newProto := &enginepb.FileRegistry{}

	r.mu.Lock()
	defer r.mu.Unlock()
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files == nil {
		newProto.Files = make(map[string]*enginepb.FileEntry)
	}
	newProto.Files[filename] = entry
	return r.writeRegistry(newProto)
}

// MaybeDeleteEntry deletes the entry for filename, if it exists, and persists the registry, if changed.
func (r *PebbleFileRegistry) MaybeDeleteEntry(filename string) error {
	filename = r.tryMakeRelativePath(filename)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[filename] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	delete(newProto.Files, filename)
	return r.writeRegistry(newProto)
}

// MaybeRenameEntry moves the entry under src to dst, if src exists. If src does not exist, but dst
// exists, dst is deleted. Persists the registry if changed.
func (r *PebbleFileRegistry) MaybeRenameEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[src] == nil && r.mu.currProto.Files[dst] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
		delete(newProto.Files, src)
	}
	return r.writeRegistry(newProto)
}

// MaybeLinkEntry copies the entry under src to dst, if src exists. If src does not exist, but dst
// exists, dst is deleted. Persists the registry if changed.
func (r *PebbleFileRegistry) MaybeLinkEntry(src, dst string) error {
	src = r.tryMakeRelativePath(src)
	dst = r.tryMakeRelativePath(dst)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.mu.currProto.Files[src] == nil && r.mu.currProto.Files[dst] == nil {
		return nil
	}
	newProto := &enginepb.FileRegistry{}
	proto.Merge(newProto, r.mu.currProto)
	if newProto.Files[src] == nil {
		delete(newProto.Files, dst)
	} else {
		newProto.Files[dst] = newProto.Files[src]
	}
	return r.writeRegistry(newProto)
}

func (r *PebbleFileRegistry) tryMakeRelativePath(filename string) string {
	// Logic copied from file_registry.cc.
	//
	// This may not work for Windows.
	// TODO(sbhola): see if we can use filepath.Rel() here. filepath.Rel() will turn "/b/c"
	// relative to directory "/a" into "../b/c" which would not be compatible with what
	// we do in RocksDB, which is why we have copied the code below.

	dbDir := r.DBDir

	// Unclear why this should ever happen, but there is a test case for this in file_registry_test.cc
	// so we have duplicated this here.
	//
	// Check for the rare case when we're referring to the db directory itself (without slash).
	if filename == dbDir {
		return ""
	}
	if len(dbDir) > 0 && dbDir[len(dbDir)-1] != os.PathSeparator {
		dbDir = dbDir + string(os.PathSeparator)
	}
	if !strings.HasPrefix(filename, dbDir) {
		return filename
	}
	filename = filename[len(dbDir):]
	if len(filename) > 0 && filename[0] == os.PathSeparator {
		filename = filename[1:]
	}
	return filename
}

func (r *PebbleFileRegistry) writeRegistry(newProto *enginepb.FileRegistry) error {
	if r.ReadOnly {
		return fmt.Errorf("cannot write file registry since db is read-only")
	}
	b, err := protoutil.Marshal(newProto)
	if err != nil {
		return err
	}
	if err = SafeWriteToFile(r.FS, r.DBDir, r.registryFilename, b); err != nil {
		return err
	}
	r.mu.currProto = newProto
	return nil
}

func (r *PebbleFileRegistry) getRegistryCopy() *enginepb.FileRegistry {
	r.mu.Lock()
	defer r.mu.Unlock()
	rv := &enginepb.FileRegistry{}
	proto.Merge(rv, r.mu.currProto)
	return rv
}
