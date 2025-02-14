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

package kvserver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/storage/fs"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/pkg/errors"
	"golang.org/x/time/rate"
)

// SSTSnapshotStorage provides an interface to create scratches and owns the
// directory of scratches created. A scratch manages the SSTs created during a
// specific snapshot.
type SSTSnapshotStorage struct {
	engine  storage.Engine
	limiter *rate.Limiter
	dir     string
}

// NewSSTSnapshotStorage creates a new SST snapshot storage.
func NewSSTSnapshotStorage(engine storage.Engine, limiter *rate.Limiter) SSTSnapshotStorage {
	return SSTSnapshotStorage{
		engine:  engine,
		limiter: limiter,
		dir:     filepath.Join(engine.GetAuxiliaryDir(), "sstsnapshot"),
	}
}

// NewScratchSpace creates a new storage scratch space for SSTs for a specific
// snapshot.
func (s *SSTSnapshotStorage) NewScratchSpace(
	rangeID roachpb.RangeID, snapUUID uuid.UUID,
) *SSTSnapshotStorageScratch {
	snapDir := filepath.Join(s.dir, strconv.Itoa(int(rangeID)), snapUUID.String())
	return &SSTSnapshotStorageScratch{
		storage: s,
		snapDir: snapDir,
	}
}

// Clear removes all created directories and SSTs.
func (s *SSTSnapshotStorage) Clear() error {
	return os.RemoveAll(s.dir)
}

// SSTSnapshotStorageScratch keeps track of the SST files incrementally created
// when receiving a snapshot. Each scratch is associated with a specific
// snapshot.
type SSTSnapshotStorageScratch struct {
	storage    *SSTSnapshotStorage
	ssts       []string
	snapDir    string
	dirCreated bool
}

func (s *SSTSnapshotStorageScratch) filename(id int) string {
	return filepath.Join(s.snapDir, fmt.Sprintf("%d.sst", id))
}

func (s *SSTSnapshotStorageScratch) createDir() error {
	// TODO(peter): The directory creation needs to be plumbed through the Engine
	// interface. Right now, this is creating a directory on disk even when the
	// Engine has an in-memory filesystem. The only reason everything still works
	// is because RocksDB MemEnvs allow the creation of files when the parent
	// directory doesn't exist.
	err := os.MkdirAll(s.snapDir, 0755)
	s.dirCreated = s.dirCreated || err == nil
	return err
}

// NewFile adds another file to SSTSnapshotStorageScratch. This file is lazily
// created when the file is written to the first time. A nonzero value for
// syncSize calls Sync after syncSize bytes have been written since last sync.
func (s *SSTSnapshotStorageScratch) NewFile(
	ctx context.Context, syncSize int64,
) (*SSTSnapshotStorageFile, error) {
	id := len(s.ssts)
	filename := s.filename(id)
	s.ssts = append(s.ssts, filename)
	f := &SSTSnapshotStorageFile{
		scratch:  s,
		filename: filename,
		ctx:      ctx,
		syncSize: syncSize,
	}
	return f, nil
}

// WriteSST writes SST data to a file. The method closes
// the provided SST when it is finished using it. If the provided SST is empty,
// then no file will be created and nothing will be written.
func (s *SSTSnapshotStorageScratch) WriteSST(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	f, err := s.NewFile(ctx, 0)
	if err != nil {
		return err
	}
	defer func() {
		// Closing an SSTSnapshotStorageFile multiple times is idempotent. Nothing
		// actionable if closing fails.
		_ = f.Close()
	}()
	if _, err := f.Write(data); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

// SSTs returns the names of the files created.
func (s *SSTSnapshotStorageScratch) SSTs() []string {
	return s.ssts
}

// Clear removes the directory and SSTs created for a particular snapshot.
func (s *SSTSnapshotStorageScratch) Clear() error {
	return os.RemoveAll(s.snapDir)
}

// SSTSnapshotStorageFile is an SST file managed by a
// SSTSnapshotStorageScratch.
type SSTSnapshotStorageFile struct {
	scratch        *SSTSnapshotStorageScratch
	created        bool
	file           fs.File
	filename       string
	ctx            context.Context
	bytesSinceSync int64
	syncSize       int64
}

func (f *SSTSnapshotStorageFile) openFile() error {
	if f.created {
		if f.file == nil {
			return errors.Errorf("file has already been closed")
		}
		return nil
	}
	if !f.scratch.dirCreated {
		if err := f.scratch.createDir(); err != nil {
			return err
		}
	}
	file, err := f.scratch.storage.engine.CreateFile(f.filename)
	if err != nil {
		return err
	}
	f.file = file
	f.created = true
	return nil
}

// Write writes contents to the file while respecting the limiter passed into
// SSTSnapshotStorageScratch. Writing empty contents is okay and is treated as
// a noop. The file must have not been closed.
func (f *SSTSnapshotStorageFile) Write(contents []byte) (int, error) {
	if len(contents) == 0 {
		return 0, nil
	}
	if err := f.openFile(); err != nil {
		return 0, err
	}
	limitBulkIOWrite(f.ctx, f.scratch.storage.limiter, len(contents))
	if _, err := f.file.Write(contents); err != nil {
		return 0, err
	}
	var err error
	if f.syncSize > 0 {
		f.bytesSinceSync += int64(len(contents))
		if f.bytesSinceSync >= f.syncSize {
			f.bytesSinceSync = 0
			err = f.Sync()
		}
	}
	return len(contents), err
}

// Close closes the file. Calling this function multiple times is idempotent.
// The file must have been written to before being closed.
func (f *SSTSnapshotStorageFile) Close() error {
	// We throw an error for empty files because it would be an error to ingest
	// an empty SST so catch this error earlier.
	if !f.created {
		return errors.New("file is empty")
	}
	if f.file == nil {
		return nil
	}
	if err := f.file.Close(); err != nil {
		return err
	}
	f.file = nil
	return nil
}

// Sync syncs the file to disk. Implements writeCloseSyncer in engine.
func (f *SSTSnapshotStorageFile) Sync() error {
	return f.file.Sync()
}
