// Copyright 2019 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
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
//
//	http://license.coscl.org.cn/MulanPSL2
//
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
package kvserver

import (
	"context"
	"io/ioutil"
	"os"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver/rditer"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/time/rate"
)

func TestSSTSnapshotStorage(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.TODO()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newEngine(t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)

	// Check that the storage lazily creates the directories on first write.
	_, err := os.Stat(scratch.snapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}

	f, err := scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, f.Close())
	}()

	// Check that even though the files aren't created, they are still recorded in SSTs().
	require.Equal(t, len(scratch.SSTs()), 1)

	// Check that the storage lazily creates the files on write.
	for _, fileName := range scratch.SSTs() {
		_, err := os.Stat(fileName)
		if !os.IsNotExist(err) {
			t.Fatalf("expected %s to not exist", fileName)
		}
	}

	_, err = f.Write([]byte("foo"))
	require.NoError(t, err)

	// After writing to files, check that they have been flushed to disk.
	for _, fileName := range scratch.SSTs() {
		require.FileExists(t, fileName)
		data, err := ioutil.ReadFile(fileName)
		require.NoError(t, err)
		require.Equal(t, data, []byte("foo"))
	}

	// Check that closing is idempotent.
	require.NoError(t, f.Close())
	require.NoError(t, f.Close())

	// Check that writing to a closed file is an error.
	_, err = f.Write([]byte("foo"))
	require.EqualError(t, err, "file has already been closed")

	// Check that closing an empty file is an error.
	f, err = scratch.NewFile(ctx, 0)
	require.NoError(t, err)
	require.EqualError(t, f.Close(), "file is empty")
	_, err = f.Write([]byte("foo"))
	require.NoError(t, err)

	// Check that Clear removes the directory.
	require.NoError(t, scratch.Clear())
	_, err = os.Stat(scratch.snapDir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", scratch.snapDir)
	}
	require.NoError(t, sstSnapshotStorage.Clear())
	_, err = os.Stat(sstSnapshotStorage.dir)
	if !os.IsNotExist(err) {
		t.Fatalf("expected %s to not exist", sstSnapshotStorage.dir)
	}
}

// TestMultiSSTWriterInitSST tests that multiSSTWriter initializes each of the
// SST files associated with the three replicated key ranges by writing a range
// deletion tombstone that spans the entire range of each respectively.
func TestMultiSSTWriterInitSST(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	testRangeID := roachpb.RangeID(1)
	testSnapUUID := uuid.Must(uuid.FromBytes([]byte("foobar1234567890")))
	testLimiter := rate.NewLimiter(rate.Inf, 0)

	cleanup, eng := newEngine(t)
	defer cleanup()
	defer eng.Close()

	sstSnapshotStorage := NewSSTSnapshotStorage(eng, testLimiter)
	scratch := sstSnapshotStorage.NewScratchSpace(testRangeID, testSnapUUID)
	desc := roachpb.RangeDescriptor{
		StartKey: roachpb.RKey("d"),
		EndKey:   roachpb.RKeyMax,
	}
	keyRanges := rditer.MakeReplicatedKeyRanges(&desc)

	msstw, err := newMultiSSTWriter(ctx, scratch, keyRanges, 0)
	require.NoError(t, err)
	err = msstw.Finish(ctx)
	require.NoError(t, err)

	var actualSSTs [][]byte
	fileNames := msstw.scratch.SSTs()
	for _, file := range fileNames {
		sst, err := eng.ReadFile(file)
		require.NoError(t, err)
		actualSSTs = append(actualSSTs, sst)
	}

	// Construct an SST file for each of the key ranges and write a rangedel
	// tombstone that spans from Start to End.
	var expectedSSTs [][]byte
	for _, r := range keyRanges {
		func() {
			sstFile := &storage.MemFile{}
			sst := storage.MakeIngestionSSTWriter(sstFile)
			defer sst.Close()
			err := sst.ClearRange(r.Start, r.End)
			require.NoError(t, err)
			err = sst.Finish()
			require.NoError(t, err)
			expectedSSTs = append(expectedSSTs, sstFile.Data())
		}()
	}

	require.Equal(t, len(actualSSTs), len(expectedSSTs))
	for i := range fileNames {
		require.Equal(t, actualSSTs[i], expectedSSTs[i])
	}
}
