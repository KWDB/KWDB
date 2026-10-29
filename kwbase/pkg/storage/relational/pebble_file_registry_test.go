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
	"runtime/debug"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/storage/relational/enginepb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/kr/pretty"
	"github.com/stretchr/testify/require"
)

func checkEquality(t *testing.T, fs vfs.FS, expected map[string]*enginepb.FileEntry) {
	registry := &PebbleFileRegistry{FS: fs, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	registry.mu.Lock()
	defer registry.mu.Unlock()
	if diff := pretty.Diff(registry.mu.currProto.Files, expected); diff != nil {
		t.Log(string(debug.Stack()))
		t.Fatalf("%s\n%v", strings.Join(diff, "\n"), registry.mu.currProto.Files)
	}
}

func TestFileRegistryRelativePaths(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mem := vfs.NewMem()
	fileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	type TestCase struct {
		dbDir            string
		filename         string
		expectedFilename string
	}
	testCases := []TestCase{
		{"/", "/foo", "foo"},
		{"/rocksdir", "/rocksdirfoo", "/rocksdirfoo"},
		{"/rocksdir", "/rocksdir/foo", "foo"},
		// We get the occasional double-slash.
		{"/rocksdir", "/rocksdir//foo", "foo"},
		{"/mydir", "/mydir", ""},
		{"/mydir", "/mydir/", ""},
		{"/mydir", "/mydir//", ""},
		{"/mnt/otherdevice/", "/mnt/otherdevice/myfile", "myfile"},
		{"/mnt/otherdevice/myfile", "/mnt/otherdevice/myfile", ""},
	}

	for _, tc := range testCases {
		require.NoError(t, mem.MkdirAll(tc.dbDir, 0755))
		registry := &PebbleFileRegistry{FS: mem, DBDir: tc.dbDir}
		require.NoError(t, registry.Load())
		require.NoError(t, registry.SetFileEntry(tc.filename, fileEntry))
		entry := registry.GetFileEntry(tc.expectedFilename)
		if diff := pretty.Diff(entry, fileEntry); diff != nil {
			t.Fatalf("filename: %s: %s\n%v", tc.filename, strings.Join(diff, "\n"), entry)
		}
	}
}

func TestFileRegistryOps(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mem := vfs.NewMem()
	fooFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	barFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Store, EncryptionSettings: []byte("bar")}
	bazFileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("baz")}

	require.NoError(t, mem.MkdirAll("/mydb", 0755))
	registry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb"}
	require.NoError(t, registry.Load())
	require.Nil(t, registry.GetFileEntry("file1"))

	// {file1 => foo}
	require.NoError(t, registry.SetFileEntry("file1", fooFileEntry))
	expected := make(map[string]*enginepb.FileEntry)
	expected["file1"] = fooFileEntry
	checkEquality(t, mem, expected)

	// {file1 => foo, file2 => bar}
	require.NoError(t, registry.SetFileEntry("file2", barFileEntry))
	expected["file2"] = barFileEntry
	checkEquality(t, mem, expected)

	// {file3 => foo, file2 => bar}
	require.NoError(t, registry.MaybeRenameEntry("file1", "file3"))
	expected["file3"] = fooFileEntry
	delete(expected, "file1")
	checkEquality(t, mem, expected)

	// {file3 => foo, file2 => bar, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file2", "file4"))
	expected["file4"] = barFileEntry
	checkEquality(t, mem, expected)

	// {file3 => foo, file4 => bar}
	require.NoError(t, registry.MaybeLinkEntry("file5", "file2"))
	delete(expected, "file2")
	checkEquality(t, mem, expected)

	// {file3 => foo}
	require.NoError(t, registry.MaybeRenameEntry("file7", "file4"))
	delete(expected, "file4")
	checkEquality(t, mem, expected)

	// {file3 => foo, blue/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb/blue/baz", bazFileEntry))
	expected["blue/baz"] = bazFileEntry
	checkEquality(t, mem, expected)

	entry := registry.GetFileEntry("/mydb/blue/baz")
	if diff := pretty.Diff(entry, bazFileEntry); diff != nil {
		t.Fatalf("%s\n%v", strings.Join(diff, "\n"), entry)
	}

	// {file3 => foo}
	require.NoError(t, registry.MaybeDeleteEntry("/mydb/blue/baz"))
	delete(expected, "blue/baz")
	checkEquality(t, mem, expected)

	// {file3 => foo, green/baz => baz} (since latter file uses relative path).
	require.NoError(t, registry.SetFileEntry("/mydb//green/baz", bazFileEntry))
	expected["green/baz"] = bazFileEntry
	checkEquality(t, mem, expected)

	// Noops
	require.NoError(t, registry.MaybeDeleteEntry("file1"))
	require.NoError(t, registry.MaybeRenameEntry("file4", "file5"))
	require.NoError(t, registry.MaybeLinkEntry("file6", "file7"))
	checkEquality(t, mem, expected)

	// Open a read-only registry. All updates should fail.
	roRegistry := &PebbleFileRegistry{FS: mem, DBDir: "/mydb", ReadOnly: true}
	require.NoError(t, roRegistry.Load())
	require.Error(t, roRegistry.SetFileEntry("file3", bazFileEntry))
	require.Error(t, roRegistry.MaybeDeleteEntry("file3"))
	require.Error(t, roRegistry.MaybeRenameEntry("file3", "file4"))
	require.Error(t, roRegistry.MaybeLinkEntry("file3", "file4"))
}

func TestFileRegistryCheckNoFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	mem := vfs.NewMem()
	fileEntry :=
		&enginepb.FileEntry{EnvType: enginepb.EnvType_Data, EncryptionSettings: []byte("foo")}
	registry := &PebbleFileRegistry{FS: mem}
	require.NoError(t, registry.checkNoRegistryFile())
	require.NoError(t, registry.Load())
	require.NoError(t, registry.SetFileEntry("/foo", fileEntry))
	registry = &PebbleFileRegistry{FS: mem}
	require.Error(t, registry.checkNoRegistryFile())
}
