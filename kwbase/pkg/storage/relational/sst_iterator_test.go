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
	"io/ioutil"
	"path/filepath"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func runTestSSTIterator(t *testing.T, iter SimpleIterator, allKVs []MVCCKeyValue) {
	// Drop the first kv so we can test Seek.
	expected := allKVs[1:]

	// Run the test multiple times to check re-Seeking.
	for i := 0; i < 3; i++ {
		var kvs []MVCCKeyValue
		for iter.SeekGE(expected[0].Key); ; iter.Next() {
			ok, err := iter.Valid()
			if err != nil {
				t.Fatalf("%+v", err)
			}
			if !ok {
				break
			}
			kv := MVCCKeyValue{
				Key: MVCCKey{
					Key:       append([]byte(nil), iter.UnsafeKey().Key...),
					Timestamp: iter.UnsafeKey().Timestamp,
				},
				Value: append([]byte(nil), iter.UnsafeValue()...),
			}
			kvs = append(kvs, kv)
		}
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("%+v", err)
		} else if ok {
			t.Fatalf("expected !ok")
		}

		lastElemKey := expected[len(expected)-1].Key
		seekTo := MVCCKey{Key: lastElemKey.Key.Next()}

		iter.SeekGE(seekTo)
		if ok, err := iter.Valid(); err != nil {
			t.Fatalf("%+v", err)
		} else if ok {
			foundKey := iter.UnsafeKey()
			t.Fatalf("expected !ok seeking to lastEmem.Next(). foundKey %s < seekTo %s: %t",
				foundKey, seekTo, foundKey.Less(seekTo))
		}

		if !reflect.DeepEqual(kvs, expected) {
			t.Fatalf("got %+v but expected %+v", kvs, expected)
		}
	}
}

func TestSSTIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sstFile := &MemFile{}
	sst := MakeIngestionSSTWriter(sstFile)
	defer sst.Close()
	var allKVs []MVCCKeyValue
	for i := 0; i < 10; i++ {
		kv := MVCCKeyValue{
			Key: MVCCKey{
				Key:       []byte{'A' + byte(i)},
				Timestamp: hlc.Timestamp{WallTime: int64(i)},
			},
			Value: []byte{'a' + byte(i)},
		}
		if err := sst.Put(kv.Key, kv.Value); err != nil {
			t.Fatalf("%+v", err)
		}
		allKVs = append(allKVs, kv)
	}

	if err := sst.Finish(); err != nil {
		t.Fatalf("%+v", err)
	}

	t.Run("Disk", func(t *testing.T) {
		tempDir, cleanup := testutils.TempDir(t)
		defer cleanup()

		path := filepath.Join(tempDir, "data.sst")
		if err := ioutil.WriteFile(path, sstFile.Data(), 0600); err != nil {
			t.Fatalf("%+v", err)
		}

		iter, err := NewSSTIterator(path)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestSSTIterator(t, iter, allKVs)
	})
	t.Run("Mem", func(t *testing.T) {
		iter, err := NewMemSSTIterator(sstFile.Data(), false)
		if err != nil {
			t.Fatalf("%+v", err)
		}
		defer iter.Close()
		runTestSSTIterator(t, iter, allKVs)
	})
}

func TestCockroachComparer(t *testing.T) {
	defer leaktest.AfterTest(t)()

	keyAMetadata := MVCCKey{
		Key: []byte("a"),
	}
	keyA2 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}
	keyA1 := MVCCKey{
		Key:       []byte("a"),
		Timestamp: hlc.Timestamp{WallTime: 1},
	}
	keyB2 := MVCCKey{
		Key:       []byte("b"),
		Timestamp: hlc.Timestamp{WallTime: 2},
	}

	if x := MVCCComparer.Compare(EncodeKey(keyAMetadata), EncodeKey(keyA1)); x != -1 {
		t.Errorf("expected key metadata to sort first got: %d", x)
	}
	if x := MVCCComparer.Compare(EncodeKey(keyA2), EncodeKey(keyA1)); x != -1 {
		t.Errorf("expected higher timestamp to sort first got: %d", x)
	}
	if x := MVCCComparer.Compare(EncodeKey(keyA2), EncodeKey(keyB2)); x != -1 {
		t.Errorf("expected lower key to sort first got: %d", x)
	}
}
