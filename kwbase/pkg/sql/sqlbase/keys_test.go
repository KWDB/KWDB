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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/hashrouter/api"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestKeyAddress(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		key roachpb.Key
	}{
		{MakeDescMetadataKey(123)},
		{MakeDescMetadataKey(124)},
		{NewPublicTableKey(0, "BAR").Key()},
		{NewPublicTableKey(1, "BAR").Key()},
		{NewPublicTableKey(1, "foo").Key()},
		{NewPublicTableKey(2, "foo").Key()},
	}
	var lastKey roachpb.Key
	for i, test := range testCases {
		resultAddr, err := keys.Addr(test.key)
		if err != nil {
			t.Fatal(err)
		}
		result := resultAddr.AsRawKey()
		if result.Compare(lastKey) <= 0 {
			t.Errorf("%d: key address %q is <= %q", i, result, lastKey)
		}
		lastKey = result
	}
}

func TestMakeNameMetadataKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeNameMetadataKey(1, 2, "test")
	if len(key) == 0 {
		t.Error("MakeNameMetadataKey returned empty key")
	}
}

func TestDecodeNameMetadataKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeNameMetadataKey(1, 2, "test")
	parentID, parentSchemaID, name, err := DecodeNameMetadataKey(key)
	if err != nil {
		t.Errorf("DecodeNameMetadataKey failed: %v", err)
	}
	if parentID != 1 {
		t.Errorf("Expected parentID 1, got %d", parentID)
	}
	if parentSchemaID != 2 {
		t.Errorf("Expected parentSchemaID 2, got %d", parentSchemaID)
	}
	if name != "test" {
		t.Errorf("Expected name 'test', got '%s'", name)
	}
}

func TestMakeDeprecatedNameMetadataKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeDeprecatedNameMetadataKey(1, "test")
	if len(key) == 0 {
		t.Error("MakeDeprecatedNameMetadataKey returned empty key")
	}
}

func TestMakeAllDescsMetadataKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeAllDescsMetadataKey()
	if len(key) == 0 {
		t.Error("MakeAllDescsMetadataKey returned empty key")
	}
}

func TestMakeDescMetadataKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeDescMetadataKey(123)
	if len(key) == 0 {
		t.Error("MakeDescMetadataKey returned empty key")
	}
}

func TestMakeTsPrimaryTagKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	hp := []api.HashPoint{100}
	key := MakeTsPrimaryTagKey(1, hp)
	if len(key) == 0 {
		t.Error("MakeTsPrimaryTagKey returned empty key")
	}
}

func TestMakeTsHashPointKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeTsHashPointKey(1, 100, 200)
	if len(key) == 0 {
		t.Error("MakeTsHashPointKey returned empty key")
	}
}

func TestMakeTsRangeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeTsRangeKey(1, 100, 200)
	if len(key) == 0 {
		t.Error("MakeTsRangeKey returned empty key")
	}
}

func TestDecodeTsRangeKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	key := MakeTsRangeKey(1, 100, 200)
	tbDescID, hashPoint, err := DecodeTsRangeKey(key, true, 200)
	if err != nil {
		t.Errorf("DecodeTsRangeKey failed: %v", err)
	}
	if tbDescID != 1 {
		t.Errorf("Expected tbDescID 1, got %d", tbDescID)
	}
	if hashPoint != 100 {
		t.Errorf("Expected hashPoint 100, got %d", hashPoint)
	}
}

func TestIndexKeyValDirs(t *testing.T) {
	defer leaktest.AfterTest(t)()
	index := &IndexDescriptor{
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC, IndexDescriptor_DESC},
	}
	dirs := IndexKeyValDirs(index)
	if len(dirs) != 4 {
		t.Errorf("Expected 4 directions, got %d", len(dirs))
	}
}

func TestPrettyKey(t *testing.T) {
	defer leaktest.AfterTest(t)()
	valDirs := []encoding.Direction{encoding.Ascending}
	key := roachpb.Key("/Table/51/1")
	result := PrettyKey(valDirs, key, 0)
	if len(result) == 0 {
		t.Error("PrettyKey returned empty string")
	}
}

func TestPrettySpan(t *testing.T) {
	defer leaktest.AfterTest(t)()
	valDirs := []encoding.Direction{encoding.Ascending}
	span := roachpb.Span{Key: []byte("/Table/51/1"), EndKey: []byte("/Table/51/2")}
	result := PrettySpan(valDirs, span, 0)
	if len(result) == 0 {
		t.Error("PrettySpan returned empty string")
	}
}

func TestPrettySpans(t *testing.T) {
	defer leaktest.AfterTest(t)()
	index := &IndexDescriptor{
		ColumnDirections: []IndexDescriptor_Direction{IndexDescriptor_ASC},
	}
	spans := []roachpb.Span{
		{Key: []byte("/Table/51/1"), EndKey: []byte("/Table/51/2")},
	}
	result := PrettySpans(index, spans, 0)
	if len(result) == 0 {
		t.Error("PrettySpans returned empty string")
	}
}
