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

package privilege_test

import (
	"bytes"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestPrivilegeDecode(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		raw              uint32
		privileges       privilege.List
		stringer, sorted string
	}{
		{0, privilege.List{}, "", ""},
		// We avoid 0 as a privilege value even though we use 1 << privValue.
		{1, privilege.List{}, "", ""},
		{2, privilege.List{privilege.ALL}, "ALL", "ALL"},
		{1024, privilege.List{privilege.EXECUTE}, "EXECUTE", "EXECUTE"},
		{10, privilege.List{privilege.ALL, privilege.DROP}, "ALL, DROP", "ALL,DROP"},
		{144, privilege.List{privilege.GRANT, privilege.DELETE}, "GRANT, DELETE", "DELETE,GRANT"},
		{1022,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT,
				privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
			"ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG",
			"ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE,ZONECONFIG",
		},
		{2046,
			privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT,
				privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG, privilege.EXECUTE},
			"ALL, CREATE, DROP, GRANT, SELECT, INSERT, DELETE, UPDATE, ZONECONFIG, EXECUTE",
			"ALL,CREATE,DELETE,DROP,EXECUTE,GRANT,INSERT,SELECT,UPDATE,ZONECONFIG",
		},
	}

	for _, tc := range testCases {
		pl := privilege.ListFromBitField(tc.raw)
		if len(pl) != len(tc.privileges) {
			t.Fatalf("%+v: wrong privilege list from raw: %+v", tc, pl)
		}
		for i := 0; i < len(pl); i++ {
			if pl[i] != tc.privileges[i] {
				t.Fatalf("%+v: wrong privilege list from raw: %+v", tc, pl)
			}
		}
		if pl.String() != tc.stringer {
			t.Fatalf("%+v: wrong String() output: %q", tc, pl.String())
		}
		if pl.SortedString() != tc.sorted {
			t.Fatalf("%+v: wrong SortedString() output: %q", tc, pl.SortedString())
		}
	}
}

func TestKindMask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		kind privilege.Kind
		mask uint32
		name string
	}{
		{privilege.ALL, 1 << privilege.ALL, "ALL"},
		{privilege.CREATE, 1 << privilege.CREATE, "CREATE"},
		{privilege.DROP, 1 << privilege.DROP, "DROP"},
		{privilege.GRANT, 1 << privilege.GRANT, "GRANT"},
		{privilege.SELECT, 1 << privilege.SELECT, "SELECT"},
		{privilege.INSERT, 1 << privilege.INSERT, "INSERT"},
		{privilege.DELETE, 1 << privilege.DELETE, "DELETE"},
		{privilege.UPDATE, 1 << privilege.UPDATE, "UPDATE"},
		{privilege.ZONECONFIG, 1 << privilege.ZONECONFIG, "ZONECONFIG"},
		{privilege.EXECUTE, 1 << privilege.EXECUTE, "EXECUTE"},
	}

	for _, tc := range testCases {
		if got := tc.kind.Mask(); got != tc.mask {
			t.Errorf("%s.Mask() = %d, want %d", tc.name, got, tc.mask)
		}
	}
}

func TestListMethods(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test Len, Swap, Less (sorting)
	pl := privilege.List{privilege.DELETE, privilege.CREATE, privilege.ALL}
	if pl.Len() != 3 {
		t.Errorf("List.Len() = %d, want 3", pl.Len())
	}

	// Test sorting
	sort.Sort(pl)
	expected := privilege.List{privilege.ALL, privilege.CREATE, privilege.DELETE}
	for i, p := range pl {
		if p != expected[i] {
			t.Errorf("List sorted incorrectly at index %d: got %v, want %v", i, p, expected[i])
		}
	}

	// Test ToBitField
	bitfield := pl.ToBitField()
	expectedBits := privilege.ALL.Mask() | privilege.CREATE.Mask() | privilege.DELETE.Mask()
	if bitfield != expectedBits {
		t.Errorf("List.ToBitField() = %d, want %d", bitfield, expectedBits)
	}

	// Test SortedNames
	names := pl.SortedNames()
	expectedNames := []string{"ALL", "CREATE", "DELETE"}
	if len(names) != len(expectedNames) {
		t.Errorf("List.SortedNames() length = %d, want %d", len(names), len(expectedNames))
	}
	for i, name := range names {
		if name != expectedNames[i] {
			t.Errorf("List.SortedNames() at index %d: got %s, want %s", i, name, expectedNames[i])
		}
	}

	// Test Format
	var buf bytes.Buffer
	pl.Format(&buf)
	expectedFormat := "ALL, CREATE, DELETE"
	if buf.String() != expectedFormat {
		t.Errorf("List.Format() = %q, want %q", buf.String(), expectedFormat)
	}
}

func TestListFromStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test valid case
	validStrings := []string{"ALL", "CREATE", "SELECT"}
	pl, err := privilege.ListFromStrings(validStrings)
	if err != nil {
		t.Fatalf("ListFromStrings(%v) error: %v", validStrings, err)
	}
	if len(pl) != 3 {
		t.Errorf("ListFromStrings(%v) length = %d, want 3", validStrings, len(pl))
	}

	// Test case insensitivity
	lowercaseStrings := []string{"all", "create", "select"}
	pl2, err := privilege.ListFromStrings(lowercaseStrings)
	if err != nil {
		t.Fatalf("ListFromStrings(%v) error: %v", lowercaseStrings, err)
	}
	if len(pl2) != 3 {
		t.Errorf("ListFromStrings(%v) length = %d, want 3", lowercaseStrings, len(pl2))
	}

	// Test invalid privilege
	invalidStrings := []string{"ALL", "INVALID"}
	_, err = privilege.ListFromStrings(invalidStrings)
	if err == nil {
		t.Errorf("ListFromStrings(%v) should have returned an error", invalidStrings)
	}
}

func TestPredefinedPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test ReadData
	readDataExpected := privilege.List{privilege.GRANT, privilege.SELECT}
	if len(privilege.ReadData) != len(readDataExpected) {
		t.Errorf("ReadData length = %d, want %d", len(privilege.ReadData), len(readDataExpected))
	}
	for i, p := range privilege.ReadData {
		if p != readDataExpected[i] {
			t.Errorf("ReadData at index %d: got %v, want %v", i, p, readDataExpected[i])
		}
	}

	// Test ReadWriteData
	readWriteDataExpected := privilege.List{privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE}
	if len(privilege.ReadWriteData) != len(readWriteDataExpected) {
		t.Errorf("ReadWriteData length = %d, want %d", len(privilege.ReadWriteData), len(readWriteDataExpected))
	}
	for i, p := range privilege.ReadWriteData {
		if p != readWriteDataExpected[i] {
			t.Errorf("ReadWriteData at index %d: got %v, want %v", i, p, readWriteDataExpected[i])
		}
	}
}
