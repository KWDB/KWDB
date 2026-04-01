// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
)

// func TestRemovePublicTableNamespaceEntry(t *testing.T) {
// 	ctx := context.Background()

// 	txn := &kv.Txn{}

// 	parentID := ID(1)
// 	name := "test_table"

// 	err := RemovePublicTableNamespaceEntry(ctx, txn, parentID, name)
// 	if err != nil {
// 		t.Logf("RemovePublicTableNamespaceEntry returned: %v", err)
// 	}
// }

// func TestRemoveSchemaNamespaceEntry(t *testing.T) {
// 	ctx := context.Background()

// 	txn := &kv.Txn{}

// 	parentID := ID(1)
// 	name := "test_schema"

// 	err := RemoveSchemaNamespaceEntry(ctx, txn, parentID, name)
// 	if err != nil {
// 		t.Logf("RemoveSchemaNamespaceEntry returned: %v", err)
// 	}
// }

// func TestRemoveDatabaseNamespaceEntry(t *testing.T) {
// 	ctx := context.Background()

// 	txn := &kv.Txn{}

// 	name := "test_database"

// 	err := RemoveDatabaseNamespaceEntry(ctx, txn, name, false)
// 	if err != nil {
// 		t.Logf("RemoveDatabaseNamespaceEntry returned: %v", err)
// 	}
// }

// func TestRemoveObjectNamespaceEntry(t *testing.T) {
// 	ctx := context.Background()

// 	txn := &kv.Txn{}

// 	testCases := []struct {
// 		parentID       ID
// 		parentSchemaID ID
// 		name           string
// 		kvTrace        bool
// 	}{
// 		{keys.RootNamespaceID, keys.RootNamespaceID, "test_database", false},
// 		{ID(1), keys.RootNamespaceID, "test_schema", false},
// 		{ID(1), keys.PublicSchemaID, "test_table", false},
// 	}

// 	for _, tc := range testCases {
// 		err := RemoveObjectNamespaceEntry(ctx, txn, tc.parentID, tc.parentSchemaID, tc.name, tc.kvTrace)
// 		if err != nil {
// 			t.Logf("RemoveObjectNamespaceEntry(%v, %v, %q) returned: %v", tc.parentID, tc.parentSchemaID, tc.name, err)
// 		}
// 	}
// }

func TestMakeObjectNameKey(t *testing.T) {
	ctx := context.Background()

	settings := cluster.MakeTestingClusterSettings()

	testCases := []struct {
		parentID       ID
		parentSchemaID ID
		name           string
	}{
		{ID(1), keys.PublicSchemaID, "test_table"},
		{ID(1), keys.RootNamespaceID, "test_schema"},
		{keys.RootNamespaceID, keys.RootNamespaceID, "test_database"},
	}

	for _, tc := range testCases {
		key := MakeObjectNameKey(ctx, settings, tc.parentID, tc.parentSchemaID, tc.name)
		t.Logf("MakeObjectNameKey(%v, %v, %q) = %v", tc.parentID, tc.parentSchemaID, tc.name, key)
	}
}
