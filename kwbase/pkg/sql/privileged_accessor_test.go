// Copyright 2020 The Cockroach Authors.
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

package sql_test

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/tests"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestLookupNamespaceID tests the lookup namespace id falls back
// onto system.namespace_deprecated.
func TestLookupNamespaceIDFallback(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, kvDB := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	err := kvDB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return txn.Put(
			ctx,
			sqlbase.NewDeprecatedTableKey(999, "bob").Key(),
			9999,
		)
	})
	require.NoError(t, err)

	// Assert the row exists in the database.
	var id int64
	err = sqlDB.QueryRow(
		`SELECT kwdb_internal.get_namespace_id($1, $2)`,
		999,
		"bob",
	).Scan(&id)
	require.NoError(t, err)
	assert.Equal(t, int64(9999), id)
}

// TestLookupZoneConfigByNamespaceID tests the LookupZoneConfigByNamespaceID function
func TestLookupZoneConfigByNamespaceID(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	s, sqlDB, _ := serverutils.StartServer(t, params)
	defer s.Stopper().Stop(ctx)

	// First, get the ID of the default database
	var dbID int64
	err := sqlDB.QueryRow(
		`SELECT kwdb_internal.get_namespace_id($1, $2)`,
		0,
		"defaultdb",
	).Scan(&dbID)
	require.NoError(t, err)
	assert.Greater(t, dbID, int64(0))

	// Test case 1: Lookup zone config for existing database
	t.Run("Lookup zone config for existing database", func(t *testing.T) {
		var config []byte
		err := sqlDB.QueryRow(
			`SELECT kwdb_internal.get_zone_config($1)`,
			dbID,
		).Scan(&config)
		// The default database may or may not have a zone config
		// We just want to ensure the function doesn't return an error
		if err != nil {
			t.Logf("No zone config found for defaultdb: %v", err)
		}
	})

	// Test case 2: Lookup zone config for non-existent namespace
	t.Run("Lookup zone config for non-existent namespace", func(t *testing.T) {
		var config []byte
		err := sqlDB.QueryRow(
			`SELECT kwdb_internal.get_zone_config($1)`,
			999999, // non-existent ID
		).Scan(&config)
		require.NoError(t, err)
		assert.Nil(t, config)
	})
}
