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

package sql_test

import (
	gosql "database/sql"
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/config/zonepb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// disableGCTTLStrictEnforcement disables strict GC TTL enforcement for tests.
func disableGCTTLStrictEnforcement(t *testing.T, db *gosql.DB) (cleanup func()) {
	_, err := db.Exec(`SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = false`)
	require.NoError(t, err)
	return func() {
		_, err := db.Exec(`SET CLUSTER SETTING kv.gc_ttl.strict_enforcement.enabled = DEFAULT`)
		require.NoError(t, err)
	}
}

// addImmediateGCZoneConfig sets GC TTL to 0 for the given table ID.
func addImmediateGCZoneConfig(sqlDB *gosql.DB, id sqlbase.ID) (zonepb.ZoneConfig, error) {
	cfg := zonepb.DefaultZoneConfig()
	cfg.GC.TTLSeconds = 0
	buf, err := protoutil.Marshal(&cfg)
	if err != nil {
		return cfg, err
	}
	_, err = sqlDB.Exec(`UPSERT INTO system.zones VALUES ($1, $2)`, id, buf)
	return cfg, err
}

// zoneExists checks if a zone config exists for the given ID.
func zoneExists(sqlDB *gosql.DB, expected *zonepb.ZoneConfig, id sqlbase.ID) error {
	rows, err := sqlDB.Query(`SELECT * FROM system.zones WHERE id = $1`, id)
	if err != nil {
		return err
	}
	defer rows.Close()
	if exists := (expected != nil); exists != rows.Next() {
		return fmt.Errorf("zone config exists = %v", exists)
	}
	if expected != nil {
		var storedID sqlbase.ID
		var val []byte
		if err := rows.Scan(&storedID, &val); err != nil {
			return fmt.Errorf("row scan failed: %s", err)
		}
		if storedID != id {
			return fmt.Errorf("e = %d, v = %d", id, storedID)
		}
		var cfg zonepb.ZoneConfig
		if err := protoutil.Unmarshal(val, &cfg); err != nil {
			return err
		}
		if !expected.Equal(cfg) {
			return fmt.Errorf("e = %v, v = %v", expected, cfg)
		}
	}
	return nil
}
