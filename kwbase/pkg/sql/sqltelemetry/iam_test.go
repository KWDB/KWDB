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

package sqltelemetry_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqltelemetry"
)

func TestIncIAMOptionCounter(t *testing.T) {
	// Test that we can call IncIAMOptionCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMOptionCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMOptionCounter(sqltelemetry.CreateRole, "NOLOGIN")
	sqltelemetry.IncIAMOptionCounter(sqltelemetry.AlterRole, "VALID UNTIL")
}

func TestIncIAMCreateCounter(t *testing.T) {
	// Test that we can call IncIAMCreateCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMCreateCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMCreateCounter(sqltelemetry.Role)
	sqltelemetry.IncIAMCreateCounter(sqltelemetry.User)
}

func TestIncIAMAlterCounter(t *testing.T) {
	// Test that we can call IncIAMAlterCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMAlterCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMAlterCounter(sqltelemetry.Role)
	sqltelemetry.IncIAMAlterCounter(sqltelemetry.User)
}

func TestIncIAMDropCounter(t *testing.T) {
	// Test that we can call IncIAMDropCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMDropCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMDropCounter(sqltelemetry.Role)
	sqltelemetry.IncIAMDropCounter(sqltelemetry.User)
}

func TestIncIAMGrantCounter(t *testing.T) {
	// Test that we can call IncIAMGrantCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMGrantCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMGrantCounter(true)
	sqltelemetry.IncIAMGrantCounter(false)
}

func TestIncIAMRevokeCounter(t *testing.T) {
	// Test that we can call IncIAMRevokeCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMRevokeCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMRevokeCounter(true)
	sqltelemetry.IncIAMRevokeCounter(false)
}

func TestIncIAMGrantPrivilegesCounter(t *testing.T) {
	// Test that we can call IncIAMGrantPrivilegesCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMGrantPrivilegesCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnDatabase)
	sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnTable)
	sqltelemetry.IncIAMGrantPrivilegesCounter(sqltelemetry.OnSchema)
}

func TestIncIAMRevokePrivilegesCounter(t *testing.T) {
	// Test that we can call IncIAMRevokePrivilegesCounter without panic
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("IncIAMRevokePrivilegesCounter should not panic, but it did: %v", r)
		}
	}()

	sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnDatabase)
	sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnTable)
	sqltelemetry.IncIAMRevokePrivilegesCounter(sqltelemetry.OnSchema)
}

func TestTurnConnAuditingOnUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnConnAuditingOnUseCounter
	if counter == nil {
		t.Error("TurnConnAuditingOnUseCounter should not be nil")
	}
}

func TestTurnConnAuditingOffUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnConnAuditingOffUseCounter
	if counter == nil {
		t.Error("TurnConnAuditingOffUseCounter should not be nil")
	}
}

func TestTurnAuthAuditingOnUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnAuthAuditingOnUseCounter
	if counter == nil {
		t.Error("TurnAuthAuditingOnUseCounter should not be nil")
	}
}

func TestTurnAuthAuditingOffUseCounter(t *testing.T) {
	counter := sqltelemetry.TurnAuthAuditingOffUseCounter
	if counter == nil {
		t.Error("TurnAuthAuditingOffUseCounter should not be nil")
	}
}
