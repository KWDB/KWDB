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

package roleoption_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/roleoption"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestOptionMask(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		option roleoption.Option
		mask   uint32
		name   string
	}{
		{roleoption.CREATEROLE, 1 << roleoption.CREATEROLE, "CREATEROLE"},
		{roleoption.NOCREATEROLE, 1 << roleoption.NOCREATEROLE, "NOCREATEROLE"},
		{roleoption.PASSWORD, 1 << roleoption.PASSWORD, "PASSWORD"},
		{roleoption.LOGIN, 1 << roleoption.LOGIN, "LOGIN"},
		{roleoption.NOLOGIN, 1 << roleoption.NOLOGIN, "NOLOGIN"},
		{roleoption.VALIDUNTIL, 1 << roleoption.VALIDUNTIL, "VALIDUNTIL"},
	}

	for _, tc := range testCases {
		if got := tc.option.Mask(); got != tc.mask {
			t.Errorf("%s.Mask() = %d, want %d", tc.name, got, tc.mask)
		}
	}
}

func TestToOption(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test valid options
	validOptions := map[string]roleoption.Option{
		"CREATEROLE":   roleoption.CREATEROLE,
		"NOCREATEROLE": roleoption.NOCREATEROLE,
		"PASSWORD":     roleoption.PASSWORD,
		"LOGIN":        roleoption.LOGIN,
		"NOLOGIN":      roleoption.NOLOGIN,
		"VALID_UNTIL":  roleoption.VALIDUNTIL,
	}

	for str, expected := range validOptions {
		actual, err := roleoption.ToOption(str)
		if err != nil {
			t.Errorf("ToOption(%q) error: %v", str, err)
		}
		if actual != expected {
			t.Errorf("ToOption(%q) = %v, want %v", str, actual, expected)
		}
	}

	// Test case insensitivity
	lowercaseOptions := map[string]roleoption.Option{
		"createrole":   roleoption.CREATEROLE,
		"nocreaterole": roleoption.NOCREATEROLE,
		"password":     roleoption.PASSWORD,
		"login":        roleoption.LOGIN,
		"nologin":      roleoption.NOLOGIN,
		"valid_until":  roleoption.VALIDUNTIL,
	}

	for str, expected := range lowercaseOptions {
		actual, err := roleoption.ToOption(str)
		if err != nil {
			t.Errorf("ToOption(%q) error: %v", str, err)
		}
		if actual != expected {
			t.Errorf("ToOption(%q) = %v, want %v", str, actual, expected)
		}
	}

	// Test invalid option
	invalidOptions := []string{"INVALID", "", "UNKNOWN"}
	for _, str := range invalidOptions {
		_, err := roleoption.ToOption(str)
		if err == nil {
			t.Errorf("ToOption(%q) should have returned an error", str)
		}
	}
}

func TestListContains(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.LOGIN},
	}

	// Test existing options
	if !rol.Contains(roleoption.CREATEROLE) {
		t.Error("List should contain CREATEROLE")
	}
	if !rol.Contains(roleoption.LOGIN) {
		t.Error("List should contain LOGIN")
	}

	// Test non-existing options
	if rol.Contains(roleoption.NOCREATEROLE) {
		t.Error("List should not contain NOCREATEROLE")
	}
	if rol.Contains(roleoption.PASSWORD) {
		t.Error("List should not contain PASSWORD")
	}
}

func TestListToBitField(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test valid case
	rol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.LOGIN},
	}

	bitfield, err := rol.ToBitField()
	if err != nil {
		t.Fatalf("List.ToBitField() error: %v", err)
	}

	expected := roleoption.CREATEROLE.Mask() | roleoption.LOGIN.Mask()
	if bitfield != expected {
		t.Errorf("List.ToBitField() = %d, want %d", bitfield, expected)
	}

	// Test redundant options (should return error)
	redundantRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.CREATEROLE},
	}

	_, err = redundantRol.ToBitField()
	if err == nil {
		t.Error("List.ToBitField() should return error for redundant options")
	}
}

func TestListCheckRoleOptionConflicts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test no conflicts
	noConflictRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.LOGIN},
	}

	if err := noConflictRol.CheckRoleOptionConflicts(); err != nil {
		t.Errorf("List.CheckRoleOptionConflicts() error: %v", err)
	}

	// Test CREATEROLE vs NOCREATEROLE conflict
	createroleConflictRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.NOCREATEROLE},
	}

	if err := createroleConflictRol.CheckRoleOptionConflicts(); err == nil {
		t.Error("List.CheckRoleOptionConflicts() should return error for CREATEROLE vs NOCREATEROLE conflict")
	}

	// Test LOGIN vs NOLOGIN conflict
	loginConflictRol := roleoption.List{
		{Option: roleoption.LOGIN},
		{Option: roleoption.NOLOGIN},
	}

	if err := loginConflictRol.CheckRoleOptionConflicts(); err == nil {
		t.Error("List.CheckRoleOptionConflicts() should return error for LOGIN vs NOLOGIN conflict")
	}
}

func TestListGetSQLStmts(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with no options
	emptyRol := roleoption.List{}
	stmts, err := emptyRol.GetSQLStmts("CREATE")
	if err != nil {
		t.Errorf("List.GetSQLStmts() error: %v", err)
	}
	if stmts != nil {
		t.Errorf("List.GetSQLStmts() should return nil for empty list")
	}

	// Test with valid options
	validRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.LOGIN},
	}

	stmts, err = validRol.GetSQLStmts("ALTER")
	if err != nil {
		t.Errorf("List.GetSQLStmts() error: %v", err)
	}
	if len(stmts) != 2 {
		t.Errorf("List.GetSQLStmts() returned %d statements, want 2", len(stmts))
	}

	// Test with PASSWORD option (should be skipped)
	passwordRol := roleoption.List{
		{Option: roleoption.PASSWORD, HasValue: true, Value: func() (bool, string, error) { return false, "test", nil }},
		{Option: roleoption.LOGIN},
	}

	stmts, err = passwordRol.GetSQLStmts("CREATE")
	if err != nil {
		t.Errorf("List.GetSQLStmts() error: %v", err)
	}
	if len(stmts) != 1 {
		t.Errorf("List.GetSQLStmts() returned %d statements, want 1 (PASSWORD should be skipped)", len(stmts))
	}

	// Test with conflicting options
	conflictingRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
		{Option: roleoption.NOCREATEROLE},
	}

	_, err = conflictingRol.GetSQLStmts("ALTER")
	if err == nil {
		t.Error("List.GetSQLStmts() should return error for conflicting options")
	}
}

func TestListGetHashedPassword(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test with no password option
	noPasswordRol := roleoption.List{
		{Option: roleoption.CREATEROLE},
	}

	_, err := noPasswordRol.GetHashedPassword()
	if err == nil {
		t.Error("List.GetHashedPassword() should return error when no password option")
	}

	// Test with null password
	nullPasswordRol := roleoption.List{
		{Option: roleoption.PASSWORD, HasValue: true, Value: func() (bool, string, error) { return true, "", nil }},
	}

	hashedPassword, err := nullPasswordRol.GetHashedPassword()
	if err != nil {
		t.Errorf("List.GetHashedPassword() error: %v", err)
	}
	if len(hashedPassword) != 0 {
		t.Errorf("List.GetHashedPassword() should return empty byte array for null password")
	}

	// Test with empty password
	emptyPasswordRol := roleoption.List{
		{Option: roleoption.PASSWORD, HasValue: true, Value: func() (bool, string, error) { return false, "", nil }},
	}

	_, err = emptyPasswordRol.GetHashedPassword()
	if err == nil {
		t.Error("List.GetHashedPassword() should return error for empty password")
	}

	// Test with valid password
	validPasswordRol := roleoption.List{
		{Option: roleoption.PASSWORD, HasValue: true, Value: func() (bool, string, error) { return false, "testpassword", nil }},
	}

	hashedPassword, err = validPasswordRol.GetHashedPassword()
	if err != nil {
		t.Errorf("List.GetHashedPassword() error: %v", err)
	}
	if len(hashedPassword) == 0 {
		t.Error("List.GetHashedPassword() should return non-empty byte array for valid password")
	}
}
