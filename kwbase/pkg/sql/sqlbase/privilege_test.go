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
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestIsPrivilegeSet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var isInSet bool
	var privList privilege.List = privilege.List{}
	var bits uint32
	for pri := privilege.ALL; pri <= privilege.EXECUTE; pri++ {
		bits = privList.ToBitField()
		isInSet = isPrivilegeSet(bits, pri)
		if isInSet {
			t.Errorf("expected privilege:%s is not in privilege set, but privilege is in privilege set", pri.String())
		}
		privList = append(privList, pri)
		bits = privList.ToBitField()
		isInSet = isPrivilegeSet(bits, pri)
		if !isInSet {
			t.Errorf("expected privilege:%s is  in privilege set, but privilege is not in privilege set", pri.String())
		}
	}
}

func TestPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()
	descriptor := NewDefaultPrivilegeDescriptor()

	testCases := []struct {
		grantee       string // User to grant/revoke privileges on.
		grant, revoke privilege.List
		show          []UserPrivilegeString
	}{
		{"", nil, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{security.RootUser, privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{security.RootUser, privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{"foo", privilege.List{privilege.INSERT, privilege.DROP}, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"DROP", "INSERT"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{"bar", nil, privilege.List{privilege.INSERT, privilege.ALL},
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"DROP", "INSERT"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{"foo", privilege.List{privilege.ALL}, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{"foo", nil, privilege.List{privilege.SELECT, privilege.INSERT},
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"CREATE", "DELETE", "DROP", "EXECUTE", "GRANT", "UPDATE", "ZONECONFIG"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		{"foo", nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{security.RootUser, []string{"ALL"}},
			},
		},
		// Validate checks that root still has ALL privileges, but we do not call it here.
		{security.RootUser, nil, privilege.List{privilege.ALL},
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
			},
		},
		{"foo", privilege.List{privilege.EXECUTE}, nil,
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"EXECUTE"}},
			},
		},
		{"foo", privilege.List{privilege.CREATE, privilege.EXECUTE}, privilege.List{privilege.EXECUTE},
			[]UserPrivilegeString{
				{AdminRole, []string{"ALL"}},
				{"foo", []string{"CREATE"}},
			},
		},
	}

	for tcNum, tc := range testCases {
		if tc.grantee != "" {
			if tc.grant != nil {
				descriptor.Grant(tc.grantee, tc.grant)
			}
			if tc.revoke != nil {
				descriptor.Revoke(tc.grantee, tc.revoke)
			}
		}
		show := descriptor.Show()
		if len(show) != len(tc.show) {
			t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
				tcNum, descriptor, show, tc.show)
		}
		for i := 0; i < len(show); i++ {
			if show[i].User != tc.show[i].User || show[i].PrivilegeString() != tc.show[i].PrivilegeString() {
				t.Fatalf("#%d: show output for descriptor %+v differs, got: %+v, expected %+v",
					tcNum, descriptor, show, tc.show)
			}
		}
	}
}

func TestCheckPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		priv privilege.Kind
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.DROP, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", privilege.CREATE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", privilege.ALL, true},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", privilege.ALL, false},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", privilege.CREATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", privilege.UPDATE, false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", privilege.DROP, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.ALL}),
			"foo", privilege.DROP, true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.EXECUTE}),
			"foo", privilege.EXECUTE, true},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", privilege.EXECUTE, false},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.CheckPrivilege(tc.user, tc.priv); found != tc.exp {
			t.Errorf("#%d: CheckPrivilege(%s, %v) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.priv, tc.pd, found, tc.exp)
		}
	}
}

func TestAnyPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		pd   *PrivilegeDescriptor
		user string
		exp  bool
	}{
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE}),
			"bar", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.ALL}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{}),
			"foo", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"foo", true},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP}),
			"bar", false},
		{NewPrivilegeDescriptor("foo", privilege.List{privilege.EXECUTE}),
			"bar", false},
	}

	for tcNum, tc := range testCases {
		if found := tc.pd.AnyPrivilege(tc.user); found != tc.exp {
			t.Errorf("#%d: AnyPrivilege(%s) for descriptor %+v = %t, expected %t",
				tcNum, tc.user, tc.pd, found, tc.exp)
		}
	}
}

// TestPrivilegeValidate exercises validation for non-system descriptors.
func TestPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()
	id := ID(keys.MinUserDescID)
	descriptor := NewDefaultPrivilegeDescriptor()
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("foo", privilege.List{privilege.ALL})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant("bar", privilege.List{privilege.EXECUTE})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke("bar", privilege.List{privilege.EXECUTE})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err != nil {
		t.Fatal(err)
	}
	descriptor.Revoke(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	// TODO(marc): validate fails here because we do not aggregate
	// privileges into ALL when all are set.
	descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
	descriptor.Revoke(security.RootUser, privilege.List{privilege.ALL})
	if err := descriptor.Validate(id); err == nil {
		t.Fatal("unexpected success")
	}
}

// TestSystemPrivilegeValidate exercises validation for system config
// descriptors. We use a dummy system table installed for testing
// purposes.
func TestSystemPrivilegeValidate(t *testing.T) {
	defer leaktest.AfterTest(t)()

	id := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[id]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", id)
	}
	SystemAllowedPrivileges[id] = privilege.List{
		privilege.SELECT,
		privilege.GRANT,
	}
	defer delete(SystemAllowedPrivileges, id)

	rootWrongPrivilegesErr := "user root must have exactly SELECT, GRANT " +
		"privileges on system object with ID=.*"
	adminWrongPrivilegesErr := "user admin must have exactly SELECT, GRANT " +
		"privileges on system object with ID=.*"

	{
		// Valid: root user has one of the allowable privilege sets.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
		)
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.SELECT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has exactly the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}
	}

	{
		// Valid: root has exactly the allowed privileges.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(
			privilege.List{privilege.SELECT, privilege.GRANT},
		)

		// Valid: foo has a subset of the allowed privileges.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo can have privileges revoked, including privileges it doesn't currently have.
		descriptor.Revoke("foo", privilege.List{privilege.GRANT, privilege.UPDATE, privilege.ALL})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Invalid: root user has too many privileges.
		descriptor.Grant(security.RootUser, privilege.List{privilege.UPDATE})
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}
	}

	{
		// Invalid: root has a non-allowable privilege set.
		descriptor := NewCustomSuperuserPrivilegeDescriptor(privilege.List{privilege.UPDATE})
		if err := descriptor.Validate(id); !testutils.IsError(err, rootWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", rootWrongPrivilegesErr, err)
		}

		// Invalid: root's invalid privileges are revoked and replaced with allowable privileges,
		// but admin is still wrong.
		descriptor.Revoke(security.RootUser, privilege.List{privilege.UPDATE})
		descriptor.Grant(security.RootUser, privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id); !testutils.IsError(err, adminWrongPrivilegesErr) {
			t.Fatalf("expected err=%s, got err=%v", adminWrongPrivilegesErr, err)
		}

		// Valid: admin's invalid privileges are revoked and replaced with allowable privileges.
		descriptor.Revoke(AdminRole, privilege.List{privilege.UPDATE})
		descriptor.Grant(AdminRole, privilege.List{privilege.SELECT, privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}

		// Valid: foo has less privileges than root.
		descriptor.Grant("foo", privilege.List{privilege.GRANT})
		if err := descriptor.Validate(id); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFixPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Use a non-system ID.
	userID := ID(keys.MinUserDescID)
	userPrivs := privilege.List{privilege.ALL}

	// And create an entry for a fake system table.
	systemID := ID(keys.MaxReservedDescID)
	if _, exists := SystemAllowedPrivileges[systemID]; exists {
		t.Fatalf("system object with maximum id %d already exists--is the reserved id space full?", systemID)
	}
	systemPrivs := privilege.List{
		privilege.SELECT,
		privilege.GRANT,
	}
	SystemAllowedPrivileges[systemID] = systemPrivs
	defer delete(SystemAllowedPrivileges, systemID)

	type userPrivileges map[string]privilege.List

	testCases := []struct {
		id       ID
		input    userPrivileges
		modified bool
		output   userPrivileges
	}{
		{
			// Empty privileges for system ID.
			systemID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
			},
		},
		{
			// Valid requirements for system ID.
			systemID,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// Too many privileges for system ID.
			systemID,
			userPrivileges{
				security.RootUser: privilege.List{privilege.ALL},
				AdminRole:         privilege.List{privilege.ALL},
				"foo":             privilege.List{privilege.ALL},
				"bar":             privilege.List{privilege.SELECT, privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser: systemPrivs,
				AdminRole:         systemPrivs,
				"foo":             privilege.List{},
				"bar":             privilege.List{privilege.SELECT},
			},
		},
		{
			// Empty privileges for non-system ID.
			userID,
			userPrivileges{},
			true,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
			},
		},
		{
			// Valid requirements for non-system ID.
			userID,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.SELECT},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				"foo": privilege.List{privilege.ALL},
				"bar": privilege.List{privilege.UPDATE},
			},
			true,
			userPrivileges{
				security.RootUser: privilege.List{privilege.ALL},
				AdminRole:         privilege.List{privilege.ALL},
				"foo":             privilege.List{privilege.ALL},
				"bar":             privilege.List{privilege.UPDATE},
			},
		},
		{
			// Valid requirements for non-system ID.
			userID,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.EXECUTE},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
			false,
			userPrivileges{
				security.RootUser: userPrivs,
				AdminRole:         userPrivs,
				"foo":             privilege.List{privilege.EXECUTE},
				"bar":             privilege.List{privilege.GRANT},
				"baz":             privilege.List{privilege.SELECT, privilege.GRANT},
			},
		},
		{
			// All privileges are allowed for non-system ID, but we need super users.
			userID,
			userPrivileges{
				"foo": privilege.List{privilege.ALL},
				"bar": privilege.List{privilege.EXECUTE},
			},
			true,
			userPrivileges{
				security.RootUser: privilege.List{privilege.ALL},
				AdminRole:         privilege.List{privilege.ALL},
				"foo":             privilege.List{privilege.ALL},
				"bar":             privilege.List{privilege.EXECUTE},
			},
		},
	}

	for num, testCase := range testCases {
		desc := &PrivilegeDescriptor{}
		for u, p := range testCase.input {
			desc.Grant(u, p)
		}

		if a, e := desc.MaybeFixPrivileges(testCase.id), testCase.modified; a != e {
			t.Errorf("#%d: expected modified=%t, got modified=%t", num, e, a)
			continue
		}

		if a, e := len(desc.Users), len(testCase.output); a != e {
			t.Errorf("#%d: expected %d users (%v), got %d (%v)", num, e, testCase.output, a, desc.Users)
			continue
		}

		for u, p := range testCase.output {
			outputUser, ok := desc.findUser(u)
			if !ok {
				t.Errorf("#%d: expected user %s in output, but not found (%v)", num, u, desc.Users)
			}
			if a, e := privilege.ListFromBitField(outputUser.Privileges), p; a.ToBitField() != e.ToBitField() {
				t.Errorf("#%d: user %s: expected privileges %v, got %v", num, u, e, a)
			}
		}
	}
}

func TestFindRemoveUser(t *testing.T) {
	defer leaktest.AfterTest(t)()
	pd := NewPrivilegeDescriptor("foo", privilege.List{privilege.CREATE, privilege.DROP})
	_, ret := pd.findUser("u1")
	if ret {
		t.Errorf("expected: user u1 does not exist, actually:  exist")
	}
	UserPriv := pd.findOrCreateUser("u1")
	if UserPriv == nil {
		t.Errorf("expected: user u1  exist, actually: does not exist")
	}

	pd.removeUser("foo")
	_, ret = pd.findUser("foo")
	if ret {
		t.Errorf("expected: user foo does not exist, actually: exist")
	}
}

func TestListFromStrings(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		names      []string
		privileges privilege.List
	}{
		{
			names:      []string{"ALL"},
			privileges: privilege.List{privilege.ALL},
		},
		{
			names:      []string{"EXECUTE"},
			privileges: privilege.List{privilege.EXECUTE},
		},
		{
			names:      []string{"ALL", "GRANT", "SELECT", "INSERT", "UPDATE"},
			privileges: privilege.List{privilege.ALL, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
		},
		{
			names:      []string{"ALL", "CREATE", "DROP", "GRANT", "SELECT", "INSERT", "DELETE", "UPDATE"},
			privileges: privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE},
		},
		{
			names:      []string{"CREATE", "DROP", "GRANT", "SELECT", "INSERT", "DELETE", "UPDATE", "ZONECONFIG"},
			privileges: privilege.List{privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		},
		{
			names:      []string{"ALL", "CREATE", "DROP", "GRANT", "SELECT", "INSERT", "DELETE", "UPDATE", "ZONECONFIG"},
			privileges: privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		},
		{
			names:      []string{"ALL", "CREATE", "DROP", "GRANT", "SELECT", "INSERT", "DELETE", "UPDATE", "ZONECONFIG", "EXECUTE"},
			privileges: privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG, privilege.EXECUTE},
		}}

	for num, testCase := range testCases {
		kinds, err := privilege.ListFromStrings(testCase.names)
		if err != nil {
			t.Errorf("#%d: expected: convert string to kind success, actually: is faied", num)
		}
		if kinds.Len() != testCase.privileges.Len() {
			t.Errorf("#%d: expected: length of kinds equal length of testCase , actually: does not equal", num)
		}
		for i, kind := range kinds {
			if kind != testCase.privileges[i] {
				t.Errorf("#%d: expected result %v, but got %v", num, testCase.privileges, kinds)
			}
		}
	}

	names := []string{"CREATE", "DROP", "other", "SELECT", "INSERT", "DELETE", "UPDATE", "ZONECONFIG", "ADMIN", "MANAGE", "READ"}
	_, err := privilege.ListFromStrings(names)
	if err == nil {
		t.Errorf("expected: convert string to kind failed, actually:  successed")
	}
}

func TestSortedNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		names      []string
		privileges privilege.List
	}{
		{
			names:      []string{"ALL"},
			privileges: privilege.List{privilege.ALL},
		},
		{
			names:      []string{"ALL", "GRANT", "INSERT", "SELECT", "UPDATE"},
			privileges: privilege.List{privilege.ALL, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
		},
		{
			names:      []string{"ALL", "CREATE", "DELETE", "DROP", "GRANT", "INSERT", "SELECT", "UPDATE"},
			privileges: privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE},
		},
		{
			names:      []string{"CREATE", "DELETE", "DROP", "GRANT", "INSERT", "SELECT", "UPDATE", "ZONECONFIG"},
			privileges: privilege.List{privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		},
		{
			names:      []string{"CREATE", "DELETE", "DROP", "EXECUTE", "GRANT", "INSERT", "SELECT", "UPDATE", "ZONECONFIG"},
			privileges: privilege.List{privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG, privilege.EXECUTE},
		},
		{
			names:      []string{"ALL", "CREATE", "DELETE", "DROP", "GRANT", "INSERT", "SELECT", "UPDATE", "ZONECONFIG"},
			privileges: privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		}}

	for num, testCase := range testCases {
		resultNames := testCase.privileges.SortedNames()
		if len(resultNames) != len(testCase.names) {
			t.Errorf("#%d: expected: resultNames length equal testCase length, actually: does not equal", num)
		}
		for i, name := range resultNames {
			if name != testCase.names[i] {
				t.Errorf("#%d: expected: result %v, actually: got %v", num, testCase.names, resultNames)
			}
		}
	}

}

func TestSortedString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		resultString string
		privileges   privilege.List
	}{
		{
			resultString: "ALL",
			privileges:   privilege.List{privilege.ALL},
		},
		{
			resultString: "ALL,GRANT,INSERT,SELECT,UPDATE",
			privileges:   privilege.List{privilege.ALL, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.UPDATE},
		},
		{
			resultString: "ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE",
			privileges:   privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE},
		},
		{
			resultString: "CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE,ZONECONFIG",
			privileges:   privilege.List{privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		},
		{
			resultString: "CREATE,DELETE,DROP,EXECUTE,GRANT,INSERT,SELECT,UPDATE,ZONECONFIG",
			privileges:   privilege.List{privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG, privilege.EXECUTE},
		},
		{
			resultString: "ALL,CREATE,DELETE,DROP,GRANT,INSERT,SELECT,UPDATE,ZONECONFIG",
			privileges:   privilege.List{privilege.ALL, privilege.CREATE, privilege.DROP, privilege.GRANT, privilege.SELECT, privilege.INSERT, privilege.DELETE, privilege.UPDATE, privilege.ZONECONFIG},
		}}

	for num, testCase := range testCases {
		resultName := testCase.privileges.SortedString()
		if resultName != testCase.resultString {
			t.Errorf("#%d: expected:  %s , but got %s", num, testCase.resultString, resultName)
		}
	}

}
