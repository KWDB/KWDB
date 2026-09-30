// Copyright 2026 Shanghai Yunxi Technology Co, Ltd.
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
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/privilege"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestHasColumnPrivilege(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		name       string
		privileges privilege.List
		priv       privilege.Kind
		want       bool
	}{
		{name: "no privileges", priv: privilege.SELECT, want: false},
		{name: "matching privilege", privileges: privilege.List{privilege.SELECT}, priv: privilege.SELECT, want: true},
		{name: "different privilege", privileges: privilege.List{privilege.SELECT}, priv: privilege.UPDATE, want: false},
		{name: "all includes select", privileges: privilege.List{privilege.ALL}, priv: privilege.SELECT, want: true},
		{name: "all includes insert", privileges: privilege.List{privilege.ALL}, priv: privilege.INSERT, want: true},
		{name: "all includes update", privileges: privilege.List{privilege.ALL}, priv: privilege.UPDATE, want: true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := hasColumnPrivilege(tc.privileges.ToBitField(), tc.priv)
			if got != tc.want {
				t.Errorf("hasColumnPrivilege(%v, %s) = %t, want %t", tc.privileges, tc.priv, got, tc.want)
			}
		})
	}
}

func TestCanUserSeePrivilegeGrantee(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const currentUser = "test_user"
	testCases := []struct {
		name     string
		isAdmin  bool
		grantee  string
		memberOf map[string]bool
		want     bool
	}{
		{name: "administrator sees arbitrary grantee", isAdmin: true, grantee: "unrelated_role", want: true},
		{name: "current user", grantee: currentUser, want: true},
		{name: "public role", grantee: sqlbase.PublicRole, want: true},
		{name: "direct role", grantee: "direct_role", memberOf: map[string]bool{"direct_role": true}, want: true},
		{name: "indirect role without admin option", grantee: "parent_role", memberOf: map[string]bool{"parent_role": false}, want: true},
		{name: "unrelated role", grantee: "unrelated_role", memberOf: map[string]bool{"direct_role": true}, want: false},
		{name: "nil membership", grantee: "unrelated_role", want: false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := canUserSeePrivilegeGrantee(tc.isAdmin, currentUser, tc.grantee, tc.memberOf)
			if got != tc.want {
				t.Errorf("canUserSeePrivilegeGrantee(%t, %q, %q, %v) = %t, want %t",
					tc.isAdmin, currentUser, tc.grantee, tc.memberOf, got, tc.want)
			}
		})
	}
}
