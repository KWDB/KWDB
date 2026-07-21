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

package ddl_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestUserName(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []struct {
		username   string
		normalized string
		err        string
		sqlstate   string
	}{
		{"Abc123", "abc123", "", ""},
		{"0123121132", "0123121132", "", ""},
		{"HeLlO", "hello", "", ""},
		{"Ομηρος", "ομηρος", "", ""},
		{"_HeLlO", "_hello", "", ""},
		{"a-BC-d", "a-bc-d", "", ""},
		{"A.Bcd", "a.bcd", "", ""},
		{"WWW.BIGSITE.NET", "www.bigsite.net", "", ""},
		{"", "", `username "" invalid`, pgcode.InvalidName},
		{"-ABC", "", `username "-abc" invalid`, pgcode.InvalidName},
		{".ABC", "", `username ".abc" invalid`, pgcode.InvalidName},
		{"*.wildcard", "", `username "\*.wildcard" invalid`, pgcode.InvalidName},
		{"foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof", "", `username "foofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoofoof" is too long`, pgcode.NameTooLong},
		{"M", "m", "", ""},
		{".", "", `username "." invalid`, pgcode.InvalidName},
	}

	for _, tc := range testCases {
		normalized, err := sqlutil.NormalizeAndValidateUsername(tc.username)
		if !testutils.IsError(err, tc.err) {
			t.Errorf("%q: expected %q, got %v", tc.username, tc.err, err)
			continue
		}
		if err != nil {
			if pgcode := pgerror.GetPGCode(err); pgcode != tc.sqlstate {
				t.Errorf("%q: expected SQLSTATE %s, got %s", tc.username, tc.sqlstate, pgcode)
				continue
			}
		}
		if normalized != tc.normalized {
			t.Errorf("%q: expected %q, got %q", tc.username, tc.normalized, normalized)
		}
	}
}
