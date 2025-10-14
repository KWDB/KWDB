// Copyright 2019 The Cockroach Authors.
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

// "make test" would normally test this file, but it should only be tested
// within docker compose. We also can't use just "gss" here because that
// tag is reserved for the toplevel Makefile's linux-gnu build.

//go:build gss_compose
// +build gss_compose

package gss

import (
	gosql "database/sql"
	"fmt"
	"os/exec"
	"regexp"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq"
)

func TestGSS(t *testing.T) {
	connector, err := pq.NewConnector("user=root sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	db := gosql.OpenDB(connector)
	defer db.Close()

	tests := []struct {
		// The hba.conf file/setting.
		conf string
		user string
		// Error message of hba conf
		hbaErr string
		// Error message of gss login.
		gssErr string
	}{
		{
			conf:   `host all all all gss include_realm=0 nope=1`,
			hbaErr: `unsupported option`,
		},
		{
			conf:   `host all all all gss include_realm=1`,
			hbaErr: `include_realm must be set to 0`,
		},
		{
			conf:   `host all all all gss`,
			hbaErr: `missing "include_realm=0"`,
		},
		{
			conf:   `host all all all gss include_realm=0`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all tester all gss include_realm=0`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all nope all gss include_realm=0`,
			user:   "tester",
			gssErr: "no server.host_based_authentication.configuration entry",
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=MY.EX`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX`,
			user:   "tester",
			gssErr: `GSSAPI realm \(MY.EX\) didn't match any configured realm`,
		},
		{
			conf:   `host all all all gss include_realm=0 krb_realm=NOPE.EX krb_realm=MY.EX`,
			user:   "tester",
			gssErr: `GSS authentication requires an enterprise license`,
		},
	}
	for i, tc := range tests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			if _, err := db.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, tc.conf); !IsError(err, tc.hbaErr) {
				t.Fatalf("expected err %v, got %v", tc.hbaErr, err)
			}
			if tc.hbaErr != "" {
				return
			}
			if _, err := db.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'`, tc.user)); err != nil {
				t.Fatal(err)
			}
			out, err := exec.Command("psql", "-c", "SELECT 1", "-U", tc.user).CombinedOutput()
			err = errors.Wrap(err, strings.TrimSpace(string(out)))
			if !IsError(err, tc.gssErr) {
				t.Errorf("expected err %v, got %v", tc.gssErr, err)
			}
		})
	}
}

func TestGSSFileDescriptorCount(t *testing.T) {
	// When the docker-compose.yml added a ulimit for the kwbase
	// container the open file count would just stop there, it wouldn't
	// cause kwbase to panic or error like I had hoped since it would
	// allow a test to assert that multiple gss connections didn't leak
	// file descriptors. Another possibility would be to have something
	// track the open file count in the kwbase container, but that seems
	// brittle and probably not worth the effort. However this test is
	// useful when doing manual tracking of file descriptor count.
	t.Skip("skip")

	rootConnector, err := pq.NewConnector("user=root sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	rootDB := gosql.OpenDB(rootConnector)
	defer rootDB.Close()

	if _, err := rootDB.Exec(`SET CLUSTER SETTING server.host_based_authentication.configuration = $1`, "host all all all gss include_realm=0"); err != nil {
		t.Fatal(err)
	}
	const user = "tester"
	if _, err := rootDB.Exec(fmt.Sprintf(`CREATE USER IF NOT EXISTS '%s'`, user)); err != nil {
		t.Fatal(err)
	}

	start := timeutil.Now()
	for i := 0; i < 1000; i++ {
		fmt.Println(i, timeutil.Since(start))
		out, err := exec.Command("psql", "-c", "SELECT 1", "-U", user).CombinedOutput()
		if IsError(err, "GSS authentication requires an enterprise license") {
			t.Log(string(out))
			t.Fatal(err)
		}
	}
}

func IsError(err error, re string) bool {
	if err == nil && re == "" {
		return true
	}
	if err == nil || re == "" {
		return false
	}
	matched, merr := regexp.MatchString(re, err.Error())
	if merr != nil {
		return false
	}
	return matched
}
