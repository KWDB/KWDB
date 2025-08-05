// Copyright 2018 The Cockroach Authors.
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

package base_test

import (
	"context"
	"fmt"
	"net"
	"os"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

type addrs struct{ listen, adv, http, advhttp, sql, advsql string }

func (a *addrs) String() string {
	return fmt.Sprintf("--listen-addr=%s --advertise-addr=%s --http-addr=%s (http adv: %s) --sql-addr=%s (sql adv: %s)",
		a.listen, a.adv, a.http, a.advhttp, a.sql, a.advsql)
}

func TestValidateAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Prepare some reference strings that will be checked in the
	// test below.
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}
	localAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(localAddr, ":") {
		localAddr = "[" + localAddr + "]"
	}

	// We need to know what a port name resolution error look like,
	// because they may be different across systems/libcs.
	_, err = net.DefaultResolver.LookupPort(context.Background(), "tcp", "nonexistent")
	if err == nil {
		t.Fatal("expected port resolution failure, got no error")
	}
	portExpectedErr := err.Error()
	// For the host name resolution error we can reliably expect "no such host"
	// below, but before we test anything we need to ensure we indeed have
	// a reliably non-resolvable host name.
	_, err = net.DefaultResolver.LookupIPAddr(context.Background(), "nonexistent.example.com")
	if err == nil {
		t.Fatal("expected host resolution failure, got no error")
	}

	// The test cases.
	testData := []struct {
		in          addrs
		expectedErr string
		expected    addrs
	}{
		// Common case: no server flags, all defaults.
		{addrs{":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"}},

		// Another common case: --listen-addr=<somehost>
		{addrs{hostname + ":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":26257", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Another common case: --listen-addr=localhost
		{addrs{"localhost:26257", "", ":8080", "", ":5432", ""}, "",
			addrs{localAddr + ":26257", "localhost:26257", localAddr + ":8080", "localhost:8080", localAddr + ":5432", "localhost:5432"}},

		// Correct use: --listen-addr=<someaddr> --advertise-host=<somehost>
		{addrs{hostAddr + ":26257", hostname + ":", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":26257", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Explicit port number in advertise addr.
		{addrs{hostAddr + ":26257", hostname + ":12345", ":8080", "", ":5432", ""}, "",
			addrs{hostAddr + ":26257", hostname + ":12345", hostAddr + ":8080", hostname + ":8080", hostAddr + ":5432", hostname + ":5432"}},

		// Use a non-numeric port number.
		{addrs{":postgresql", "", ":http", "", ":postgresql", ""}, "",
			addrs{":5432", hostname + ":5432", ":80", hostname + ":80", ":5432", hostname + ":5432"}},

		// Make HTTP local only.
		{addrs{":26257", "", "localhost:8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", localAddr + ":8080", "localhost:8080", ":5432", hostname + ":5432"}},

		// Local server but public HTTP.
		{addrs{"localhost:26257", "", hostname + ":8080", "", ":5432", ""}, "",
			addrs{localAddr + ":26257", "localhost:26257", hostAddr + ":8080", hostname + ":8080", localAddr + ":5432", "localhost:5432"}},

		// Make SQL local only.
		{addrs{":26257", "", ":8080", "", "localhost:5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", localAddr + ":5432", "localhost:5432"}},

		// Not-unreasonable case: addresses set empty. Means using port 0.
		{addrs{"", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		// A colon means "all-addr, auto-port".
		{addrs{":", "", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", ":", "", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", ":", "", "", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", ":", ""}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},
		{addrs{"", "", "", "", "", ":"}, "",
			addrs{":0", hostname + ":0", ":0", hostname + ":0", ":0", hostname + ":0"}},

		// Advertise port 0 means reuse listen port. We don't
		// auto-allocate ports for advertised addresses.
		{addrs{":12345", ":0", "", "", ":5432", ""}, "",
			addrs{":12345", hostname + ":12345", ":0", hostname + ":0", ":5432", hostname + ":5432"}},
		{addrs{":12345", "", "", "", ":5432", ":0"}, "",
			addrs{":12345", hostname + ":12345", ":0", hostname + ":0", ":5432", hostname + ":5432"}},

		// Expected errors.

		// Missing port number.
		{addrs{"localhost", "", "", "", "", ""}, "invalid --listen-addr.*missing port in address", addrs{}},
		{addrs{":26257", "", "localhost", "", "", ""}, "invalid --http-addr.*missing port in address", addrs{}},
		// Invalid port number.
		{addrs{"localhost:-1231", "", "", "", "", ""}, "invalid port", addrs{}},
		{addrs{"localhost:nonexistent", "", "", "", "", ""}, portExpectedErr, addrs{}},
		// Invalid address.
		//{addrs{"nonexistent.example.com:26257", "", "", "", "", ""}, "no such host", addrs{}},
		{addrs{"333.333.333.333:26257", "", "", "", "", ""}, "no such host", addrs{}},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d/%s", i, test.in), func(t *testing.T) {
			cfg := base.Config{
				Addr:             test.in.listen,
				AdvertiseAddr:    test.in.adv,
				HTTPAddr:         test.in.http,
				SQLAddr:          test.in.sql,
				SQLAdvertiseAddr: test.in.advsql,
			}

			if err := cfg.ValidateAddrs(context.Background(), base.StartSingleNodeCmdName); err != nil {
				if !testutils.IsError(err, test.expectedErr) {
					t.Fatalf("expected error %q, got %v", test.expectedErr, err)
				}
				return
			}
			if test.expectedErr != "" {
				t.Fatalf("expected error %q, got success", test.expectedErr)
			}

			got := addrs{cfg.Addr, cfg.AdvertiseAddr, cfg.HTTPAddr, cfg.HTTPAdvertiseAddr, cfg.SQLAddr, cfg.SQLAdvertiseAddr}
			gotStr := got.String()
			expStr := test.expected.String()

			if gotStr != expStr {
				t.Fatalf("expected %q,\ngot %q", expStr, gotStr)
			}
		})
	}
}

func TestValidateBrpcAddrs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Prepare some reference strings that will be checked in the
	// test below.
	hostname, err := os.Hostname()
	if err != nil {
		t.Fatal(err)
	}
	hostAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, hostname)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(hostAddr, ":") {
		hostAddr = "[" + hostAddr + "]"
	}
	localAddr, err := base.LookupAddr(context.Background(), net.DefaultResolver, "localhost")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(localAddr, ":") {
		localAddr = "[" + localAddr + "]"
	}
	// For the host name resolution error we can reliably expect "no such host"
	// below, but before we test anything we need to ensure we indeed have
	// a reliably non-resolvable host name.
	_, err = net.DefaultResolver.LookupIPAddr(context.Background(), "nonexistent.example.com")
	if err == nil {
		t.Fatal("expected host resolution failure, got no error")
	}

	brpcExpectedErr := "--brpc-addr not specified"

	// The test cases.
	testData := []struct {
		in          addrs
		expectedErr string
		expected    addrs
		brpc        string
	}{
		//normal
		{addrs{":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"},
			"127.0.0.1:26257"},
		{addrs{":26257", "", ":8080", "", ":5432", ""}, "",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"},
			"0.0.0.0:26257"},
		// Expected errors.
		{addrs{":26257", "", ":8080", "", ":5432", ""}, brpcExpectedErr,
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"},
			base.DefaultBRPVAddr},
		{addrs{":26257", "", ":8080", "", ":5432", ""}, "no such host",
			addrs{":26257", hostname + ":26257", ":8080", hostname + ":8080", ":5432", hostname + ":5432"},
			"333.333.333.333:26257"},
	}

	for i, test := range testData {
		t.Run(fmt.Sprintf("%d/%s", i, test.in), func(t *testing.T) {
			cfg := base.Config{
				Addr:             test.in.listen,
				AdvertiseAddr:    test.in.adv,
				HTTPAddr:         test.in.http,
				SQLAddr:          test.in.sql,
				SQLAdvertiseAddr: test.in.advsql,
				BRPCAddr:         test.brpc,
			}

			if err := cfg.ValidateAddrs(context.Background(), base.StartCmdName); err != nil {
				if !testutils.IsError(err, test.expectedErr) {
					t.Fatalf("expected error %q, got %v", test.expectedErr, err)
				}
				return
			}
			if test.expectedErr != "" {
				t.Fatalf("expected error %q, got success", test.expectedErr)
			}

			got := addrs{cfg.Addr, cfg.AdvertiseAddr, cfg.HTTPAddr, cfg.HTTPAdvertiseAddr, cfg.SQLAddr, cfg.SQLAdvertiseAddr}
			gotStr := got.String()
			expStr := test.expected.String()

			if gotStr != expStr {
				t.Fatalf("expected %q,\ngot %q", expStr, gotStr)
			}
		})
	}
}
