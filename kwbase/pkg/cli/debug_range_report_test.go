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

package cli

import (
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/spf13/pflag"
)

const rangeReportTestdata = "testdata/range_report"

type mockConsoleServer struct {
	loginRequired bool
	loginCalled   bool
	loginUsername string
	loginPassword string
	rangeJSON     string
}

func newMockConsoleServer(
	t *testing.T, loginRequired bool, rangeFixture string,
) (*httptest.Server, *mockConsoleServer) {
	t.Helper()

	rangeJSON, err := ioutil.ReadFile(filepath.Join(rangeReportTestdata, rangeFixture))
	if err != nil {
		t.Fatal(err)
	}
	allocatorJSON, err := ioutil.ReadFile(filepath.Join(rangeReportTestdata, "allocator_minimal.json"))
	if err != nil {
		t.Fatal(err)
	}
	rangelogJSON, err := ioutil.ReadFile(filepath.Join(rangeReportTestdata, "rangelog_minimal.json"))
	if err != nil {
		t.Fatal(err)
	}

	mock := &mockConsoleServer{
		loginRequired: loginRequired,
		rangeJSON:     string(rangeJSON),
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/login":
			mock.loginCalled = true
			if !mock.loginRequired {
				http.Error(w, "login disabled", http.StatusBadRequest)
				return
			}
			body, err := ioutil.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			var req serverpb.UserLoginRequest
			if err := json.Unmarshal(body, &req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			mock.loginUsername = req.Username
			mock.loginPassword = req.Password
			http.SetCookie(w, &http.Cookie{Name: "session", Value: "test-session"})
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte("{}"))
		case r.Method == http.MethodGet:
			if mock.loginRequired {
				if cookie, err := r.Cookie("session"); err != nil || cookie.Value == "" {
					http.Error(w, "unauthorized", http.StatusUnauthorized)
					return
				}
			}
			w.Header().Set("Content-Type", "application/json")
			switch r.URL.Path {
			case "/_status/range/1":
				_, _ = w.Write([]byte(mock.rangeJSON))
			case "/_status/allocator/range/1":
				_, _ = w.Write(allocatorJSON)
			case "/_admin/v1/rangelog/1":
				_, _ = w.Write(rangelogJSON)
			default:
				http.NotFound(w, r)
			}
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}))
	return srv, mock
}

func withRangeReportWorkDir(t *testing.T, fn func(dir string)) {
	t.Helper()
	dir, cleanup := testutils.TempDir(t)
	defer cleanup()
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chdir(dir); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := os.Chdir(cwd); err != nil {
			t.Fatal(err)
		}
	}()
	fn(dir)
}

func runDebugRangeReportCmd(args ...string) error {
	TestingReset()
	debugRangeReportCmd.Flags().Visit(func(f *pflag.Flag) {
		if err := f.Value.Set(f.DefValue); err != nil {
			panic(err)
		}
	})
	var positional []string
	var flags []string
	for _, arg := range args {
		if strings.HasPrefix(arg, "-") {
			flags = append(flags, arg)
		} else {
			positional = append(positional, arg)
		}
	}
	if err := debugRangeReportCmd.ParseFlags(flags); err != nil {
		return err
	}
	return debugRangeReportCmd.RunE(debugRangeReportCmd, positional)
}

func TestDebugRangeReportScenarios(t *testing.T) {
	defer leaktest.AfterTest(t)()

	scenarios := []struct {
		name          string
		loginRequired bool
		rangeFixture  string
		args          func(url string) []string
		check         func(t *testing.T, mock *mockConsoleServer, dir, url string)
	}{
		{
			name:          "secure",
			loginRequired: true,
			rangeFixture:  "range_minimal.json",
			args: func(url string) []string {
				return []string{
					"1",
					"--url=" + url,
					"--user=testuser",
					"--password=secret",
				}
			},
			check: func(t *testing.T, mock *mockConsoleServer, dir, url string) {
				t.Helper()
				if !mock.loginCalled {
					t.Fatal("expected Console login in secure mode")
				}
				if mock.loginUsername != "testuser" {
					t.Fatalf("expected login username testuser, got %q", mock.loginUsername)
				}
				outPath := filepath.Join(dir, "reports-range-1.html")
				html, err := ioutil.ReadFile(outPath)
				if err != nil {
					t.Fatalf("expected output file: %v", err)
				}
				text := string(html)
				if !strings.Contains(text, "Range Report r1") {
					t.Fatalf("expected report title in HTML, got: %s", text[:min(200, len(text))])
				}
				if !strings.Contains(text, url+"/#/reports/range/1") {
					t.Fatal("expected source URL in HTML")
				}
			},
		},
		{
			name:          "insecure",
			loginRequired: false,
			rangeFixture:  "range_minimal.json",
			args: func(url string) []string {
				return []string{
					"1",
					"--url=" + url,
					"--insecure",
				}
			},
			check: func(t *testing.T, mock *mockConsoleServer, dir, url string) {
				t.Helper()
				if mock.loginCalled {
					t.Fatal("did not expect Console login in insecure mode")
				}
				outPath := filepath.Join(dir, "reports-range-1.html")
				if _, err := os.Stat(outPath); err != nil {
					t.Fatalf("expected output file: %v", err)
				}
			},
		},
		{
			name:          "compatibility",
			loginRequired: false,
			rangeFixture:  "range_with_unknown_fields.json",
			args: func(url string) []string {
				return []string{
					"1",
					"--url=" + url,
					"--insecure",
				}
			},
			check: func(t *testing.T, mock *mockConsoleServer, dir, url string) {
				t.Helper()
				if mock.loginCalled {
					t.Fatal("did not expect Console login")
				}
				outPath := filepath.Join(dir, "reports-range-1.html")
				html, err := ioutil.ReadFile(outPath)
				if err != nil {
					t.Fatalf("expected output file: %v", err)
				}
				if !strings.Contains(string(html), "Range Report r1") {
					t.Fatal("expected HTML report after unmarshaling unknown proto fields")
				}
			},
		},
	}

	for _, tc := range scenarios {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			srv, mock := newMockConsoleServer(t, tc.loginRequired, tc.rangeFixture)
			defer srv.Close()

			withRangeReportWorkDir(t, func(dir string) {
				if err := runDebugRangeReportCmd(tc.args(srv.URL)...); err != nil {
					t.Fatalf("%s range-report failed: %v", tc.name, err)
				}
				tc.check(t, mock, dir, srv.URL)
			})
		})
	}
}

func TestDebugRangeReportSecureRequiresCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv, _ := newMockConsoleServer(t, true /* loginRequired */, "range_minimal.json")
	defer srv.Close()

	err := runDebugRangeReportCmd("1", "--url="+srv.URL)
	if !testutils.IsError(err, "--user is required") {
		t.Fatalf("expected missing user error, got: %v", err)
	}
}

func TestDebugRangeReportInsecureRejectsCredentials(t *testing.T) {
	defer leaktest.AfterTest(t)()

	srv, _ := newMockConsoleServer(t, false /* loginRequired */, "range_minimal.json")
	defer srv.Close()

	err := runDebugRangeReportCmd(
		"1",
		"--url="+srv.URL,
		"--insecure",
		"--user=root",
	)
	if !testutils.IsError(err, "--insecure cannot be combined") {
		t.Fatalf("expected credential conflict error, got: %v", err)
	}
}

func TestConsoleClientGetJSONUnknownFields(t *testing.T) {
	defer leaktest.AfterTest(t)()

	rangeJSON, err := ioutil.ReadFile(filepath.Join(rangeReportTestdata, "range_with_unknown_fields.json"))
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(rangeJSON)
	}))
	defer srv.Close()

	client, err := newConsoleClient(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	var resp serverpb.RangeResponse
	if err := client.getJSON("/range", &resp); err != nil {
		t.Fatalf("getJSON should ignore unknown fields: %v", err)
	}
	if len(resp.ResponsesByNodeID) != 1 {
		t.Fatalf("expected one node response, got %d", len(resp.ResponsesByNodeID))
	}
	nodeResp := resp.ResponsesByNodeID[1]
	if len(nodeResp.Infos) != 1 {
		t.Fatalf("expected one range info, got %d", len(nodeResp.Infos))
	}
	desc := nodeResp.Infos[0].State.Desc
	if desc == nil {
		t.Fatal("expected range descriptor to be parsed")
	}
	if desc.RangeID != 1 {
		t.Fatalf("unexpected parsed range id: %d", desc.RangeID)
	}
}

func TestNormalizeConsoleURLInsecureDefaultScheme(t *testing.T) {
	defer leaktest.AfterTest(t)()

	got, err := normalizeConsoleURL("http://10.0.0.1:8080", true /* insecure */)
	if err != nil {
		t.Fatal(err)
	}
	if got != "http://10.0.0.1:8080" {
		t.Fatalf("expected http URL preserved, got %q", got)
	}

	got, err = normalizeConsoleURL("https://10.0.0.1:8007/#/reports/range/1", false)
	if err != nil {
		t.Fatal(err)
	}
	if got != "https://10.0.0.1:8007" {
		t.Fatalf("expected hash stripped from URL, got %q", got)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
