// Copyright 2017 The Cockroach Authors.
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

package log_test

import (
	"context"
	"regexp"
	"runtime"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	raven "github.com/getsentry/raven-go"
	"github.com/kr/pretty"
)

// Renumber lines so they're stable no matter what changes above. (We
// could make the regexes accept any string of digits, but we also
// want to make sure that the correct line numbers get captured).
//
//line crash_reporting_packet_test.go:1000

// interceptingTransport is an implementation of raven.Transport that delegates
// calls to the Send method to the send function contained within.
type interceptingTransport struct {
	send func(url, authHeader string, packet *raven.Packet)
}

// Send implements the raven.Transport interface.
func (it interceptingTransport) Send(url, authHeader string, packet *raven.Packet) error {
	it.send(url, authHeader, packet)
	return nil
}

func TestCrashReportingPacket(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var packets []*raven.Packet

	st := cluster.MakeTestingClusterSettings()
	// Enable all crash-reporting settings.
	log.DiagnosticsReportingEnabled.Override(&st.SV, true)

	defer log.TestingSetCrashReportingURL("https://ignored:ignored@ignored/ignored")()

	// Install a Transport that locally records packets rather than sending them
	// to Sentry over HTTP.
	defer func(transport raven.Transport) {
		raven.DefaultClient.Transport = transport
	}(raven.DefaultClient.Transport)
	raven.DefaultClient.Transport = interceptingTransport{
		send: func(_, _ string, packet *raven.Packet) {
			packets = append(packets, packet)
		},
	}

	expectPanic := func(name string) {
		if r := recover(); r == nil {
			t.Fatalf("'%s' failed to panic", name)
		}
	}

	log.SetupCrashReporter(ctx, "test")

	const (
		panicPre  = "boom"
		panicPost = "baam"
	)

	func() {
		defer expectPanic("before server start")
		defer log.RecoverAndReportPanic(ctx, &st.SV)
		panic(log.Safe(panicPre))
	}()

	func() {
		defer expectPanic("after server start")
		defer log.RecoverAndReportPanic(ctx, &st.SV)
		s, _, _ := serverutils.StartServer(t, base.TestServerArgs{})
		s.Stopper().Stop(ctx)
		panic(log.Safe(panicPost))
	}()

	const prefix = "crash_reporting_packet_test.go:"

	expectations := []struct {
		serverID *regexp.Regexp
		tagCount int
		message  string
	}{
		{regexp.MustCompile(`^$`), 7, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1053"
			}
			message += ": " + panicPre
			return message
		}()},
		{regexp.MustCompile(`^[a-z0-9]{8}-1$`), 11, func() string {
			message := prefix
			// gccgo stack traces are different in the presence of function literals.
			if runtime.Compiler == "gccgo" {
				message += "[0-9]+" // TODO(bdarnell): verify on gccgo
			} else {
				message += "1061"
			}
			message += ": " + panicPost
			return message
		}()},
	}

	if e, a := len(expectations), len(packets); e != a {
		t.Fatalf("expected %d packets, but got %d", e, a)
	}

	for _, exp := range expectations {
		p := packets[0]
		packets = packets[1:]
		t.Run("", func(t *testing.T) {
			if !log.ReportSensitiveDetails {
				e, a := "<redacted>", p.ServerName
				if e != a {
					t.Errorf("expected ServerName to be '<redacted>', but got '%s'", a)
				}
			}

			tags := make(map[string]string, len(p.Tags))
			for _, tag := range p.Tags {
				tags[tag.Key] = tag.Value
			}

			if e, a := exp.tagCount, len(tags); e != a {
				t.Errorf("expected %d tags, but got %d", e, a)
			}

			if serverID := tags["server_id"]; !exp.serverID.MatchString(serverID) {
				t.Errorf("expected server_id '%s' to match %s", serverID, exp.serverID)
			}

			if msg := p.Message; msg != exp.message {
				t.Errorf("expected %s, got %s", exp.message, msg)
			}
		})
	}
}

func TestInternalErrorReporting(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var packets []*raven.Packet

	st := cluster.MakeTestingClusterSettings()
	// Enable all crash-reporting settings.
	log.DiagnosticsReportingEnabled.Override(&st.SV, true)

	defer log.TestingSetCrashReportingURL("https://ignored:ignored@ignored/ignored")()

	// Install a Transport that locally records packets rather than sending them
	// to Sentry over HTTP.
	defer func(transport raven.Transport) {
		raven.DefaultClient.Transport = transport
	}(raven.DefaultClient.Transport)
	raven.DefaultClient.Transport = interceptingTransport{
		send: func(_, _ string, packet *raven.Packet) {
			packets = append(packets, packet)
		},
	}

	log.SetupCrashReporter(ctx, "test")

	s, sqlDB, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	if _, err := sqlDB.Exec("SELECT kwdb_internal.force_assertion_error('woo')"); !testutils.IsError(err, "internal error") {
		t.Fatalf("expected internal error, got %v", err)
	}

	// TODO(knz): With Ben's changes to report errors in pgwire as well
	// as the executor, we get twice the sentry reports. This needs to
	// be fixed, then the test below can check for len(packets) == 1.
	if len(packets) < 1 {
		t.Fatalf("expected at least 1 reporting packet")
	}

	p := packets[0]

	t.Logf("%# v", pretty.Formatter(p))

	// Work around the linter.
	// We don't care about "slow matchstring" below because there are only few iterations.
	m := func(a, b string) (bool, error) { return regexp.MatchString(a, b) }

	if ok, _ := m(`builtins.go:\d+:.*`, p.Message); !ok {
		t.Errorf("expected assertion error location in message, got:\n%s", p.Message)
	}

	if ok, _ := m(`.*sem/builtins`, p.Culprit); !ok {
		t.Errorf("expected builtins in culprit, got %q", p.Culprit)
	}

	expectedExtra := []struct {
		key   string
		reVal string
	}{
		{"1: details", "%s\n.*<string>"},
		{"2: stacktrace", `.*builtins.go.*\n.*eval.go.*Eval`},
		{"3: details", `%s\(\).*\n.*force_assertion_error`},
		{"4: stacktrace", ".*eval.go.*Eval"},
	}
	for _, ex := range expectedExtra {
		data, ok := p.Extra[ex.key]
		if !ok {
			t.Errorf("expected detail %q in extras, was not found", ex.key)
			continue
		}
		sdata, ok := data.(string)
		if !ok {
			t.Errorf("expected detail %q of type string, found %T (%q)", ex.key, data, data)
			continue
		}
		if ok, _ := m(ex.reVal, sdata); !ok {
			t.Errorf("expected detail %q to match:\n%s\ngot:\n%s", ex.key, ex.reVal, sdata)
		}
	}

	if len(p.Interfaces) < 2 {
		t.Fatalf("expected 2 reportable objects, got %d", len(p.Interfaces))
	}
	msgCount := 0
	excCount := 0
	otherCount := 0
	for _, iv := range p.Interfaces {
		switch part := iv.(type) {
		case *raven.Message:
			if ok, _ := m(
				`\*errors.errorString\n`+
					`\*safedetails.withSafeDetails: %s \(1\)\n`+
					`builtins.go:\d+: \*withstack.withStack \(2\)\n`+
					`\*assert.withAssertionFailure\n`+
					`\*errutil.withMessage\n`+
					`\*safedetails.withSafeDetails: %s\(\) \(3\)\n`+
					`eval.go:\d+: \*withstack.withStack \(4\)\n`+
					`\*telemetrykeys.withTelemetry: kwdb_internal.force_assertion_error\(\) \(5\)\n`+
					`\(check the extra data payloads\)`,
				part.Message,
			); !ok {
				t.Errorf("expected stack of message, got:\n%s", part.Message)
			}
			msgCount++
		case *raven.Exception:
			if ok, _ := m(
				`builtins.go:\d+:.*: %s`,
				part.Value,
			); !ok {
				t.Errorf("expected builtin in exception head, got:\n%s", part.Value)
			}
			if len(part.Stacktrace.Frames) < 2 {
				t.Errorf("expected two entries in stack trace, got %d", len(part.Stacktrace.Frames))
			} else {
				fr := part.Stacktrace.Frames
				// Raven stack traces are inverted.
				if !strings.HasSuffix(fr[len(fr)-1].Filename, "builtins.go") ||
					!strings.HasSuffix(fr[len(fr)-2].Filename, "eval.go") {
					t.Errorf("expected builtins and eval at top of stack trace, got:\n%+v\n%+v",
						fr[len(fr)-1],
						fr[len(fr)-2],
					)
				}
			}
			excCount++
		default:
			otherCount++
		}
	}
	if msgCount != 1 || excCount != 1 || otherCount != 0 {
		t.Fatalf("unexpected objects, got %d %d %d, expected 1 1 0", msgCount, excCount, otherCount)
	}
}
