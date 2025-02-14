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

package grpcutil

import (
	"errors"
	"regexp"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/petermattis/goid"
)

func TestShouldPrint(t *testing.T) {
	const duration = 100 * time.Millisecond

	formatRe, err := regexp.Compile("^foo")
	if err != nil {
		t.Fatal(err)
	}
	argRe, err := regexp.Compile("[a-z][0-9]")
	if err != nil {
		t.Fatal(err)
	}

	testutils.RunTrueAndFalse(t, "formatMatch", func(t *testing.T, formatMatch bool) {
		testutils.RunTrueAndFalse(t, "argsMatch", func(t *testing.T, argsMatch bool) {
			format := "bar=%s"
			if formatMatch {
				format = "foobar=%s"
			}
			args := []interface{}{errors.New("baz")}
			if argsMatch {
				args = []interface{}{errors.New("a1")}
			}
			curriedShouldPrint := func() bool {
				return shouldPrint(formatRe, argRe, duration, format, args...)
			}

			// First call should always print.
			if !curriedShouldPrint() {
				t.Error("expected first call to print")
			}

			// Call from another goroutine should always print.
			done := make(chan bool)
			go func() {
				done <- curriedShouldPrint()
			}()
			if !<-done {
				t.Error("expected other-goroutine call to print")
			}

			// Should print if non-matching.
			alwaysPrint := !(formatMatch && argsMatch)

			if alwaysPrint {
				if !curriedShouldPrint() {
					t.Error("expected second call to print")
				}
			} else {
				if curriedShouldPrint() {
					t.Error("unexpected second call to print")
				}
			}

			if !alwaysPrint {
				// Force printing by pretending the previous output was well in the
				// past.
				spamMu.Lock()
				spamMu.gids[goid.Get()] = timeutil.Now().Add(-time.Hour)
				spamMu.Unlock()
			}
			if !curriedShouldPrint() {
				t.Error("expected third call to print")
			}
		})
	})
}
