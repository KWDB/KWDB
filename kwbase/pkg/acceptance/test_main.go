// Copyright 2016 The Cockroach Authors.
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

// +build !acceptance

// Our Docker-based acceptance tests are comparatively slow to run, so we use
// the above build tag to separate invocations of `go test` which are intended
// to run the acceptance tests from those which are not. The corollary file to
// this one is main_test.go. Acceptance tests against remote clusters (which
// aren't run unless `-remote` is passed explicitly) are run through this file
// via the standard facilities (i.e. `make test` and without `acceptance` build
// tag).

package acceptance

import (
	"fmt"
	"os"
	"testing"
)

// MainTest is an exported implementation of TestMain for use by other
// packages.
func MainTest(m *testing.M) {
	fmt.Fprintln(os.Stderr, "not running with `acceptance` build tag; skipping")
}
