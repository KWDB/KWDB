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

//go:build nightly
// +build nightly

package tree_test

import (
	"path/filepath"
	"testing"

	_ "gitee.com/kwbasedb/kwbase/pkg/sql/sem/builtins"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

// TestPrettyData reads in a single SQL statement from a file, formats it at
// all line lengths, and compares that output to a known-good output file. It
// is most useful when changing or implementing the doc interface for a node,
// and should be used to compare and verify the changed output.
func TestPrettyData(t *testing.T) {
	defer leaktest.AfterTest(t)()
	matches, err := filepath.Glob(filepath.Join("testdata", "pretty", "*.sql"))
	if err != nil {
		t.Fatal(err)
	}
	cfg := testPrettyCfg
	cfg.Align = tree.PrettyNoAlign
	t.Run("ref", func(t *testing.T) {
		runTestPrettyData(t, "ref", cfg, matches, false /*short*/)
	})
	cfg.Align = tree.PrettyAlignAndDeindent
	t.Run("align-deindent", func(t *testing.T) {
		runTestPrettyData(t, "align-deindent", cfg, matches, false /*short*/)
	})
	cfg.Align = tree.PrettyAlignOnly
	t.Run("align-only", func(t *testing.T) {
		runTestPrettyData(t, "align-only", cfg, matches, false /*short*/)
	})
}
