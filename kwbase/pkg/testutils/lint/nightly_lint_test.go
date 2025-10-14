// Copyright 2016 The Cockroach Authors.
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

//go:build lint && nightly
// +build lint,nightly

package lint

import (
	"bytes"
	"os"
	"os/exec"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/cmd/urlcheck/lib/urlcheck"
	sqlparser "gitee.com/kwbasedb/kwbase/pkg/sql/parser"
)

func TestNightlyLint(t *testing.T) {
	_, pkgSpecified := os.LookupEnv("PKG")

	// TestHelpURLs checks that all help texts have a valid documentation URL.
	t.Run("TestHelpURLs", func(t *testing.T) {
		if testing.Short() {
			t.Skip("short flag")
		}
		if pkgSpecified {
			t.Skip("PKG specified")
		}

		t.Parallel()
		var buf bytes.Buffer
		for key, body := range sqlparser.HelpMessages {
			msg := sqlparser.HelpMessage{Command: key, HelpMessageBody: body}
			buf.WriteString(msg.String())
		}
		cmd := exec.Command("grep", "-nE", urlcheck.URLRE)
		cmd.Stdin = &buf
		if err := urlcheck.CheckURLsFromGrepOutput(cmd); err != nil {
			t.Fatal(err)
		}
	})
}
