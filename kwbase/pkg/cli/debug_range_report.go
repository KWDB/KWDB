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
	"fmt"
	"os"
	"path/filepath"

	"gitee.com/kwbasedb/kwbase/pkg/util/envutil"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

var debugRangeReportOpts struct {
	consoleURL string
	username   string
	password   string
	insecure   bool
}

var debugRangeReportCmd = &cobra.Command{
	Use:   "range-report <range-id>",
	Short: "fetch Console range report data and write HTML to the current directory",
	Long: `
Connect to a KaiwuDB Console URL, authenticate with the given credentials,
fetch range status / allocator / range log data, and write a static HTML report
to the current working directory as reports-range-<id>.html.

The console URL may include a hash route, e.g.:
  https://10.110.105.71:8007/#/reports/range/1

Example (secure Console):
  kwbase debug range-report 2 \
    --url=https://10.110.105.71:8007 \
    --user=root1 \
    --password='secret'

Example (insecure Console, no login):
  kwbase debug range-report 1 \
    --url=http://10.110.105.71:8080 \
    --insecure
`,
	Args: cobra.ExactArgs(1),
	RunE: runDebugRangeReport,
}

func runDebugRangeReport(cmd *cobra.Command, args []string) error {
	rangeID, err := parsePositiveInt(args[0])
	if err != nil {
		return errors.Wrap(err, "invalid range id")
	}
	if debugRangeReportOpts.consoleURL == "" {
		return errors.New("--url is required")
	}

	password := debugRangeReportOpts.password
	if password == "" {
		password, _ = envutil.EnvString("KWBASE_PASSWORD", 1)
	}

	if !debugRangeReportOpts.insecure {
		if debugRangeReportOpts.username == "" {
			return errors.New("--user is required (or pass --insecure for clusters without Console login)")
		}
		if password == "" {
			return errors.New("--password is required (or set KAIWUDB_PASSWORD, or pass --insecure)")
		}
	} else if debugRangeReportOpts.username != "" || password != "" {
		return errors.New("--insecure cannot be combined with --user or --password")
	}

	baseURL, err := normalizeConsoleURL(debugRangeReportOpts.consoleURL, debugRangeReportOpts.insecure)
	if err != nil {
		return err
	}
	client, err := newConsoleClient(baseURL)
	if err != nil {
		return err
	}
	if !debugRangeReportOpts.insecure {
		if err := client.login(debugRangeReportOpts.username, password); err != nil {
			return err
		}
	}
	data, err := client.fetchRangeReport(rangeID)
	if err != nil {
		return err
	}
	if len(collectRangeInfos(data)) == 0 {
		return errors.Errorf("no range info returned for r%d", rangeID)
	}

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}
	sourceURL := fmt.Sprintf("%s/#/reports/range/%d", baseURL, rangeID)
	outPath := filepath.Join(cwd, fmt.Sprintf("reports-range-%d.html", rangeID))
	htmlText := generateRangeReportHTML(data, sourceURL)
	if err := os.WriteFile(outPath, []byte(htmlText), 0644); err != nil {
		return err
	}
	fmt.Fprintf(os.Stdout, "range report written: %s\n", outPath)
	return nil
}

func init() {
	f := debugRangeReportCmd.Flags()
	f.StringVar(&debugRangeReportOpts.consoleURL, "url", "", "KaiwuDB Console base URL (required)")
	f.StringVar(&debugRangeReportOpts.username, "user", "", "Console login username (required unless --insecure)")
	f.StringVar(&debugRangeReportOpts.password, "password", "", "Console login password (or KAIWUDB_PASSWORD; required unless --insecure)")
	f.BoolVar(&debugRangeReportOpts.insecure, "insecure", false, "connect without Console login (for insecure clusters)")
}
