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

package cli

import (
	"archive/zip"
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/rpc"
	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/server"
	"gitee.com/kwbasedb/kwbase/pkg/server/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/pmezard/go-difflib/difflib"
	"github.com/stretchr/testify/assert"
)

// TestZipContainsAllInternalTables verifies that we don't add new internal tables
// without also taking them into account in a `debug zip`. If this test fails,
// add your table to either of the []string slices referenced in the test (which
// are used by `debug zip`) or add it as an exception after having verified that
// it indeed should not be collected (this is rare).
// NB: if you're adding a new one, you'll also have to update TestZip.
func TestZipContainsAllInternalTables(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer s.Stopper().Stop(context.Background())

	rows, err := db.Query(`
SELECT concat('kwdb_internal.', table_name) as name FROM [ SHOW TABLES FROM kwdb_internal ] WHERE
    table_name NOT IN (
-- whitelisted tables that don't need to be in debug zip
'backward_dependencies',
'builtin_functions',
'create_statements',
'forward_dependencies',
'index_columns',
'table_columns',
'table_indexes',
'ranges',
'ranges_no_leases',
'predefined_comments',
'session_trace',
'session_variables',
'tables',
'kwdb_user_pre_computing',
'kwdb_tse_info'
)
ORDER BY name ASC`)
	assert.NoError(t, err)

	var tables []string
	for rows.Next() {
		var table string
		assert.NoError(t, rows.Scan(&table))
		tables = append(tables, table)
	}
	tables = append(
		tables,
		"system.jobs",
		"system.descriptor",
		"system.namespace",
		"system.namespace2",
	)
	sort.Strings(tables)

	var exp []string
	exp = append(exp, debugZipTablesPerNode...)
	exp = append(exp, debugZipTablesPerCluster...)
	sort.Strings(exp)

	assert.Equal(t, exp, tables)
}

// This test the operation of zip over secure clusters.
func TestZip(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := newCLITest(cliTestParams{
		storeSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.cleanup()

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	datadriven.RunTest(t, "testdata/zip/testzip", func(t *testing.T, td *datadriven.TestData) string {
		return out
	})
}

func TestZipSpecialNames(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	c := newCLITest(cliTestParams{
		storeSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.cleanup()

	c.RunWithArgs([]string{"sql", "-e", `
create database "a:b";
create database "a-b";
create database "a-b-1";
create database "SYSTEM";
create table "SYSTEM.JOBS"(x int);
create database "../system";
create table defaultdb."a:b"(x int);
create table defaultdb."a-b"(x int);
create table defaultdb."pg_catalog.pg_class"(x int);
create table defaultdb."../system"(x int);
`})

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	re := regexp.MustCompile(`(?m)^.*(table|database).*$`)
	out = strings.Join(re.FindAllString(out, -1), "\n")

	datadriven.RunTest(t, "testdata/zip/specialnames",
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
}

// This tests the operation of zip over unavailable clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestUnavailableZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(sh): PR 4485 caused timeout, skipped temporarily, to be resolved by fengyouxu.
	t.Skip()
	if testing.Short() {
		t.Skip("short flag")
	}
	if util.RaceEnabled {
		// Race builds make the servers so slow that they report spurious
		// unavailability.
		t.Skip("not running under race")
	}

	// unavailableCh is used by the replica command filter
	// to conditionally block requests and simulate unavailability.
	var unavailableCh atomic.Value
	closedCh := make(chan struct{})
	close(closedCh)
	unavailableCh.Store(closedCh)
	knobs := &kvserver.StoreTestingKnobs{
		TestingRequestFilter: func(ctx context.Context, _ roachpb.BatchRequest) *roachpb.Error {
			select {
			case <-unavailableCh.Load().(chan struct{}):
			case <-ctx.Done():
			}
			return nil
		},
	}

	// Make a 2-node cluster, with an option to make the first node unavailable.
	tc := testcluster.StartTestCluster(t, 2, base.TestClusterArgs{
		ServerArgsPerNode: map[int]base.TestServerArgs{
			0: {Insecure: true, Knobs: base.TestingKnobs{Store: knobs}},
			1: {Insecure: true},
		},
	})
	defer tc.Stopper().Stop(context.Background())

	// Sanity test: check that a simple operation works.
	if _, err := tc.ServerConn(0).Exec("SELECT * FROM system.users"); err != nil {
		t.Fatal(err)
	}

	// Make the first two nodes unavailable.
	ch := make(chan struct{})
	unavailableCh.Store(ch)
	defer close(ch)

	// Zip it. We fake a CLI test context for this.
	c := cliTest{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
	}
	stderr = os.Stdout
	defer func() { stderr = log.OrigStderr }()

	// Keep the timeout short so that the test doesn't take forever.
	out, err := c.RunWithCapture("debug zip " + os.DevNull + " --timeout=.5s")
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	// In order to avoid non-determinism here, we erase the output of
	// the range retrieval.
	re := regexp.MustCompile(`(?m)^(requesting ranges.*found|writing: debug/nodes/\d+/ranges).*\n`)
	out = re.ReplaceAllString(out, ``)

	datadriven.RunTest(t, "testdata/zip/unavailable",
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})
}

func eraseNonDeterministicZipOutput(out string) string {
	re := regexp.MustCompile(`(?m)postgresql://.*$`)
	out = re.ReplaceAllString(out, `postgresql://...`)
	re = regexp.MustCompile(`(?m)SQL address: .*$`)
	out = re.ReplaceAllString(out, `SQL address: ...`)
	re = regexp.MustCompile(`(?m)log file.*$`)
	out = re.ReplaceAllString(out, `log file ...`)
	re = regexp.MustCompile(`(?m)RPC connection to .*$`)
	out = re.ReplaceAllString(out, `RPC connection to ...`)
	re = regexp.MustCompile(`(?m)\^- resulted in.*$`)
	out = re.ReplaceAllString(out, `^- resulted in ...`)
	return out
}

// This tests the operation of zip over partial clusters.
//
// We cannot combine this test with TestZip above because TestPartialZip
// needs a TestCluster, the TestCluster hides its SSL certs, and we
// need the SSL certs dir to run a CLI test securely.
func TestPartialZip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(WY)
	t.Skip()
	if testing.Short() {
		t.Skip("short flag")
	}
	if util.RaceEnabled {
		// We want a low timeout so that the test doesn't take forever;
		// however low timeouts make race runs flaky with false positives.
		t.Skip("not running under race")
	}

	ctx := context.Background()

	// Three nodes. We want to see what `zip` thinks when one of the nodes is down.
	tc := testcluster.StartTestCluster(t, 3,
		base.TestClusterArgs{ServerArgs: base.TestServerArgs{Insecure: true}})
	defer tc.Stopper().Stop(ctx)

	// Switch off the second node.
	tc.StopServer(1)

	// Zip it. We fake a CLI test context for this.
	c := cliTest{
		t:          t,
		TestServer: tc.Server(0).(*server.TestServer),
	}
	stderr = os.Stdout
	defer func() { stderr = log.OrigStderr }()

	out, err := c.RunWithCapture("debug zip " + os.DevNull)
	if err != nil {
		t.Fatal(err)
	}

	// Strip any non-deterministic messages.
	out = eraseNonDeterministicZipOutput(out)

	datadriven.RunTest(t, "testdata/zip/partial1",
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now do it again and exclude the down node explicitly.
	out, err = c.RunWithCapture("debug zip " + os.DevNull + " --exclude-nodes=2")
	if err != nil {
		t.Fatal(err)
	}

	out = eraseNonDeterministicZipOutput(out)
	datadriven.RunTest(t, "testdata/zip/partial1_excluded",
		func(t *testing.T, td *datadriven.TestData) string {
			return out
		})

	// Now mark the stopped node as decommissioned, and check that zip
	// skips over it automatically.
	s := tc.Server(0)
	conn, err := s.RPCContext().GRPCDialNode(s.ServingRPCAddr(), s.NodeID(),
		rpc.DefaultClass).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	as := serverpb.NewAdminClient(conn)
	req := &serverpb.DecommissionRequest{
		NodeIDs:         []roachpb.NodeID{2},
		Decommissioning: true,
	}
	if _, err := as.Decommission(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	// We use .Override() here instead of SET CLUSTER SETTING in SQL to
	// override the 1m15s minimum placed on the cluster setting. There
	// is no risk to see the override bumped due to a gossip update
	// because this setting is not otherwise set in the test cluster.
	kvserver.TimeUntilStoreDead.Override(&s.ClusterSettings().SV, kvserver.TestTimeUntilStoreDead)

	datadriven.RunTest(t, "testdata/zip/partial2",
		func(t *testing.T, td *datadriven.TestData) string {

			testutils.SucceedsSoon(t, func() error {
				out, err = c.RunWithCapture("debug zip " + os.DevNull)
				if err != nil {
					t.Fatal(err)
				}

				// Strip any non-deterministic messages.
				out = eraseNonDeterministicZipOutput(out)

				if out != td.Expected {
					diff, _ := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
						A:        difflib.SplitLines(td.Expected),
						B:        difflib.SplitLines(out),
						FromFile: "Expected",
						FromDate: "",
						ToFile:   "Actual",
						ToDate:   "",
						Context:  1,
					})
					return errors.Newf("Diff:\n%s", diff)
				}
				return nil
			})
			return out
		})
}

// This checks that SQL retry errors are properly handled.
func TestZipRetries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, _, _ := serverutils.StartServer(t, base.TestServerArgs{Insecure: true})
	defer s.Stopper().Stop(context.Background())

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()

	zipName := filepath.Join(dir, "test.zip")

	func() {
		out, err := os.Create(zipName)
		if err != nil {
			t.Fatal(err)
		}
		z := newZipper(out)
		defer func() {
			if err := z.close(); err != nil {
				t.Fatal(err)
			}
		}()

		sqlURL := url.URL{
			Scheme:   "postgres",
			User:     url.User(security.RootUser),
			Host:     s.ServingSQLAddr(),
			RawQuery: "sslmode=disable",
		}
		sqlConn := makeSQLConn(sqlURL.String())
		defer sqlConn.Close()

		if err := dumpTableDataForZip(
			z, sqlConn, 3*time.Second,
			"test", `generate_series(1,15000) as t(x)`,
			`if(x<11000,x,kwdb_internal.force_retry('1h'))`); err != nil {
			t.Fatal(err)
		}
	}()

	r, err := zip.OpenReader(zipName)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = r.Close() }()
	var fileList bytes.Buffer
	for _, f := range r.File {
		fmt.Fprintln(&fileList, f.Name)
	}
	const expected = `test/generate_series(1,15000) as t(x).txt
test/generate_series(1,15000) as t(x).txt.err.txt
test/generate_series(1,15000) as t(x).1.txt
test/generate_series(1,15000) as t(x).1.txt.err.txt
test/generate_series(1,15000) as t(x).2.txt
test/generate_series(1,15000) as t(x).2.txt.err.txt
test/generate_series(1,15000) as t(x).3.txt
test/generate_series(1,15000) as t(x).3.txt.err.txt
test/generate_series(1,15000) as t(x).4.txt
test/generate_series(1,15000) as t(x).4.txt.err.txt
`
	assert.Equal(t, expected, fileList.String())
}

// This test the operation of zip over secure clusters.
func TestToHex(t *testing.T) {
	defer leaktest.AfterTest(t)()

	dir, cleanupFn := testutils.TempDir(t)
	defer cleanupFn()
	c := newCLITest(cliTestParams{
		storeSpecs: []base.StoreSpec{{
			Path: dir,
		}},
	})
	defer c.cleanup()

	// Create a job to have non-empty system.jobs table.
	c.RunWithArgs([]string{"sql", "-e", "CREATE STATISTICS foo FROM system.namespace"})

	_, err := c.RunWithCapture("debug zip " + dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	r, err := zip.OpenReader(dir + "/debug.zip")
	if err != nil {
		t.Fatal(err)
	}

	type hexField struct {
		idx int
		msg protoutil.Message
	}
	// Stores index and type of marshaled messages in the table row.
	// Negative indices work from the end - this is needed because parsing the
	// fields is not alway s precise as there can be spaces in the fields but the
	// hex fields are always in the end of the row and they don't contain spaces.
	hexFiles := map[string][]hexField{
		"debug/system.jobs.txt": {
			{idx: -2, msg: &jobspb.Payload{}},
			{idx: -1, msg: &jobspb.Progress{}},
		},
		"debug/system.descriptor.txt": {
			{idx: 2, msg: &sqlbase.Descriptor{}},
		},
	}

	for _, f := range r.File {
		fieldsToCheck, ok := hexFiles[f.Name]
		if !ok {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			t.Fatal(err)
		}
		defer rc.Close()

		buf := new(bytes.Buffer)
		if _, err = buf.ReadFrom(rc); err != nil {
			t.Fatal(err)
		}
		// Skip header.
		if _, err = buf.ReadString('\n'); err != nil {
			t.Fatal(err)
		}
		line, err := buf.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}

		fields := strings.Fields(line)
		for _, hf := range fieldsToCheck {
			i := hf.idx
			if i < 0 {
				i = len(fields) + i
			}
		}
	}
	if err = r.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestNodeRangeSelection(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Avoid leaking configuration changes after the tests end.
	defer initCLIDefaults()

	testData := []struct {
		args         []string
		wantIncluded []int
		wantExcluded []int
	}{
		{[]string{""}, []int{1, 200, 3}, nil},
		{[]string{"--nodes=1"}, []int{1}, []int{2, 3, 100}},
		{[]string{"--nodes=1,3"}, []int{1, 3}, []int{2, 4, 100}},
		{[]string{"--nodes=1-3"}, []int{1, 2, 3}, []int{4, 100}},
		{[]string{"--nodes=1-3,5"}, []int{1, 2, 3, 5}, []int{4, 100}},
		{[]string{"--nodes=1-3,5,10-20"}, []int{2, 5, 15}, []int{7}},
		{[]string{"--nodes=1,1-2"}, []int{1, 2}, []int{3, 100}},
		{[]string{"--nodes=1-3", "--exclude-nodes=2"}, []int{1, 3}, []int{2, 4, 100}},
		{[]string{"--nodes=1,3", "--exclude-nodes=3"}, []int{1}, []int{2, 3, 4, 100}},
		{[]string{"--exclude-nodes=2-7"}, []int{1, 8, 9, 10}, []int{2, 3, 4, 5, 6, 7}},
	}

	f := debugZipCmd.Flags()
	for _, tc := range testData {
		initCLIDefaults()
		if err := f.Parse(tc.args); err != nil {
			t.Fatalf("Parse(%#v) got unexpected error: %v", tc.args, err)
		}

		for _, wantIncluded := range tc.wantIncluded {
			assert.True(t, zipCtx.nodes.isIncluded(roachpb.NodeID(wantIncluded)))
		}
		for _, wantExcluded := range tc.wantExcluded {
			assert.False(t, zipCtx.nodes.isIncluded(roachpb.NodeID(wantExcluded)))
		}
	}
}
