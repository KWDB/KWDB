// Copyright 2017 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package importer

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/jobs"
	"gitee.com/kwbasedb/kwbase/pkg/jobs/jobspb"
	"gitee.com/kwbasedb/kwbase/pkg/kv/kvserver"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event"
	"gitee.com/kwbasedb/kwbase/pkg/security/audit/event/target"
	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfra"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/jobutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/testcluster"
	"gitee.com/kwbasedb/kwbase/pkg/util/hlc"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"github.com/jackc/pgx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPgdumpCreateCities = `CREATE TABLE cities (
	city VARCHAR(80) NOT NULL,
	CONSTRAINT cities_pkey PRIMARY KEY (city ASC),
	FAMILY "primary" (city)
)`
	testPgdumpCreateWeather = `CREATE TABLE weather (
	city VARCHAR(80) NULL,
	temp_lo INT8 NULL,
	temp_hi INT8 NULL,
	prcp FLOAT4 NULL,
	date DATE NULL,
	CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES cities(city),
	INDEX weather_auto_index_weather_city_fkey (city ASC),
	FAMILY "primary" (city, temp_lo, temp_hi, prcp, date, rowid)
)`
	testPgdumpFk = `
CREATE TABLE public.cities (
    city character varying(80) NOT NULL
);

ALTER TABLE public.cities OWNER TO postgres;

CREATE TABLE public.weather (
    city character varying(80),
    temp_lo int8,
    temp_hi int8,
    prcp real,
    date date
);

ALTER TABLE public.weather OWNER TO postgres;

COPY public.cities (city) FROM stdin;
Berkeley
\.

COPY public.weather (city, temp_lo, temp_hi, prcp, date) FROM stdin;
Berkeley	45	53	0	1994-11-28
\.

ALTER TABLE ONLY public.cities
    ADD CONSTRAINT cities_pkey PRIMARY KEY (city);

ALTER TABLE ONLY public.weather
    ADD CONSTRAINT weather_city_fkey FOREIGN KEY (city) REFERENCES public.cities(city);
`

	testPgdumpFkCircular = `
CREATE TABLE public.a (
    i int8 NOT NULL,
    k int8
);

CREATE TABLE public.b (
    j int8 NOT NULL
);

COPY public.a (i, k) FROM stdin;
2	2
\.

COPY public.b (j) FROM stdin;
2
\.

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_pkey PRIMARY KEY (i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_pkey PRIMARY KEY (j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_i_fkey FOREIGN KEY (i) REFERENCES public.b(j);

ALTER TABLE ONLY public.a
    ADD CONSTRAINT a_k_fkey FOREIGN KEY (k) REFERENCES public.a(i);

ALTER TABLE ONLY public.b
    ADD CONSTRAINT b_j_fkey FOREIGN KEY (j) REFERENCES public.a(i);
`
)

func TestExportImportRoundTrip(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	tests := []struct {
		stmts    string
		tbl      string
		expected string
	}{
		// Note that the directory names that are being imported from and exported into
		// need to differ across runs, so we let the test runner format the stmts field
		// with a unique directory name per run.
		{
			stmts: `EXPORT INTO CSV 'nodelocal://01/%[1]s' FROM SELECT ARRAY['a', 'b', 'c'];
							IMPORT TABLE t (x TEXT[]) CSV DATA ('nodelocal://01/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY['a', 'b', 'c']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://01/%[1]s' FROM SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63'];
							IMPORT TABLE t (x BYTES[]) CSV DATA ('nodelocal://01/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT ARRAY[b'abc', b'\141\142\143', b'\x61\x62\x63']`,
		},
		{
			stmts: `EXPORT INTO CSV 'nodelocal://01/%[1]s' FROM SELECT 'dog' COLLATE en;
							IMPORT TABLE t (x STRING COLLATE en) CSV DATA ('nodelocal://01/%[1]s/n1.0.csv')`,
			tbl:      "t",
			expected: `SELECT 'dog' COLLATE en`,
		},
	}

	for i, test := range tests {
		sqlDB.Exec(t, fmt.Sprintf(`DROP TABLE IF EXISTS %s`, test.tbl))
		sqlDB.Exec(t, fmt.Sprintf(test.stmts, fmt.Sprintf("run%d", i)))
		sqlDB.CheckQueryResults(t, fmt.Sprintf(`SELECT * FROM %s`, test.tbl), sqlDB.QueryStr(t, test.expected))
	}
}

func TestDatabaseExportImportAudit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	ae := (tc.Servers[0].AuditServer().GetHandler()).(*event.AuditEvent)
	DBMetric := ae.GetMetric(target.ObjectDatabase)

	sqlDB.Exec(t, `set cluster setting audit.enabled=true;`)
	sqlDB.Exec(t, `set cluster setting audit.log.enabled=true;`)
	sqlDB.Exec(t, `create audit a_test on database for all to root;`)
	sqlDB.Exec(t, `alter audit a_test enable;`)
	sqlDB.Exec(t, `create database d1;`)
	sqlDB.Exec(t, `use d1;`)
	sqlDB.Exec(t, `create table t1(a int);`)
	sqlDB.Exec(t, `insert into t1 values (1);`)
	sqlDB.Exec(t, `insert into t1 values (2);`)
	sqlDB.Exec(t, `insert into t1 values (3);`)
	sqlDB.Exec(t, `create table t2(b string);`)
	sqlDB.Exec(t, `insert into t2 values ('abc');`)
	sqlDB.Exec(t, `use defaultdb;`)

	// export database audit test
	sqlDB.Exec(t, `export into csv "nodelocal://1/db" from database d1`)
	time.Sleep(1 * time.Second)
	exportDBExpect := DBMetric.Count(target.Export)
	if DBMetric.Count(target.Export) != exportDBExpect {
		t.Errorf("expected db count:%d, but got %d", exportDBExpect, DBMetric.Count(target.Export))
	}

	// import database audit test
	sqlDB.Exec(t, `drop database d1;`)
	sqlDB.Exec(t, `import database csv data ("nodelocal://1/db");`)
	time.Sleep(1 * time.Second)
	importDBExpect := DBMetric.Count(target.Import)
	if DBMetric.Count(target.Import) != importDBExpect {
		t.Errorf("expected db count:%d, but got %d", importDBExpect, DBMetric.Count(target.Import))
	}

}

func TestBackupRestoreSQL(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create database d1;`)
	sqlDB.Exec(t, `use d1;`)
	sqlDB.Exec(t, `create table t1(a int);`)
	sqlDB.Exec(t, `insert into t1 values (1);`)
	sqlDB.Exec(t, `insert into t1 values (2);`)
	sqlDB.Exec(t, `insert into t1 values (3);`)
	sqlDB.Exec(t, `create table t2(b string);`)
	sqlDB.Exec(t, `insert into t2 values ('abc');`)
	sqlDB.Exec(t, `use defaultdb;`)

	// test backup
	sqlDB.ExpectErr(t, "The BACKUP can only be used in the enterprise version", `BACKUP TABLE d1.t1 TO "nodelocal://1/tb1";`)
	sqlDB.ExpectErr(t, "The BACKUP can only be used in the enterprise version", `BACKUP DATABASE d1 TO "nodelocal://1/db1";`)

	// test restore
	sqlDB.ExpectErr(t, "The RESTORE can only be used in the enterprise version", `RESTORE TABLE t1 FROM "nodelocal://1/tb1";`)
	sqlDB.ExpectErr(t, "The RESTORE can only be used in the enterprise version", `RESTORE DATABASE d1 FROM "nodelocal://1/db1";`)
}

func TestExportChunkSize(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	sqlDB.Exec(t, `create database d1;`)
	sqlDB.Exec(t, `use d1;`)
	sqlDB.Exec(t, `create table t1(a int);`)
	sqlDB.Exec(t, `insert into t1 values (1);`)
	sqlDB.Exec(t, `insert into t1 values (2);`)
	sqlDB.Exec(t, `insert into t1 values (3);`)
	sqlDB.Exec(t, `create table t2(b string);`)
	sqlDB.Exec(t, `insert into t2 values ('abc');`)
	sqlDB.Exec(t, `use defaultdb;`)

	// test export
	sqlDB.ExpectErr(t, "The chunk size must be less than 100000", `EXPORT INTO CSV "nodelocal://1/tb1" FROM TABLE d1.t1 with chunk_rows='10000000';`)
	sqlDB.ExpectErr(t, "The chunk size must be less than 100000", `EXPORT INTO CSV "nodelocal://1/tb1" FROM DATABASE d1 with chunk_rows='10000000';`)
}

func TestTableExportImportAudit(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.Background()
	baseDir, cleanup := testutils.TempDir(t)
	defer cleanup()
	tc := testcluster.StartTestCluster(
		t, 1, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)
	ae := (tc.Servers[0].AuditServer().GetHandler()).(*event.AuditEvent)
	tbMetric := ae.GetMetric(target.ObjectTable)

	sqlDB.Exec(t, `set cluster setting audit.enabled=true;`)
	sqlDB.Exec(t, `set cluster setting audit.log.enabled=true;`)
	sqlDB.Exec(t, `create audit b_test on table for all to root;`)
	sqlDB.Exec(t, `alter audit b_test enable;`)
	sqlDB.Exec(t, `create table t1(a int);`)
	sqlDB.Exec(t, `insert into t1 values (1);`)
	sqlDB.Exec(t, `insert into t1 values (2);`)
	sqlDB.Exec(t, `insert into t1 values (3);`)

	// export table audit test
	sqlDB.Exec(t, `export into csv "nodelocal://1/" from table t1 ;`)
	time.Sleep(1 * time.Second)
	exportTbExpect := tbMetric.Count(target.Export)
	if tbMetric.Count(target.Export) != exportTbExpect {
		t.Errorf("expected table count:%d, but got %d", exportTbExpect, tbMetric.Count(target.Export))
	}

	// import table audit test
	sqlDB.Exec(t, `drop table t1;`)
	sqlDB.Exec(t, `import table create using "nodelocal://1/meta.sql" csv data ("nodelocal://1/n1.0.csv");`)
	time.Sleep(1 * time.Second)
	importTbExpect := tbMetric.Count(target.Import)
	if tbMetric.Count(target.Import) != importTbExpect {
		t.Errorf("expected table count:%d, but got %d", importTbExpect, tbMetric.Count(target.Import))
	}

}

func BenchmarkImport(b *testing.B) {
	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	baseDir := filepath.Join("testdata", "csv")
	ctx := context.Background()
	tc := testcluster.StartTestCluster(b, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	testFiles := makeCSVData(b, numFiles, b.N*100, nodes, 16)

	b.ResetTimer()

	sqlDB.Exec(b,
		fmt.Sprintf(
			`IMPORT TABLE t (a INT8 PRIMARY KEY, b STRING, INDEX (b), INDEX (a, b))
			CSV DATA (%s)`,
			strings.Join(testFiles.files, ","),
		))
}

// TestImportLivenessWithLeniency tests that a temporary node liveness
// transition during IMPORT doesn't cancel the job, but allows the
// owning node to continue processing.
func TestImportLivenessWithLeniency(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("TODO:(baozhixiao):fix this UT")

	defer func(oldInterval time.Duration) {
		jobs.DefaultAdoptInterval = oldInterval
	}(jobs.DefaultAdoptInterval)
	jobs.DefaultAdoptInterval = 100 * time.Millisecond
	jobs.DefaultCancelInterval = 100 * time.Millisecond

	const nodes = 1
	nl := jobs.NewFakeNodeLiveness(nodes)
	serverArgs := base.TestServerArgs{
		Knobs: base.TestingKnobs{
			RegistryLiveness: nl,
		},
	}

	var allowResponse chan struct{}
	params := base.TestClusterArgs{ServerArgs: serverArgs}
	params.ServerArgs.Knobs.Store = &kvserver.StoreTestingKnobs{
		TestingResponseFilter: jobutils.BulkOpResponseFilter(&allowResponse),
	}

	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, params)
	defer tc.Stopper().Stop(ctx)
	conn := tc.Conns[0]
	sqlDB := sqlutils.MakeSQLRunner(conn)

	// Prevent hung HTTP connections in leaktest.
	sqlDB.Exec(t, `SET CLUSTER SETTING cloudstorage.timeout = '3s'`)
	// We want to know exactly how much leniency is configured.
	sqlDB.Exec(t, `SET CLUSTER SETTING jobs.registry.leniency = '1m'`)
	sqlDB.Exec(t, `SET CLUSTER SETTING kv.bulk_ingest.batch_size = '300B'`)
	sqlDB.Exec(t, `CREATE DATABASE liveness`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const rows = 5000
		if r.Method == "GET" {
			for i := 0; i < rows; i++ {
				fmt.Fprintln(w, i)
			}
		}
	}))
	defer srv.Close()

	const query = `IMPORT TABLE liveness.t (i INT8 PRIMARY KEY) CSV DATA ($1)`

	// Start an IMPORT and wait until it's done one addsstable.
	allowResponse = make(chan struct{})
	errCh := make(chan error)
	go func() {
		_, err := conn.Exec(query, srv.URL)
		errCh <- err
	}()
	// Allow many, but not all, addsstables to complete.
	for i := 0; i < 50; i++ {
		select {
		case allowResponse <- struct{}{}:
		case err := <-errCh:
			t.Fatal(err)
		}
	}
	// Fetch the new job ID and lease since we know it's running now.
	var jobID int64
	originalLease := &jobspb.Payload{}
	{
		var expectedLeaseBytes []byte
		sqlDB.QueryRow(
			t, `SELECT id, payload FROM system.jobs ORDER BY created DESC LIMIT 1`,
		).Scan(&jobID, &expectedLeaseBytes)
		expectedLeaseBytes, err := hex.DecodeString(string(expectedLeaseBytes[2:]))
		if err != nil {
			t.Fatal(err)
		}
		if err := protoutil.Unmarshal(expectedLeaseBytes, originalLease); err != nil {
			t.Fatal(err)
		}
	}

	// addsstable is done, make the node slightly tardy.
	nl.FakeSetExpiration(1, hlc.Timestamp{
		WallTime: hlc.UnixNano() - (15 * time.Second).Nanoseconds(),
	})

	// Wait for the registry cancel loop to run and not cancel the job.
	<-nl.SelfCalledCh
	<-nl.SelfCalledCh
	close(allowResponse)

	// Set the node to be fully live again.  This prevents the registry
	// from canceling all of the jobs if the test node is saturated
	// and the import runs slowly.
	nl.FakeSetExpiration(1, hlc.MaxTimestamp)

	// Verify that the client didn't see anything amiss.
	if err := <-errCh; err != nil {
		t.Fatalf("import job should have completed: %s", err)
	}

	// The job should have completed normally.
	jobutils.WaitForJob(t, sqlDB, jobID)
}

// TestImportMVCCChecksums verifies that MVCC checksums are correctly
// computed by issuing a secondary index change that runs a CPut on the
// index. See #23984.
func TestImportMVCCChecksums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	ctx := context.Background()
	defer s.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE d`)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, "1,1,1")
		}
	}))
	defer srv.Close()

	sqlDB.Exec(t, `IMPORT TABLE d.t (
		a INT8 PRIMARY KEY,
		b INT8,
		c INT8,
		INDEX (b) STORING (c)
	) CSV DATA ($1)`, srv.URL)
	sqlDB.Exec(t, `UPDATE d.t SET c = 2 WHERE a = 1`)
}

// TestImportClientDisconnect ensures that an import job can complete even if
// the client connection which started it closes. This test uses a helper
// subprocess to force a closed client connection without needing to rely
// on the driver to close a TCP connection. See TestImportClientDisconnectHelper
// for the subprocess.
func TestImportClientDisconnect(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	args := base.TestClusterArgs{}
	tc := testcluster.StartTestCluster(t, 1, args)
	defer tc.Stopper().Stop(ctx)

	tc.WaitForNodeLiveness(t)
	require.NoError(t, tc.WaitForFullReplication())

	conn := tc.ServerConn(0)
	runner := sqlutils.MakeSQLRunner(conn)
	runner.Exec(t, "SET CLUSTER SETTING kv.protectedts.poll_interval = '100ms';")

	// Make a server that will tell us when somebody has sent a request, wait to
	// be signaled, and then serve a CSV row for our table.
	allowResponse := make(chan struct{})
	gotRequest := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			return
		}
		select {
		case gotRequest <- struct{}{}:
		default:
		}
		select {
		case <-allowResponse:
		case <-ctx.Done(): // Deal with test failures.
		}
		_, _ = w.Write([]byte("1,asdfasdfasdfasdf"))
	}))
	defer srv.Close()

	// Make credentials for the new connection.
	runner.Exec(t, `CREATE USER testuser`)
	runner.Exec(t, `GRANT admin TO testuser`)
	pgURL, cleanup := sqlutils.PGUrl(t, tc.Server(0).ServingSQLAddr(),
		"TestImportClientDisconnect-testuser", url.User("testuser"))
	defer cleanup()

	// Kick off the import on a new connection which we're going to close.
	done := make(chan struct{})
	ctxToCancel, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		defer close(done)
		connCfg, err := pgx.ParseConnectionString(pgURL.String())
		assert.NoError(t, err)
		db, err := pgx.Connect(connCfg)
		assert.NoError(t, err)
		defer func() { _ = db.Close() }()
		_, err = db.ExecEx(ctxToCancel, `IMPORT TABLE foo (k INT PRIMARY KEY, v STRING) CSV DATA ($1)`,
			nil /* options */, srv.URL)
		assert.Equal(t, context.Canceled, err)
	}()

	// Wait for the import job to start.
	var jobID string
	testutils.SucceedsSoon(t, func() error {
		row := conn.QueryRow("SELECT job_id FROM [SHOW JOBS] WHERE job_type = 'IMPORT' ORDER BY created DESC LIMIT 1")
		return row.Scan(&jobID)
	})

	// Wait for it to actually start.
	<-gotRequest

	// Cancel the import context and wait for the goroutine to exit.
	cancel()
	<-done

	// Allow the import to proceed.
	close(allowResponse)

	// Wait for the job to get marked as succeeded.
	testutils.SucceedsSoon(t, func() error {
		var status string
		if err := conn.QueryRow("SELECT status FROM [SHOW JOB " + jobID + "]").Scan(&status); err != nil {
			return err
		}
		const succeeded = "succeeded"
		if status != succeeded {
			return errors.Errorf("expected %s, got %v", succeeded, status)
		}
		return nil
	})
}

// TestImportClientDisconnect ensures that table will be create in the target database.
func TestUseCreateFile(t *testing.T) {
	defer leaktest.AfterTest(t)()

	const (
		nodes    = 3
		numFiles = nodes + 2
	)
	baseDir := filepath.Join("testdata", "csv")
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])

	testFiles := makeCSVData(t, numFiles, 100, nodes, 16)
	t.Run("create table with name t", func(t *testing.T) {
		createFile := fmt.Sprintf(`'nodelocal://01/%s'`, fmt.Sprintf("meta1.sql"))

		sqlDB.Exec(t,
			fmt.Sprintf(
				`IMPORT TABLE CREATE USING %s
			CSV DATA (%s)`,
				createFile, strings.Join(testFiles.files, ","),
			))
		sqlDB.Exec(t, `SELECT * FROM t`)
	})

	t.Run("create table with name db1.t", func(t *testing.T) {
		createFile := fmt.Sprintf(`'nodelocal://01/%s'`, fmt.Sprintf("meta2.sql"))
		sqlDB.Exec(t, `CREATE DATABASE db1`)
		sqlDB.Exec(t,
			fmt.Sprintf(
				`IMPORT TABLE CREATE USING %s
			CSV DATA (%s)`,
				createFile, strings.Join(testFiles.files, ","),
			))
		sqlDB.Exec(t, `SELECT * FROM db1.t`)
	})

}

// TestImportDatabase tests import database by using testdata.
func TestImportDatabase(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const nodes = 3
	baseDir := filepath.Join("testdata", "db")
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: baseDir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE db2`)
	sqlDB.Exec(t, `CREATE TABLE db2.a2(a int primary key)`)
	sqlDB.Exec(t, `CREATE TABLE db2.b2(b int primary key)`)
	sqlDB.Exec(t, `INSERT INTO db2.a2 VALUES (123)`)
	sqlDB.Exec(t, `INSERT INTO db2.b2 VALUES (456)`)
	// case1: two normal table
	// db.a is normal, db.b is normal (successful)
	t.Run("two normal table", func(t *testing.T) {
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/normal')`)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.a2`),
		)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case2: one normal, one lost csv files
	// db.a is normal, db.b is missing CSV file (successful)
	t.Run("one normal, one lost csv files", func(t *testing.T) {
		// expect succeed
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/lostTableCSV')`)
		// a is empty, lost csv file
		if len(sqlDB.QueryStr(t, `SELECT * FROM db.a`)) != 0 {
			t.Fatalf("the table's row number must be zero")
		}
		// b is normal
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case3: one normal, one lost sql file
	// db.a lost SQL file, db.b is normal (error reported)
	t.Run("one normal, one lost sql file", func(t *testing.T) {
		// a lost sql file
		// b is normal
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/lostTableSQL')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case4: no database sql file
	// database db missing SQL file (error reported)
	t.Run("lost db sql file", func(t *testing.T) {
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/lostDBSQL')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case5: import blank(empty) csv file
	// db.a CSV file is empty, db.b is normal (successful)
	t.Run("import blank(empty) csv file", func(t *testing.T) {
		// expect succeed
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/blank')`)
		if len(sqlDB.QueryStr(t, `SELECT * FROM db.a`)) != 0 {
			t.Fatalf("the empty table's row number must be zero")
		}
	})
	// case6: database rollback
	// csv file format does not match the meta.sql file, and the established table needs to be rolled back (error message)
	t.Run("database rollback", func(t *testing.T) {
		_, err := db.Query(`IMPORT DATABASE CSV DATA ('nodelocal://1/rollback')`)
		require.EqualError(t, err,
			"pq: nodelocal://1/rollback/public/t1//n1.0.csv: error parsing row 1: parse \"home\" as INT4: could not parse \"shanxi\" as type int: strconv.ParseInt: parsing \"shanxi\": invalid syntax (row: \"shanxi,xiaoming\")")
	})
	// case7: import nil database
	t.Run("import nil database", func(t *testing.T) {
		_, err := db.Query(`IMPORT DATABASE CSV DATA ('nodelocal://1/nilDatabase')`)
		require.EqualError(t, err,
			"pq: cannot import an empty database")
	})
}

// TestImportErr tests import err.
func TestImportErr(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const nodes = 3
	baseDir := filepath.Join("testdata", "db")
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: baseDir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)
	sqlDB.Exec(t, `CREATE TABLE t1 (id int primary key, name string )`)

	sqlDB.Exec(t, `CREATE TABLE t2 (id int, name string, score int)`)
	sqlDB.Exec(t, `INSERT INTO t2 VALUES (1,'xiaoming', 58),(2,'xiaoli',59),(3,'xiaobao',60)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/moreColumn' FROM TABLE t2`)

	sqlDB.Exec(t, `CREATE TABLE t3 (id int)`)
	sqlDB.Exec(t, `INSERT INTO t3 VALUES (1),(2),(3)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/lessColumn' FROM TABLE t3`)

	sqlDB.Exec(t, `CREATE TABLE t4 (home string, name string)`)
	sqlDB.Exec(t, `INSERT INTO t4 VALUES ('shanxi','xiaoming'),('hebei','xiaoli'),('shandong','xiaobao')`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/errorType' FROM TABLE t4`)

	sqlDB.Exec(t, `CREATE TABLE t5 (id int)`)
	sqlDB.Exec(t, `INSERT INTO t5 VALUES (1),(1),(1)`)
	sqlDB.Exec(t, `EXPORT INTO CSV 'nodelocal://1/collisions' FROM TABLE t5`)

	// case1: lost csv file
	t.Run("lost csv file", func(t *testing.T) {
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT INTO t1 CSV DATA ('nodelocal://1/noCSV')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
	})

	// case2: import csv file with three columns into a tale with two columns
	t.Run("import more columns", func(t *testing.T) {
		_, err := db.Query(`IMPORT INTO t1 CSV DATA ('nodelocal://1/moreColumn/n1.0.csv')`)
		require.EqualError(t, err,
			`pq: nodelocal://1/moreColumn/n1.0.csv: error parsing row 1: expected 2 fields, got 3 (row: "1,xiaoming,58")`)
	})

	// case3: import csv file with one column into a tale with two columns
	t.Run("import less columns", func(t *testing.T) {
		_, err := db.Query(`IMPORT INTO t1 CSV DATA ('nodelocal://1/lessColumn/n1.0.csv')`)
		require.EqualError(t, err,
			"pq: nodelocal://1/lessColumn/n1.0.csv: error parsing row 1: expected 2 fields, got 1 (row: \"1\")")
	})

	// case4: import csv file with error type
	t.Run("import error type", func(t *testing.T) {
		_, err := db.Query(`IMPORT INTO t1 CSV DATA ('nodelocal://1/errorType/n1.0.csv')`)
		require.EqualError(t, err,
			"pq: nodelocal://1/errorType/n1.0.csv: error parsing row 1: parse \"id\" as INT4: could not parse \"shanxi\" as type int: strconv.ParseInt: parsing \"shanxi\": invalid syntax (row: \"shanxi,xiaoming\")")
	})

	// case5: import duplicateKey
	t.Run("import key collisions", func(t *testing.T) {
		_, err := db.Query(`IMPORT INTO t1 CSV DATA ('nodelocal://1/collisions/n1.0.csv')`)
		require.EqualError(t, err,
			"pq: nodelocal://1/collisions/n1.0.csv: error parsing row 1: expected 2 fields, got 1 (row: \"1\")")
	})
}

// TestImportDatabaseWithSchema tests import database with schema by using testdata.
func TestImportDatabaseWithSchema(t *testing.T) {
	defer leaktest.AfterTest(t)()
	const nodes = 3
	baseDir := filepath.Join("testdata", "dbWithSc")
	ctx := context.Background()
	tc := testcluster.StartTestCluster(t, nodes, base.TestClusterArgs{ServerArgs: base.TestServerArgs{ExternalIODir: baseDir}})
	defer tc.Stopper().Stop(ctx)

	sqlDB := sqlutils.MakeSQLRunner(tc.Conns[0])
	sqlDB.Exec(t, `CREATE DATABASE db2`)
	sqlDB.Exec(t, `use db2`)
	sqlDB.Exec(t, `CREATE SCHEMA sc2`)
	sqlDB.Exec(t, `CREATE TABLE db2.sc2.a2(a int primary key)`)
	sqlDB.Exec(t, `CREATE TABLE db2.sc2.b2(b int primary key)`)
	sqlDB.Exec(t, `INSERT INTO db2.sc2.a2 VALUES (123)`)
	sqlDB.Exec(t, `INSERT INTO db2.sc2.b2 VALUES (456)`)
	// public
	sqlDB.Exec(t, `CREATE TABLE db2.public.a2(a int primary key)`)
	sqlDB.Exec(t, `CREATE TABLE db2.public.b2(b int primary key)`)
	sqlDB.Exec(t, `INSERT INTO db2.public.a2 VALUES (111)`)
	sqlDB.Exec(t, `INSERT INTO db2.public.b2 VALUES (222)`)
	// case1: two normal table
	// db.sc.a normal, db.sc.b is normal (successful)
	t.Run("two normal table", func(t *testing.T) {
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/normal')`)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.a2`),
		)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case2: one normal, one lost csv file
	// db.sc.a normal, db.sc.b lost CSV file (successful)
	t.Run("one normal, one lost csv file", func(t *testing.T) {
		// expect succeed
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/lostTableCSV')`)
		// a is empty, lost csv file
		if len(sqlDB.QueryStr(t, `SELECT * FROM db.sc.a`)) != 0 {
			t.Fatalf("the table's row number must be zero")
		}
		// b is normal
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case3: one normal, one lost sql files
	// db.sc.a missing SQL file, db.sc.b is normal (error reported)
	t.Run("one normal, one lost sql files", func(t *testing.T) {
		// a lost sql file
		// b is normal
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/lostTableSQL')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case4: no database sql file
	// database db missing SQL file (error reported)
	t.Run("lost db sql file", func(t *testing.T) {
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/lostDBSQL')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case5: import blank(empty) csv file
	// db.sc.a CSV file is empty, db.sc.b is normal (successful)
	t.Run("import blank(empty) csv file", func(t *testing.T) {
		// expect succeed
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/blank')`)
		if len(sqlDB.QueryStr(t, `SELECT * FROM db.sc.a`)) != 0 {
			t.Fatalf("the empty table's row number must be zero")
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case6: lost schema meta
	// schema sc missing SQL file (successful)
	t.Run("lost schema meta", func(t *testing.T) {
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/lostSchemaSQL')`)
		// sc
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.a2`),
		)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case7: have schema SQL in public
	// public folder contains meta. SQL (successful)
	t.Run("have schema SQL in public", func(t *testing.T) {
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/havePublicSQL')`)
		// we just skip meta.sql in public, so expect succeed now
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case8: two normal schema (include public)
	// db.a, db.b, db.sc.a, db.sc.b normal (successful)
	t.Run("two normal schema", func(t *testing.T) {
		sqlDB.Exec(t, `IMPORT DATABASE CSV DATA ('nodelocal://1/normalTwoSc')`)
		// public
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.public.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.public.a2`),
		)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.public.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.public.b2`),
		)
		// sc
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.a2`),
		)
		sqlDB.CheckQueryResults(t,
			fmt.Sprintf(`SELECT * FROM db.sc.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.sc2.b2`),
		)
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case9: more than one schema sql
	// schema sc folder contains multiple schema SQL files (error message)
	t.Run("more than one schema sql", func(t *testing.T) {
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/twoSchemaSQL')`)
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
	// case10: lost schema file dir
	// schema sc missing folder (i.e. table folder directly under db folder) (error reported)
	t.Run("lost schema file dir", func(t *testing.T) {
		// Lost schema file dir will read the wrong dir
		// It regards tables' dir as schema's dir
		// So it will try to read table's SQL file
		// just like /testdata/dbWithSc/lostSchemaDir/a/n1.0.csv/meta.sql
		_, err := sqlDB.DB.ExecContext(context.Background(), `IMPORT DATABASE CSV DATA ('nodelocal://1/lostSchemaDir')`)
		if !strings.Contains(err.Error(), "not a directory") {
			t.Fatal(err)
		}
		sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
	})
}

// TestResumePos tests resumePos during importing tables.
func TestResumePos(t *testing.T) {
	defer leaktest.AfterTest(t)()
	baseDir := filepath.Join("testdata", "resume")

	// we can't set resumeCtx's err to context canceled, so we inject a retryJobError
	injectError := func() error {
		return jobs.NewRetryJobError("")
	}

	params := base.TestServerArgs{
		ExternalIODir: baseDir,
		UseDatabase:   "defaultdb",
		Knobs: base.TestingKnobs{
			DistSQL: &execinfra.TestingKnobs{
				InjectErrorAfterUpdateImportJob: injectError,
			},
		},
	}
	ctx := context.Background()
	srv, db, _ := serverutils.StartServer(t, params)
	defer srv.Stopper().Stop(ctx)
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE DATABASE db2`)
	sqlDB.Exec(t, `CREATE TABLE db2.a2(a int primary key)`)
	sqlDB.Exec(t, `CREATE TABLE db2.b2(b int primary key)`)
	sqlDB.Exec(t, `INSERT INTO db2.a2 VALUES (1), (2), (3), (4), (5), (6)`)
	sqlDB.Exec(t, `INSERT INTO db2.b2 VALUES (1), (2), (3)`)

	_, err := sqlDB.DB.ExecContext(ctx, `IMPORT DATABASE CSV DATA ('nodelocal://1/db')`)
	// sample err: `pg: job 868128088611946497: : restarting in background`
	ans := strings.Split(err.Error(), " ")
	// sample ans: []string{pg:, job, 868128088611946497:, :, restarting, in, background} (len 7, cap 7)
	jobID, _ := strconv.Atoi(strings.TrimRight(ans[2], ":"))
	jr := srv.JobRegistry().(*jobs.Registry)
	// job will restart after 30s. SucceedsSoon will be failed after 45s.
	testutils.SucceedsSoon(t, func() error {
		loaded, err := jr.LoadJob(ctx, int64(jobID))
		require.NoError(t, err)
		st, err := loaded.CurrentStatus(ctx)
		require.NoError(t, err)
		if st != jobs.StatusSucceeded {
			return errors.Errorf("expected %s, got %s", jobs.StatusSucceeded, st)
		}
		return nil
	})

	// We import a database with two tables: a and b.
	// We return a retryJobError after <table a>'s runImport and make sure it has updated its resumePos,
	// so <table a>'s resumePos has been set to math.MaxInt64.
	// Before this commit, <table b>'s resumePos will also be math.MaxInt64, so it will skip all tables.
	// But now, we keep a map for every table's resumePos, everything will be ok.
	// Because of the retryJobError, the import will finally succeed, so we can select to check the data.
	// (BTW: Actually, <context cancel> is the only retryable error, but we can't make a real one in test,
	// so we return a retryJobError instead. If it's not retryable error, the <database>'s import will over
	// when <table a>'s import fail and will do revert, so we can't select any data in database.)
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SELECT * FROM db.a`), sqlDB.QueryStr(t, `SELECT * FROM db2.a2`),
	)
	sqlDB.CheckQueryResults(t,
		fmt.Sprintf(`SELECT * FROM db.b`), sqlDB.QueryStr(t, `SELECT * FROM db2.b2`),
	)
	sqlDB.Exec(t, `DROP DATABASE IF EXISTS db CASCADE`)
}
