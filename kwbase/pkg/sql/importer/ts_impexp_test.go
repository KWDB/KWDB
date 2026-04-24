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

package importer_test

import (
	"context"
	"io/ioutil"
	"path/filepath"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestExportWithconflictingOptions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("skip ts UT")
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: dir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TS DATABASE tsdb;`)
	sqlDB.Exec(t, `USE tsdb;`)
	sqlDB.Exec(t, `create table st (k_timestamp timestamp not null, abc int) attributes (location varchar(64), color varchar(64));`)
	sqlDB.Exec(t, `create table st_a using st (location, color) attributes ('tianjin', 'red');`)
	//sqlDB.Exec(t, `-- create table t1(k_timestamp timestamp not null, a int) attributes (location varchar(64) 'beijing', color varchar(64) 'blue');`)
	// export with meta_only and data_only
	t.Run(`only meta and only data`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/db' FROM DATABASE tsdb WITH meta_only, data_only")
		require.EqualError(t, err,
			`pq: can't use meta_only and data_only at the same time`)
	})
	// with meta_only and delimiter
	t.Run(`meta_only and delimiter`, func(t *testing.T) {
		// only print warning, but how to check it in export database?
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/db' FROM DATABASE tsdb WITH meta_only, delimiter = ','")
		if err != nil {
			t.Fatal(err)
		}
	})
	// with meta_only and chunk_rows
	t.Run(`meta_only and chunk_rows`, func(t *testing.T) {
		// only print warning, but how to check it in export database?
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/db' FROM DATABASE tsdb WITH meta_only, chunk_rows = '10'")
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestImpExpNormalTsTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("skip ts UT")
	baseDir := filepath.Join("testdata", "ts")
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: baseDir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TS DATABASE tsdb;`)
	sqlDB.Exec(t, `USE tsdb;`)
	createSQL := `CREATE TABLE t1 (k_timestamp TIMESTAMP NOT NULL, a INT4 NULL) TAGS (location VARCHAR(64) 'beijing', color VARCHAR(64) 'blue');`
	sqlDB.Exec(t, createSQL)
	// export only meta and check meta.sql is existed
	t.Run(`export only meta`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/t1' FROM TABLE t1 WITH meta_only")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "t1", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// export with meta_only and delimiter, print warning but execute successfully
	t.Run(`export with only meta and delimiter`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/t1/md' FROM TABLE t1 WITH meta_only, DELIMITER=','")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "t1", "md", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// export with meta_only and chunk_rows, print warning but execute successfully
	t.Run(`export with only meta and chunk_rows`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/t1/mc' FROM TABLE t1 WITH meta_only, CHUNK_ROWS='10'")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "t1", "mc", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// wrongMeta.sql has wrong sql sentence which attributes loss s
	t.Run(`import normal ts wrong meta`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS t1 CASCADE;`)
		// import csv data
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/normal/wrongMeta.sql" CSV DATA ("nodelocal://1/instance/st_a/n1.0.csv");`)
		require.EqualError(t, err,
			`pq: at or near "attribute": syntax error`)
		_, err = db.Exec(`SHOW CREATE TABLE t1;`)
		require.EqualError(t, err,
			`pq: relation "t1" does not exist`)
		// import meta
		_, err = db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/normal/wrongMeta.sql";`)
		require.EqualError(t, err,
			`pq: at or near "attribute": syntax error`)
		_, err = db.Exec(`SHOW CREATE TABLE t1;`)
		require.EqualError(t, err,
			`pq: relation "t1" does not exist`)
	})
}

func TestImpExpTemplateTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("skip ts UT")
	baseDir := filepath.Join("testdata", "ts")
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: baseDir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	createSQL := `CREATE TABLE st (k_timestamp TIMESTAMP NOT NULL, abc INT4 NULL) TAGS (location VARCHAR(64), color VARCHAR(64));`
	sqlDB.Exec(t, `CREATE TS DATABASE tsdb;`)
	sqlDB.Exec(t, `USE tsdb;`)
	sqlDB.Exec(t, createSQL)
	// export
	t.Run(`export only meta`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/st' FROM TABLE st")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "st", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// export with meta_only and delimiter, print warning but execute successfully
	t.Run(`export with only meta and delimiter`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/st/md' FROM TABLE st WITH meta_only, DELIMITER=','")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "st", "md", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// export with meta_only and chunk_rows, print warning but execute successfully
	t.Run(`export with only meta and chunk_rows`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/st/mc' FROM TABLE st WITH meta_only, CHUNK_ROWS='10'")
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "st", "mc", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		if string(meta) != createSQL+"\n" {
			t.Fatalf("export's content is different from create SQL\ncreate:%v\nexport:%v\n", createSQL, string(meta))
		}
	})
	// import into template with file path
	t.Run(`import into template with file path`, func(t *testing.T) {
		_, err := db.Exec("IMPORT INTO st CSV DATA ('nodelocal://1/instance/st_a/n1.0.csv')")
		require.EqualError(t, err,
			`pq: the csv data directory is empty or is a file, please give the template table's directory`)
	})
	// import only meta with empty csv dir
	t.Run(`import only meta with empty csv dir`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st CASCADE;`)
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/template/meta.sql" CSV DATA ("nodelocal://1/template");`)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "template", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		showCreateAll := sqlDB.QueryStr(t, "SHOW CREATE TABLE st")
		if showCreate, importStr := showCreateAll[0][1], string(meta); showCreate+";\n" != importStr {
			t.Fatalf("export's content is different from show create SQL\nshowCreate:%v\nimportStr:%v\n", showCreate, importStr)
		}
	})
	// import only meta
	t.Run(`import only meta`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st CASCADE;`)
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/template/meta.sql";`)
		if err != nil {
			t.Fatal(err)
		}
		meta, err := ioutil.ReadFile(filepath.Join(baseDir, "template", "meta.sql"))
		if err != nil {
			t.Fatal(err)
		}
		showCreateAll := sqlDB.QueryStr(t, "SHOW CREATE TABLE st")
		if showCreate, importStr := showCreateAll[0][1], string(meta); showCreate+";\n" != importStr {
			t.Fatalf("export's content is different from show create SQL\nshowCreate:%v\nimportStr:%v\n", showCreate, importStr)
		}
	})
	// wrongMeta.sql has wrong sql sentence which attributes loss s
	t.Run(`import template table with wrong meta`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st CASCADE;`)
		// import csv data
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/template/wrongMeta.sql";`)
		require.EqualError(t, err,
			`pq: at or near "attribute": syntax error`)
		_, err = db.Exec(`SHOW CREATE TABLE st;`)
		require.EqualError(t, err,
			`pq: relation "st" does not exist`)
	})
	// csv data must be a dir, not file
	t.Run(`wrong csv data dir`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st CASCADE;`)
		// import csv data
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/template/metaWithChild.sql" CSV DATA ("nodelocal://1/instance/st_a/n1.0.csv");`)
		require.EqualError(t, err,
			`pq: the csv data directory is empty or is a file, please give the template table's directory`)
		_, err = db.Exec(`SHOW CREATE TABLE st;`)
		require.EqualError(t, err,
			`pq: relation "st" does not exist`)
	})
}

func TestImpExpInstanceTable(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip("skip ts UT")
	baseDir := filepath.Join("testdata", "ts")
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: baseDir})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TS DATABASE tsdb;`)
	sqlDB.Exec(t, `USE tsdb;`)
	sqlDB.Exec(t, `create table st (k_timestamp timestamp not null, abc int) attributes (location varchar(64), color varchar(64));`)
	sqlDB.Exec(t, `create table st_a using st (location, color) attributes ('tianjin', 'red');`)
	// export
	t.Run(`export`, func(t *testing.T) {
		_, err := db.Exec("EXPORT INTO CSV 'nodelocal://1/t' FROM TABLE st_a")
		require.EqualError(t, err,
			`pq: can't only export instance table`)
	})
	// import into
	t.Run(`import into instance`, func(t *testing.T) {
		_, err := db.Exec("IMPORT INTO st_a CSV DATA ('nodelocal://1/instance/st_a/n1.0.csv')")
		require.EqualError(t, err,
			`pq: can't import into instance table st_a`)
	})
	// import instance already had template
	t.Run(`import instance already had template`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st_a CASCADE;`)
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/instance/meta.sql" CSV DATA ("nodelocal://1/instance/st_a/n1.0.csv");`)
		require.EqualError(t, err,
			`pq: can't only import instance table`)
	})
	// import instance without template
	t.Run(`import instance without template`, func(t *testing.T) {
		sqlDB.Exec(t, `DROP TABLE IF EXISTS st CASCADE;`)
		_, err := db.Exec(`IMPORT TABLE CREATE USING "nodelocal://1/instance/meta.sql" CSV DATA ("nodelocal://1/instance/st_a/n1.0.csv");`)
		require.EqualError(t, err,
			`pq: can't only import instance table`)
	})
}
