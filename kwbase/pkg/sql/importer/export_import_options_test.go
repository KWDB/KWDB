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
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

//	 Test import and export covering the following scenarios:
//			1. The user field contains an empty string
//			2. The user field contains null values
func TestNullAndEmptyString(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE user_info (id int PRIMARY KEY,name string NOT NULL,"comment" string)`)

	sqlDB.Exec(t, `INSERT INTO user_info VALUES (1,'SDXX','A big boss, rich'),(2,'DXXX','')`)
	sqlDB.Exec(t, `INSERT INTO user_info VALUES (3,'XXDXX')`)

	sqlDB.QueryStr(t, `EXPORT INTO CSV 'nodelocal://1/' FROM TABLE user_info`)
	exp := sqlDB.QueryStr(t, `SELECT * FROM user_info`)

	sqlDB.Exec(t, `DELETE FROM user_info WHERE true`)
	sqlDB.QueryStr(t, `IMPORT INTO user_info CSV DATA ("nodelocal://01/n1.0.csv");`)
	sqlDB.CheckQueryResults(t, `SELECT * FROM user_info`, exp)
}

// Test user specified delimiter import/export
func TestStrWithDelimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)

	sqlDB.Exec(t, `CREATE TABLE user_info (id int PRIMARY KEY,name string NOT NULL,"comment" string)`)
	sqlDB.Exec(t, `INSERT INTO user_info VALUES (1,'comma_in_str','ddsddadd,dddas');`)
	sqlDB.Exec(t, `INSERT INTO user_info VALUES (2,'no_comma_in_str','ddsddadddddas');`)
	exp := sqlDB.QueryStr(t, `SELECT * FROM user_info`)
	delimiters := []string{",", ";", "\t", "|", ":", " ", "@", "$", "*", "#", "a", "1", ".", "!", "?", "\\", "A", "a", "&", "<", "''"}

	for i, delimiter := range delimiters {
		t.Run(delimiter, func(t *testing.T) {
			sqlDB.QueryStr(t, fmt.Sprintf(`EXPORT INTO CSV 'nodelocal://1/%d' FROM TABLE user_info with delimiter='%s'`, i, delimiter))
			sqlDB.Exec(t, `DELETE FROM user_info WHERE true`)
			sqlDB.QueryStr(t, fmt.Sprintf(`IMPORT INTO user_info CSV DATA ("nodelocal://01/%d/n1.0.csv") with delimiter='%s';`, i, delimiter))
			sqlDB.CheckQueryResults(t, `SELECT * FROM user_info`, exp)
		})
	}

}

// The test includes importing and exporting temporal and relational tables, covering the following scenarios:
//  1. Error reported when specifying multiple delimiters
//  2. Error reported when specifying Chinese characters
func TestInvalidDelimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	dir, cleanupDir := testutils.TempDir(t)
	defer cleanupDir()

	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		ExternalIODir: dir,
	})
	defer srv.Stopper().Stop(context.Background())
	sqlDB := sqlutils.MakeSQLRunner(db)
	t.Run(`relational`, func(t *testing.T) {
		sqlDB.Exec(t, `CREATE TABLE user_info (id int PRIMARY KEY,name string NOT NULL,"comment" string)`)

		// export relational table test.
		sqlDB.ExpectErr(t, `pq: invalid delimiter value: must be only one character`, `EXPORT INTO CSV 'nodelocal://1/' FROM TABLE user_info with delimiter=',,'`)
		sqlDB.ExpectErr(t, `pq: delimiter exceeds the limit of the char type`, `EXPORT INTO CSV 'nodelocal://1/' FROM TABLE user_info with delimiter='中'`)
		sqlDB.ExpectErr(t, `pq: delimiter exceeds the limit of the char type`, `EXPORT INTO CSV 'nodelocal://1/' FROM TABLE user_info with delimiter='😄'`)

		// import relational table test.
		sqlDB.ExpectErr(t, `pq: invalid delimiter value: must be only one character`, `IMPORT INTO user_info CSV DATA ("nodelocal://01/n1.0.csv") with delimiter=',,'`)
		sqlDB.ExpectErr(t, `pq: delimiter exceeds the limit of the char type`, `IMPORT INTO user_info CSV DATA ("nodelocal://01/n1.0.csv") with delimiter='中'`)
		sqlDB.ExpectErr(t, `pq: delimiter exceeds the limit of the char type`, `EXPORT INTO CSV 'nodelocal://1/' FROM TABLE user_info with delimiter='😄'`)

	})
}
