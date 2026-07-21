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

package ddl_test

import (
	"context"
	"fmt"
	"strconv"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/sqlutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var (
	intVal = 0
	strVal = "position_"
)

func prepareTemplateTable(
	t *testing.T, stable *tree.TableName, tags *tree.Tags,
) (*sqlutils.SQLRunner, func(context.Context)) {
	srv, db, _ := serverutils.StartServer(t, base.TestServerArgs{
		UseDatabase: stable.Catalog(),
	})
	stopper := srv.Stopper().Stop
	sqlDB := sqlutils.MakeSQLRunner(db)

	// create ts database
	sqlDB.Exec(t, fmt.Sprintf(`CREATE TS DATABASE %s`, stable.Catalog()))

	// prepare template table
	sql := fmt.Sprintf("CREATE TABLE %s.%s (ts TIMESTAMP NOT NULL, col1 INT, col2 VARCHAR) TAGS (", stable.Catalog(), stable.Table())
	for i, tag := range *tags {
		sql += tag.TagName.String() + " " + tag.TagType.Name()
		if i < len(*tags)-1 {
			sql += ", "
		}
	}
	sql += ")"

	fmt.Println(sql)
	sqlDB.Exec(t, sql)

	return sqlDB, stopper
}

func makeTagValFromStable(tags *tree.Tags) *tree.Tags {
	ret := tree.Tags{}
	for _, tag := range *tags {
		t := tree.Tag{}
		switch tag.TagType {
		case types.Int:
			intVal++
			t.TagVal = tree.NewDInt(tree.DInt(intVal))
		case types.VarChar:
			t.TagVal = tree.NewDString(strVal + strconv.Itoa(intVal))
		}

		ret = append(ret, t)
	}
	return &ret
}

func TestCreateTables(t *testing.T) {
	defer leaktest.AfterTest(t)()
	t.Skip()
	stable := tree.NewTableName("tsdb0000", "stbl0000")
	tags := &tree.Tags{
		{
			TagName: tree.Name("device_id"),
			TagType: types.Int,
		},
		{
			TagName: tree.Name("position"),
			TagType: types.VarChar,
		},
	}
	runner, stopper := prepareTemplateTable(t, stable, tags)
	defer stopper(context.Background())

	t.Run("test1", func(t *testing.T) {
		// child tables
		childs := tree.InsTableDefs{
			{
				Name:        tree.MakeTableName("", "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
			{
				Name:        tree.MakeTableName("", "c2"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
		}
		// sql
		createSQL := "CREATE TABLE "
		for _, tbl := range childs {
			createSQL += tbl.Name.Table() + " USING " + tbl.UsingSource.Table() + " TAGS ("
			for i, tag := range tbl.Tags {
				createSQL += tag.TagVal.String()
				if i < len(tbl.Tags)-1 {
					createSQL += ", "
				}
			}
			createSQL += ") "
		}
		dropSQL := "DROP TABLE "
		for i, tbl := range childs {
			dropSQL += tbl.Name.Table()
			if i < len(childs)-1 {
				dropSQL += ", "
			}
		}
		// execution
		rows := runner.Query(t, createSQL)
		defer rows.Close()
		defer runner.Query(t, dropSQL)
		// judge
		c, f, s := 0, 0, 0
		for rows.Next() {
			if err := rows.Scan(&c, &f, &s); err != nil {
				t.Fatal(err)
			}
			if c != len(childs) || f != 0 || s != 0 {
				t.Fatal("unexpected result set!")
			}
			fmt.Printf("created: %d, failed: %d, skipped: %d\n", c, f, s)
		}
	})

	t.Run("test2", func(t *testing.T) {
		// child tables
		childs := tree.InsTableDefs{
			{
				Name:        tree.MakeTableName(stable.CatalogName, "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
			{
				Name:        tree.MakeTableName(stable.CatalogName, "c2"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
		}
		// sql
		createSQL := "CREATE TABLE "
		for _, tbl := range childs {
			createSQL += tbl.Name.FQString() + " USING " + tbl.UsingSource.Table() + " TAGS ("
			for i, tag := range tbl.Tags {
				createSQL += tag.TagVal.String()
				if i < len(tbl.Tags)-1 {
					createSQL += ", "
				}
			}
			createSQL += ") "
		}
		dropSQL := "DROP TABLE "
		for i, tbl := range childs {
			dropSQL += tbl.Name.FQString()
			if i < len(childs)-1 {
				dropSQL += ", "
			}
		}
		// execution
		rows := runner.Query(t, createSQL)
		defer rows.Close()
		defer runner.Query(t, dropSQL)
		c, f, s := 0, 0, 0
		for rows.Next() {
			if err := rows.Scan(&c, &f, &s); err != nil {
				t.Fatal(err)
			}
			if c != len(childs) || f != 0 || s != 0 {
				t.Fatal("unexpected result set!")
			}
			fmt.Printf("created: %d, failed: %d, skipped: %d\n", c, f, s)
		}
	})

	t.Run("test3", func(t *testing.T) {
		// child tables
		childs := tree.InsTableDefs{
			{
				Name:        tree.MakeTableName(stable.CatalogName, "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
			{
				Name:        tree.MakeTableName("nodb", "c2"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
		}
		// sql
		createSQL := "CREATE TABLE "
		for _, tbl := range childs {
			createSQL += tbl.Name.FQString() + " USING " + tbl.UsingSource.Table() + " TAGS ("
			for i, tag := range tbl.Tags {
				createSQL += tag.TagVal.String()
				if i < len(tbl.Tags)-1 {
					createSQL += ", "
				}
			}
			createSQL += ") "
		}
		dropSQL := "DROP TABLE "
		childs = childs[:1]
		for i, tbl := range childs {
			dropSQL += tbl.Name.FQString()
			if i < len(childs)-1 {
				dropSQL += ", "
			}
		}
		// execution
		rows := runner.Query(t, createSQL)
		defer rows.Close()
		defer runner.Query(t, dropSQL)
		c, f, s := 0, 0, 0
		for rows.Next() {
			if err := rows.Scan(&c, &f, &s); err != nil {
				t.Fatal(err)
			}
			if c != 1 || f != 1 || s != 0 {
				t.Fatal("unexpected result set!")
			}
			fmt.Printf("created: %d, failed: %d, skipped: %d\n", c, f, s)
		}
	})

	t.Run("test4", func(t *testing.T) {
		// child tables
		childs := tree.InsTableDefs{
			{
				Name:        tree.MakeTableName("", "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
			{
				Name:        tree.MakeTableName("", "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
		}
		// sql
		createSQL := "CREATE TABLE "
		for _, tbl := range childs {
			createSQL += tbl.Name.Table() + " USING " + tbl.UsingSource.Table() + " TAGS ("
			for i, tag := range tbl.Tags {
				createSQL += tag.TagVal.String()
				if i < len(tbl.Tags)-1 {
					createSQL += ", "
				}
			}
			createSQL += ") "
		}
		dropSQL := "DROP TABLE "
		childs = childs[:1]
		for i, tbl := range childs {
			dropSQL += tbl.Name.Table()
			if i < len(childs)-1 {
				dropSQL += ", "
			}
		}
		// execution
		rows := runner.Query(t, createSQL)
		defer rows.Close()
		defer runner.Query(t, dropSQL)
		c, f, s := 0, 0, 0
		for rows.Next() {
			if err := rows.Scan(&c, &f, &s); err != nil {
				t.Fatal(err)
			}
			if c != 1 || f != 0 || s != 1 {
				t.Fatal("unexpected result set!")
			}
			fmt.Printf("created: %d, failed: %d, skipped: %d\n", c, f, s)
		}
	})

	t.Run("test5", func(t *testing.T) {
		// child tables
		childs := tree.InsTableDefs{
			{
				Name:        tree.MakeTableName("", "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
			{
				Name:        tree.MakeTableName("", "c1"),
				UsingSource: *stable,
				Tags:        *makeTagValFromStable(tags),
			},
		}
		// sql
		createSQL := "CREATE TABLE "
		for _, tbl := range childs {
			createSQL += tbl.Name.Table() + " USING " + tbl.UsingSource.Table() + " TAGS ("
			for i, tag := range tbl.Tags {
				createSQL += tag.TagVal.String()
				if i < len(tbl.Tags)-1 {
					createSQL += ", "
				}
			}
			createSQL += ") "
		}
		dropSQL := "DROP TABLE c1"
		// execution
		runner.Query(t, "CREATE TABLE c1 USING stbl0000 TAGS (1, '1')")
		rows := runner.Query(t, createSQL)
		defer rows.Close()
		defer runner.Query(t, dropSQL)
		c, f, s := 0, 0, 0
		for rows.Next() {
			if err := rows.Scan(&c, &f, &s); err != nil {
				t.Fatal(err)
			}
			if c != 0 || f != 0 || s != 2 {
				t.Fatal("unexpected result set!")
			}
			fmt.Printf("created: %d, failed: %d, skipped: %d\n", c, f, s)
		}
	})
}
