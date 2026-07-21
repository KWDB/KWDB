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

package sql

import (
	"bytes"
	gosql "database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/cdc/cdcpb"
	"github.com/stretchr/testify/require"
)

const TestKafkaHost = "localhost:9099"

// AssertMessage assert pipe message.
func AssertMessage(t *testing.T, expect string, actor []byte) {
	if !bytes.Equal([]byte(expect), actor) {
		t.Fatalf("expect:%s actor:%s", expect, string(actor))
	}
}

// PipeTest is pipe unit test.
func PipeTest(t *testing.T, db *gosql.DB) {
	var expectMsg []string
	var expectMsg1 []string
	cdcpb.MockData = make(map[string][][]byte)
	_, err := db.Exec(`set cluster setting server.tsinsert_direct.enabled = false`)
	require.NoError(t, err)

	// create a test database and tables
	_, err = db.Exec(`CREATE TS DATABASE d1`)
	require.NoError(t, err)
	_, err = db.Exec(`CREATE TABLE d1.ts(
k_timestamp timestamptz not null,a1 int2,a2 int4,a3 int8,a4 float4,a5 float8,a6 double,a7 char,a8 char(10),a9 nchar,
a10 nchar(10),a11 bool,a12 varbytes,a13 varbytes(10),a14 timestamp,a15 varchar(100))
tags(t1 int2 not null,t2 int4,t3 int8,t4 float4,t5 float8,t6 double,t7 char(1),t8 char(10),
t9 nchar(1),t10 nchar(10),t11 bool,t12 varbytes(1),t13 varbytes(10),t14 varchar(100)) primary tags(t1)`)
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`CREATE PIPE p1 FOR table d1.ts(*) WITH options(enable='on',buffer_size='0',sink='mock://%s?topic_name=test_topic');`, TestKafkaHost))
	require.NoError(t, err)

	_, err = db.Exec(fmt.Sprintf(`CREATE PIPE p2 FOR table d1.ts(*) where a14='2011-11-11 11:11:11' WITH options(enable='on',buffer_size='0',sink='mock://%s?topic_name=test_topic1');`, TestKafkaHost))
	require.NoError(t, err)

	_, err = db.Exec(`CREATE TABLE d1.ts1 (ts timestamp not null, usage_system int8) tags(hostname varchar(30) not null, tag1 int) primary tags (hostname)`)
	require.NoError(t, err)
	_, err = db.Exec(fmt.Sprintf(`create pipe p3 for table d1.ts1(*) where usage_system>95 with options(enable='on',buffer_size='0',sink='mock://%s?topic_name=test_topic3');`, TestKafkaHost))
	require.NoError(t, err)
	time.Sleep(time.Second * 2)

	_, err = db.Exec(`INSERT INTO d1.ts values
			('2018-10-10 10:00:00',1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','2011-11-11 11:11:11','test时间精度通用查询测试！！！@TEST1',1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','test时间精度通用查询测试！！！@TEST1')`)
	expectMsg = append(expectMsg, `[1539165600000,1,2,3,4.4,5.5,6.6,"a","aaaaaaaaaa","a","aaaaaaaaaa",true,"\\xaa","\\xaa",1321009871000,"test时间精度通用查询测试！！！@TEST1",1,2,3,4.4,5.5,6.6,"a","aaaaaaaaaa","a","aaaaaaaaaa",true,"\\xaa","\\xaa","test时间精度通用查询测试！！！@TEST1"]`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO d1.ts values
		('2018-10-10 10:00:10',null,2,null,4.4,null,6.6,null,'aaaaaaaaaa',null,'aaaaaaaaaa',null,b'\xaa',null,'2011-11-11 11:11:11',null,2,null,3,null,5.5,null,'a',null,'a',null,true,null,b'\xaa',null)`)
	require.NoError(t, err)
	expectMsg = append(expectMsg, `[1539165610000,null,2,null,4.4,null,6.6,null,"aaaaaaaaaa",null,"aaaaaaaaaa",null,"\\xaa",null,1321009871000,null,2,null,3,null,5.5,null,"a",null,"a",null,true,null,"\\xaa",null]`)

	_, err = db.Exec(`INSERT INTO d1.ts values
		('2018-10-10 10:00:20',1,null,3,null,5.5,null,'a',null,'a',null,true,null,b'\xaa',null,'test时间精度通用查询测试！！！@TEST1',3,2,null,4.4,null,6.6,null,'aaaaaaaaaa',null,'aaaaaaaaaa',null,b'\xaa',null,'test时间精度通用查询测试！！！@TEST1')`)
	expectMsg = append(expectMsg, `[1539165620000,1,null,3,null,5.5,null,"a",null,"a",null,true,null,"\\xaa",null,"test时间精度通用查询测试！！！@TEST1",3,2,null,4.4,null,6.6,null,"aaaaaaaaaa",null,"aaaaaaaaaa",null,"\\xaa",null,"test时间精度通用查询测试！！！@TEST1"]`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO d1.ts values
			('2018-10-10 10:00:30',900,50000,-1000,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','2011-11-11 11:11:11','test时间精度通用查询测试！！！@TEST1',1,2,3,4.4,5.5,6.6,'a','aaaaaaaaaa','a','aaaaaaaaaa',true,b'\xaa',b'\xaa','test时间精度通用查询测试！！！@TEST1')`)
	expectMsg = append(expectMsg, `[1539165630000,900,50000,-1000,4.4,5.5,6.6,"a","aaaaaaaaaa","a","aaaaaaaaaa",true,"\\xaa","\\xaa",1321009871000,"test时间精度通用查询测试！！！@TEST1",1,2,3,4.400000095367432,5.5,6.6,"a","aaaaaaaaaa","a","aaaaaaaaaa",true,"\\xaa","\\xaa","test时间精度通用查询测试！！！@TEST1"]`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO d1.ts (k_timestamp, a1,a12, t1) values
		('2018-10-11 10:00:00',1,b'\xaa',10),('2018-10-11 10:00:10',2,null,10),('2018-10-11 10:00:20',null,b'\xaa',10),('2018-10-11 10:00:30',null,null,10),('2018-10-11 10:00:40',1,b'\xaa',10)`)
	expectMsg = append(expectMsg, `[1539252000000,1,null,null,null,null,null,null,null,null,null,null,"\\xaa",null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539252010000,2,null,null,null,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539252020000,null,null,null,null,null,null,null,null,null,null,null,"\\xaa",null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539252030000,null,null,null,null,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539252040000,1,null,null,null,null,null,null,null,null,null,null,"\\xaa",null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null]`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO d1.ts (k_timestamp, a1,a4, t1) values
		($1,$2,$3,$4),($5,$6,$7,$8)`,
		"2018-10-12 10:00:00", 1, 4.4, 10, "2018-10-12 10:00:10", 2, 5.5, 10)
	expectMsg = append(expectMsg, `[1539338400000,1,null,null,4.4,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539338410000,2,null,null,5.5,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null]`)
	require.NoError(t, err)

	_, err = db.Exec(`INSERT INTO d1.ts (k_timestamp, a1,a4, t1) values
		($1,$2,$3,$4),($5,$6,$7,$8)`,
		"2018-10-12 10:00:00", 1, 4.4, 10, "2018-10-12 10:00:10", 2, 5.5, 10)
	expectMsg = append(expectMsg, `[1539338400000,1,null,null,4.4,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null],[1539338410000,2,null,null,5.5,null,null,null,null,null,null,null,null,null,null,null,10,null,null,null,null,null,null,null,null,null,null,null,null,null]`)
	require.NoError(t, err)

	const startTime = 17360861000001
	sql := "INSERT INTO d1.ts1(ts, usage_system, hostname) VALUES"
	for i := 0; i < 100; i++ {
		if i > 0 {
			sql += ","
		}
		sql += fmt.Sprintf(`(%d, %d, 'host_%d')`, int64(startTime+i*1000), i, i)
		if i > 95 {
			expectMsg1 = append(expectMsg1, fmt.Sprintf(`[%d,%d,"host_%d",null]`, startTime+i*1000, i, i))
		}
	}
	_, err = db.Exec(sql)
	require.NoError(t, err)

	rows, err := db.Query(`SELECT name,status FROM [show pipes]`)
	require.NoError(t, err)

	i := 1
	for rows.Next() {
		var name, status string
		if err = rows.Scan(&name, &status); err != nil {
			require.NoError(t, err)
		}
		require.Equal(t, fmt.Sprintf("p%d", i), name)
		require.Equal(t, "Enable", status)
		i++
	}

	expectJSON := `{"kind": "insert","database": "d1","schema": "public","table": "ts","columnnames": ["k_timestamp","a1","a2","a3","a4","a5","a6","a7","a8","a9","a10","a11","a12","a13","a14","a15","t1","t2","t3","t4","t5","t6","t7","t8","t9","t10","t11","t12","t13","t14"],"columntypes": ["TIMESTAMPTZ(3)","INT2","INT4","INT8","FLOAT4","FLOAT8","FLOAT8","CHAR","CHAR(10)","NCHAR","NCHAR(10)","BOOL","VARBYTES(254)","VARBYTES(10)","TIMESTAMP(3)","VARCHAR(100)","INT2","INT4","INT8","FLOAT4","FLOAT8","FLOAT8","CHAR","CHAR(10)","NCHAR","NCHAR(10)","BOOL","VARBYTES(1)","VARBYTES(10)","VARCHAR(100)"],"columnvalues": [`

	time.Sleep(time.Second * 1)
	require.EqualValues(t, 7, len(cdcpb.MockData["test_topic"]))
	for i, v := range cdcpb.MockData["test_topic"] {
		AssertMessage(t, expectJSON+expectMsg[i]+"]}", v)
		var data map[string]interface{}
		err := json.Unmarshal(v, &data)
		require.NoError(t, err)
	}
	require.EqualValues(t, 3, len(cdcpb.MockData["test_topic1"]))

	_, err = db.Exec(`DROP PIPE p1`)
	require.NoError(t, err)

	_, err = db.Exec(`DROP PIPE p2`)
	require.NoError(t, err)
	_, err = db.Exec(`DROP PIPE p3`)
	require.NoError(t, err)
	_, err = db.Exec(`drop database d1 cascade;`)
	require.NoError(t, err)
}
