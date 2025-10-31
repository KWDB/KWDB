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

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/base"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/testutils/serverutils"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

type TestbaseResponse struct {
	Code int `json:"code"`
}

// TestRestfulInfluxdb tests Influxdb protocol
func TestRestfulInfluxdb(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	testCases := []struct {
		influxdb string
		insert   string
		create   string
	}{
		{
			influxdb: `iot,tag1=iotpc01 e1=1`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" float8 column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01',1)`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" float8)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1=1u`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" int8 column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01',1)`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" int8)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1=1i`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" int8 column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01',1)`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" int8)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1=false`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" bool column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01',false)`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" bool)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1="test="`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','test=')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1="tes,t"`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','tes,t')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 e1="tes"t"`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','tes"t')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,tag1=iotpc01 1e="test"`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"1e" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','test')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"1e" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,Tag1=iotpc01 e1="test"`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"Tag1" varchar tag,"e1" varchar column) values('3077b62fad6c28bcd808677790d93d60ec81a977229dfad67b6e2623e1a81540',now(),'iotpc01','test')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"Tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `iot,$tag1=iotpc01 e1="test"`,
			insert:   `insert without schema into "iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"$tag1" varchar tag,"e1" varchar column) values('87c29612f40d97b03df6097ca26d358578519d1f8aa5dc08e84a3679eabd74d5',now(),'iotpc01','test')`,
			create:   `create table "iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"$tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `i.ot,tag1=iotpc01 e1="test"`,
			insert:   `insert without schema into "i.ot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','test')`,
			create:   `create table "i.ot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `Iot,tag1=iotpc01 e1="test"`,
			insert:   `insert without schema into "Iot"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"tag1" varchar tag,"e1" varchar column) values('7ec78703182df4f0a72fa3b8d1abaae021ff1ff8a66f8fadf0534f09099739cf',now(),'iotpc01','test')`,
			create:   `create table "Iot"(k_timestamp timestamptz(9) not null,"e1" varchar)tags(primary_tag varchar not null,"tag1" varchar)primary tags(primary_tag)`,
		},
		{
			influxdb: `'t16','tag1'=testcase1 'e1'=27451392u 1687898000000000000`,
			insert:   `insert without schema into "'t16'"(primary_tag varchar tag,k_timestamp timestamptz(9) column,"'tag1'" varchar tag,"'e1'" int8 column) values('6a40a689be620aa76a0c477f15771544f4bcb154e0cb3f2704ae03a5733d34f2',1687898000000000000,'testcase1',27451392)`,
			create:   `create table "'t16'"(k_timestamp timestamptz(9) not null,"'e1'" int8)tags(primary_tag varchar not null,"'tag1'" varchar)primary tags(primary_tag)`,
		},
	}
	for i, test := range testCases {
		t.Run(test.influxdb, func(t *testing.T) {
			insert, create := makeInfluxDBStmt(ctx, test.influxdb)
			if insert != test.insert {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.insert, insert)
			}
			if create != test.create {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.create, create)
			}
		})
	}
}

// TestRestfulopentsdbTelnet tests opentsdbTelnet protocol
func TestRestfulopentsdbTelnet(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	testCases := []struct {
		opentsdbtelnet string
		insert         string
		create         string
		err            string
	}{
		{
			opentsdbtelnet: `meters.current groupid=3`,
			insert:         ``,
			create:         ``,
			err:            `missing indicator items`,
		},
		{
			opentsdbtelnet: `meters.current 1648432611000000 groupid=3`,
			insert:         ``,
			create:         ``,
			err:            `value is not float type`,
		},
		{
			opentsdbtelnet: `meters.current 1648432611000000 test groupid=3`,
			insert:         ``,
			create:         ``,
			err:            `value is not float type`,
		},
		{
			opentsdbtelnet: `meters.current 1648432611000000 false groupid=3`,
			insert:         ``,
			create:         ``,
			err:            `value is not float type`,
		},
		{
			opentsdbtelnet: `meters.current 1648432611000000 11.3 groupid=3`,
			insert:         `insert without schema into "meters.current"(k_timestamp timestamptz column, value float8 column, primary_tag varchar tag,"groupid" varchar tag)values(1648432611000,11.3,'32137862b4162d271e2cd50990d4e1f4dd61444264fc49dc9567f62a3b5d746c','3')`,
			create:         `create table "meters.current"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"groupid" varchar)primary tags(primary_tag)`,
			err:            ``,
		},
		{
			opentsdbtelnet: `meters.current 1648432611000000 11.3 Groupid=3`,
			insert:         `insert without schema into "meters.current"(k_timestamp timestamptz column, value float8 column, primary_tag varchar tag,"Groupid" varchar tag)values(1648432611000,11.3,'c9c808b25ab40ef99416cc3153bef67e35ad3d27cc413f8715beaf37298a058c','3')`,
			create:         `create table "meters.current"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"Groupid" varchar)primary tags(primary_tag)`,
			err:            ``,
		},
		{
			opentsdbtelnet: `meters.current 1648432611 11.3 11.4 location=California.LosAngeles groupid=3`,
			insert:         ``,
			create:         ``,
			err:            `value is not just a column`,
		},
	}
	for i, test := range testCases {
		t.Run(test.opentsdbtelnet, func(t *testing.T) {
			insert, create, err := makeOpenTSDBTelnet(ctx, test.opentsdbtelnet)
			if !testutils.IsError(err, test.err) {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.err, err)
			}
			if insert != test.insert {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.insert, insert)
			}
			if create != test.create {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.create, create)
			}
		})
	}
}

// TestRestfulopentsdbJson tests opentsdbJson protocol
func TestRestfulopentsdbJson(t *testing.T) {
	defer leaktest.AfterTest(t)()
	ctx := context.TODO()
	testCases := []struct {
		opentsdbJSON string
		stmt         map[string]string
		err          string
	}{
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice","timestamp": 13468146400,"value": 11,"tags": {"host": false}}]`,
			stmt:         map[string]string{},
			err:          `tags type is not support`,
		},
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice","value": 11,"tags": {"host": "kaiwudb01"}}]`,
			stmt:         map[string]string{},
			err:          `timestamp or value column is nil`,
		},
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice", "timestamp": 13468146400, "timestamp": 13468146400,"value": 11,"tags": {"host": "kaiwudb01"}}]`,
			stmt: map[string]string{
				`insert without schema into "sys.cpu.nice"(primary_tag varchar tag,value float8 column,k_timestamp timestamptz column,"host" varchar tag)values('e5d90e9d5a35edf70b4edf0ce0d4c16c44448e74466644086fc6f94e6c421b9f',11,1346814640000,'kaiwudb01')`: `create table "sys.cpu.nice"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"host" varchar)primary tags(primary_tag)`},
			err: ``,
		},
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice", "timestamp": 13468146400, "timestamp": 13468146400,"value": 11,"value": 11,"tags": {"host": "kaiwudb01"}}]`,
			stmt: map[string]string{
				`insert without schema into "sys.cpu.nice"(primary_tag varchar tag,value float8 column,k_timestamp timestamptz column,"host" varchar tag)values('e5d90e9d5a35edf70b4edf0ce0d4c16c44448e74466644086fc6f94e6c421b9f',11,1346814640000,'kaiwudb01')`: `create table "sys.cpu.nice"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"host" varchar)primary tags(primary_tag)`},
			err: ``,
		},
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice", "timestamp": 13468146400, "value": 11,"tags": {"host": 15}}]`,
			stmt: map[string]string{
				`insert without schema into "sys.cpu.nice"(primary_tag varchar tag,value float8 column,k_timestamp timestamptz column,"host" float8 tag)values('3acc4dcaa49696f23c82892b5fc9191e76d355136c48fe5fabe34ee80c564988',11,1346814640000,15)`: `create table "sys.cpu.nice"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"host" float8)primary tags(primary_tag)`},
			err: ``,
		},
		{
			opentsdbJSON: `[{"metric": "sys.cpu.nice", "timestamp": 13468146400, "value": 11,"tags": {"%host": 15}}]`,
			stmt: map[string]string{
				`insert without schema into "sys.cpu.nice"(primary_tag varchar tag,value float8 column,k_timestamp timestamptz column,"%host" float8 tag)values('bf4a127dfe4c09a8897b46204ebf30beb20c2f78e62129eedb4bb8089b878800',11,1346814640000,15)`: `create table "sys.cpu.nice"(k_timestamp timestamptz not null, value float8 not null)Tags(primary_tag varchar not null,"%host" float8)primary tags(primary_tag)`},
			err: ``,
		},
	}
	for i, test := range testCases {
		t.Run(test.opentsdbJSON, func(t *testing.T) {
			stmt, err := makeOpenTSDBJson(ctx, test.opentsdbJSON)
			if !testutils.IsError(err, test.err) {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.err, err)
			}
			if !reflect.DeepEqual(stmt, test.stmt) {
				t.Errorf("case%d fail: expected %s error, got %v", i+1, test.stmt, stmt)
			}
		})
	}
}

// TestHttpRestfulApi tests Restful Api http connect
func TestHttpRestfulApi(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	if _, err := db.Exec("CREATE USER u1 PASSWORD 'Znbase@2024'"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("grant admin to u1"); err != nil {
		t.Fatal(err)
	}
	defer s.Stopper().Stop(context.TODO())
	client, err := s.GetAdminAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		url          string
		method       string
		responseCode int
	}{
		{
			url:          s.AdminURL() + "/restapi/login",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/influxdb",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/opentsdbjson",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/opentsdbtelnet",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/insert",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/ddl",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/telegraf",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/query",
			method:       "POST",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/session",
			method:       "GET",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/session",
			method:       "DELETE",
			responseCode: http.StatusOK,
		},
		{
			url:          s.AdminURL() + "/restapi/error",
			method:       "GET",
			responseCode: http.StatusNotFound,
		},
	}
	for _, test := range testCases {
		req, err := http.NewRequest(test.method, test.url, nil)
		if err != nil {
			t.Fatalf("Failed to create request %s: %v", test.url, err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Basic dTE6Wm5iYXNlQDIwMjQ=")

		response, err := client.Do(req)
		if err != nil {
			t.Fatalf("Failed to send request %s: %v", test.url, err)
		}
		response.Body.Close()
		if response.StatusCode != test.responseCode {
			t.Errorf("test case %s:Unexpected status code: %d, response: %d", test.url, response.StatusCode, test.responseCode)
		}
	}
}

// TestRestfulApi tests Restful Api
func TestRestfulApi(t *testing.T) {
	defer leaktest.AfterTest(t)()
	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
	if _, err := db.Exec("CREATE USER u1 PASSWORD 'Znbase@2024'"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("grant admin to u1"); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Exec("create database db_znbase"); err != nil {
		t.Fatal(err)
	}

	defer s.Stopper().Stop(context.TODO())
	client, err := s.GetAdminAuthenticatedHTTPClient()
	if err != nil {
		t.Fatal(err)
	}
	testCases := []struct {
		url  string
		code int
		sql  string
	}{
		{
			url:  s.AdminURL() + "/restapi/query",
			code: RestfulResponseCodeSuccess,
			sql:  "show databases;",
		},
		{
			url:  s.AdminURL() + "/restapi/query",
			code: RestfulResponseCodeFail,
			sql:  "create database d1;",
		},
		{
			url:  s.AdminURL() + "/restapi/query",
			code: RestfulResponseCodeFail,
			sql:  "drop database d1;",
		},
		{
			url:  s.AdminURL() + "/restapi/ddl",
			code: RestfulResponseCodeSuccess,
			sql:  "create table t1(a int);",
		},
		{
			url:  s.AdminURL() + "/restapi/insert",
			code: RestfulResponseCodeSuccess,
			sql:  "insert into t1 values(1);",
		},
		{
			url:  s.AdminURL() + "/restapi/query?db=DB_ZNBASE",
			code: RestfulResponseCodeFail,
			sql:  "show tables;",
		},
		{
			url:  s.AdminURL() + "/restapi/query?db=db_znbase",
			code: RestfulResponseCodeSuccess,
			sql:  "show tables;",
		},
	}
	for _, test := range testCases {
		req, err := http.NewRequest("POST", test.url, bytes.NewBufferString(test.sql))
		if err != nil {
			t.Fatalf("test case %s: Failed to create request: %v", test.url, err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Authorization", "Basic dTE6Wm5iYXNlQDIwMjQ=")

		response, err := client.Do(req)
		if err != nil {
			t.Fatalf("test case %s: Failed to send request: %v", test.url, err)
		}
		body, err := ioutil.ReadAll(response.Body)
		if err != nil {
			t.Fatalf("test case %s:Failed to read response body: %v", test.url, err)
		}
		response.Body.Close()

		var result TestbaseResponse
		err = json.Unmarshal(body, &result)
		if err != nil {
			t.Fatalf("test case %s: Failed to unmarshal: %v", test.url, err)
		}
		if result.Code != test.code {
			t.Errorf("test case %s: Unexpected code: %d, response: %d", test.url, result.Code, test.code)
		}
	}
}

// TestExecuteWithRetry tests Restful Api
//func TestExecuteWithRetry(t *testing.T) {
//	defer leaktest.AfterTest(t)()
//	s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
//	defer s.Stopper().Stop(context.TODO())
//
//	testCases := []struct {
//		insert string
//		create string
//		err    string
//	}{
//		{
//			insert: `insert into t1(a) values(1)`,
//			create: `create table t1(a int)`,
//			err:    ``,
//		},
//		{
//			insert: `insert into t2(a) values(1)`,
//			create: `create table t2(a float)`,
//			err:    ``,
//		},
//		{
//			insert: `insert into t3(a, b) values(1, 2)`,
//			create: `create table t3(a float)`,
//			err:    `pq: column "b" does not exist`,
//		},
//		{
//			insert: `insert into t4(a) values(false)`,
//			create: `create table t4(a float)`,
//			err:    `pq: value type bool doesn't match type float of column "a"`,
//		},
//	}
//	for _, test := range testCases {
//		_, err := executeWithRetry(db, test.insert, test.create)
//		if !testutils.IsError(err, test.err) {
//			t.Errorf("expected %s error, got %v", test.err, err)
//		}
//	}
//}
