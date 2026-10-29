// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mlagent"
	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

const (
	address  = "127.0.0.1:8972"
	testUser = "testuser"
)

type mlTestCase struct {
	sql           string
	expected      [][]string
	expectedError string
	user          string
}

var createTestTable = `create table %s
(
    timestamp                        text,
    Active_Energy_Delivered_Received bigint,
    Current_Phase_Average            numeric,
    Active_Power                     numeric,
    Performance_Ratio                numeric,
    Wind_Speed                       numeric,
    Weather_Temperature_Celsius      numeric,
    Weather_Relative_Humidity        numeric,
    Global_Horizontal_Radiation      numeric,
    Diffuse_Horizontal_Radiation     numeric,
    Wind_Direction                   numeric,
    Weather_Daily_Rainfall           bigint,
    Radiation_Global_Tilted          numeric,
    Radiation_Diffuse_Tilted         numeric
);`

var baseCases = []mlTestCase{
	{
		`SELECT kwdbml.import_model('1','2','3','4','5','{}','{}',b'8',b'9','{}');`,
		[][]string{},
		`pq: kwdbml.import_model\(\): KaiwuDB ML setting 'ml.agent.addr' does not exist`,
		security.RootUser,
	},
	{
		fmt.Sprintf(`SET CLUSTER SETTING ml.agent.addr = '%s';`, address),
		[][]string{},
		"",
		security.RootUser,
	},
	{
		fmt.Sprintf(createTestTable, "kwdb_inferencing_test"),
		[][]string{},
		"",
		security.RootUser,
	},
	{
		"insert into kwdb_inferencing_test values('167000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
		[][]string{},
		"",
		security.RootUser,
	},
	{
		"insert into kwdb_inferencing_test values('168000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
		[][]string{},
		"",
		security.RootUser,
	},
	{
		"CREATE DATABASE kwdb",
		[][]string{},
		"",
		security.RootUser,
	},
	{
		"CREATE USER kwdbmlusr",
		[][]string{},
		"",
		security.RootUser,
	},
}

var testModelCases = []mlTestCase{
	{
		`SELECT kwdbml.import_model( 
		'PowerGen', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
		[][]string{{"public.powergen,1"}},
		"",
		security.RootUser,
	},
	{
		`SELECT kwdbml.import_model( 
		'public.PowerGen', 
		'description for ML model PowerGen v2', 
		'R', 
		'tensorflow_2.9', 
		'Python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v2',
		b'transform',
		'[{"objective":"mse", "score":5.8}]');`,
		[][]string{{"public.powergen,2"}},
		"",
		security.RootUser,
	},
	{
		`SELECT kwdbml.import_model( 
		'PowerGen', 
		'description for ML model PowerGen v3', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v3',
		b'transform',
		'[{"objective":"mse", "score":5.9}]');`,
		[][]string{{"public.powergen,3"}},
		"",
		security.RootUser,
	},
	{
		`SELECT kwdbml.import_model( 
		'PowerGen2', 
		'description for ML model PowerGen2 v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		null);`,
		[][]string{{"public.powergen2,1"}},
		"",
		security.RootUser,
	},
}

var testErrorModelCases = []mlTestCase{
	{
		fmt.Sprintf(`SELECT kwdbml.import_model( 
		'%s', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`, mlagent.TestModelOutputNull),
		[][]string{{fmt.Sprintf("public.%s,1", mlagent.TestModelOutputNull)}},
		"",
		security.RootUser,
	},
	{
		fmt.Sprintf(`SELECT kwdbml.import_model( 
		'%s', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`, mlagent.TestModelStatusNull),
		[][]string{{fmt.Sprintf("public.%s,1", mlagent.TestModelStatusNull)}},
		"",
		security.RootUser,
	},
	{
		fmt.Sprintf(`SELECT schema,name,status
						FROM kwdbml.show_models() WHERE name in ('%s', '%s') ORDER BY name;`,
			mlagent.TestModelOutputNull,
			mlagent.TestModelStatusNull),
		[][]string{
			{
				"public", mlagent.TestModelOutputNull, `{"message": "failed to get service output", "status": "Error"}`,
			},
			{
				"public", mlagent.TestModelStatusNull, `{"message": "the KaiwuDB ML model status is empty", "status": "Error"}`,
			},
		},
		"",
		security.RootUser,
	},
}

func doTest(_ *testing.T, _ []mlTestCase) {
	/* ml related tables have been changed to user table, these tests have no sense.
	ctx := context.Background()
	s, sqlConn, _ := serverutils.StartServer(t, base.TestServerArgs{})
	defer sqlConn.Close()

	tlsConfig, _ := s.RPCContext().GetClientTLSConfig()
	mls := mlagent.StartMLTestServer(t, address, tlsConfig)

	defer mls.Stop()
	defer s.Stopper().Stop(ctx)

	dbs := make(map[string]*sqlutils.SQLRunner, 2)
	dbs[security.RootUser] = sqlutils.MakeSQLRunner(sqlConn)
	dbs[security.RootUser].Exec(t, `CREATE USER $1`, testUser)
	pgURL, cleanupFunc := sqlutils.PGUrl(t, s.ServingSQLAddr(), t.Name(), url.User(testUser))
	defer cleanupFunc()
	testDB, err := gosql.Open("postgres", pgURL.String())
	if err != nil {
		t.Fatal(err)
	}
	dbs[testUser] = sqlutils.MakeSQLRunner(testDB)
	defer func() {
		_ = testDB.Close()
	}()
	for _, tc := range testCases {
		if tc.expectedError != "" {
			dbs[tc.user].ExpectErr(t, tc.expectedError, tc.sql)
		} else {
			dbs[tc.user].CheckQueryResults(t, tc.sql, tc.expected)
		}
	}*/
}

func TestMLImportModel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.import_model('1','2','3','4','5','{}','{}',b'8',b'9','{}','11');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): unknown signature: kwdbml.import_model\(STRING, STRING, STRING, STRING, STRING, JSONB, JSONB, BYTES, BYTES, JSONB, JSONB\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('1','2','3','4','5',null,'{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): unknown signature: kwdbml.import_model\(\) the 6 arg not null`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('','2','3','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('create','2','3','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): model name 'create' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('kwdbml_test.powergen','2','3','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): schema 'kwdbml_test' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.model_name_length_more_than_max_characters','2','3','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			fmt.Sprintf(
				`pq: kwdbml.import_model\(\): 'public.model_name_length_more_than_max_characters' must be no more than %d characters`,
				pb.MaxServiceNameLength,
			),
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','3','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): problem type '3' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','4','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): framework '4' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','5','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): runtime '5' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10','{}','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): features '\{\{\}\}' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10','[]','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): features '\{\[\]\}' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10', '[{}]','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): features name '' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10', '[{"name":"Current_Phase_Average", "type":"json"}]','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): features type 'json' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','{}',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): targets '\{\{\}\}' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[]]',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): targets '\{\[\]\}' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{}]',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): targets name '' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"interval"}]',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): targets type 'interval' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',b'9','{}');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): metrics '\{\}' is unsupported`,
			security.RootUser,
		},
		/*{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',b'9',
				'[{}]');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): objective '' is unsupported for the problemType b`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','2','b','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',b'9',
				'[{"objective":"mse", "score":5.7}]');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): objective 'mse' is unsupported for the problemType b`,
			security.RootUser,
		},*/
		{
			`SELECT kwdbml.import_model('public.powergen','version desc 1','B','Tensorflow_2.9','Python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file1',null,
				'[{"objective":"f1", "score":5.7}]');`,
			[][]string{{"public.powergen,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','version desc 1','B','Tensorflow_2.9','Python_3.9',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file1',null,
				'[{"objective":"f1", "score":5.7}]');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): runtime 'python_3.9' is inconsistent with last version 'python_3.10'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','version desc 1','B','Tensorflow_2.9','Python_3.10',
				'[{"name":"Current_Phase_Average1", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file1',null,
				'[{"objective":"f1", "score":5.7}]');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): features '\[\{"name": "Current_Phase_Average1", "type": "float"\}\]' is inconsistent with last version '\[\{"name": "Current_Phase_Average", "type": "float"\}\]'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('public.powergen','version desc 1','B','Tensorflow_2.9','Python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"int"}]',b'file1',null,
				'[{"objective":"f1", "score":5.7}]');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): targets '\[\{"name": "Active_Power", "type": "int"\}\]' is inconsistent with last version '\[\{"name": "Active_Power", "type": "float"\}\]'`,
			testUser,
		},
		{
			`DROP DATABASE defaultdb;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('notexist.powergen_pv','version desc 1','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',b'file3');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): schema 'notexist' does not exist`,
			testUser,
		},
		{
			`SELECT kwdbml.import_model('powergen_pv','version desc 1','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',b'file3');`,
			[][]string{{"public.powergen_pv,1"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.import_model('Public.PowerGen','version desc 2','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',b'file3');`,
			[][]string{},
			`pq: kwdbml.import_model\(\): user 'testuser' does not have 'CREATE' privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model('Public.PowerGen','version desc 2','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',b'file3');`,
			[][]string{{"public.powergen,2"}},
			``,
			testUser,
		},
		{
			`revoke root from testuser`,
			[][]string{},
			``,
			security.RootUser,
		},

		{
			`SELECT kwdbml.import_model('Public.PowerGen','version desc 3','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',
				b'file3', '[]');`,
			[][]string{{"public.powergen,3"}},
			`pq: kwdbml.import_model\(\): user 'testuser' does not have 'CREATE' privilege on model 'public.powergen'`,
			testUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','create');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},

		{
			`SELECT kwdbml.import_model('Public.PowerGen','version desc 3','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',
				b'file3', '[]');`,
			[][]string{{"public.powergen,3"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.import_model('Public.PowerGen','version desc 4','Binary_Classification','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'file2',
				b'file3', NULL);`,
			[][]string{{"public.powergen,4"}},
			"",
			testUser,
		},
		{
			`SELECT schema, name, m.description, v.description, problem_type, features, target, framework, runtime, m.creator,
						 v.creator, version, training_job_id, metrics, entity, md5, transformer_entity, transformer_md5, status
							FROM kwdbml.ml_models m, kwdbml.ml_model_versions v where m.id = v.model_id order by name,version;`,
			[][]string{
				{
					"public", "powergen", "version desc 1", "version desc 1", "b", `[{"name": "Current_Phase_Average", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10", security.RootUser, security.RootUser,
					"1", "NULL", `[{"objective": "f1", "score": 5.7}]`, "\\x66696c6531", "826e8142e6baabe8af779f5f490cf5f5", "NULL", "NULL",
					`{"status": "Initializing"}`,
				},
				{
					"public", "powergen", "version desc 1", "version desc 2", "b", `[{"name": "Current_Phase_Average", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10", security.RootUser, testUser,
					"2", "NULL", "NULL", "\\x66696c6532", "1c1c96fd2cf8330db0bfa936ce82f3b9", "\\x66696c6533", "2548729e9c3c60cc3789dfb2408e475d",
					`{"status": "Initializing"}`,
				},
				{
					"public", "powergen", "version desc 1", "version desc 3", "b", `[{"name": "Current_Phase_Average", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10", security.RootUser, testUser,
					"3", "NULL", "NULL", "\\x66696c6532", "1c1c96fd2cf8330db0bfa936ce82f3b9", "\\x66696c6533", "2548729e9c3c60cc3789dfb2408e475d",
					`{"status": "Initializing"}`,
				},
				{
					"public", "powergen", "version desc 1", "version desc 4", "b", `[{"name": "Current_Phase_Average", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10", security.RootUser, testUser,
					"4", "NULL", "NULL", "\\x66696c6532", "1c1c96fd2cf8330db0bfa936ce82f3b9", "\\x66696c6533", "2548729e9c3c60cc3789dfb2408e475d",
					`{"status": "Initializing"}`,
				},
				{
					"public", "powergen_pv", "version desc 1", "version desc 1", "b", `[{"name": "Current_Phase_Average", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10", testUser, testUser,
					"1", "NULL", "NULL", "\\x66696c6532", "1c1c96fd2cf8330db0bfa936ce82f3b9", "\\x66696c6533", "2548729e9c3c60cc3789dfb2408e475d",
					`{"status": "Initializing"}`,
				},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_models m where m.last_updated > created`,
			[][]string{{"1"}},
			"",
			security.RootUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLSetActiveVersion(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.set_active_version('PowerGen', 5);`,
			[][]string{},
			`pq: kwdbml.set_active_version\(\): the version '5' of model 'public.powergen' does not exist`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.set_active_version('PowerGen', 2);`,
			[][]string{},
			`pq: kwdbml.set_active_version\(\): user 'testuser' does not have 'UPDATE' privilege on model 'PowerGen'`,
			testUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			"",
			security.RootUser,
		},

		{
			`SELECT kwdbml.set_active_version('PowerGen', 2);`,
			[][]string{{"2"}},
			"",
			testUser,
		},
		{
			`revoke root from testuser`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','update');`,
			[][]string{{
				"testuser",
			}},
			``,
			security.RootUser,
		},

		{
			`SELECT kwdbml.set_active_version('PowerGen', 2);`,
			[][]string{{"2"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','testuser','update');`,
			[][]string{{
				"testuser",
			}},
			``,
			security.RootUser,
		},

		{
			`SELECT kwdbml.set_active_version('PowerGen', 2);`,
			[][]string{{"2"}},
			"",
			security.RootUser,
		},
		{
			`SELECT description,status,version FROM kwdbml.show_model('PowerGen') WHERE status='active';`,
			[][]string{
				{"description for ML model PowerGen v2", "active", "2"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.set_active_version('PowerGen', 0);`,
			[][]string{{"3"}},
			"",
			security.RootUser,
		},
		{
			`SELECT description,status,version FROM kwdbml.show_model('PowerGen') WHERE status='active';`,
			[][]string{
				{"description for ML model PowerGen v3", "active", "3"},
			},
			"",
			security.RootUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLShowModel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT * FROM kwdbml.show_model('public.PowerGen', 2, 2);`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_model\(STRING, INT8, INT8\)`,
			security.RootUser,
		},
		{
			`SELECT version FROM kwdbml.show_model('.PowerGen');`,
			[][]string{{"1"}, {"2"}, {"3"}},
			"",
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model('public.');`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model('');`,
			[][]string{},
			"pq: object name is empty",
			security.RootUser,
		},

		{
			`SELECT * FROM kwdbml.show_model('test.PowerGen');`,
			[][]string{},
			`pq: schema 'test' does not exist`,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model('public.PowerGen', -1);`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model('PowerGen', 4);`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,runtime,status,version,
train_data,metrics FROM kwdbml.show_model('public.PowerGen', 3);`,
			[][]string{},
			`pq: user 'testuser' does not have 'SELECT' privilege on model 'public.PowerGen'`,
			testUser,
		},
		{
			`SELECT kwdbml.show_model('public.PowerGen', 3);`,
			[][]string{},
			`pq: user 'testuser' does not have 'SELECT' privilege on model 'public.PowerGen'`,
			testUser,
		},
		{
			`SELECT kwdbml.show_model('public.PowerGen');`,
			[][]string{},
			`pq: user 'testuser' does not have 'SELECT' privilege on model 'public.PowerGen'`,
			testUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,runtime,status,version,
train_data,metrics FROM kwdbml.show_model('public.model_does_not_exist', 3);`,
			[][]string{},
			"",
			testUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,runtime,status,version,
train_data,metrics FROM kwdbml.show_model('public.PowerGen', 3);`,
			[][]string{{"public", "powergen", "description for ML model PowerGen v3", security.RootUser, "r",
				`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
				`[{"name": "Active_Power", "type": "float"}]`,
				"tensorflow_2.9", "python_3.10", "inactive", "3", "NULL", `[{"objective": "mse", "score": 5.9}]`,
			}},
			"",
			testUser,
		},

		{
			`revoke root from testuser`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,runtime,status,version,
train_data,metrics FROM kwdbml.show_model('public.PowerGen', 3);`,
			[][]string{{"public", "powergen", "description for ML model PowerGen v3", security.RootUser, "r",
				`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
				`[{"name": "Active_Power", "type": "float"}]`,
				"tensorflow_2.9", "python_3.10", "inactive", "3", "NULL", `[{"objective": "mse", "score": 5.9}]`,
			}},
			"",
			testUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,runtime,status,version,
train_data,metrics FROM kwdbml.show_model('public.PowerGen', 3);`,
			[][]string{{"public", "powergen", "description for ML model PowerGen v3", security.RootUser, "r",
				`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
				`[{"name": "Active_Power", "type": "float"}]`,
				"tensorflow_2.9", "python_3.10", "inactive", "3", "NULL", `[{"objective": "mse", "score": 5.9}]`,
			}},
			"",
			security.RootUser,
		},

		{
			`SELECT description,status,version,metrics FROM kwdbml.show_model('PowerGen') ORDER BY version;`,
			[][]string{
				{"description for ML model PowerGen v1", "active", "1", `[{"objective": "mse", "score": 5.7}]`},
				{"description for ML model PowerGen v2", "inactive", "2", `[{"objective": "mse", "score": 5.8}]`},
				{"description for ML model PowerGen v3", "inactive", "3", `[{"objective": "mse", "score": 5.9}]`},
			},
			"",
			security.RootUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLShowModels(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			"SELECT * FROM kwdbml.show_models(111);",
			[][]string{},
			`pq: unknown signature: kwdbml.show_models\(INT8\)`,
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
						runtime,active_version,last_metrics,status
						FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{
				{
					"public", "powergen", "description for ML model PowerGen v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", `[{"objective": "mse", "score": 5.7}]`, `{"status": "Ready"}`,
				},
				{
					"public", "powergen2", "description for ML model PowerGen2 v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", "NULL", `{"status": "Ready"}`,
				},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
             runtime,active_version,last_metrics,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{},
			"",
			testUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
            runtime,active_version,last_metrics,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{
				{
					"public", "powergen", "description for ML model PowerGen v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", `[{"objective": "mse", "score": 5.7}]`, `{"status": "Ready"}`,
				},
				{
					"public", "powergen2", "description for ML model PowerGen2 v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", "NULL", `{"status": "Ready"}`,
				},
			},
			"",
			testUser,
		},
		{
			`revoke root from testuser`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
            runtime,active_version,last_metrics,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{
				{
					"public", "powergen", "description for ML model PowerGen v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", `[{"objective": "mse", "score": 5.7}]`, `{"status": "Ready"}`,
				},
			},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen2','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
             runtime,active_version,last_metrics,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{
				{
					"public", "powergen", "description for ML model PowerGen v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", `[{"objective": "mse", "score": 5.7}]`, `{"status": "Ready"}`,
				},
				{
					"public", "powergen2", "description for ML model PowerGen2 v1", security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`,
					"tensorflow_2.9", "python_3.10", "1", "NULL", `{"status": "Ready"}`,
				},
			},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen2','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,name,description,creator,problem_type,features,target,framework,
             runtime,active_version,last_metrics,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{},
			"",
			testUser,
		},
		{
			"SELECT count(*) FROM kwdbml.show_models() WHERE schema='public' AND name='powergen';",
			[][]string{{"1"}},
			"",
			security.RootUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLRemoveModel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{

		{
			`SELECT kwdbml.remove_model('PowerGen', 1);`,
			[][]string{},
			`pq: kwdbml.remove_model\(\): failed to delete the active model version 'public.powergen,1'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen', 4);`,
			[][]string{},
			`pq: kwdbml.remove_model\(\): the version '4' of model 'public.powergen' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen20', 2);`,
			[][]string{},
			`pq: kwdbml.remove_model\(\): model 'public.powergen20' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen20');`,
			[][]string{},
			`pq: kwdbml.remove_model\(\): model 'public.powergen20' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen', 3);`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen', 2);`,
			[][]string{{"public.powergen"}},
			`pq: kwdbml.remove_model\(\): user 'testuser' does not have 'DELETE' privilege on model 'PowerGen'`,
			testUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen', 2);`,
			[][]string{{"public.powergen"}},
			"",
			testUser,
		},
		{
			`revoke root from testuser`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.PowerGen', 
		'description for ML model PowerGen v2', 
		'R', 
		'tensorflow_2.9', 
		'Python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v2',
		b'transform',
		'[{"objective":"mse", "score":5.8}]');`,
			[][]string{{"public.powergen,4"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','delete');`,
			[][]string{{
				"testuser",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen', 4);`,
			[][]string{{"public.powergen"}},
			"",
			testUser,
		},
		{
			`SELECT count(*) FROM kwdbml.show_model('PowerGen');`,
			[][]string{{"1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('public.PowerGen');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_privileges WHERE schema ='public' AND name = 'powergen'`,
			[][]string{{"0"}},
			"",
			security.RootUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_privileges WHERE schema ='public' AND name = 'powergen2'`,
			[][]string{{"0"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen2');`,
			[][]string{{"public.powergen2"}},
			`pq: kwdbml.remove_model\(\): user 'testuser' does not have 'DELETE' privilege on model 'PowerGen2'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen2','testuser','delete');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_model('PowerGen2');`,
			[][]string{{"public.powergen2"}},
			"",
			testUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_privileges WHERE schema ='public' AND name = 'powergen2'`,
			[][]string{{"0"}},
			"",
			security.RootUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_models WHERE schema = 'public' AND name = 'PowerGen'
			UNION ALL 
				SELECT count(*) FROM kwdbml.ml_models m,kwdbml.ml_model_versions v 
				WHERE v.model_id = m.id AND schema = 'public' AND name = 'PowerGen'
			UNION ALL
			SELECT count(*) FROM kwdbml.ml_jobs j,kwdbml.ml_models m 
				WHERE j.model_id = m.id AND schema = 'public' AND name = 'PowerGen'
			UNION ALL
			SELECT count(*) FROM kwdbml.ml_privileges WHERE type = 1 AND schema = 'public' AND name = 'PowerGen'`,
			[][]string{{"0"}, {"0"}, {"0"}, {"0"}},
			"",
			security.RootUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLGrantModel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.grant_privilege_on_model();`,
			[][]string{},
			`pq: unknown signature: kwdbml.grant_privilege_on_model\(\)`,
			security.RootUser,
		},
		{
			`create user user1`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`create user user2`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.modelcanauth', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.modelcanauth,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): privilege is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): user name is empty`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('','user1','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','SELEcT|privilege_NOT_exist|UPDATE');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "privilege_NOT_exist"`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user_not_exist','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.model_not_exist','user1','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): model 'public.model_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','delete|predict');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','delete|predict|create');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{{"304"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{{"1904"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','delete|predict|create');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','delete|predict|create');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user2';",
			[][]string{{"304"}},
			"",
			security.RootUser,
		},
		{
			`grant root to testuser`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user2','all');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user2';",
			[][]string{},
			"",
			security.RootUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','create');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},

		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{{"16"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{{"1904"}},
			"",
			security.RootUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','select');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','select|delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},

		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user2','select|delete|predict|create');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user2';",
			[][]string{{"1328"}},
			"",
			security.RootUser,
		},
		{
			`revoke root from testuser`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user2','all');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user1','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user1','delete|predict|create');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): user 'testuser' does not have 'ALL' privilege on model 'public.modelcanauth'`,
			testUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','modelcanauth',1,1904,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user1','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user1','delete|predict|create');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='modelcanauth' and type=1 and grantee='user1';",
			[][]string{{"304"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user1','all');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='modelcanauth' and type=1 and grantee='user1';",
			[][]string{{"1904"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user2','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.modelcanauth','user2','delete|predict|create');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.ModelCanAuth','User2','Delete|Predict|Create');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='modelcanauth' and type=1 and grantee='user2';",
			[][]string{{"304"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','delete|predict|create');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user 'testuser' does not have 'ALL' privilege on model 'public.powergen'`,
			testUser,
		},

		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','powergen',1,1904,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','delete|predict|create');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='user1';",
			[][]string{{
				"1600",
			}},
			"",
			security.RootUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLRevokeModel(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.revoke_privilege_on_model();`,
			[][]string{},
			`pq: unknown signature: kwdbml.revoke_privilege_on_model\(\)`,
			security.RootUser,
		},
		{
			`create user user1`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`create user u2`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.model6', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.model6,1"}},
			"",
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): privilege is empty`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user name is empty`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('','user1','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','SELEcT|privilege_NOT_exist|UPDATE');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "privilege_NOT_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','create|SELEcT|privilege_NOT_exist|UPDATE');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "privilege_NOT_exist"`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user_not_exist','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.model_not_exist','user1','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): model 'public.model_not_exist' does not exist`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','user1','delete|predict');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user 'user1' has no existing privileges on model 'public.powergen'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','all');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='testuser';",
			[][]string{{
				"1904",
			}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','testuser','all');`,
			[][]string{{
				"testuser",
			}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=1 and grantee='testuser';",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','powergen',1,1904,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.model6','u2','all');`,
			[][]string{{"u2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='model6' and type=1 and grantee='u2';",
			[][]string{{
				"1904",
			}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.model6','u2','delete|predict|create|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('Public.Model6','U2','Delete|Predict|Create');`,
			[][]string{{"u2"}},
			``,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_model('public.model6','u2','delete|predict|create');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user 'u2' has no existing privileges on model 'public.model6'`,
			security.RootUser,
		},

		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='model6' and type=1 and grantee='u2';",
			[][]string{{
				"1600",
			}},
			"",
			security.RootUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLCheckModelPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.has_model_privilege();`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_model_privilege\(\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user1','all','more-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_model_privilege\(STRING, STRING, STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','less-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_model_privilege\(STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen',1,'type-err');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_model_privilege\(STRING, INT8, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('','root','all');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','','all');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','root','');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): model 'public.powergen' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen_non_root', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen_non_root,1"}},
			"",
			testUser,
		},
		{
			`SELECT * FROM kwdbml.ml_privileges;`,
			[][]string{
				{"testuser", "public", "powergen_non_root", "1", "1904", "testuser"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user_not_exist','');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','root','');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): privilege is empty`,
			security.RootUser,
		},
		{
			`create user user0`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user0','public','powergen',1,1904,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','all');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): user 'testuser' does not have any privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','select');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): user 'testuser' does not have any privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testUser','create');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','create');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','delete');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('Public.Powergen','User0','Update');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','train');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','predict');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','evaluate');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','unsupported_privilege');`,
			[][]string{{
				"false",
			}},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "unsupported_privilege"`,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user_not_exist','select');`,
			[][]string{{
				"false",
			}},
			`kwdbml.has_model_privilege\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','all');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','create');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','update');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','train');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','predict');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user0','evaluate');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`create user user8`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user8','public','powergen',1,1024,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user8','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user8','privilege_not_exist | seLEct');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "privilege_not_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user8','seLEct | privilege_not_exist');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "privilege_not_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user8','deLETe|upDaTe|seLEct');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`create user user66`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user66','public','powergen',1,16,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user66','create');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user66','train');`,
			[][]string{},
			`pq: kwdbml.has_model_privilege\(\): not a valid KaiwuDB ML privilege for model: "train"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','user66','delete');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','root','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','testuser','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','user66','delete');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','testuser','delete');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','user66','delete');`,
			[][]string{{
				"false",
			}},
			``,
			testUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLShowModelPrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.show_model_privilege();`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_model_privilege\(\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user1','all','more-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_model_privilege\(STRING, STRING, STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen',1);`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_model_privilege\(STRING, INT8\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('','root');`,
			[][]string{},
			`pq: object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('notexist.powergen','');`,
			[][]string{},
			`pq: schema 'notexist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','');`,
			[][]string{},
			`pq: user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','*');`,
			[][]string{},
			`pq: user '\*' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','root');`,
			[][]string{},
			`pq: model 'public.powergen' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen,1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen_non_root', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen_non_root,1"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user_not_exist');`,
			[][]string{},
			`pq: user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','root');`,
			[][]string{{"(root,public,powergen,admin,ALL,{})"}},
			``,
			security.RootUser,
		},
		{
			`create user user0`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user0','public','powergen',1,1904,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user0');`,
			[][]string{},
			`pq: user 'testuser' does not have any privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testUser','create');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user0');`,
			[][]string{{"(user0,public,powergen,root,ALL,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','testuser');`,
			[][]string{{"(testuser,public,powergen,root,CREATE,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user_not_exist');`,
			[][]string{},
			`user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`DROP USER user0`,
			[][]string{},
			`pq: cannot drop role/user user0: grants still exist on ML model 'public.powergen'`,
			security.RootUser,
		},
		{
			`DELETE FROM kwdbml.ml_privileges WHERE grantee='user0'`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`DROP USER user0`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`create user user8`,
			[][]string{},
			``,
			security.RootUser,
		},

		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user8','public','powergen',1,1024,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen','user8');`,
			[][]string{{"(user8,public,powergen,root,SELECT,{})"}},
			``,
			security.RootUser,
		},
		{
			`create user user66`,
			[][]string{},
			``,
			security.RootUser,
		},

		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user66','public','powergen',1,1040,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('Public.Powergen','User66');`,
			[][]string{{"(user66,public,powergen,root,CREATE|SELECT,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen');`,
			[][]string{
				{"(testuser,public,powergen,root,CREATE,{})"},
				{"(user66,public,powergen,root,CREATE|SELECT,{})"},
				{"(user8,public,powergen,root,SELECT,{})"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen_non_root','root');`,
			[][]string{{"(root,public,powergen_non_root,admin,ALL,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen_non_root','testuser');`,
			[][]string{{"(testuser,public,powergen_non_root,testuser,ALL,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen_non_root','testuser');`,
			[][]string{{"(testuser,public,powergen_non_root,testuser,ALL,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen_non_root','user66');`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege('public.powergen_non_root','user66');`,
			[][]string{},
			``,
			testUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLRoleModel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []mlTestCase{
		{
			`CREATE ROLE roleCustom;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','roleCustom','select');`,
			[][]string{{"rolecustom"}},
			``,
			security.RootUser,
		},
		{
			`CREATE ROLE roleRead;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`GRANT roleCustom TO roleRead;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`GRANT roleRead TO testuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen','testuser');`,
			[][]string{
				{"testuser", "public", "powergen", "admin", "", "{rolecustom,roleread}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`CREATE ROLE roleDev;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','roleDev','create|update|delete|select');`,
			[][]string{{"roledev"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','roleDev','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`GRANT roleDev TO testuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen','testuser');`,
			[][]string{
				{"testuser", "public", "powergen", "admin", "", "{rolecustom,roledev,roleread}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
				{"roledev", "public", "powergen", "root", "CREATE|DELETE|SELECT|UPDATE", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`CREATE ROLE rolePredict;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','rolePredict','predict');`,
			[][]string{{"rolepredict"}},
			``,
			security.RootUser,
		},
		{
			`GRANT rolePredict TO testuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','select|predict');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','testuser','select|predict');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): user 'testuser' has no existing privileges on model 'public.powergen'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','all');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','Evaluate');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen','testuser');`,
			[][]string{
				{"testuser", "public", "powergen", "root", "EVALUATE", "{roledev,rolepredict,roleread}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
				{"roledev", "public", "powergen", "root", "CREATE|DELETE|SELECT|UPDATE", "{}"},
				{"rolepredict", "public", "powergen", "root", "PREDICT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`Create user subuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`GRANT testuser TO subuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen','subuser');`,
			[][]string{
				{"subuser", "public", "powergen", "admin", "", "{rolecustom,roledev,rolepredict,roleread,testuser}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
				{"roledev", "public", "powergen", "root", "CREATE|DELETE|SELECT|UPDATE", "{}"},
				{"rolepredict", "public", "powergen", "root", "PREDICT", "{}"},
				{"testuser", "public", "powergen", "root", "EVALUATE", "{roledev,rolepredict,roleread}"},
			},
			``,
			testUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen') ORDER BY grantee desc;`,
			[][]string{
				{"testuser", "public", "powergen", "root", "EVALUATE", "{roledev,rolepredict,roleread}"},
				{"rolepredict", "public", "powergen", "root", "PREDICT", "{}"},
				{"roledev", "public", "powergen", "root", "CREATE|DELETE|SELECT|UPDATE", "{}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
			},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen','roleDev','Delete');`,
			[][]string{{"roledev"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen','testuser','All');`,
			[][]string{{
				"false",
			}},
			``,
			testUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen','roleDev');`,
			[][]string{
				{"roledev", "public", "powergen", "root", "CREATE|SELECT|UPDATE", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'public.powergen_non_root', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.powergen_non_root,1"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen_non_root','Root','select');`,
			[][]string{{"root"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen_non_root','root');`,
			[][]string{
				{"root", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen_non_root','root','delete');`,
			[][]string{{"root"}},
			`pq: kwdbml.revoke_privilege_on_model\(\): failed to revoke 'DELETE' privilege from 'root', should first revoke 'root' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','root','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen_non_root','Admin','Select');`,
			[][]string{{"admin"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen_non_root','admin');`,
			[][]string{
				{"admin", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen_non_root','admin','delete');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): failed to revoke 'DELETE' privilege from 'admin', should first revoke 'admin' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','admin','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`Create user adminusr;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`GRANT admin TO adminusr;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen_non_root','Adminusr','Select');`,
			[][]string{{"adminusr"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_model_privilege('public.powergen_non_root','adminusr');`,
			[][]string{
				{"adminusr", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model('public.powergen_non_root','adminusr','delete');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_model\(\): failed to revoke 'DELETE' privilege from 'adminusr', should first revoke 'adminusr' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege('public.powergen_non_root','adminusr','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
	}
	cases := baseCases
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLPrediction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			"SELECT kwdbml.predict(*) FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): the ML model name is required`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING public.powergen FROM kwdb_inferencing_test;`,
			[][]string{
				{"167000000", "0.5"},
				{"168000000", "0.5"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING not.exist FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: at or near "not": syntax error`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING not_exist FROM kwdb_inferencing_test;`,
			[][]string{
				{"167000000", "0.5"},
				{"168000000", "0.5"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING public.not_exist FROM kwdb_inferencing_test;`,
			[][]string{
				{"167000000", "0.5"},
				{"168000000", "0.5"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING defaultdb.public.kwdb_inferencing_test FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: at or near ".": syntax error`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius 
USING (SELECT name FROM kwdbml.show_models() LIMIT 1) FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: at or near ".": syntax error`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING "(SELECT name FROM kwdbml.show_models() LIMIT 1)" FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: kwdbml.predict\(\): schema '"\(select name from kwdbml' does not exist`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING "public.powergen" FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: kwdbml.predict\(\): schema '"public' does not exist`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING 'public.powergen' FROM kwdb_inferencing_test;`,
			[][]string{},
			`pq: at or near "public.powergen": syntax error`,
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict() USING public.powergen5 FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): model 'public.powergen5' does not exist`,
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict() USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): the column number of predict data is not match with the ML feature definition, expect '2', but got '0'`,
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict(*) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): the column number of predict data is not match with the ML feature definition, expect '2', but got '14'`,
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict('*', Weather_Temperature_Celsius) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): the value '\*' is not compatible with the expected data type 'float' for column 'Current_Phase_Average'`,
			security.RootUser,
		},
		{
			"SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{
				{"167000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
				{"168000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
			},
			"",
			security.RootUser,
		},
		{
			"SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{},
			"pq: user testuser does not have SELECT privilege on relation kwdb_inferencing_test",
			testUser,
		},
		{
			fmt.Sprintf(`GRANT SELECT on table kwdb_inferencing_test to %s`, testUser),
			[][]string{},
			``,
			security.RootUser,
		},
		{
			"SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): user 'testuser' does not have 'PREDICT' privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','predict');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			"SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.powergen FROM kwdb_inferencing_test;",
			[][]string{
				{"167000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
				{"168000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
			},
			"",
			testUser,
		},
		{
			"SELECT kwdbml.predict(*) USING model_not_exist FROM kwdb_inferencing_test;",
			[][]string{},
			"pq: kwdbml.predict\\(\\): model 'model_not_exist' does not exist",
			testUser,
		},
		{
			fmt.Sprintf(`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.%s FROM kwdb_inferencing_test;`, mlagent.TestModelOutputNull),
			[][]string{},
			`pq: kwdbml.predict\(\): the KaiwuDB ML model is not ready, status: Error, message: failed to get service output, use kwdbml.show_models\(\) for detail info`,
			security.RootUser,
		},
		{
			fmt.Sprintf(`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.%s FROM kwdb_inferencing_test;`, mlagent.TestModelStatusNull),
			[][]string{},
			`pq: kwdbml.predict\(\): the KaiwuDB ML model is not ready, status: Error, message: the KaiwuDB ML model status is empty, use kwdbml.show_models\(\) for detail info`,
			security.RootUser,
		},
		{
			`SELECT schema,name,status FROM kwdbml.show_models() ORDER BY name;`,
			[][]string{
				{
					"public", mlagent.TestModelOutputNull, `{"message": "failed to get service output", "status": "Error"}`,
				},
				{
					"public", mlagent.TestModelStatusNull, `{"message": "the KaiwuDB ML model status is empty", "status": "Error"}`,
				},
				{
					"public", "powergen", `{"status": "Ready"}`,
				},
				{
					"public", "powergen2", `{"status": "Ready"}`,
				},
			},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`SELECT kwdbml.import_model( 
		'%s', 
		'description for ML model PowerGen v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"Active_Power", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`, mlagent.TestModelVersionNotExist),
			[][]string{{fmt.Sprintf("public.%s,1", mlagent.TestModelVersionNotExist)}},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.%s FROM kwdb_inferencing_test;`, mlagent.TestModelVersionNotExist),
			[][]string{},
			`pq: kwdbml.predict\(\): the KaiwuDB ML model is not ready, status: Initializing, message: the KaiwuDB ML model is not ready, please try again later, use kwdbml.show_models\(\) for detail info`,
			security.RootUser,
		},
		{
			fmt.Sprintf(`UPDATE kwdbml.ml_models SET status='{"status": "Ready"}' WHERE name='%s';`, mlagent.TestModelVersionNotExist),
			[][]string{},
			``,
			security.RootUser,
		},
		{
			fmt.Sprintf(`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.%s FROM kwdb_inferencing_test;`, mlagent.TestModelVersionNotExist),
			[][]string{},
			`pq: kwdbml.predict\(\): the KaiwuDB ML model is not ready, please try again later`,
			security.RootUser,
		},
	}
	cases := baseCases
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testErrorModelCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLPredictDataType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`create table t_datatype_test
(
    c_bool                 BOOL,
    c_bit                  BIT(3),
		c_int                  INT,
    c_int2                 SMALLINT,
    c_int_null             INT4,
    c_float                FLOAT,
    c_float4               FLOAT4,
    c_decimal              DECIMAL(10,5),
    c_numeric              NUMERIC,
    c_string               STRING,
    c_char                 CHAR(10),
    c_varchar              VARCHAR(10),
    c_string_collate       STRING COLLATE en,
    c_bytes                BYTES,
    c_blob                 BLOB,
    c_date                 DATE,
		c_time                 TIME,
    c_timetz               TIMETZ,
    c_timestamp            TIMESTAMP,
    c_timestamptz          TIMESTAMPTZ,
    c_uuid                 UUID,
    c_inet                 INET,
    c_int_array            INT[]
);`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`insert into t_datatype_test values(true,B'101',
1,2,null,0.1,0.2,0.3,0.4,
'str\"1"i\ng','char','var''char',
'english' COLLATE en,b'bytes',b'blob',
'2016-03-27','05:40:00','05:40:00','2016-03-26 10:10:10-05:00','2016-03-26 10:10:10-05:00',
'63616665-6630-3064-6465-616462656562','192.168.0.1',ARRAY[10,20,30]
);`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'model_datatype', 
		'description for ML model datatype v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[
			{"name":"c_bool", "type":"bool"},
			{"name":"c_bit", "type":"string"},
			{"name":"c_int", "type":"int"},
			{"name":"c_int2", "type":"int"},
			{"name":"c_int_null", "type":"int"},
			{"name":"c_float", "type":"float"},
			{"name":"c_float4", "type":"float"},
			{"name":"c_decimal", "type":"float"},
			{"name":"c_numeric", "type":"float"},
			{"name":"c_string", "type":"string"},
			{"name":"c_char", "type":"string"},
			{"name":"c_varchar", "type":"string"},
			{"name":"c_string_collate", "type":"string"},
			{"name":"c_bytes", "type":"string"},
			{"name":"c_blob", "type":"string"},
			{"name":"c_date", "type":"string"},
			{"name":"c_time", "type":"string"},
			{"name":"c_timetz", "type":"string"},
			{"name":"c_timestamp", "type":"string"},
			{"name":"c_timestamptz", "type":"string"},
			{"name":"c_uuid", "type":"string"},
			{"name":"c_inet", "type":"string"},
			{"name":"c_int_array", "type":"array"}
]', 
		'[{"name":"prediction", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.model_datatype,1"}},
			"",
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict(*) USING public.model_datatype FROM t_datatype_test;",
			[][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}},
			"",
			security.RootUser,
		},
		{
			`create table t_datatype_error_test(
    c_interval         INTERVAL,
    c_jsonb            JSONB
);`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`insert into t_datatype_error_test values('1-2 3 4:5:6','{}');`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_model( 
		'model_datatype_err', 
		'description for ML model datatype v1', 
		'Regression', 
		'tensorflow_2.9', 
		'python_3.10', 
		'[
			{"name":"c_interval", "type":"string"},
			{"name":"c_jsonb", "type":"string"}
]', 
		'[{"name":"prediction", "type":"float"}]', 
		b'entity.v1',
		b'transform',
		'[{"objective":"mse", "score":5.7}]');`,
			[][]string{{"public.model_datatype_err,1"}},
			"",
			security.RootUser,
		},
		{
			"SELECT kwdbml.predict(*) USING public.model_datatype_err FROM t_datatype_error_test;",
			[][]string{},
			`pq: kwdbml.predict\(\): the value '1 year 2 mons 3 days 04:05:06' is not compatible with the expected data type 'string' for column 'c_interval'`,
			security.RootUser,
		},
	}

	cases := baseCases
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLCache(t *testing.T) {
	defer leaktest.AfterTest(t)()

	/*
			tc := serverutils.StartTestCluster(t, 2, base.TestClusterArgs{})
			defer tc.Stopper().Stop(context.TODO())

			tlsConfig, _ := tc.Server(0).RPCContext().GetClientTLSConfig()
			mls := mlagent.StartMLTestServer(t, address, tlsConfig)

			defer mls.Stop()

			db1 := sqlutils.MakeSQLRunner(tc.ServerConn(0))
			db1.Exec(t, `CREATE USER $1`, testUser)

			pgURL, cleanupFunc := sqlutils.PGUrl(t, tc.Server(1).ServingSQLAddr(), t.Name(), url.User(testUser))
			defer cleanupFunc()

			conn2, err := gosql.Open("postgres", pgURL.String())
			if err != nil {
				t.Fatal(err)
			}

			defer conn2.Close()

			db2 := sqlutils.MakeSQLRunner(conn2)
			db1.CheckQueryResults(t, baseCases[1].sql, baseCases[1].expected)
			db1.CheckQueryResults(t, baseCases[2].sql, baseCases[2].expected)
			db1.CheckQueryResults(t, baseCases[3].sql, baseCases[3].expected)
			db1.CheckQueryResults(t, testModelCases[0].sql, testModelCases[0].expected)
			db1.CheckQueryResults(
				t,
				fmt.Sprintf(`GRANT SELECT on table kwdb_inferencing_test to %s`, testUser),
				[][]string{},
			)

			grantSQL := "SELECT kwdbml.grant_privilege_on_model('public.powergen','testuser','all');"
			checkSQL := `SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) USING public.powergen
		FROM kwdb_inferencing_test;`
			revokeSQL := "SELECT kwdbml.revoke_privilege_on_model('public.powergen','testuser','predict');"

			db2.ExpectErr(
				t,
				`pq: kwdbml.predict\(\): user 'testuser' does not have 'PREDICT' privilege on model 'public.powergen'`,
				checkSQL,
			)
			db1.CheckQueryResults(t, grantSQL, [][]string{{testUser}})

			db1.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})
			db2.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})

			db1.CheckQueryResults(
				t,
				"DELETE FROM kwdbml.ml_privileges WHERE name='powergen' and grantee='testuser'",
				[][]string{},
			)
			db1.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})
			db2.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})

			db1.CheckQueryResults(t,
				`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor)
				values('testuser','public','powergen',1,1904,'root')`,
				[][]string{},
			)
			db2.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})

			db1.CheckQueryResults(t, revokeSQL, [][]string{{testUser}})
			db1.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})
			db2.ExpectErr(t, `pq: kwdbml.predict\(\): user 'testuser' does not have 'PREDICT' privilege on model 'public.powergen'`, checkSQL)

			db1.CheckQueryResults(t, `CREATE ROLE rolePredict;`, [][]string{})
			db1.CheckQueryResults(t, `SELECT kwdbml.grant_privilege_on_model('public.powergen','rolePredict','predict');`, [][]string{{"rolepredict"}})
			db1.CheckQueryResults(t, `GRANT rolePredict TO testuser;`, [][]string{})
			db2.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})
			db1.CheckQueryResults(
				t,
				"DELETE FROM kwdbml.ml_privileges WHERE name='powergen' and grantee='rolepredict'",
				[][]string{},
			)
			db2.CheckQueryResults(t, checkSQL, [][]string{{"{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"}})
			db1.CheckQueryResults(t,
				`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor)
				values('rolepredict','public','powergen',1,1904,'root')`,
				[][]string{},
			)
			db1.CheckQueryResults(t, `REVOKE rolePredict FROM testuser;`, [][]string{})
			db2.ExpectErr(t, `pq: kwdbml.predict\(\): user 'testuser' does not have 'PREDICT' privilege on model 'public.powergen'`, checkSQL)
	*/
}

func TestMLStmt(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT timestamp, Weather_Temperature_Celsius,
kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'value'
USING public.powergen FROM kwdb_inferencing_test;`,
			[][]string{
				{"167000000", "0.5", "0.5"},
				{"168000000", "0.5", "0.5"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius,
kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction'
USING public.powergen2 FROM kwdb_inferencing_test;`,
			[][]string{
				{"167000000", "0.5", "0.314"},
				{"168000000", "0.5", "0.314"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT a.timestamp, b.Weather_Temperature_Celsius,
kwdbml.predict(a.Current_Phase_Average, b.Weather_Temperature_Celsius)
USING powergen FROM kwdb_inferencing_test a,kwdb_inferencing_test b
WHERE a.timestamp=b.timestamp;`,
			[][]string{
				{"167000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
				{"168000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT * FROM (
SELECT timestamp, Weather_Temperature_Celsius, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING public.powergen FROM kwdb_inferencing_test
);`,
			[][]string{
				{"167000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
				{"168000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT a.timestamp, b.Weather_Temperature_Celsius, prediction FROM 
(SELECT timestamp, kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius) AS prediction
USING public.powergen2 FROM kwdb_inferencing_test) a
INNER JOIN kwdb_inferencing_test b ON a.timestamp=b.timestamp ;`,
			[][]string{
				{"167000000", "0.5", "{\"prediction\": 0.314}"},
				{"168000000", "0.5", "{\"prediction\": 0.314}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT * USING public.powergen2 FROM kwdbml.predict(0.1, 0.5);`,
			[][]string{
				{"{\"prediction\": 0.314}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, (
SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING public.powergen FROM kwdb_inferencing_test a
WHERE a.timestamp=b.timestamp
) FROM kwdb_inferencing_test b;`,
			[][]string{
				{"167000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
				{"168000000", "0.5", "{\"name\": \"Active_Power\", \"type\": \"float\", \"value\": 0.5}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, (
SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
FROM kwdb_inferencing_test a
WHERE a.timestamp=b.timestamp
) USING public.powergen FROM kwdb_inferencing_test b;`,
			[][]string{},
			`pq: kwdbml.predict\(\): the ML model name is required`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius, (
SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
USING public.powergen2 FROM kwdb_inferencing_test a
WHERE a.timestamp=b.timestamp
) USING public.powergen FROM kwdb_inferencing_test b;`,
			[][]string{
				{"167000000", "0.5", "{\"prediction\": 0.314}"},
				{"168000000", "0.5", "{\"prediction\": 0.314}"},
			},
			"",
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius FROM kwdb_inferencing_test
WHERE kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction' = 0.314;`,
			[][]string{},
			`pq: unsupported comparison operator: <jsonb> = <decimal>`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius FROM kwdb_inferencing_test
WHERE kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction' != NULL;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius FROM kwdb_inferencing_test
WHERE kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->>'prediction' = '0.314';`,
			[][]string{},
			`pq: kwdbml.predict\(\): the ML model name is required`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius USING public.powergen2 FROM kwdb_inferencing_test
WHERE kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->>'prediction' = '0.314';`,
			[][]string{
				{"167000000", "0.5"},
				{"168000000", "0.5"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius,
kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction'
USING public.powergen2 FROM kwdb_inferencing_test
WHERE kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction' NOTNULL;`,
			[][]string{
				{"167000000", "0.5", "0.314"},
				{"168000000", "0.5", "0.314"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius,
kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction' AS prediction
USING public.powergen2 FROM kwdb_inferencing_test
WHERE prediction = 0.314;`,
			[][]string{},
			`pq: column "prediction" does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction' AS prediction,
count(*)
USING public.powergen2 FROM kwdb_inferencing_test
GROUP BY prediction;`,
			[][]string{
				{"0.314", "2"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT count(*) USING public.powergen2 FROM kwdb_inferencing_test
GROUP BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction';`,
			[][]string{
				{"2"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction',count(*)
USING public.powergen2 FROM kwdb_inferencing_test
GROUP BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction';`,
			[][]string{
				{"0.314", "2"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius),count(*)
USING public.powergen2 FROM kwdb_inferencing_test
GROUP BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius);`,
			[][]string{
				{"{\"prediction\": 0.314}", "2"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius),count(*)
USING public.powergen2 FROM kwdb_inferencing_test
GROUP BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)
HAVING kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->>'prediction'='0.314'
;`,
			[][]string{
				{"{\"prediction\": 0.314}", "2"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius
USING public.powergen2 FROM kwdb_inferencing_test
ORDER BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->'prediction';`,
			[][]string{},
			`pq: unimplemented: can't order by column type jsonb`,
			security.RootUser,
		},
		{
			`SELECT timestamp, Weather_Temperature_Celsius
USING public.powergen2 FROM kwdb_inferencing_test
ORDER BY kwdbml.predict(Current_Phase_Average, Weather_Temperature_Celsius)->>'prediction';`,
			[][]string{
				{"167000000", "0.5"},
				{"168000000", "0.5"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT count(*) FROM (SELECT kwdbml.show_model(name) FROM kwdbml.show_models());`,
			[][]string{
				{"6"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT a.schema,b.name,b.version,b.description FROM kwdbml.show_models() a
INNER JOIN kwdbml.show_model('public.powergen') b ON a.name=b.name AND a.schema=b.schema;`,
			[][]string{
				{"public", "powergen", "1", "description for ML model PowerGen v1"},
				{"public", "powergen", "2", "description for ML model PowerGen v2"},
				{"public", "powergen", "3", "description for ML model PowerGen v3"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,count(*) FROM kwdbml.show_models() GROUP BY schema;`,
			[][]string{
				{"public", "4"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model(name,'testuser','Evaluate') FROM kwdbml.show_models();`,
			[][]string{
				{"testuser"},
				{"testuser"},
				{"testuser"},
				{"testuser"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege(name,'testuser','Evaluate') FROM kwdbml.show_models();`,
			[][]string{
				{"true"},
				{"true"},
				{"true"},
				{"true"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT a.schema,a.name,b.grantee,privileges FROM kwdbml.show_models() a
INNER JOIN kwdbml.show_model_privilege('powergen') b ON a.name=b.name AND a.schema=b.schema;`,
			[][]string{
				{"public", "powergen", "testuser", "EVALUATE"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_model_privilege(name) FROM kwdbml.show_models();`,
			[][]string{
				{"(testuser,public,powergen,root,EVALUATE,{})"},
				{"(testuser,public,powergen2,root,EVALUATE,{})"},
				{"(testuser,public,model_output_null,root,EVALUATE,{})"},
				{"(testuser,public,model_status_null,root,EVALUATE,{})"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT schema,name,
CASE kwdbml.has_model_privilege(name,'root','create') WHEN true THEN 'create' ELSE '' END
FROM kwdbml.show_models() WHERE kwdbml.has_model_privilege(name,'admin','all') = true;`,
			[][]string{
				{"public", "powergen", "create"},
				{"public", "powergen2", "create"},
				{"public", "model_output_null", "create"},
				{"public", "model_status_null", "create"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_model(name,'testuser','Evaluate') FROM kwdbml.show_models();`,
			[][]string{
				{"testuser"},
				{"testuser"},
				{"testuser"},
				{"testuser"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_model_privilege(name,'testuser','Evaluate') FROM kwdbml.show_models();`,
			[][]string{
				{"false"},
				{"false"},
				{"false"},
				{"false"},
			},
			``,
			security.RootUser,
		},
	}
	cases := baseCases
	cases = append(cases, testModelCases[0:4]...)
	cases = append(cases, testErrorModelCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}
