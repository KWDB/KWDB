// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.
//
// Shanghai Yunxi Technology Co, Ltd makes no representations or warranties about the suitability
// of the software, either express or implied, including but not limited to the implied warranties
// of merchantability, fitness for a particular purpose, or non-infringement. Shanghai Yunxi
// Technology Co, Ltd shall not be liable for any damages suffered by licensee as a result
// of using, modifying or distributing this software or its derivatives.
//

package builtins

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mlagent"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var testPipelineCases = []mlTestCase{
	{
		`SELECT kwdbml.import_training_pipeline( 
	'PowerGenPipeline', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
		[][]string{{"public.powergenpipeline"}},
		"",
		security.RootUser,
	},
	{
		`SELECT kwdbml.import_training_pipeline( 
	'public.PowerGenPipeline1', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity2',
	b'transformer_entity2');`,
		[][]string{{"public.powergenpipeline1"}},
		"",
		security.RootUser,
	},
}

func TestMLImportPipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []mlTestCase{
		{
			`SELECT kwdbml.import_training_pipeline( '1', '2', '3', '4', '5', '6', '7', b'8', b'9');`,
			[][]string{},
			`pq: kwdbml.import_training_pipeline\(\): training pipeline name '1' is unsupported`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline('public.pipeline1','2','BINARY_CLASSIFICATION','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',b'9');`,
			[][]string{{"public.pipeline1"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline('public.pipeline1','2','BINARY_CLASSIFICATION','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',null);`,
			[][]string{},
			`pq: kwdbml.import_training_pipeline\(\): pipeline 'public.pipeline1' already exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline('Public.Pipeline2','2','BINARY_CLASSIFICATION','Tensorflow_2.9','python_3.10',
				'[{"name":"Current_Phase_Average", "type":"float"}]','[{"name":"Active_Power", "type":"float"}]',b'8',null);`,
			[][]string{{"public.pipeline2"}},
			"",
			testUser,
		},
		{
			`SELECT * FROM kwdbml.ml_privileges ORDER BY grantee;`,
			[][]string{
				{testUser, "public", "pipeline2", "2", "1184", testUser},
			},
			"",
			security.RootUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLShowPipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{
		{
			`SELECT * FROM kwdbml.show_training_pipeline('');`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_training_pipeline('PowerGenPipeline2');`,
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT schema, name, description, creator, problem_type, features, target, framework, runtime
			FROM kwdbml.show_training_pipeline('public.PowerGenPipeline');`,
			[][]string{{"public", "powergenpipeline",
				"Training pipeline to train a model for power generation prediction.", security.RootUser, "r",
				`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
				`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10"}},
			"",
			security.RootUser,
		},
		{
			`SELECT schema, name, description, creator, problem_type, features, target, framework, runtime
			FROM kwdbml.show_training_pipeline('public.PowerGenPipeline1');`,
			[][]string{},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.PowerGenPipeline1','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT schema, name, description, creator, problem_type, features, target, framework, runtime
			FROM kwdbml.show_training_pipeline('public.PowerGenPipeline1');`,
			[][]string{{"public", "powergenpipeline1",
				"Training pipeline to train a model for power generation prediction.", security.RootUser, "r",
				`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
				`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10"}},
			"",
			testUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testPipelineCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLShowPipelines(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{
		{
			`SELECT schema, name, description, creator, problem_type, features, target, framework, runtime
FROM kwdbml.show_training_pipelines() order by name;`,
			[][]string{
				{"public", "powergenpipeline", "Training pipeline to train a model for power generation prediction.",
					security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10"},
				{"public", "powergenpipeline1", "Training pipeline to train a model for power generation prediction.",
					security.RootUser, "r",
					`[{"name": "Current_Phase_Average", "type": "float"}, {"name": "Weather_Temperature_Celsius", "type": "float"}]`,
					`[{"name": "Active_Power", "type": "float"}]`, "tensorflow_2.9", "python_3.10"},
			},
			"",
			security.RootUser,
		},
		{
			"SELECT count(*) FROM kwdbml.show_training_pipelines() WHERE schema='public' AND name='powergenpipeline';",
			[][]string{{"0"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.PowerGenPipeline','testuser','select');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			"SELECT count(*) FROM kwdbml.show_training_pipelines() WHERE schema='public' AND name='powergenpipeline';",
			[][]string{{"1"}},
			"",
			testUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testPipelineCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLRemovePipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{
		{
			`SELECT kwdbml.remove_training_pipeline('');`,
			[][]string{},
			`pq: kwdbml.remove_training_pipeline\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_training_pipeline('public.PowerGenPipeline2');`,
			[][]string{},
			`pq: kwdbml.remove_training_pipeline\(\): training pipeline 'public.powergenpipeline2' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_training_pipeline('PowerGenPipeline');`,
			[][]string{{"public.powergenpipeline"}},
			"",
			security.RootUser,
		},
		{
			`SELECT count(*) FROM kwdbml.ml_training_pipelines WHERE schema = 'public' AND name = 'powergenpipeline'
			UNION ALL 	
			SELECT count(*) FROM kwdbml.ml_jobs j,kwdbml.ml_training_pipelines p 
				WHERE j.training_pipeline_id = p.id AND schema = 'public' AND name = 'powergenpipeline'
			UNION ALL
			SELECT count(*) FROM kwdbml.ml_privileges WHERE type = 2 AND schema = 'public' AND name = 'powergenpipeline'`,
			[][]string{{"0"}, {"0"}, {"0"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_training_pipeline('public.PowerGenPipeline1');`,
			[][]string{},
			`pq: kwdbml.remove_training_pipeline\(\): user 'testuser' does not have 'DELETE' privilege on training pipeline 'public.PowerGenPipeline1'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.PowerGenPipeline1','testuser','delete');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.remove_training_pipeline('public.PowerGenPipeline1');`,
			[][]string{{"public.powergenpipeline1"}},
			"",
			testUser,
		},
	}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testPipelineCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLTrain(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{
		{
			`SELECT kwdbml.train_model('not_exist','PowerGen', 'kwdb_inferencing_test',  false, false);`,
			[][]string{},
			`pq: kwdbml.train_model\(\): failed to retrieve ml training pipeline: training pipeline 'public.not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'not_exist_table',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: fetch-data-source-sample: relation "not_exist_table" does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'create table aaa {a int}',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: at or near "create": syntax error`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'select insert update',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: at or near "update": syntax error`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'Defaultdb.Test.kwdb_inferencing_test', true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: fetch-data-source-sample: relation "defaultdb.test.kwdb_inferencing_test" does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'select * from kwdb_inferencing_test',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: the column number of input data is not match with the ML feature definition, expect '3', but got '14'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'select timestamp, Active_Power from kwdb_inferencing_test',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: the column number of input data is not match with the ML feature definition, expect '3', but got '2'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen', 'select timestamp, Active_Energy_Delivered_Received, Active_Power from kwdb_inferencing_test',  true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: the value '167000000' is not compatible with the expected data type 'float' for column 'Current_Phase_Average'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('PowerGenPipeline','PowerGen',
'select timestamp, Active_Energy_Delivered_Received, Active_Power from kwdb_inferencing_test ',
true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: the value '167000000' is not compatible with the expected data type 'float' for column 'Current_Phase_Average'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb_inferencing_test', true, true);`,
			[][]string{},
			`pq: kwdbml.train_model\(\): user 'testuser' does not have 'TRAIN' privilege on training pipeline 'powergenpipeline'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergenpipeline','testUser','train');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb_inferencing_test', true, true);`,
			[][]string{},
			`pq: kwdbml.train_model\(\): user 'testuser' does not have 'CREATE' privilege on model 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_model('public.powergen','testUser','create');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb_inferencing_test', true, true);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: fetch-data-source-sample: user testuser does not have SELECT privilege on relation kwdb_inferencing_test`,
			testUser,
		},
		{
			"GRANT SELECT ON TABLE kwdb_inferencing_test TO testuser;",
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb_inferencing_test', true, true);`,
			[][]string{{mlagent.TestTrainResponseRunID}},
			"",
			testUser,
		},
		{
			"SELECT role,member FROM system.role_members WHERE member='kwdbmlusr';",
			[][]string{
				{"testuser", "kwdbmlusr"},
			},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(createTestTable, "kwdb.train_a"),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"insert into kwdb.train_a values('167000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"insert into kwdb.train_a values('168000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(createTestTable, "kwdb.train_b"),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"insert into kwdb.train_b values('167000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"insert into kwdb.train_b values('168000000',2,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,3,0.11,0.22);",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`DELETE FROM kwdbml.ml_jobs WHERE id='%s';`, mlagent.TestTrainResponseRunID),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', '
		SELECT a.Current_Phase_Average, b.Weather_Temperature_Celsius, 
			(SELECT Active_Power FROM kwdb_inferencing_test c WHERE c.timestamp=b.timestamp)
		FROM kwdb.train_a a,kwdb.train_b b WHERE a.timestamp=b.timestamp
', true, true);`,
			[][]string{{mlagent.TestTrainResponseRunID}},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`DELETE FROM kwdbml.ml_jobs WHERE id='%s';`, mlagent.TestTrainResponseRunID),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb.train_b', true, true);`,
			[][]string{{mlagent.TestTrainResponseRunID}},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`DELETE FROM kwdbml.ml_jobs WHERE id='%s';`, mlagent.TestTrainResponseRunID),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'Kwdb.Public.Train_B', true, true);`,
			[][]string{{mlagent.TestTrainResponseRunID}},
			"",
			security.RootUser,
		},
		{
			fmt.Sprintf(`DELETE FROM kwdbml.ml_jobs WHERE id='%s';`, mlagent.TestTrainResponseRunID),
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'Public.kwdb_inferencing_test', true, true);`,
			[][]string{{mlagent.TestTrainResponseRunID}},
			"",
			security.RootUser,
		},
		{
			"SELECT role,member FROM system.role_members WHERE member='kwdbmlusr';",
			[][]string{
				{"root", "kwdbmlusr"},
				{"testuser", "kwdbmlusr"},
			},
			"",
			security.RootUser,
		},
		{
			"DROP DATABASE defaultdb;",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.train_model('public.PowerGenPipeline','PowerGen', 'kwdb_inferencing_test', true, false);`,
			[][]string{},
			`pq: internal error: kwdbml.train_model\(\): failed to construct the ML training input: fetch-data-source-sample: relation "kwdb_inferencing_test" does not exist`,
			testUser,
		},
	}
	cases := baseCases
	cases = append(cases, testModelCases[0])
	cases = append(cases, testPipelineCases[0])
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLEvaluateModel(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{}
	cases := baseCases
	cases = append(cases, testModelCases[0])
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLBatchPredict(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{}
	cases := baseCases
	cases = append(cases, testCases...)
	doTest(t, cases)
}

func TestMLGrantPipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline();`,
			[][]string{},
			`pq: unknown signature: kwdbml.grant_privilege_on_training_pipeline\(\)`,
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
			`SELECT kwdbml.import_training_pipeline( 
	'powergen', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'pipelinecanauth', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity')`,
			[][]string{{"public.pipelinecanauth"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): privilege is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('','user1','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','SELEcT|privilege_NOT_exist|TRAIN');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "privilege_NOT_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user_not_exist','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.not_exist','user1','select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): training pipeline 'public.not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','delete|train');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','delete|predict');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','delete|train');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{{"160"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{{"1184"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user2','delete|predict|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user2','train');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('Public.Powergen','User2','Delete');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user2';",
			[][]string{{"160"}},
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
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user2','all');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user2';",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('Public.powergen','User1','train');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{{"128"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{{"1184"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user2','select');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user2','select|delete|predict|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user2','select|train');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user2';",
			[][]string{{"1152"}},
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
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user2','all');`,
			[][]string{{"user2"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user1','delete|create|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "create"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user1','delete|train|select');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): user 'testuser' does not have 'ALL' privilege on training pipeline 'public.pipelinecanauth'`,
			testUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','pipelinecanauth',2,1184,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user1','delete|predict');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user1','delete|train');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='pipelinecanauth' and type=2 and grantee='user1';",
			[][]string{{"160"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user1','all');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='pipelinecanauth' and type=2 and grantee='user1';",
			[][]string{{"1184"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user2','delete|predict|train');`,
			[][]string{},
			`pq: kwdbml.grant_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinecanauth','user2','delete|train');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.PipelineCanAuth','User2','Delete|Train');`,
			[][]string{{"user2"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='pipelinecanauth' and type=2 and grantee='user2';",
			[][]string{{"160"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','user1','all');`,
			[][]string{{"user1"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','delete|predict');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','delete|train|select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user 'testuser' does not have 'ALL' privilege on training pipeline 'public.powergen'`,
			testUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','powergen',2,1184,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','delete|predict|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			testUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','select|train');`,
			[][]string{{"user1"}},
			``,
			testUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='user1';",
			[][]string{{
				"32",
			}},
			"",
			security.RootUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLRevokePipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline();`,
			[][]string{},
			`pq: unknown signature: kwdbml.revoke_privilege_on_training_pipeline\(\)`,
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
			`SELECT kwdbml.import_training_pipeline( 
	'powergen', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'pipelinetest', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.pipelinetest"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): privilege is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('','user1','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','SELEcT|privilege_NOT_exist');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "privilege_NOT_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','SELEcT|privilege_NOT_exist|TRAIN');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "privilege_NOT_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user_not_exist','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.not_exist','user1','select');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): training pipeline 'public.not_exist' does not exist`,
			security.RootUser,
		},

		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','user1','delete|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user 'user1' has no existing privileges on training pipeline 'public.powergen'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','testuser','all');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='testuser';",
			[][]string{{
				"1184",
			}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','testuser','all');`,
			[][]string{{
				"testuser",
			}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='powergen' and type=2 and grantee='testuser';",
			[][]string{},
			"",
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('testuser','public','powergen',2,1184,'testuser')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.pipelinetest','u2','all');`,
			[][]string{{"u2"}},
			``,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='pipelinetest' and type=2 and grantee='u2';",
			[][]string{{
				"1184",
			}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.pipelinetest','u2','delete|predict|train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('Public.pipelinetest','U2','Delete|TRAIN');`,
			[][]string{{"u2"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.pipelinetest','u2','delete');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user 'u2' has no existing privileges on training pipeline 'public.pipelinetest'`,
			security.RootUser,
		},
		{
			"SELECT privilege_type FROM kwdbml.ml_privileges WHERE schema='public' AND name='pipelinetest' and type=2 and grantee='u2';",
			[][]string{{
				"1024",
			}},
			"",
			security.RootUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLCheckPipelinePrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.has_training_pipeline_privilege();`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_training_pipeline_privilege\(\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user1','all','more-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_training_pipeline_privilege\(STRING, STRING, STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','less-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_training_pipeline_privilege\(STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen',1,'type-err');`,
			[][]string{},
			`pq: unknown signature: kwdbml.has_training_pipeline_privilege\(STRING, INT8, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('','root','all');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','','all');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','root','');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): training pipeline 'public.powergen' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen_non_root', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen_non_root"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user_not_exist','');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','root','');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): privilege is empty`,
			security.RootUser,
		},
		{
			`create user user0`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user0','public','powergen',2,1184,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','all');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): user 'testuser' does not have any privilege on training pipeline 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','select');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): user 'testuser' does not have any privilege on training pipeline 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','testUser','train');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','select');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','delete');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('Public.Powergen','User0','train');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','create');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "create"`,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','unsupported_privilege');`,
			[][]string{{
				"false",
			}},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "unsupported_privilege"`,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user_not_exist','select');`,
			[][]string{{
				"false",
			}},
			`kwdbml.has_training_pipeline_privilege\(\): user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','all');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','train');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user0','predict');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "predict"`,
			security.RootUser,
		},
		{
			`create user user8`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user8','public','powergen',2,1024,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user8','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user8','privilege_not_exist | seLEct');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "privilege_not_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user8','seLEct | privilege_not_exist');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "privilege_not_exist"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user8','deLETe|seLEct');`,
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
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user66','public','powergen',2,32,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user66','create');`,
			[][]string{},
			`pq: kwdbml.has_training_pipeline_privilege\(\): not a valid KaiwuDB ML privilege for training pipeline: "create"`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','user66','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','root','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','testuser','delete');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','user66','train');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','testuser','delete');`,
			[][]string{{
				"true",
			}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','user66','train');`,
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

func TestMLShowPipelinePrivileges(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testCases := []mlTestCase{
		{
			`SELECT kwdbml.show_training_pipeline_privilege();`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_training_pipeline_privilege\(\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user1','all','more-para');`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_training_pipeline_privilege\(STRING, STRING, STRING, STRING\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen',1);`,
			[][]string{},
			`pq: unknown signature: kwdbml.show_training_pipeline_privilege\(STRING, INT8\)`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('','root');`,
			[][]string{},
			`pq: object name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('notexist.powergen','');`,
			[][]string{},
			`pq: schema 'notexist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','');`,
			[][]string{},
			`pq: user name is empty`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','*');`,
			[][]string{},
			`pq: user '\*' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','root');`,
			[][]string{},
			`pq: training pipeline 'public.powergen' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen_non_root', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen_non_root"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user_not_exist');`,
			[][]string{},
			`pq: user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','root');`,
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
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user0','public','powergen',2,1184,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user0');`,
			[][]string{},
			`pq: user 'testuser' does not have any privilege on training pipeline 'public.powergen'`,
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','testUser','Train');`,
			[][]string{{"testuser"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user0');`,
			[][]string{{"(user0,public,powergen,root,ALL,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','testuser');`,
			[][]string{{"(testuser,public,powergen,root,TRAIN,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user_not_exist');`,
			[][]string{},
			`user 'user_not_exist' does not exist`,
			security.RootUser,
		},
		{
			`DROP USER user0`,
			[][]string{},
			`pq: cannot drop role/user user0: grants still exist on ML training pipeline 'public.powergen'`,
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
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user8','public','powergen',2,1024,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen','user8');`,
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
			`insert into kwdbml.ml_privileges(grantee,schema,name,type,privilege_type,grantor) values('user66','public','powergen',2,1056,'root')`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('Public.Powergen','User66');`,
			[][]string{{"(user66,public,powergen,root,DELETE|SELECT,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen');`,
			[][]string{
				{"(testuser,public,powergen,root,TRAIN,{})"},
				{"(user66,public,powergen,root,DELETE|SELECT,{})"},
				{"(user8,public,powergen,root,SELECT,{})"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen_non_root','root');`,
			[][]string{{"(root,public,powergen_non_root,admin,ALL,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen_non_root','testuser');`,
			[][]string{{"(testuser,public,powergen_non_root,testuser,ALL,{})"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen_non_root','testuser');`,
			[][]string{{"(testuser,public,powergen_non_root,testuser,ALL,{})"}},
			``,
			testUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen_non_root','user66');`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.show_training_pipeline_privilege('public.powergen_non_root','user66');`,
			[][]string{},
			``,
			testUser,
		},
	}
	cases := append(baseCases, testCases...)
	doTest(t, cases)
}

func TestMLRolePipeline(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []mlTestCase{
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen"}},
			"",
			security.RootUser,
		},
		{
			`CREATE ROLE roleCustom;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','roleCustom','select');`,
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
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen','testuser');`,
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
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','roleDev','delete|select');`,
			[][]string{{"roledev"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','roleDev','select');`,
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
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','select');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen','testuser');`,
			[][]string{
				{"testuser", "public", "powergen", "admin", "", "{rolecustom,roledev,roleread}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
				{"roledev", "public", "powergen", "root", "DELETE|SELECT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`CREATE ROLE roleTrain;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen','roleTrain','Train');`,
			[][]string{{"roletrain"}},
			``,
			security.RootUser,
		},
		{
			`GRANT roleTrain TO testuser;`,
			[][]string{},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','select|Train');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','testuser','select|Train');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): user 'testuser' has no existing privileges on training pipeline 'public.powergen'`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_training_pipeline_privilege('public.powergen','testuser');`,
			[][]string{
				{"testuser", "public", "powergen", "admin", "", "{rolecustom,roledev,roleread,roletrain}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
				{"roledev", "public", "powergen", "root", "DELETE|SELECT", "{}"},
				{"roletrain", "public", "powergen", "root", "TRAIN", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_training_pipeline_privilege('public.powergen','testuser') ORDER BY grantee DESC;`,
			[][]string{
				{"testuser", "public", "powergen", "admin", "", "{rolecustom,roledev,roleread,roletrain}"},
				{"roletrain", "public", "powergen", "root", "TRAIN", "{}"},
				{"roledev", "public", "powergen", "root", "DELETE|SELECT", "{}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT * FROM kwdbml.show_training_pipeline_privilege('public.powergen') ORDER BY grantee DESC;`,
			[][]string{
				{"roletrain", "public", "powergen", "root", "TRAIN", "{}"},
				{"roledev", "public", "powergen", "root", "DELETE|SELECT", "{}"},
				{"rolecustom", "public", "powergen", "root", "SELECT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen','roleDev','Delete');`,
			[][]string{{"roledev"}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen','testuser','All');`,
			[][]string{{
				"false",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen','roleDev');`,
			[][]string{
				{"roledev", "public", "powergen", "root", "SELECT", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.import_training_pipeline( 
	'powergen_non_root', 
	'Training pipeline to train a model for power generation prediction.', 
	'Regression', 
	'tensorflow_2.9', 
	'python_3.10', 
	'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
	'[{"name":"Active_Power", "type":"float"}]', 
	b'pipeline_entity',
	b'transformer_entity');`,
			[][]string{{"public.powergen_non_root"}},
			"",
			testUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen_non_root','Root','select');`,
			[][]string{{"root"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen_non_root','root');`,
			[][]string{
				{"root", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen_non_root','root','delete');`,
			[][]string{{"root"}},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): failed to revoke 'DELETE' privilege from 'root', should first revoke 'root' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','root','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen_non_root','Admin','Select');`,
			[][]string{{"admin"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen_non_root','admin');`,
			[][]string{
				{"admin", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen_non_root','admin','delete');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): failed to revoke 'DELETE' privilege from 'admin', should first revoke 'admin' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','admin','all');`,
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
			`SELECT kwdbml.grant_privilege_on_training_pipeline('public.powergen_non_root','Adminusr','Select');`,
			[][]string{{"adminusr"}},
			``,
			security.RootUser,
		},
		{
			`SELECT * from kwdbml.show_training_pipeline_privilege('public.powergen_non_root','adminusr');`,
			[][]string{
				{"adminusr", "public", "powergen_non_root", "admin", "ALL", "{}"},
			},
			``,
			security.RootUser,
		},
		{
			`SELECT kwdbml.revoke_privilege_on_training_pipeline('public.powergen_non_root','adminusr','delete');`,
			[][]string{},
			`pq: kwdbml.revoke_privilege_on_training_pipeline\(\): failed to revoke 'DELETE' privilege from 'adminusr', should first revoke 'adminusr' from 'admin' role`,
			security.RootUser,
		},
		{
			`SELECT kwdbml.has_training_pipeline_privilege('public.powergen_non_root','adminusr','all');`,
			[][]string{{
				"true",
			}},
			``,
			security.RootUser,
		},
	}
	cases := baseCases
	cases = append(cases, testCases...)
	doTest(t, cases)
}
