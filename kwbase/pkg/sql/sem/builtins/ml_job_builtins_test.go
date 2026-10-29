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
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mlagent"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

var testJobTestCases = []mlTestCase{
	{
		`INSERT INTO kwdbml.ml_models (id, schema, name, description, problem_type, features, target, framework,
			runtime, creator, created, last_updated, active_version_id, status) 
		VALUES ('10000000000000000000000000000000', 'public', 'powergen', '', 'r', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]',
		'[{"name":"prediction", "type":"float"}]',	'tensorflow_2.9', 'python_3.10', 'root', now(), now(),
		'10000000000000000000000000000001','{"status": "Initializing"}');`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_model_versions(id, model_id, version, creator, created, 
		metrics, description, entity, md5)
		VALUES ('10000000000000000000000000000001', '10000000000000000000000000000000', 1, 'root', now(), 
		null, '', b'entity.v1', '');`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_jobs (id, model_id, model_version_id, training_pipeline_id, creator, created,
		job_type, job_status, job_input, settings)
		VALUES ('00000000000000000000000000000001', '10000000000000000000000000000000', '10000000000000000000000000000001', NULL, 'root', now(), 'EVALUATE', 'PENDING', '{}', NULL);`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_training_pipelines (id, schema, name, description, problem_type, features, target,
							framework, runtime, creator, created, pipeline_entity, pipeline_md5)
		VALUES ('20000000000000000000000000000001', 'public', 'powergenpipeline1', '', 'r', 
		'[{"name":"Current_Phase_Average", "type":"float"}, {"name":"Weather_Temperature_Celsius", "type":"float"}]', 
		'[{"name":"prediction", "type":"float"}]', 
		'tensorflow_2.9', 'python_3.10', 	'testuser', now(), b'pipeline_entity2', '');`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_jobs (id, model_id, model_version_id, training_pipeline_id, creator, created,
		job_type, job_status, job_input, settings)
		VALUES ('00000000000000000000000000000002', NULL, NULL, '20000000000000000000000000000001', 'testuser', now(), 'TRAIN', 'RUNNING', '{}', NULL);`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_jobs (id, model_id, model_version_id, training_pipeline_id, creator, created,
		job_type, job_status, job_input, settings)
		VALUES ('` + mlagent.TestErrorJobID + `', '10000000000000000000000000000000', '10000000000000000000000000000001', NULL, 'testuser', now(), 'BATCH_PREDICT', 'PENDING', '{}', NULL);`,
		[][]string{},
		"",
		security.RootUser,
	},
	{
		`INSERT INTO kwdbml.ml_jobs (id, model_id, model_version_id, training_pipeline_id, creator, created,
		job_type, job_status, job_input, settings)
		VALUES ('` + mlagent.TestNullJobID + `', NULL, NULL, '20000000000000000000000000000001', 'testuser', now(), 'TRAIN', 'RUNNING', '{}', NULL);`,
		[][]string{},
		"",
		security.RootUser,
	},
}

func TestMLShowOneJob(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var testCases = []mlTestCase{}
	cases := []mlTestCase{baseCases[1]}
	cases = append(cases, testJobTestCases...)
	cases = append(cases, testCases...)
	doTest(t, cases)
}
