// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/security"
	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

const (
	createMLJOBS = `CREATE TABLE IF NOT EXISTS kwdbml.ml_jobs (
		id                      UUID NOT NULL,
		model_id                UUID,
		model_version_id        UUID,
		training_pipeline_id    UUID,
		creator                 STRING, 
		created                 TIMESTAMP, 
		scheduled               TIMESTAMP, 
		finished                TIMESTAMP, 
		job_type                STRING, 
		job_status              STRING, 
		job_input               STRING, 
		settings                JSONB, 
		result                  JSONB, 
		error                   STRING, 
		CONSTRAINT "primary" PRIMARY KEY (id), 
		INDEX(model_id), 
		INDEX(model_version_id), 
		INDEX(training_pipeline_id), 
		FAMILY "primary" (id, model_id, model_version_id, training_pipeline_id, creator, created, scheduled, finished, job_type, job_status, job_input, settings, result, error) 
		);`

	createMLPRIVILEGES = `CREATE TABLE IF NOT EXISTS kwdbml.ml_privileges ( 
		grantee STRING NOT NULL, 
		schema STRING NOT NULL, 
		name STRING NOT NULL, 
		type INT2 NOT NULL, 
		privilege_type INT2 NOT NULL, 
		grantor STRING NOT NULL, 
		CONSTRAINT "primary" PRIMARY KEY (grantee ASC, schema ASC, name ASC, type ASC, privilege_type ASC), 
		INDEX ml_privileges_schema_name_type_idx (schema ASC, name ASC, type ASC), 
		FAMILY "primary" (grantee, schema, name, type, privilege_type, grantor) 
		); `

	createTRAININGPIPELINES = `CREATE TABLE IF NOT EXISTS kwdbml.ml_training_pipelines ( 
		id UUID NOT NULL, 
		schema STRING NOT NULL, 
		name STRING NOT NULL, 
		description STRING NOT NULL, 
		problem_type STRING NOT NULL, 
		features JSONB NOT NULL, 
		target JSONB NOT NULL, 
		framework STRING NOT NULL, 
		runtime STRING NOT NULL, 
		creator STRING NOT NULL, 
		created TIMESTAMP NOT NULL, 
		pipeline_entity BYTES NOT NULL, 
		pipeline_md5 STRING NOT NULL, 
		transformer_entity BYTES NULL, 
		transformer_md5 STRING NULL, 
		CONSTRAINT "primary" PRIMARY KEY (id ASC), 
		UNIQUE INDEX ml_training_pipelines_schema_name_idx (schema ASC, name ASC), 
		FAMILY "primary" (id, schema, name, description, problem_type, features, target, framework, runtime, creator, created), 
		FAMILY fam_1_pipeline (pipeline_entity, pipeline_md5), 
		FAMILY fam_2_transformer (transformer_entity, transformer_md5) 
		);`

	createMLMODELS string = `CREATE TABLE IF NOT EXISTS kwdbml.ml_models ( 
		id UUID NOT NULL, 
		schema STRING NOT NULL, 
		name STRING NOT NULL, 
		description STRING NULL, 
		problem_type STRING NOT NULL, 
		features JSONB NOT NULL, 
		target JSONB NOT NULL, 
		framework STRING NOT NULL, 
		runtime STRING NOT NULL, 
		creator STRING NOT NULL, 
		created TIMESTAMP NOT NULL, 
		last_updated TIMESTAMP NOT NULL, 
		active_version_id UUID NOT NULL, 
		status JSONB NOT NULL, 
		CONSTRAINT "primary" PRIMARY KEY (id ASC), 
		UNIQUE INDEX ml_models_schema_name_idx (schema ASC, name ASC), 
		FAMILY "primary" (id, schema, name, description, problem_type, features, target, framework, runtime, creator, created, last_updated, active_version_id, status) 
		);`

	createMLMODELVERSIONS string = `CREATE TABLE IF NOT EXISTS kwdbml.ml_model_versions ( 
		id UUID NOT NULL, 
		model_id UUID NOT NULL, 
		version INT4 NOT NULL, 
		creator STRING NOT NULL, 
		created TIMESTAMP NOT NULL, 
		training_job_id UUID NULL, 
		metrics JSONB NULL, 
		description STRING NULL, 
		entity BYTES NOT NULL, 
		md5 STRING NOT NULL, 
		transformer_entity BYTES NULL, 
		transformer_md5 STRING NULL, 
		CONSTRAINT "primary" PRIMARY KEY (id ASC), 
		CONSTRAINT foreign_model_id FOREIGN KEY (model_id) REFERENCES kwdbml.public.ml_models(id), 
		INDEX ml_model_versions_model_id_idx (model_id ASC), 
		FAMILY "primary" (id, model_id, version, creator, created, training_job_id, metrics, description), 
		FAMILY fam_1_entity (entity, md5), 
		FAMILY fam_2_transformer (transformer_entity, transformer_md5) 
		);`
)

// Initializer initializer
type Initializer struct {
	ctx              context.Context
	cs               *cluster.Settings
	internalExecutor sqlutil.InternalExecutor
}

var mu syncutil.Mutex

// NewMLInitializer new a instance
func NewMLInitializer(
	ctx context.Context, cs *cluster.Settings, ie sqlutil.InternalExecutor,
) *Initializer {
	Initializer := &Initializer{
		ctx:              ctx,
		cs:               cs,
		internalExecutor: ie,
	}

	return Initializer
}

// create ml related tables and users
func (ml *Initializer) createTables() error {
	sqlUser := security.RootUser

	if _, err := ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-kwdbml",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		`CREATE DATABASE IF NOT EXISTS kwdbml;`,
	); err != nil {
		return err
	}

	_, err := ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-table-mljobs",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		createMLJOBS,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	_, err = ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-table-mlprivileges",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		createMLPRIVILEGES,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	_, err = ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-table-trainingpipelines",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		createTRAININGPIPELINES,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	_, err = ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-table-mlmodels",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		createMLMODELS,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	_, err = ml.internalExecutor.ExecEx(
		ml.ctx,
		"create-table-mlmodelversions",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		createMLMODELVERSIONS,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	rows, err := ml.internalExecutor.QueryEx(
		ml.ctx,
		"query user",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		`select username from system.users where username = 'kwdbmlusr'`,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	// create internal user
	if len(rows) == 0 {
		_, err := ml.internalExecutor.ExecEx(
			ml.ctx,
			"create-kwdbmlusr",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
			`create user kwdbmlusr with password NULL;`,
		)
		if err != nil {
			return err
		}
	}

	sqlMlUser := []string{
		"grant select on table kwdbml.ml_privileges to kwdbmlusr;",
		"grant update on table kwdbml.ml_privileges to kwdbmlusr;",
		"grant select on table kwdbml.ml_training_pipelines to kwdbmlusr;",
		"grant update on table kwdbml.ml_training_pipelines to kwdbmlusr;",
		"grant select on table kwdbml.ml_jobs to kwdbmlusr;",
		"grant update on table kwdbml.ml_jobs to kwdbmlusr;",
		"grant select on table kwdbml.ml_models to kwdbmlusr;",
		"grant update on table kwdbml.ml_models to kwdbmlusr;",
		"grant select on table kwdbml.ml_model_versions to kwdbmlusr;",
		"grant update on table kwdbml.ml_model_versions to kwdbmlusr;"}

	for i := range sqlMlUser {
		sql := sqlMlUser[i]

		_, err = ml.internalExecutor.ExecEx(
			ml.ctx,
			"create-grant-kwdbmlusr",
			nil,
			sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
			sql,
		)
		if err != nil {
			return errors.Wrapf(err, "")
		}
	}

	// grant root role to kwdbmlusr
	_, err = ml.internalExecutor.ExecEx(
		ml.ctx,
		"grant-root-to-kwdbmlusr",
		nil,
		sqlbase.InternalExecutorSessionDataOverride{User: sqlUser},
		`GRANT root TO kwdbmlusr;`,
	)
	if err != nil {
		return errors.Wrapf(err, "")
	}

	return nil
}

// Initialize ml env
func (ml *Initializer) Initialize() error {
	mu.Lock()
	defer mu.Unlock()

	if err := ml.createTables(); err != nil {
		return err
	}

	// import internal model
	if err := ImportTSLLM(ml.ctx, ml.internalExecutor); err != nil {
		return err
	}

	return nil
}
