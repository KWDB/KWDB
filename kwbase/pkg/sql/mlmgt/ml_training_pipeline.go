// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	"context"
	"fmt"
	"strings"

	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
	"gitee.com/kwbasedb/kwbase/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// ImportTrainingPipeline imports a training pipeline definition into the system
func (m *MLManager) ImportTrainingPipeline(
	ctx context.Context, pipeline *tree.MLPipeline,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	newPipeline, err := mgt.createPipeline(pipeline)
	if err != nil {
		return "", err
	}

	pipelineName := newPipeline.getPipelineName()

	old, err := mgt.retrievePipeline(newPipeline.Schema, newPipeline.Name)
	if err != nil {
		if pgerror.GetPGCode(err) != pgcode.InvalidSchemaName {
			return "", err
		}
	}

	if old != nil {
		return "", pgerror.Newf(pgcode.DuplicateObject, "pipeline '%s' already exist", pipelineName)
	}

	newPipeline.PipelineID = uuid.MakeV4().String()

	if _, err = mgt.insertPipelineRecord(newPipeline); err != nil {
		return "", err
	}

	if _, err = mgt.grantPrivileges(
		mlTrainingPipeline,
		newPipeline.Schema,
		newPipeline.Name,
		m.Planner.User(),
		mlTrainingPipelineAllPrivilegeMask,
		true,
	); err != nil {
		return "", err
	}

	return pipelineName, nil
}

// TrainModel initiates model training using the specified pipeline
func (m *MLManager) TrainModel(
	ctx context.Context,
	inputPipelineName, inputModelName, trainData string,
	allowNewVersion bool,
	deploy bool,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	pipelineSchema, pipelineName, err := mgt.extractSchemaObjectName(inputPipelineName)
	if err != nil {
		return "", err
	}

	modelSchema, modelName, err := mgt.extractSchemaObjectName(inputModelName)
	if err != nil {
		return "", err
	}

	pipeline, err := mgt.retrievePipeline(pipelineSchema, pipelineName)
	if err != nil {
		return "", errors.Wrap(err, "failed to retrieve ml training pipeline")
	}

	if isAuthed, err := mgt.checkPrivileges(
		mlTrainingPipeline, m.Planner.User(), pipelineSchema, pipelineName, mlTrainPrivilegeMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on training pipeline '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlTrainingPipeline, mlTrainPrivilegeMask),
			pipelineName,
		)
	}

	checkedFullModelName := mlUtils.buildFullName(modelSchema, modelName)

	model, err := mgt.hasModel(modelSchema, modelName)
	if err != nil {
		return "", err
	}

	var modelIDString string

	// check if the target model is an existing one.
	if model != nil {
		if isAuthed, err := mgt.checkPrivileges(
			mlModel, m.Planner.User(), model.Schema, model.Name, mlCreatePrivilegeMask,
		); err != nil {
			return "", err
		} else if !isAuthed {
			return "", errors.Newf(
				"user '%s' does not have '%s' privilege on model '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlModel, mlCreatePrivilegeMask),
				model.getModelName(),
			)
		}

		if allowNewVersion == false {
			// TODO need to define a new error code.
			return "", pgerror.Newf(
				pgcode.InvalidParameterValue,
				"failed to creat a new version for an existing model '%s' "+
					"since the parameter 'new_version_if_exists' is '%t'",
				checkedFullModelName,
				allowNewVersion)
		}

		modelIDString = model.ModelID
	}

	// model id is empty for the first train and should be non-empty if it has an existing model with same model metadata.
	request, err := mgt.constructTrainingRequest(
		pipeline, modelIDString, checkedFullModelName, trainData, allowNewVersion, deploy,
	)
	if err != nil {
		return "", pgerror.Wrap(
			err, pgcode.Internal, "failed to construct the ML training input")
	}

	grpcConn, err := mlUtils.newClient(m.Planner)
	if err != nil {
		return "", err
	}

	resp, err := grpcConn.Train(ctx, request)

	if err != nil {
		// TODO need to use MLe specified error code.
		return "", err
	}

	var modelID interface{}
	if modelIDString != "" {
		modelID = modelIDString
	}

	if err = mgt.createJob(resp.RunId, modelID, nil, pipeline.PipelineID, pb.JobTypeTrain, resp.Status,
		request.DataSource.Query, m.Planner.User()); err != nil {
		return "", err
	}

	log.Infof(ctx, "model training job is started with id '%s'.", resp.RunId)

	return resp.RunId, nil
}

// EvaluateModel evaluates a trained model against test data
func (m *MLManager) EvaluateModel(
	ctx context.Context,
	inputPipelineName, modelName string,
	modelVersion int32,
	inputData string,
	objectives []string,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	pipelineSchema, pipelineName, err := mgt.extractSchemaObjectName(inputPipelineName)
	if err != nil && !strings.Contains(err.Error(), "object name is empty") {
		return "", err
	}

	var pipeline *pipelineRecord
	if len(pipelineSchema) > 0 && len(pipelineName) > 0 {
		pipeline, err = mgt.retrievePipeline(pipelineSchema, pipelineName)
		if err != nil {
			return "", errors.Wrap(err, "failed to retrieve ml training pipeline")
		}

		if isAuthed, err := mgt.checkPrivileges(
			mlTrainingPipeline, m.Planner.User(), pipelineSchema, pipelineName, mlTrainPrivilegeMask,
		); err != nil {
			return "", err
		} else if !isAuthed {
			return "", pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have '%s' privilege on training pipeline '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlTrainingPipeline, mlTrainPrivilegeMask),
				pipelineName,
			)
		}
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return "", err
	}

	model, err := mgt.fetchModelVersion(schema, name, modelVersion)
	if err != nil {
		return "", err
	}

	if isAuthed, err := mgt.checkPrivileges(
		mlModel, m.Planner.User(), model.Schema, model.Name, mlEvaluatePrivilegeMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", errors.Newf(
			"user '%s' does not have '%s' privilege on model '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlModel, mlEvaluatePrivilegeMask),
			model.getModelName(),
		)
	}

	inputQuery, err := mgt.buildDataSourceQuery(&model.Features, &model.Target, inputData)
	if err != nil {
		return "", err
	}

	// TODO check objectives? empty

	res, err := mgt.evaluateModel(pipeline, model, inputQuery, objectives)
	if err != nil {
		return "", err
	}

	if pipeline != nil {
		if err = mgt.createJob(res.RunId, model.ModelID, model.VersionID, pipeline.PipelineID,
			pb.JobTypeEvaluate, res.Status, inputQuery, m.Planner.User()); err != nil {
			return "", err
		}
	} else {
		if err = mgt.createJob(res.RunId, model.ModelID, model.VersionID, nil,
			pb.JobTypeEvaluate, res.Status, inputQuery, m.Planner.User()); err != nil {
			return "", err
		}
	}

	return res.RunId, nil
}

// BatchPredict performs batch prediction using a trained model
func (m *MLManager) BatchPredict(
	ctx context.Context,
	inputPipelineName, modelName string,
	modelVersion int32,
	inputData, outputTable, writeMode string,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	pipelineSchema, pipelineName, err := mgt.extractSchemaObjectName(inputPipelineName)
	if err != nil && !strings.Contains(err.Error(), "object name is empty") {
		return "", err
	}

	var pipeline *pipelineRecord
	if len(pipelineSchema) > 0 && len(pipelineName) > 0 {
		pipeline, err = mgt.retrievePipeline(pipelineSchema, pipelineName)
		if err != nil {
			return "", errors.Wrap(err, "failed to retrieve ml training pipeline")
		}

		if isAuthed, err := mgt.checkPrivileges(
			mlTrainingPipeline, m.Planner.User(), pipelineSchema, pipelineName, mlTrainPrivilegeMask,
		); err != nil {
			return "", err
		} else if !isAuthed {
			return "", pgerror.Newf(
				pgcode.InsufficientPrivilege,
				"user '%s' does not have '%s' privilege on training pipeline '%s'",
				m.Planner.User(),
				mlUtils.constructPrivilegeString(mlTrainingPipeline, mlTrainPrivilegeMask),
				pipelineName,
			)
		}
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return "", err
	}

	model, err := mgt.fetchModelVersion(schema, name, modelVersion)
	if err != nil {
		return "", err
	}

	if isAuthed, err := mgt.checkPrivileges(
		mlModel, m.Planner.User(), model.Schema, model.Name, mlPredictPrivilegeMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", errors.Newf(
			"user '%s' does not have '%s' privilege on model '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlModel, mlPredictPrivilegeMask),
			model.getModelName(),
		)
	}

	writeMode, err = mlUtils.checkOutputTableAction(writeMode)
	if err != nil {
		return "", err
	}

	inputQuery, err := mgt.buildDataSourceQuery(&model.Features, &[]*pb.Target{}, inputData)
	if err != nil {
		return "", errors.Wrap(err, "failed to build input query")
	}

	outColumns, err := mgt.checkOutputTable(&model.Features, &model.Target, inputData, outputTable, writeMode)
	if err != nil {
		return "", errors.Wrapf(err, "failed to check output table '%s'", outputTable)
	}

	res, err := mgt.batchPredict(pipeline, model, inputQuery, outputTable, writeMode, outColumns)
	if err != nil {
		return "", err
	}

	if pipeline != nil {
		if err = mgt.createJob(res.RunId, model.ModelID, model.VersionID, pipeline.PipelineID,
			pb.JobTypeBatchPredict, res.Status, inputQuery, m.Planner.User()); err != nil {
			return "", err
		}
	} else {
		if err = mgt.createJob(res.RunId, model.ModelID, model.VersionID, nil,
			pb.JobTypeBatchPredict, res.Status, inputQuery, m.Planner.User()); err != nil {
			return "", err
		}
	}
	return res.RunId, nil
}

// FineTune fine-tunes an existing model with additional training data
func (m *MLManager) FineTune(
	ctx context.Context, modelName string, dataSource string, columnName string,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	schema, name, err := mgt.extractSchemaObjectName(modelName)
	if err != nil {
		return "", err
	}

	// For fine-tune, we need to check if the model exists and user has privilege
	model, err := mgt.hasModel(schema, name)
	if err != nil {
		return "", err
	}

	if model == nil {
		return "", pgerror.Newf(
			pgcode.UndefinedObject,
			"model '%s' does not exist",
			modelName,
		)
	}

	// Validate that the target column exists in the data source table
	_, schema, table, err := mgt.extractTablePath(dataSource)
	if err != nil {
		return "", pgerror.Wrap(err, pgcode.InvalidParameterValue, "failed to parse data source table path")
	}

	// Use table_schema instead of table_catalog for KwDB multi-schema architecture
	columnCheckQuery := fmt.Sprintf(
		"SELECT 1 FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 AND column_name = $3 LIMIT 1",
	)
	row, err := mgt.planner.InternalExecutor().QueryRow(
		mgt.ctx, "check-column-existence", mgt.planner.Txn(), columnCheckQuery, schema, table, columnName,
	)
	if err != nil {
		return "", pgerror.Wrap(err, pgcode.InvalidParameterValue, "failed to check column existence")
	}
	if row == nil {
		return "", pgerror.Newf(
			pgcode.UndefinedColumn,
			"column '%s' does not exist in table '%s'",
			columnName, dataSource,
		)
	}

	// Build the data source for fine-tune request
	// dataSource is table name, columnName is the target column
	fineTuneDataSource := &pb.DataSource{
		Address:  mgt.planner.RPCContext().AdvertiseAddr(),
		Database: mgt.getExSession().Database,
		Query:    fmt.Sprintf("SELECT %s FROM %s LIMIT 50000", columnName, dataSource),
	}

	request := &pb.FineTuneRequest{
		Tsllmname:         modelName,
		DataSource:        fineTuneDataSource,
		ImmediatelyDeploy: true,
		User:              m.Planner.User(),
	}

	grpcConn, err := mlUtils.newClient(m.Planner)
	if err != nil {
		return "", err
	}

	resp, err := grpcConn.FineTune(ctx, request)
	if err != nil {
		return "", err
	}

	// Create a job record for the fine-tune operation
	if err = mgt.createJob(resp.RunId, model.ModelID, nil, nil,
		pb.JobTypeFineTune, resp.Status, dataSource, m.Planner.User()); err != nil {
		return "", err
	}

	log.Infof(ctx, "model fine-tune job is started with id '%s'.", resp.RunId)

	return resp.RunId, nil
}

// FindTrainingPipelines searches for training pipelines matching criteria
// FindTrainingPipelines searches for training pipelines matching criteria
func (m *MLManager) FindTrainingPipelines(ctx context.Context) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	roleMembers, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(m.Planner.User())
	if err != nil {
		return nil, err
	}

	stmt := `SELECT tp.schema, tp.name, description, creator, created, problem_type, features, target, framework, runtime
FROM kwdbml.ml_training_pipelines tp`

	if !isAdmin {
		stmt = fmt.Sprintf(` %s
			INNER JOIN kwdbml.ml_privileges p
			ON p.name = tp.name AND p.type = %d
			AND p.privilege_type >= %d
			AND p.grantee IN ('%s')`,
			stmt,
			mlTrainingPipeline,
			mlSelectPrivilegeMask,
			strings.Join(roleMembers, "','"),
		)
	}

	stmt = fmt.Sprintf(` %s ORDER BY tp.created`, stmt)

	return m.Planner.InternalExecutor().Query(ctx, "fetch-all-ml-pipelines", m.Planner.Txn(), stmt)
}

// FindTrainingPipeline finds a specific training pipeline by its ID
func (m *MLManager) FindTrainingPipeline(
	ctx context.Context, pipelineName string,
) ([]tree.Datums, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	schema, name, err := mgt.extractSchemaObjectName(pipelineName)
	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.InvalidParameterValue {
			return []tree.Datums{}, nil
		}
		return nil, err
	}

	roleMembers, isAdmin, err := mgt.extractRoleMembersWithAdminFlag(m.Planner.User())
	if err != nil {
		return nil, err
	}

	stmt := `SELECT tp.schema, tp.name, description, creator, created, problem_type, features, target, framework, runtime
FROM kwdbml.ml_training_pipelines tp`

	if !isAdmin {
		stmt = fmt.Sprintf(` %s
			INNER JOIN kwdbml.ml_privileges p
			ON p.name = tp.name AND p.type = %d
			AND p.privilege_type >= %d
			AND p.grantee IN ('%s')`,
			stmt,
			mlTrainingPipeline,
			mlSelectPrivilegeMask,
			strings.Join(roleMembers, "','"),
		)
	}

	stmt = fmt.Sprintf(` %s WHERE tp.schema = '%s' AND tp.name = '%s'`, stmt, schema, name)

	return m.Planner.InternalExecutor().Query(ctx, "fetch-ml-pipeline", m.Planner.Txn(), stmt)
}

// RemoveTrainingPipeline removes a training pipeline and its associated resources
func (m *MLManager) RemoveTrainingPipeline(
	ctx context.Context, pipelineName string,
) (string, error) {
	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	schema, name, err := mgt.extractSchemaObjectName(pipelineName)
	if err != nil {
		return "", err
	}

	pipeline, err := mgt.retrievePipeline(schema, name)
	if err != nil {
		return "", err
	}

	if isAuthed, err := mgt.checkPrivileges(
		mlTrainingPipeline, m.Planner.User(), schema, name, mlDeletePrivilegeMask,
	); err != nil {
		return "", err
	} else if !isAuthed {
		return "", pgerror.Newf(
			pgcode.InsufficientPrivilege,
			"user '%s' does not have '%s' privilege on training pipeline '%s'",
			m.Planner.User(),
			mlUtils.constructPrivilegeString(mlTrainingPipeline, mlDeletePrivilegeMask),
			pipelineName,
		)
	}

	if active, err := mgt.isActivePipeline(pipeline.PipelineID); err != nil {
		return "", err
	} else if active {
		return "", errors.Errorf(
			"failed to delete the ml training pipeline '%s' since it's used by a long run job",
			pipeline.getPipelineName(),
		)
	}

	if err = mgt.deleteMLPrivilege(mlTrainingPipeline, schema, name); err != nil {
		return "", err
	}

	if err = mgt.deletePipeline(pipeline.PipelineID); err != nil {
		return "", err
	}

	return pipeline.getPipelineName(), nil
}

// GrantTrainingPipeline grants privileges on a training pipeline to a user
func (m *MLManager) GrantTrainingPipeline(
	ctx context.Context, pipelineName string, user string, privilegeStr string,
) (string, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	return mgt.grantMLPrivileges(mlTrainingPipeline, pipelineName, user, privilegeStr)
}

// RevokeTrainingPipeline revokes privileges on a training pipeline from a user
func (m *MLManager) RevokeTrainingPipeline(
	ctx context.Context, pipelineName string, user string, privilegeStr string,
) (string, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return "", err
	}

	return mgt.revokeMLPrivileges(mlTrainingPipeline, pipelineName, user, privilegeStr)
}

// CheckTrainingPipelinePrivileges checks if a user has the required privileges on a training pipeline
func (m *MLManager) CheckTrainingPipelinePrivileges(
	ctx context.Context, pipelineName string, user string, privilegeStr string,
) (bool, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return false, err
	}

	return mgt.checkMLPrivileges(mlTrainingPipeline, pipelineName, user, privilegeStr)
}

// ShowTrainingPipelinePrivileges displays the privileges granted on a training pipeline
func (m *MLManager) ShowTrainingPipelinePrivileges(
	ctx context.Context, modelName string, user string,
) ([]tree.Datums, error) {
	user = strings.ToLower(user)

	mgt, err := newMLManagement(ctx, m.Planner)
	if err != nil {
		return nil, err
	}

	return mgt.showMLPrivileges(mlTrainingPipeline, modelName, user)
}
