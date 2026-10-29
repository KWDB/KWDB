// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package mlmgt

import (
	ejson "encoding/json"

	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	pb "gitee.com/kwbasedb/kwbase/pkg/sql/mlagent/serverpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

func (mgt *mlManagement) createPipeline(param *tree.MLPipeline) (*pipelineRecord, error) {
	schema, name, err := mgt.extractSchemaObjectName(string(*param.Name))
	if err != nil {
		return nil, err
	}

	pipeline := &pipelineRecord{
		Creator: mgt.planner.User(),
		Schema:  schema,
	}

	pipeline.Name = name
	if !lex.CheckNameValid(pipeline.Name) {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "training pipeline name '%s' is unsupported", pipeline.Name,
		)
	}

	pipeline.Description = string(*param.Description)

	pipeline.ProblemType, err = mlUtils.checkProblemType(string(*param.ProblemType))
	if err != nil {
		return nil, err
	}

	pipeline.Framework, err = mlUtils.checkFramework(string(*param.Framework))
	if err != nil {
		return nil, err
	}

	pipeline.Runtime, err = mlUtils.checkRuntime(string(*param.Runtime))
	if err != nil {
		return nil, err
	}

	if pipeline.FeaturesJSON, pipeline.Features, err = mlUtils.checkFeatures(*param.Features); err != nil {
		return nil, err
	}

	if pipeline.TargetJSON, pipeline.Target, err = mlUtils.checkTargets(*param.Target); err != nil {
		return nil, err
	}

	if param.PipelineEntity != nil {
		pipeline.PipelineEntity = []byte(*param.PipelineEntity)
		if pipeline.PipelineEntityMd5, err = mlUtils.computeChecksum(pipeline.PipelineEntity); err != nil {
			return nil, err
		}
	}

	if param.TransformerEntity != nil {
		pipeline.TransformerEntity = []byte(*param.TransformerEntity)
		if pipeline.TransformerMd5, err = mlUtils.computeChecksum(pipeline.TransformerEntity); err != nil {
			return nil, err
		}
	}

	return pipeline, nil
}

func (mgt *mlManagement) insertPipelineRecord(p *pipelineRecord) (string, error) {
	num, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"create-ml-pipeline",
		mgt.planner.Txn(),
		`INSERT INTO kwdbml.ml_training_pipelines (id, Schema, name, description, problem_type, features, target,
							framework, runtime, Creator, created, pipeline_entity, pipeline_md5, transformer_entity, transformer_md5) 
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)`,
		p.PipelineID,
		p.Schema,
		p.Name,
		p.Description,
		p.ProblemType,
		p.FeaturesJSON,
		p.TargetJSON,
		p.Framework,
		p.Runtime,
		p.Creator,
		"now()",
		p.PipelineEntity,
		p.PipelineEntityMd5,
		p.getTransformerEntityOrNull(),
		p.getTransformerMd5OrNull(),
	)

	if err != nil {
		if pgerror.GetPGCode(err) == pgcode.UniqueViolation {
			return "", pgerror.Newf(
				pgcode.DuplicateObject, "training pipeline '%s' already exists", p.getPipelineName(),
			)
		}
		return "", errors.Wrap(err, "failed to create new ml training pipeline")
	}

	if num != 1 {
		return "", errors.Newf("unexpected number of rows affected: %d", num)
	}

	return p.PipelineID, nil
}

func (mgt *mlManagement) deletePipeline(pipelineID string) error {
	if _, err := mgt.planner.InternalExecutor().Exec(
		mgt.ctx,
		"remove-one-ml-pipeline",
		mgt.planner.Txn(),
		"DELETE FROM kwdbml.ml_training_pipelines WHERE id = $1",
		pipelineID,
	); err != nil {
		return errors.Wrap(err, "failed to delete the ml training pipeline")
	}

	return nil
}

func (mgt *mlManagement) isActivePipeline(pipelineID string) (bool, error) {
	if rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"check-job-status",
		mgt.planner.Txn(),
		`SELECT p.id FROM kwdbml.ml_jobs j, kwdbml.ml_training_pipelines p 
		WHERE j.training_pipeline_id = p.id AND p.id = $1
		AND (job_status = $2 OR job_status = $3)`,
		pipelineID,
		pb.JobStatusPending,
		pb.JobStatusRunning,
	); err != nil {
		return false, errors.Wrap(err, "failed to fetch the ML job status")
	} else if len(rows) > 0 {
		return true, nil
	}

	return false, nil
}

func (mgt *mlManagement) retrievePipeline(schema, name string) (*pipelineRecord, error) {
	rows, err := mgt.planner.InternalExecutor().Query(
		mgt.ctx,
		"retrieve-one-pipeline",
		mgt.planner.Txn(),
		`SELECT id, Schema, name, description, problem_type, framework, runtime, features, target, 
						pipeline_entity, transformer_entity 
		FROM kwdbml.ml_training_pipelines 
		WHERE Schema = $1 AND name = $2 LIMIT 1`,
		schema,
		name,
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch the ml training pipeline metadata")
	}
	if len(rows) < 1 {
		return nil, pgerror.Newf(pgcode.InvalidSchemaName, "training pipeline '%s.%s' does not exist", schema, name)
	}

	return mgt.parseMLPipeline(rows[0])
}

func (mgt *mlManagement) constructTrainingRequest(
	pipeline *pipelineRecord, modelID, modelName, trainData string, allowNewVersion, deploy bool,
) (*pb.TrainingRequest, error) {
	inputQuery, err := mgt.buildDataSourceQuery(&pipeline.Features, &pipeline.Target, trainData)
	if err != nil {
		return nil, err
	}

	var modelMeta = pb.ModelMeta{
		Framework:   pipeline.Framework,
		Runtime:     pipeline.Runtime,
		ProblemType: pipeline.ProblemType,
		Features:    pipeline.Features,
		Target:      pipeline.Target,
	}

	pipelineInfo := pb.PipelineInfo{
		PipelineName: pipeline.getPipelineName(),
		ModelMeta:    &modelMeta,
	}

	if pipeline.PipelineEntity != nil {
		pipelineInfo.PipelineEntity = pipeline.PipelineEntity
	}

	if pipeline.TransformerEntity != nil {
		pipelineInfo.DataPreparationEntity = pipeline.TransformerEntity
	}

	modelTrainingInfo := pb.ModelTrainingInfo{
		User:        mgt.planner.User(),
		ModelId:     modelID,
		ModelName:   modelName,
		Description: "Model trained by pipeline " + pipeline.getPipelineName() + ".",
	}

	input := pb.TrainingRequest{
		PipelineInfo:       &pipelineInfo,
		ModelTrainingInfo:  &modelTrainingInfo,
		NewVersionIfExists: allowNewVersion,
		ImmediatelyDeploy:  deploy,
	}

	// use kdp as the default database, user should provide the correct database and table name in parameter 'trainData'
	dataSource := &pb.DataSource{
		Address:  mgt.planner.RPCContext().AdvertiseAddr(),
		Database: mgt.getExSession().Database,
		Query:    inputQuery,
	}

	input.DataSource = dataSource

	return &input, nil
}

func (mgt *mlManagement) parseMLPipeline(row tree.Datums) (*pipelineRecord, error) {
	pipeline := &pipelineRecord{
		PipelineID:   row[0].(*tree.DUuid).UUID.String(),
		Schema:       string(*row[1].(*tree.DString)),
		Name:         string(*row[2].(*tree.DString)),
		Description:  string(*row[3].(*tree.DString)),
		ProblemType:  string(*row[4].(*tree.DString)),
		Framework:    string(*row[5].(*tree.DString)),
		Runtime:      string(*row[6].(*tree.DString)),
		FeaturesJSON: tree.MustBeDJSON(row[7]).JSON.String(),
		TargetJSON:   tree.MustBeDJSON(row[8]).JSON.String(),
	}

	if err := ejson.Unmarshal([]byte(pipeline.FeaturesJSON), &pipeline.Features); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the JSON string of model features")
	}

	if err := ejson.Unmarshal([]byte(pipeline.TargetJSON), &pipeline.Target); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal the JSON string of model target")
	}

	if row[9] != tree.DNull {
		pipeline.PipelineEntity = []byte(*row[9].(*tree.DBytes))
	}
	if row[10] != tree.DNull {
		pipeline.TransformerEntity = []byte(*row[10].(*tree.DBytes))
	}

	return pipeline, nil
}

func (mgt *mlManagement) evaluateModel(
	pipeline *pipelineRecord, model *modelVersionRecord, inputQuery string, objectives []string,
) (*pb.CommonResponse, error) {
	req := pb.EvaluationRequest{
		ModelInfo: &pb.ModelInfo{
			User:         model.Creator,
			ModelName:    model.getModelName(),
			ModelVersion: model.genVersionString(),
			ModelMeta: &pb.ModelMeta{
				Framework:   model.Framework,
				Runtime:     model.Runtime,
				ProblemType: model.ProblemType,
				Features:    model.Features,
				Target:      model.Target,
			},
			ModelEntity: model.ModelEntity,
		},
		DataSource: &pb.DataSource{
			Address:  mgt.planner.RPCContext().AdvertiseAddr(),
			Database: mgt.getExSession().Database,
			Query:    inputQuery,
		},
		Metrics:     objectives,
		CurrentUser: mgt.planner.User(),
	}

	if model.TransformerEntity != nil {
		req.ModelInfo.DataPreparationEntity = model.TransformerEntity
	}

	if pipeline != nil {
		req.PipelineInfo = &pb.PipelineInfo{
			PipelineName:   pipeline.getPipelineName(),
			PipelineEntity: pipeline.PipelineEntity,
		}
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.Evaluate(mgt.ctx, &req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (mgt *mlManagement) batchPredict(
	pipeline *pipelineRecord,
	model *modelVersionRecord,
	inputQuery, outputTable, writeModel string,
	OutputColumns []string,
) (*pb.CommonResponse, error) {
	req := pb.BatchPredictRequest{
		ModelInfo: &pb.ModelInfo{
			User:         model.Creator,
			ModelName:    model.getModelName(),
			ModelVersion: model.genVersionString(),
			ModelMeta: &pb.ModelMeta{
				Framework:   model.Framework,
				Runtime:     model.Runtime,
				ProblemType: model.ProblemType,
				Features:    model.Features,
				Target:      model.Target,
			},
			ModelEntity: model.ModelEntity,
		},
		DataSource: &pb.DataSource{
			Address:  mgt.planner.RPCContext().AdvertiseAddr(),
			Database: mgt.getExSession().Database,
			Query:    inputQuery,
		},
		OutputTable:   outputTable,
		OutputColumns: OutputColumns,
		Strategy:      writeModel,
		CurrentUser:   mgt.planner.User(),
	}

	if model.TransformerEntity != nil {
		req.ModelInfo.DataPreparationEntity = model.TransformerEntity
	}

	if pipeline != nil {
		req.PipelineInfo = &pb.PipelineInfo{
			PipelineName:   pipeline.getPipelineName(),
			PipelineEntity: pipeline.PipelineEntity,
		}
	}

	mlClient, err := mlUtils.newClient(mgt.planner)
	if err != nil {
		return nil, err
	}

	res, err := mlClient.BatchPredict(mgt.ctx, &req)
	if err != nil {
		return nil, err
	}

	return res, nil
}

type pipelineRecord struct {
	PipelineID        string
	Schema            string
	Name              string
	Description       string
	ProblemType       string
	Framework         string
	Runtime           string
	FeaturesJSON      string
	Features          []*pb.Feature
	TargetJSON        string
	Target            []*pb.Target
	PipelineEntity    []byte
	TransformerEntity []byte
	PipelineEntityMd5 string
	TransformerMd5    string
	Creator           string
}

func (p *pipelineRecord) getPipelineName() string {
	return mlUtils.buildFullName(p.Schema, p.Name)
}

func (p *pipelineRecord) getTransformerEntityOrNull() interface{} {
	if p.TransformerMd5 == "" {
		return nil
	}
	return p.TransformerEntity
}

func (p *pipelineRecord) getTransformerMd5OrNull() interface{} {
	if p.TransformerMd5 == "" {
		return nil
	}
	return p.TransformerMd5
}
