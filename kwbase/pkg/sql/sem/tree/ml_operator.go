// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package tree

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/util/json"
)

// MLOperator is the machine-learning interface that the kwdbml.* SQL builtins
// dispatch into. The concrete implementation is provided by the *planner type
// in the parent sql package and is wired onto EvalContext.ML at execution
// time. Keeping this as an interface here lets the sem/tree package stay free
// of any planner / executor dependency.
type MLOperator interface {
	ImportModel(ctx context.Context, model *MLModel) (string, int32, error)
	FindModels(ctx context.Context) ([]Datums, error)
	FindModel(ctx context.Context, modelName string, modelVersion int32) ([]Datums, error)
	RemoveModel(ctx context.Context, modelName string, modelVersion int32) (string, error)

	SetActive(ctx context.Context, modelName string, modelVersion int32) (int32, error)
	Predict(ctx context.Context, args Datums) (json.JSON, error)

	GrantModel(ctx context.Context, modelName string, user string, privilegeStr string) (string, error)
	RevokeModel(ctx context.Context, modelName string, user string, privilegeStr string) (string, error)
	CheckModelPrivileges(ctx context.Context, modelName string, user string, privilegeStr string) (bool, error)
	ShowModelPrivileges(ctx context.Context, modelName string, user string) ([]Datums, error)

	FindResources(ctx context.Context) ([]Datums, error)

	ImportTrainingPipeline(ctx context.Context, pipeline *MLPipeline) (string, error)
	FindTrainingPipelines(ctx context.Context) ([]Datums, error)
	FindTrainingPipeline(ctx context.Context, pipelineName string) ([]Datums, error)
	RemoveTrainingPipeline(ctx context.Context, pipelineName string) (string, error)

	GrantTrainingPipeline(ctx context.Context, pipelineName string, user string, privilegeStr string) (string, error)
	RevokeTrainingPipeline(ctx context.Context, pipelineName string, user string, privilegeStr string) (string, error)
	CheckTrainingPipelinePrivileges(
		ctx context.Context, pipelineName string, user string, privilegeStr string,
	) (bool, error)
	ShowTrainingPipelinePrivileges(ctx context.Context, pipelineName string, user string) ([]Datums, error)

	GetAllMLGrantUsers(ctx context.Context) ([]Datums, error)

	TrainModel(ctx context.Context, inputPipelineName, inputModelName, trainData string,
		allowNewVersion bool, deploy bool) (string, error)
	EvaluateModel(ctx context.Context, inputPipelineName string, modelName string, modelVersion int32,
		inputData string, objectives []string) (string, error)
	BatchPredict(
		ctx context.Context, inputPipelineName string, modelName string, modelVersion int32, inputData string, outputTable string, writeMode string,
	) (string, error)

	FineTune(ctx context.Context, modelName string, dataSource string, columnName string) (string, error)

	FindJobs(
		ctx context.Context, modelName string, modelVersion int32, jobTypeStr string, jobStatusStr string,
	) ([]Datums, error)
	FindJob(ctx context.Context, jobID string) ([]Datums, error)
	RemoveJob(ctx context.Context, jobID string) (DUuid, error)
	CancelJob(ctx context.Context, jobID string) (DUuid, error)
}

// MLModel carries the parameters of `kwdbml.import_model(...)`.
type MLModel struct {
	Name              *DString
	Description       *DString
	ProblemType       *DString
	Framework         *DString
	Runtime           *DString
	Features          *DJSON
	Target            *DJSON
	ModelEntity       *DBytes
	TransformerEntity *DBytes
	Metrics           *DJSON
}

// MLPipeline carries the parameters of `kwdbml.import_training_pipeline(...)`.
type MLPipeline struct {
	Name              *DString
	Description       *DString
	ProblemType       *DString
	Framework         *DString
	Runtime           *DString
	Features          *DJSON
	Target            *DJSON
	PipelineEntity    *DBytes
	TransformerEntity *DBytes
}
