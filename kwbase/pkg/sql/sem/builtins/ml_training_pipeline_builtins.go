// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package builtins

import (
	stdstrings "strings"

	sqlpgcode "gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	sqlpgerror "gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	sqltree "gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	sqltypes "gitee.com/kwbasedb/kwbase/pkg/sql/types"
	crdberrors "github.com/cockroachdb/errors"
)

func initMLTrainingPipelineBuiltins() {
	for builtinName, definition := range mlTrainingPipelineBuiltins {
		if _, alreadyRegistered := builtins[builtinName]; alreadyRegistered {
			panic("duplicate builtin: " + builtinName)
		}
		builtins[builtinName] = definition
	}
}

var mlTrainingPipelineBuiltins = map[string]builtinDefinition{
	"kwdbml.import_training_pipeline": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: true},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "description", Typ: sqltypes.String},
				{Name: "problem_type", Typ: sqltypes.String},
				{Name: "framework", Typ: sqltypes.String},
				{Name: "runtime", Typ: sqltypes.String},
				{Name: "features", Typ: sqltypes.Jsonb},
				{Name: "target", Typ: sqltypes.Jsonb},
				{Name: "pipeline_entity", Typ: sqltypes.Bytes},
				{Name: "transformer_entity", Typ: sqltypes.Bytes},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				for argumentIndex := range arguments {
					if argumentIndex == 8 || arguments[argumentIndex] != sqltree.DNull {
						continue
					}
					return nil, sqlpgerror.Newf(
						sqlpgcode.UndefinedFunction,
						`unknown signature: %s() the %d arg not null`,
						"kwdbml.import_training_pipeline",
						argumentIndex,
					)
				}

				pipelineName := sqltree.MustBeDString(arguments[0])
				pipelineDescription := sqltree.MustBeDString(arguments[1])
				problemType := sqltree.MustBeDString(arguments[2])
				frameworkName := sqltree.MustBeDString(arguments[3])
				runtimeName := sqltree.MustBeDString(arguments[4])
				featureSpec := sqltree.MustBeDJSON(arguments[5])
				targetSpec := sqltree.MustBeDJSON(arguments[6])
				pipelineBytes := sqltree.MustBeDBytes(arguments[7])

				var transformerEntity *sqltree.DBytes
				if arguments[8] != sqltree.DNull {
					transformerBytes := sqltree.MustBeDBytes(arguments[8])
					transformerEntity = &transformerBytes
				}

				pipelineDefinition := &sqltree.MLPipeline{
					Name:              &pipelineName,
					Description:       &pipelineDescription,
					ProblemType:       &problemType,
					Framework:         &frameworkName,
					Runtime:           &runtimeName,
					Features:          &featureSpec,
					Target:            &targetSpec,
					PipelineEntity:    &pipelineBytes,
					TransformerEntity: transformerEntity,
				}

				pipelineUID, importErr := evalContext.ML.ImportTrainingPipeline(
					evalContext.Context,
					pipelineDefinition,
				)
				if importErr != nil {
					return nil, importErr
				}
				return sqltree.NewDString(pipelineUID), nil
			},
			Info: "Create a machine learning training pipeline.",
		},
	),

	"kwdbml.show_training_pipeline": makeBuiltin(
		sqltree.FunctionProperties{Class: sqltree.GeneratorClass, Category: categoryML, DistsqlBlacklist: true, Impure: true, ReturnLabels: showPipelinesLabels},
		showPipelineImpl,
	),

	"kwdbml.show_training_pipelines": makeBuiltin(
		sqltree.FunctionProperties{Class: sqltree.GeneratorClass, Category: categoryML, DistsqlBlacklist: true, Impure: true, ReturnLabels: showPipelinesLabels},
		showPipelinesImpl,
	),

	"kwdbml.remove_training_pipeline": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				result, removeErr := evalContext.ML.RemoveTrainingPipeline(evalContext.Context, pipelineName)
				if removeErr != nil {
					return nil, removeErr
				}
				return sqltree.NewDString(result), nil
			},
			Info: "Remove one machine learning Pipeline.",
		},
	),

	"kwdbml.grant_privilege_on_training_pipeline": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "user", Typ: sqltypes.String},
				{Name: "privileges", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				userName := string(sqltree.MustBeDString(arguments[1]))
				privilegeText := stdstrings.TrimSpace(string(sqltree.MustBeDString(arguments[2])))

				result, grantErr := evalContext.ML.GrantTrainingPipeline(
					evalContext.Context,
					pipelineName,
					userName,
					privilegeText,
				)
				if grantErr != nil {
					return nil, grantErr
				}
				return sqltree.NewDString(result), nil
			},
			Info: "grant training pipeline privileges.",
		},
	),

	"kwdbml.revoke_privilege_on_training_pipeline": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "user", Typ: sqltypes.String},
				{Name: "privileges", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				userName := string(sqltree.MustBeDString(arguments[1]))
				privilegeText := stdstrings.TrimSpace(string(sqltree.MustBeDString(arguments[2])))

				result, revokeErr := evalContext.ML.RevokeTrainingPipeline(
					evalContext.Context,
					pipelineName,
					userName,
					privilegeText,
				)
				if revokeErr != nil {
					return nil, revokeErr
				}
				return sqltree.NewDString(result), nil
			},
			Info: "revoke training pipeline privileges.",
		},
	),

	"kwdbml.has_training_pipeline_privilege": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "user", Typ: sqltypes.String},
				{Name: "privilege", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.Bool),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				userName := string(sqltree.MustBeDString(arguments[1]))
				privilegeText := stdstrings.TrimSpace(string(sqltree.MustBeDString(arguments[2])))

				authorized, checkErr := evalContext.ML.CheckTrainingPipelinePrivileges(
					evalContext.Context,
					pipelineName,
					userName,
					privilegeText,
				)
				if checkErr != nil {
					return nil, checkErr
				}
				return sqltree.MakeDBool(sqltree.DBool(authorized)), nil
			},
			Info: "check training pipeline privilege.",
		},
	),

	"kwdbml.show_training_pipeline_privilege": makeBuiltin(
		sqltree.FunctionProperties{Class: sqltree.GeneratorClass, Category: categoryML, DistsqlBlacklist: true, Impure: true, ReturnLabels: showPrivilegeLabels},
		showTrainingPipelinePrivilegeImpl,
	),

	"kwdbml.train_model": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "model_name", Typ: sqltypes.String},
				{Name: "train_data", Typ: sqltypes.String},
				{Name: "new_version_if_exists", Typ: sqltypes.Bool},
				{Name: "immediately_deploy", Typ: sqltypes.Bool},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				modelName := string(sqltree.MustBeDString(arguments[1]))
				trainingData := string(sqltree.MustBeDString(arguments[2]))
				createNewVersion := bool(sqltree.MustBeDBool(arguments[3]))
				deployImmediately := bool(sqltree.MustBeDBool(arguments[4]))

				jobID, trainErr := evalContext.ML.TrainModel(
					evalContext.Context,
					pipelineName,
					modelName,
					trainingData,
					createNewVersion,
					deployImmediately,
				)
				if trainErr != nil {
					return nil, trainErr
				}
				return sqltree.NewDString(jobID), nil
			},
			Info: "Train a machine learning model.",
		},
	),

	"kwdbml.evaluate_model": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "model_name", Typ: sqltypes.String},
				{Name: "model_version", Typ: sqltypes.Int},
				{Name: "evaluate_data", Typ: sqltypes.String},
				{Name: "objectives", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				modelName := string(sqltree.MustBeDString(arguments[1]))
				modelVersion := int32(sqltree.MustBeDInt(arguments[2]))
				evaluationData := string(sqltree.MustBeDString(arguments[3]))
				objectives := stdstrings.Split(string(sqltree.MustBeDString(arguments[4])), "|")

				jobID, evaluateErr := evalContext.ML.EvaluateModel(
					evalContext.Context,
					pipelineName,
					modelName,
					modelVersion,
					evaluationData,
					objectives,
				)
				if evaluateErr != nil {
					return nil, evaluateErr
				}
				return sqltree.NewDString(jobID), nil
			},
			Info: "Evaluate a machine learning model.",
		},
	),

	"kwdbml.batch_predict": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "pipeline_name", Typ: sqltypes.String},
				{Name: "model_name", Typ: sqltypes.String},
				{Name: "model_version", Typ: sqltypes.Int},
				{Name: "batch_data", Typ: sqltypes.String},
				{Name: "result_table", Typ: sqltypes.String},
				{Name: "action_if_exists", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				pipelineName := string(sqltree.MustBeDString(arguments[0]))
				modelName := string(sqltree.MustBeDString(arguments[1]))
				modelVersion := int32(sqltree.MustBeDInt(arguments[2]))
				batchData := string(sqltree.MustBeDString(arguments[3]))
				resultTable := string(sqltree.MustBeDString(arguments[4]))
				existingResultAction := string(sqltree.MustBeDString(arguments[5]))

				jobID, predictErr := evalContext.ML.BatchPredict(
					evalContext.Context,
					pipelineName,
					modelName,
					modelVersion,
					batchData,
					resultTable,
					existingResultAction,
				)
				if predictErr != nil {
					return nil, predictErr
				}
				return sqltree.NewDString(jobID), nil
			},
			Info: "Batch predict with a machine learning model.",
		},
	),

	"kwdbml.fine_tune": makeBuiltin(
		sqltree.FunctionProperties{Category: categoryML, DistsqlBlacklist: true, Impure: true, NullableArgs: false},
		sqltree.Overload{
			Types: sqltree.ArgTypes{
				{Name: "model_name", Typ: sqltypes.String},
				{Name: "data_source", Typ: sqltypes.String},
				{Name: "column_name", Typ: sqltypes.String},
			},
			ReturnType: sqltree.FixedReturnType(sqltypes.String),
			Fn: func(evalContext *sqltree.EvalContext, arguments sqltree.Datums) (sqltree.Datum, error) {
				modelName := string(sqltree.MustBeDString(arguments[0]))
				dataSource := string(sqltree.MustBeDString(arguments[1]))
				columnName := string(sqltree.MustBeDString(arguments[2]))

				jobID, fineTuneErr := evalContext.ML.FineTune(
					evalContext.Context,
					modelName,
					dataSource,
					columnName,
				)
				if fineTuneErr != nil {
					return nil, fineTuneErr
				}
				return sqltree.NewDString(jobID), nil
			},
			Info: "Fine-tune an existing machine learning model.",
		},
	),
}

var showPipelinesLabels = []string{
	"schema",
	"name",
	"description",
	"creator",
	"created",
	"problem_type",
	"features",
	"target",
	"framework",
	"runtime",
}

var showPipelinesType = sqltypes.MakeLabeledTuple(
	[]sqltypes.T{
		*sqltypes.String,
		*sqltypes.String,
		*sqltypes.String,
		*sqltypes.String,
		*sqltypes.Timestamp,
		*sqltypes.String,
		*sqltypes.Jsonb,
		*sqltypes.Jsonb,
		*sqltypes.String,
		*sqltypes.String,
	},
	showPipelinesLabels,
)

var showPipelineImpl = makeGeneratorOverload(
	sqltree.ArgTypes{{Name: "pipeline_name", Typ: sqltypes.String}},
	showPipelinesType,
	makeShowPipelineImplGenerator,
	"Find a machine learning Pipeline.",
)

func makeShowPipelineImplGenerator(
	evalContext *sqltree.EvalContext, arguments sqltree.Datums,
) (sqltree.ValueGenerator, error) {
	pipelineName := string(sqltree.MustBeDString(arguments[0]))
	rows, findErr := evalContext.ML.FindTrainingPipeline(evalContext.Context, pipelineName)
	if findErr != nil {
		return nil, findErr
	}

	generator := &tableFunGenerator{
		target: rows,
		types:  showPipelinesType,
	}
	return generator, nil
}

var showPipelinesImpl = makeGeneratorOverload(
	sqltree.ArgTypes{},
	showPipelinesType,
	makeShowPipelinesImplGenerator,
	"Find all machine learning Pipelines.",
)

func makeShowPipelinesImplGenerator(
	evalContext *sqltree.EvalContext, _ sqltree.Datums,
) (sqltree.ValueGenerator, error) {
	rows, findErr := evalContext.ML.FindTrainingPipelines(evalContext.Context)
	if findErr != nil {
		return nil, findErr
	}

	generator := &tableFunGenerator{
		target: rows,
		types:  showPipelinesType,
	}
	return generator, nil
}

var showTrainingPipelinePrivilegeImpl = makeGeneratorOverload(
	sqltree.VariadicType{
		FixedTypes: []*sqltypes.T{sqltypes.String},
		VarType:    sqltypes.String,
	},
	showPrivilegeType,
	makeShowTrainingPipelinePrivilegeImplGenerator,
	"Show ML training pipeline privileges.",
)

func makeShowTrainingPipelinePrivilegeImplGenerator(
	evalContext *sqltree.EvalContext, arguments sqltree.Datums,
) (sqltree.ValueGenerator, error) {
	if argumentErr := checkVariadicTypeArgsNum(
		"kwdbml.show_training_pipeline_privilege",
		arguments,
		2,
	); argumentErr != nil {
		return nil, argumentErr
	}

	resourceName := string(sqltree.MustBeDString(arguments[0]))
	userName := "*"
	if len(arguments) == 2 && arguments[1] != sqltree.DNull {
		userName = string(sqltree.MustBeDString(arguments[1]))
		if userName == "*" {
			return nil, crdberrors.Newf("user '*' does not exist")
		}
	}

	rows, privilegeErr := evalContext.ML.ShowTrainingPipelinePrivileges(
		evalContext.Context,
		resourceName,
		userName,
	)
	if privilegeErr != nil {
		return nil, privilegeErr
	}

	generator := &tableFunGenerator{
		target: rows,
		types:  showPrivilegeType,
	}
	return generator, nil
}
