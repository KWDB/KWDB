// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package builtins

import (
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

func initMLJobBuiltins() {
	for k, v := range mlJobBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var mlJobBuiltins = map[string]builtinDefinition{
	"kwdbml.show_job": makeBuiltin(tree.FunctionProperties{
		Class:            tree.GeneratorClass,
		Category:         categoryML,
		DistsqlBlacklist: true,
		Impure:           true,
		ReturnLabels:     showJobsLabels,
	}, showJobImpl),
	"kwdbml.show_jobs": makeBuiltin(tree.FunctionProperties{
		Class:            tree.GeneratorClass,
		Category:         categoryML,
		DistsqlBlacklist: true,
		Impure:           true,
		ReturnLabels:     showJobsLabels,
	}, showJobsImpl),
	"kwdbml.cancel_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "job_id", Typ: types.Uuid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				jobID := args[0].(*tree.DUuid)

				result, err := ctx.ML.CancelJob(ctx.Context, jobID.UUID.String())
				if err != nil {
					return nil, err
				}

				return tree.NewDString(result.UUID.String()), nil
			},
			Info: "Cancel one machine learning job.",
		},
	),
	"kwdbml.remove_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{Name: "job_id", Typ: types.Uuid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				jobID := args[0].(*tree.DUuid)

				result, err := ctx.ML.RemoveJob(ctx.Context, jobID.UUID.String())
				if err != nil {
					return nil, err
				}

				return tree.NewDString(result.UUID.String()), nil
			},
			Info: "Remove one machine learning job.",
		},
	),
}

var showJobsLabels = []string{
	"id",
	"type",
	"status",
	"pipeline_name",
	"model_name",
	"model_version",
	"creator",
	"created",
	"finished",
	"settings",
	"result",
	"error",
}

var showJobsType = types.MakeLabeledTuple(
	[]types.T{
		*types.Uuid,
		*types.String,
		*types.String,
		*types.String,
		*types.String,
		*types.Int,
		*types.String,
		*types.Timestamp,
		*types.Timestamp,
		*types.Jsonb,
		*types.Jsonb,
		*types.String,
	},
	showJobsLabels,
)

var showJobImpl = makeGeneratorOverload(
	tree.ArgTypes{{Name: "job_id", Typ: types.Uuid}},
	showJobsType,
	makeShowJobImplGenerator,
	"Find one machine learning Job.",
)

func makeShowJobImplGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	jobID := args[0].(*tree.DUuid)

	rows, err := ctx.ML.FindJob(ctx.Context, jobID.UUID.String())
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showJobsType,
	}, nil
}

var showJobsImpl = makeGeneratorOverload(
	tree.VariadicType{
		VarType: types.Any,
	},
	showJobsType,
	makeShowJobsImplGenerator,
	"Find all machine learning Jobs.",
)

func makeShowJobsImplGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	jobTypeStr := "*"
	jobStatusStr := "*"
	modelName := "*"
	modelVersion := int32(0)

	if err := checkVariadicTypeArgsNum("kwdbml.show_jobs", args, 4); err != nil {
		return nil, err
	}

	if len(args) > 0 && args[0] != tree.DNull {
		val, ok := args[0].(*tree.DString)
		if !ok {
			return nil, pgerror.Newf(pgcode.UndefinedFunction,
				"unknown signature: %s() %v is not string", "kwdbml.show_jobs", args[0])
		}
		jobTypeStr = string(*val)
	}

	if len(args) > 1 && args[1] != tree.DNull {
		val, ok := args[1].(*tree.DString)
		if !ok {
			return nil, pgerror.Newf(pgcode.UndefinedFunction,
				"unknown signature: %s() %v is not string", "kwdbml.show_jobs", args[1])
		}
		jobStatusStr = string(*val)
	}

	if len(args) > 2 && args[2] != tree.DNull {
		val, ok := args[2].(*tree.DString)
		if !ok {
			return nil, pgerror.Newf(pgcode.UndefinedFunction,
				"unknown signature: %s() %v is not string", "kwdbml.show_jobs", args[2])
		}
		modelName = string(*val)
	}

	if len(args) > 3 && args[3] != tree.DNull {
		val, ok := args[3].(*tree.DInt)
		if !ok {
			return nil, pgerror.Newf(pgcode.UndefinedFunction,
				"unknown signature: %s() %v is not int", "kwdbml.show_jobs", args[3])
		}
		modelVersion = int32(*val)
		if modelVersion <= 0 {
			return &tableFunGenerator{
				target: []tree.Datums{},
				types:  showJobsType,
			}, nil
		}
	}

	rows, err := ctx.ML.FindJobs(ctx.Context, modelName, modelVersion, jobTypeStr, jobStatusStr)
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showJobsType,
	}, nil
}
