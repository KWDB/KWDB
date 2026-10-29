// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software is the confidential and proprietary information of Shanghai Yunxi Technology Co, Ltd.
// You shall not disclose such confidential information and shall use it only in accordance with
// the terms of the license agreement you entered into with Shanghai Yunxi Technology Co, Ltd.

package builtins

import (
	"context"
	"fmt"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func initMLBuiltins() {
	for k, v := range mlBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var mlBuiltins = map[string]builtinDefinition{
	"kwdbml.import_model": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     true,
		},
		tree.Overload{
			Types: tree.VariadicType{
				FixedTypes: []*types.T{types.String, types.String, types.String, types.String, types.String, types.Jsonb, types.Jsonb, types.Bytes, types.Bytes},
				VarType:    types.Jsonb,
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				if err := checkVariadicTypeArgsNum("kwdbml.import_model", args, 10); err != nil {
					return nil, err
				}
				for i, arg := range args {
					if i < 8 && arg == tree.DNull {
						return nil, pgerror.Newf(pgcode.UndefinedFunction,
							"unknown signature: %s() the %d arg not null", "kwdbml.import_model", i+1)
					}
				}
				modelName := tree.MustBeDString(args[0])
				description := tree.MustBeDString(args[1])
				problemType := tree.MustBeDString(args[2])
				framework := tree.MustBeDString(args[3])
				runtime := tree.MustBeDString(args[4])
				features := tree.MustBeDJSON(args[5])
				target := tree.MustBeDJSON(args[6])
				modelEntity := tree.MustBeDBytes(args[7])

				var transformerEntity tree.DBytes
				if args[8] != tree.DNull {
					transformerEntity = tree.MustBeDBytes(args[8])
				}

				var metrics tree.DJSON
				if len(args) == 10 && args[9] != tree.DNull {
					metrics = tree.MustBeDJSON(args[9])
				}

				model := &tree.MLModel{
					Name:              &modelName,
					Description:       &description,
					ProblemType:       &problemType,
					Framework:         &framework,
					Runtime:           &runtime,
					Features:          &features,
					Target:            &target,
					ModelEntity:       &modelEntity,
					TransformerEntity: &transformerEntity,
					Metrics:           &metrics,
				}

				modelUID, modelVer, err := ctx.ML.ImportModel(ctx.Context, model)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(fmt.Sprintf("%s,%d", modelUID, modelVer)), nil
			},
			Info: "Create a machine learning model.",
		},
	),
	"kwdbml.show_models": makeBuiltin(
		tree.FunctionProperties{
			Class:            tree.GeneratorClass,
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			ReturnLabels:     showModelsLabels,
		},
		showModelsImpl),
	"kwdbml.show_model": makeBuiltin(
		tree.FunctionProperties{
			Class:            tree.GeneratorClass,
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			ReturnLabels:     showModelLabels,
		},
		showModelImpl),
	"kwdbml.set_active_version": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "model_name", Typ: types.String},
				{Name: "model_version", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				modelName := string(tree.MustBeDString(args[0]))
				modelVersion := int32(tree.MustBeDInt(args[1]))

				version, err := ctx.ML.SetActive(ctx.Context, modelName, modelVersion)

				if err != nil {
					return nil, err
				}

				return tree.NewDInt(tree.DInt(version)), nil
			},
			Info: "Set active version of ML model.",
		},
	),
	"kwdbml.remove_model": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types:      tree.VariadicType{FixedTypes: []*types.T{types.String}, VarType: types.Int},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				argNum := len(args)
				if argNum == 0 || argNum > 2 {
					return tree.NewDString(""), errors.New("args num error")
				}
				if args[0] == tree.DNull {
					return tree.NewDString(""), errors.New("model name must not be null")
				}

				modelName, ok := tree.AsDString(args[0])
				if !ok {
					return tree.NewDString(""), errors.New("model name must be string")
				}

				var modelVersion int32
				if argNum == 2 && args[1] != tree.DNull {
					modelVersion = int32(tree.MustBeDInt(args[1]))
					if modelVersion < 1 {
						return tree.NewDString(""), errors.New("model version must more than 0")
					}
				} else {
					modelVersion = 0
				}

				result, err := ctx.ML.RemoveModel(ctx.Context, string(modelName), modelVersion)
				if err != nil {
					return nil, err
				}

				return tree.NewDString(result), nil
			},
			Info: "Remove one machine learning model.",
		},
	),
	"kwdbml.predict": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     true,
		},
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.Any,
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				if len(args) == 1 {
					switch args[0].(type) {
					case *tree.DTuple:
						args = args[0].(*tree.DTuple).D
					}
				}

				res, err := ctx.ML.Predict(ctx.Context, args)
				if err != nil {
					return nil, err
				}

				return &tree.DJSON{JSON: res}, nil
			},
			Info: "Predict on a machine learning model",
		},
	),
	"kwdbml.grant_privilege_on_model": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "model_name", Typ: types.String},
				{Name: "user", Typ: types.String},
				{Name: "privileges", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				modelName := string(tree.MustBeDString(args[0]))
				user := string(tree.MustBeDString(args[1]))
				privilegeStr := strings.TrimSpace(string(tree.MustBeDString(args[2])))

				res, err := ctx.ML.GrantModel(ctx.Context, modelName, user, privilegeStr)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(res), nil
			},
			Info: "grant model privileges.",
		},
	),
	"kwdbml.revoke_privilege_on_model": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "model_name", Typ: types.String},
				{Name: "user", Typ: types.String},
				{Name: "privileges", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				modelName := string(tree.MustBeDString(args[0]))
				user := string(tree.MustBeDString(args[1]))
				privilegeStr := strings.TrimSpace(string(tree.MustBeDString(args[2])))

				res, err := ctx.ML.RevokeModel(ctx.Context, modelName, user, privilegeStr)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(res), nil
			},
			Info: "revoke model privileges.",
		},
	),
	"kwdbml.has_model_privilege": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			NullableArgs:     false,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{Name: "model_name", Typ: types.String},
				{Name: "user", Typ: types.String},
				{Name: "privilege", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.ML == nil {
					return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
						"unsupported ML request")
				}

				modelName := string(tree.MustBeDString(args[0]))
				user := string(tree.MustBeDString(args[1]))
				privilegeStr := strings.TrimSpace(string(tree.MustBeDString(args[2])))
				bAuth, err := ctx.ML.CheckModelPrivileges(ctx.Context, modelName, user, privilegeStr)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(bAuth)), nil
			},
			Info: "auth model privilege.",
		},
	),
	"kwdbml.show_model_privilege": makeBuiltin(tree.FunctionProperties{
		Class:            tree.GeneratorClass,
		Category:         categoryML,
		DistsqlBlacklist: true,
		Impure:           true,
		ReturnLabels:     showPrivilegeLabels,
	}, showModelPrivilegeImpl),
	"kwdbml.show_ml_resources": makeBuiltin(
		tree.FunctionProperties{
			Class:            tree.GeneratorClass,
			Category:         categoryML,
			DistsqlBlacklist: true,
			Impure:           true,
			ReturnLabels:     showResourcesLabels,
		},
		showResourcesImpl),
}

type tableFunGenerator struct {
	types  *types.T
	target []tree.Datums
	iter   int
}

func (s *tableFunGenerator) ResolvedType() *types.T {
	return s.types
}

func (s *tableFunGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.iter = -1
	return nil
}

func (s *tableFunGenerator) Next(_ context.Context) (bool, error) {
	s.iter++
	return s.iter < len(s.target), nil
}

func (s *tableFunGenerator) Values() tree.Datums {
	return s.target[s.iter]
}

func (s *tableFunGenerator) Close() {}

var showModelLabels = []string{
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
	"status",
	"version",
	"train_data",
	"metrics",
	"modelstatus",
}

var showModelType = types.MakeLabeledTuple(
	[]types.T{
		*types.String,
		*types.String,
		*types.String,
		*types.String,
		*types.Timestamp,
		*types.String,
		*types.Jsonb,
		*types.Jsonb,
		*types.String,
		*types.String,
		*types.String,
		*types.Int4,
		*types.String,
		*types.Jsonb,
		*types.Jsonb,
	},
	showModelLabels,
)

var showModelImpl = makeGeneratorOverload(
	tree.VariadicType{FixedTypes: []*types.T{types.String}, VarType: types.Int4},
	showModelType,
	makeShowModelImplGenerator,
	"Find a machine learning model.",
)

func makeShowModelImplGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	if ctx.ML == nil {
		return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
			"unsupported ML request")
	}

	if err := checkVariadicTypeArgsNum("kwdbml.show_model", args, 2); err != nil {
		return nil, err
	}

	modelName := string(tree.MustBeDString(args[0]))

	var modelVersion int32
	if len(args) == 2 && args[1] != tree.DNull {
		modelVersion = int32(tree.MustBeDInt(args[1]))
		if modelVersion < 1 {
			return &tableFunGenerator{
				target: []tree.Datums{},
				types:  showModelType,
			}, nil
		}
	} else {
		modelVersion = 0
	}

	rows, err := ctx.ML.FindModel(ctx.Context, modelName, modelVersion)
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showModelType,
	}, nil
}

var showModelsLabels = []string{
	"schema",
	"name",
	"description",
	"creator",
	"created",
	"last_updated",
	"problem_type",
	"features",
	"target",
	"framework",
	"runtime",
	"active_version",
	"last_metrics",
	"status",
}

var showModelsType = types.MakeLabeledTuple(
	[]types.T{
		*types.String,
		*types.String,
		*types.String,
		*types.String,
		*types.Timestamp,
		*types.Timestamp,
		*types.String,
		*types.Jsonb,
		*types.Jsonb,
		*types.String,
		*types.String,
		*types.Int4,
		*types.Jsonb,
		*types.Jsonb,
	},
	showModelsLabels,
)

var showModelsImpl = makeGeneratorOverload(
	tree.ArgTypes{},
	showModelsType,
	makeShowModelsImplGenerator,
	"Find all machine learning model.",
)

func makeShowModelsImplGenerator(
	ctx *tree.EvalContext, _ tree.Datums,
) (tree.ValueGenerator, error) {
	if ctx.ML == nil {
		return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
			"unsupported ML request")
	}

	rows, err := ctx.ML.FindModels(ctx.Context)
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showModelsType,
	}, nil
}

func checkVariadicTypeArgsNum(funcName string, args tree.Datums, maxArgsNum int) error {
	argNum := len(args)
	if argNum > maxArgsNum {
		typeNames := make([]string, 0, argNum)
		for _, expr := range args {
			typeNames = append(typeNames, expr.ResolvedType().DetailedName())
		}
		sig := fmt.Sprintf("%s(%s)", funcName, strings.Join(typeNames, ", "))

		return pgerror.Newf(pgcode.UndefinedFunction, "unknown signature: %s", sig)
	}

	return nil
}

var showPrivilegeLabels = []string{
	"grantee",
	"schema",
	"name",
	"grantor",
	"privileges",
	"member_of",
}

var showPrivilegeType = types.MakeLabeledTuple(
	[]types.T{
		*types.String,
		*types.String,
		*types.String,
		*types.String,
		*types.String,
		*types.StringArray,
	},
	showPrivilegeLabels,
)

var showModelPrivilegeImpl = makeGeneratorOverload(
	tree.VariadicType{FixedTypes: []*types.T{types.String}, VarType: types.String},
	showPrivilegeType,
	makeShowModelPrivilegeImplGenerator,
	"Show ML model privileges.",
)

func makeShowModelPrivilegeImplGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	if ctx.ML == nil {
		return nil, pgerror.New(pgcode.ScalarOperationCannotRunWithoutFullSessionContext,
			"unsupported ML request")
	}

	if err := checkVariadicTypeArgsNum("kwdbml.show_model_privilege", args, 2); err != nil {
		return nil, err
	}

	resourceName := string(tree.MustBeDString(args[0]))
	var user string
	if len(args) == 2 && args[1] != tree.DNull {
		user = string(tree.MustBeDString(args[1]))
		if user == "*" {
			return nil, errors.Newf("user '*' does not exist")
		}
	} else {
		user = "*"
	}

	rows, err := ctx.ML.ShowModelPrivileges(ctx.Context, resourceName, user)
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showPrivilegeType,
	}, nil
}

var showResourcesLabels = []string{
	"name", "type", "CPU", "memory", "storage",
}

var showResourcesType = types.MakeLabeledTuple(
	[]types.T{*types.String, *types.String, *types.String, *types.String, *types.String},
	showResourcesLabels,
)

var showResourcesImpl = makeGeneratorOverload(
	tree.ArgTypes{},
	showResourcesType,
	makeShowResourcesImplGenerator,
	"Find all machine learning resources.",
)

func makeShowResourcesImplGenerator(
	ctx *tree.EvalContext, _ tree.Datums,
) (tree.ValueGenerator, error) {
	rows, err := ctx.ML.FindResources(ctx.Context)
	if err != nil {
		return nil, err
	}

	return &tableFunGenerator{
		target: rows,
		types:  showResourcesType,
	}, nil
}
