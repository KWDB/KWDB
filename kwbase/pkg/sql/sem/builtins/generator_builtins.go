// Copyright 2016 The Cockroach Authors.
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package builtins

import (
	"bytes"
	"context"
	"fmt"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/kv"
	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/sql/lex"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/arith"
	"gitee.com/kwbasedb/kwbase/pkg/util/duration"
	"gitee.com/kwbasedb/kwbase/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// See the comments at the start of generators.go for details about
// this functionality.

var _ tree.ValueGenerator = &seriesValueGenerator{}
var _ tree.ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range generators {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}

		if !v.props.Impure {
			panic(fmt.Sprintf("generator functions should all be impure, found %v", v))
		}
		if v.props.Class != tree.GeneratorClass {
			panic(fmt.Sprintf("generator functions should be marked with the tree.GeneratorClass "+
				"function class, found %v", v))
		}

		builtins[k] = v
	}
}

func genProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Impure:   true,
		Class:    tree.GeneratorClass,
		Category: categoryGenerator,
	}
}

func genPropsWithLabels(returnLabels []string) tree.FunctionProperties {
	return tree.FunctionProperties{
		Impure:       true,
		Class:        tree.GeneratorClass,
		Category:     categoryGenerator,
		ReturnLabels: returnLabels,
	}
}

var aclexplodeGeneratorType = types.MakeLabeledTuple(
	[]types.T{*types.Oid, *types.Oid, *types.String, *types.Bool},
	[]string{"grantor", "grantee", "privilege_type", "is_grantable"},
)

// aclExplodeGenerator supports the execution of aclexplode.
type aclexplodeGenerator struct{}

func (aclexplodeGenerator) ResolvedType() *types.T                   { return aclexplodeGeneratorType }
func (aclexplodeGenerator) Start(_ context.Context, _ *kv.Txn) error { return nil }
func (aclexplodeGenerator) Close()                                   {}
func (aclexplodeGenerator) Next(_ context.Context) (bool, error)     { return false, nil }
func (aclexplodeGenerator) Values() tree.Datums                      { return nil }

// generators is a map from name to slice of Builtins for all built-in
// generators.
//
// These functions are identified with Class == tree.GeneratorClass.
// The properties are reachable via tree.FunctionDefinition.
var generators = map[string]builtinDefinition{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"aclexplode": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ArgTypes{{"aclitems", types.StringArray}},
			aclexplodeGeneratorType,
			func(ctx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
				return aclexplodeGenerator{}, nil
			},
			"Produces a virtual table containing aclitem stuff ("+
				"returns no rows as this feature is unsupported in CockroachDB)",
		),
	),
	"generate_series": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SERIES
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}, {"step", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Timestamp}, {"end", types.Timestamp}, {"step", types.Interval}},
			seriesTSValueGeneratorType,
			makeTSSeriesGenerator,
			"Produces a virtual table containing the timestamp values from `start` to `end`, inclusive, by increment of `step`.",
		),
	),
	// kwdb_internal.testing_callback is a generator function intended for internal unit tests.
	// You give it a name and it calls a callback that had to have been installed
	// on a TestServer through its EvalContextTestingKnobs.CallbackGenerators.
	"kwdb_internal.testing_callback": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ArgTypes{{"name", types.String}},
			types.Int,
			func(ctx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
				name := string(*args[0].(*tree.DString))
				gen, ok := ctx.TestingKnobs.CallbackGenerators[name]
				if !ok {
					return nil, errors.Errorf("callback %q not registered", name)
				}
				return gen, nil
			},
			"For internal KWDB testing only. "+
				"The function calls a callback identified by `name` registered with the server by "+
				"the test.",
		),
	),

	"pg_get_keywords": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/10/static/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
		makeGeneratorOverload(
			tree.ArgTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
		),
	),

	"unnest": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-array.html
		makeGeneratorOverloadWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) *types.T {
				if len(args) == 0 || args[0].ResolvedType().Family() == types.UnknownFamily {
					return tree.UnknownReturnType
				}
				return args[0].ResolvedType().ArrayContents()
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
		),
		makeGeneratorOverloadWithReturnType(
			tree.VariadicType{
				FixedTypes: []*types.T{types.AnyArray, types.AnyArray},
				VarType:    types.AnyArray,
			},
			// TODO(rafiss): update this or docgen so that functions.md shows the
			// return type as variadic.
			func(args []tree.TypedExpr) *types.T {
				returnTypes := make([]types.T, len(args))
				labels := make([]string, len(args))
				for i, arg := range args {
					if arg.ResolvedType().Family() == types.UnknownFamily {
						return tree.UnknownReturnType
					}
					returnTypes[i] = *arg.ResolvedType().ArrayContents()
					labels[i] = "unnest"
				}
				return types.MakeLabeledTuple(returnTypes, labels)
			},
			makeVariadicUnnestGenerator,
			"Returns the input arrays as a set of rows",
		),
	),

	"information_schema._pg_expandarray": makeBuiltin(genProps(),
		makeGeneratorOverloadWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) *types.T {
				if len(args) == 0 || args[0].ResolvedType().Family() == types.UnknownFamily {
					return tree.UnknownReturnType
				}
				t := args[0].ResolvedType().ArrayContents()
				return types.MakeLabeledTuple([]types.T{*t, *types.Int}, expandArrayValueGeneratorLabels)
			},
			makeExpandArrayGenerator,
			"Returns the input array as a set of rows with an index",
		),
	),

	"kwdb_internal.unary_table": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ArgTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
		),
	),

	"generate_subscripts": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SUBSCRIPTS
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}, {"dim", types.Int}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}, {"dim", types.Int}, {"reverse", types.Bool}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.\n\n"+
				"When reverse is true, the series is returned in reverse order.",
		),
	),

	"json_array_elements":       makeBuiltin(genPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"jsonb_array_elements":      makeBuiltin(genPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"json_array_elements_text":  makeBuiltin(genPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"jsonb_array_elements_text": makeBuiltin(genPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"json_object_keys":          makeBuiltin(genProps(), jsonObjectKeysImpl),
	"jsonb_object_keys":         makeBuiltin(genProps(), jsonObjectKeysImpl),
	"json_each":                 makeBuiltin(genPropsWithLabels(jsonEachGeneratorLabels), jsonEachImpl),
	"jsonb_each":                makeBuiltin(genPropsWithLabels(jsonEachGeneratorLabels), jsonEachImpl),
	"json_each_text":            makeBuiltin(genPropsWithLabels(jsonEachGeneratorLabels), jsonEachTextImpl),
	"jsonb_each_text":           makeBuiltin(genPropsWithLabels(jsonEachGeneratorLabels), jsonEachTextImpl),

	"kwdb_internal.check_consistency": makeBuiltin(
		tree.FunctionProperties{
			Impure:   true,
			Class:    tree.GeneratorClass,
			Category: categorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ArgTypes{
				{Name: "stats_only", Typ: types.Bool},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			checkConsistencyGeneratorType,
			makeCheckConsistencyGenerator,
			"Runs a consistency check on ranges touching the specified key range. "+
				"an empty start or end key is treated as the minimum and maximum possible, "+
				"respectively. stats_only should only be set to false when targeting a "+
				"small number of ranges to avoid overloading the cluster. Each returned row "+
				"contains the range ID, the status (a roachpb.CheckConsistencyResponse_Status), "+
				"and verbose detail.\n\n"+
				"Example usage:\n"+
				"SELECT * FROM kwdb_internal.check_consistency(true, '\\x02', '\\x04')",
		),
	),
}

func makeGeneratorOverload(
	in tree.TypeList, ret *types.T, g tree.GeneratorFactory, info string,
) tree.Overload {
	return makeGeneratorOverloadWithReturnType(in, tree.FixedReturnType(ret), g, info)
}

func newUnsuitableUseOfGeneratorError() error {
	return errors.AssertionFailedf("generator functions cannot be evaluated as scalars")
}

func makeGeneratorOverloadWithReturnType(
	in tree.TypeList, retType tree.ReturnTyper, g tree.GeneratorFactory, info string,
) tree.Overload {
	return tree.Overload{
		Types:      in,
		ReturnType: retType,
		Generator:  g,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return nil, newUnsuitableUseOfGeneratorError()
		},
		Info: info,
	}
}

// keywordsValueGenerator supports the execution of pg_get_keywords().
type keywordsValueGenerator struct {
	curKeyword int
}

var keywordsValueGeneratorType = types.MakeLabeledTuple(
	[]types.T{*types.String, *types.String, *types.String},
	[]string{"word", "catcode", "catdesc"},
)

func makeKeywordsGenerator(_ *tree.EvalContext, _ tree.Datums) (tree.ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() *types.T { return keywordsValueGeneratorType }

// Close implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) Close() {}

// Start implements the tree.ValueGenerator interface.
func (k *keywordsValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	k.curKeyword = -1
	return nil
}
func (k *keywordsValueGenerator) Next(_ context.Context) (bool, error) {
	k.curKeyword++
	return k.curKeyword < len(lex.KeywordNames), nil
}

// Values implements the tree.ValueGenerator interface.
func (k *keywordsValueGenerator) Values() tree.Datums {
	kw := lex.KeywordNames[k.curKeyword]
	cat := lex.KeywordsCategories[kw]
	desc := keywordCategoryDescriptions[cat]
	return tree.Datums{tree.NewDString(kw), tree.NewDString(cat), tree.NewDString(desc)}
}

var keywordCategoryDescriptions = map[string]string{
	"R": "reserved",
	"C": "unreserved (cannot be function or type name)",
	"T": "reserved (can be function or type name)",
	"U": "unreserved",
}

// seriesValueGenerator supports the execution of generate_series()
// with integer bounds.
type seriesValueGenerator struct {
	origStart, value, start, stop, step interface{}
	nextOK                              bool
	genType                             *types.T
	next                                func(*seriesValueGenerator) (bool, error)
	genValue                            func(*seriesValueGenerator) tree.Datums
}

var seriesValueGeneratorType = types.Int

var seriesTSValueGeneratorType = types.Timestamp

var errStepCannotBeZero = pgerror.New(pgcode.InvalidParameterValue, "step cannot be 0")

func seriesIntNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(int64)
	start := s.start.(int64)
	stop := s.stop.(int64)

	if !s.nextOK {
		return false, nil
	}
	if step < 0 && (start < stop) {
		return false, nil
	}
	if step > 0 && (stop < start) {
		return false, nil
	}
	s.value = start
	s.start, s.nextOK = arith.AddWithOverflow(start, step)
	return true, nil
}

func seriesGenIntValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(s.value.(int64)))}
}

// seriesTSNext performs calendar-aware math.
func seriesTSNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(duration.Duration)
	start := s.start.(time.Time)
	stop := s.stop.(time.Time)

	if !s.nextOK {
		return false, nil
	}

	stepForward := step.Compare(duration.Duration{}) > 0
	if !stepForward && (start.Before(stop)) {
		return false, nil
	}
	if stepForward && (stop.Before(start)) {
		return false, nil
	}

	s.value = start
	s.start = duration.Add(start, step)
	return true, nil
}

func seriesGenTSValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.MakeDTimestamp(s.value.(time.Time), time.Microsecond)}
}

func makeSeriesGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	start := int64(tree.MustBeDInt(args[0]))
	stop := int64(tree.MustBeDInt(args[1]))
	step := int64(1)
	if len(args) > 2 {
		step = int64(tree.MustBeDInt(args[2]))
	}
	if step == 0 {
		return nil, errStepCannotBeZero
	}
	return &seriesValueGenerator{
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesValueGeneratorType,
		genValue:  seriesGenIntValue,
		next:      seriesIntNext,
	}, nil
}

func makeTSSeriesGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	start := args[0].(*tree.DTimestamp).Time
	stop := args[1].(*tree.DTimestamp).Time
	step := args[2].(*tree.DInterval).Duration

	if step.Compare(duration.Duration{}) == 0 {
		return nil, errStepCannotBeZero
	}

	return &seriesValueGenerator{
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesTSValueGeneratorType,
		genValue:  seriesGenTSValue,
		next:      seriesTSNext,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) ResolvedType() *types.T {
	return s.genType
}

// Start implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.nextOK = true
	s.start = s.origStart
	s.value = s.origStart
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Next(_ context.Context) (bool, error) {
	return s.next(s)
}

// Values implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Values() tree.Datums {
	x := s.genValue(s)
	return x
}

func makeVariadicUnnestGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	var arrays []*tree.DArray
	for _, a := range args {
		arrays = append(arrays, tree.MustBeDArray(a))
	}
	g := &multipleArrayValueGenerator{arrays: arrays}
	return g, nil
}

// multipleArrayValueGenerator is a value generator that returns each element of a
// list of arrays.
type multipleArrayValueGenerator struct {
	arrays    []*tree.DArray
	nextIndex int
	datums    tree.Datums
}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *multipleArrayValueGenerator) ResolvedType() *types.T {
	arraysN := len(s.arrays)
	returnTypes := make([]types.T, arraysN)
	labels := make([]string, arraysN)
	for i, arr := range s.arrays {
		returnTypes[i] = *arr.ParamTyp
		labels[i] = "unnest"
	}
	return types.MakeLabeledTuple(returnTypes, labels)
}

// Start implements the tree.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.datums = make(tree.Datums, len(s.arrays))
	s.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.nextIndex++
	for _, arr := range s.arrays {
		if s.nextIndex < arr.Len() {
			return true, nil
		}
	}
	return false, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Values() tree.Datums {
	for i, arr := range s.arrays {
		if s.nextIndex < arr.Len() {
			s.datums[i] = arr.Array[s.nextIndex]
		} else {
			s.datums[i] = tree.DNull
		}
	}
	return s.datums
}

func makeArrayGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	return &arrayValueGenerator{array: arr}, nil
}

// arrayValueGenerator is a value generator that returns each element of an
// array.
type arrayValueGenerator struct {
	array     *tree.DArray
	nextIndex int
}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) ResolvedType() *types.T {
	return s.array.ParamTyp
}

// Start implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.nextIndex++
	if s.nextIndex >= s.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Values() tree.Datums {
	return tree.Datums{s.array.Array[s.nextIndex]}
}

func makeExpandArrayGenerator(
	evalCtx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	g := &expandArrayValueGenerator{avg: arrayValueGenerator{array: arr}}
	g.buf[1] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// expandArrayValueGenerator is a value generator that returns each element of
// an array and an index for it.
type expandArrayValueGenerator struct {
	avg arrayValueGenerator
	buf [2]tree.Datum
}

var expandArrayValueGeneratorLabels = []string{"x", "n"}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) ResolvedType() *types.T {
	return types.MakeLabeledTuple(
		[]types.T{*s.avg.array.ParamTyp, *types.Int},
		expandArrayValueGeneratorLabels,
	)
}

// Start implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.avg.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.avg.nextIndex++
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Values() tree.Datums {
	// Expand array's index is 1 based.
	s.buf[0] = s.avg.array.Array[s.avg.nextIndex]
	s.buf[1] = tree.NewDInt(tree.DInt(s.avg.nextIndex + 1))
	return s.buf[:]
}

func makeGenerateSubscriptsGenerator(
	evalCtx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	var arr *tree.DArray
	dim := 1
	if len(args) > 1 {
		dim = int(tree.MustBeDInt(args[1]))
	}
	// We sadly only support 1D arrays right now.
	if dim == 1 {
		arr = tree.MustBeDArray(args[0])
	} else {
		arr = &tree.DArray{}
	}
	var reverse bool
	if len(args) == 3 {
		reverse = bool(tree.MustBeDBool(args[2]))
	}
	g := &subscriptsValueGenerator{
		avg:     arrayValueGenerator{array: arr},
		reverse: reverse,
	}
	g.buf[0] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// subscriptsValueGenerator is a value generator that returns a series
// comprising the given array's subscripts.
type subscriptsValueGenerator struct {
	avg arrayValueGenerator
	buf [1]tree.Datum
	// firstIndex is normally 1, since arrays are normally 1-indexed. But the
	// special Postgres vector types are 0-indexed.
	firstIndex int
	reverse    bool
}

var subscriptsValueGeneratorType = types.Int

// ResolvedType implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) ResolvedType() *types.T {
	return subscriptsValueGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	if s.reverse {
		s.avg.nextIndex = s.avg.array.Len()
	} else {
		s.avg.nextIndex = -1
	}
	// Most arrays are 1-indexed, but not all.
	s.firstIndex = s.avg.array.FirstIndex()
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Next(_ context.Context) (bool, error) {
	if s.reverse {
		s.avg.nextIndex--
		return s.avg.nextIndex >= 0, nil
	}
	s.avg.nextIndex++
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Values() tree.Datums {
	s.buf[0] = tree.NewDInt(tree.DInt(s.avg.nextIndex + s.firstIndex))
	return s.buf[:]
}

// EmptyGenerator returns a new, empty generator. Used when a SRF
// evaluates to NULL.
func EmptyGenerator() tree.ValueGenerator {
	return &arrayValueGenerator{array: tree.NewDArray(types.Any)}
}

// unaryValueGenerator supports the execution of kwdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.EmptyTuple

func makeUnaryGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() *types.T { return unaryValueGeneratorType }

// Start implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.done = false
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Next(_ context.Context) (bool, error) {
	if !s.done {
		s.done = true
		return true, nil
	}
	return false, nil
}

var noDatums tree.Datums

// Values implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Values() tree.Datums { return noDatums }

func jsonAsText(j json.JSON) (tree.Datum, error) {
	text, err := j.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

var (
	errJSONObjectKeysOnArray         = pgerror.New(pgcode.InvalidParameterValue, "cannot call json_object_keys on an array")
	errJSONObjectKeysOnScalar        = pgerror.Newf(pgcode.InvalidParameterValue, "cannot call json_object_keys on a scalar")
	errJSONDeconstructArrayAsObject  = pgerror.New(pgcode.InvalidParameterValue, "cannot deconstruct an array as an object")
	errJSONDeconstructScalarAsObject = pgerror.Newf(pgcode.InvalidParameterValue, "cannot deconstruct a scalar")
)

var jsonArrayElementsImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.Jsonb}},
	jsonArrayGeneratorType,
	makeJSONArrayAsJSONGenerator,
	"Expands a JSON array to a set of JSON values.",
)

var jsonArrayElementsTextImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.Jsonb}},
	jsonArrayTextGeneratorType,
	makeJSONArrayAsTextGenerator,
	"Expands a JSON array to a set of text values.",
)

var jsonArrayGeneratorLabels = []string{"value"}
var jsonArrayGeneratorType = types.Jsonb

var jsonArrayTextGeneratorType = types.String

type jsonArrayGenerator struct {
	json      tree.DJSON
	nextIndex int
	asText    bool
	buf       [1]tree.Datum
}

var errJSONCallOnNonArray = pgerror.New(pgcode.InvalidParameterValue,
	"cannot be called on a non-array")

func makeJSONArrayAsJSONGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, false)
}

func makeJSONArrayAsTextGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, true)
}

func makeJSONArrayGenerator(args tree.Datums, asText bool) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	if target.Type() != json.ArrayJSONType {
		return nil, errJSONCallOnNonArray
	}
	return &jsonArrayGenerator{
		json:   target,
		asText: asText,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) ResolvedType() *types.T {
	if g.asText {
		return jsonArrayTextGeneratorType
	}
	return jsonArrayGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Start(_ context.Context, _ *kv.Txn) error {
	g.nextIndex = -1
	g.json.JSON = g.json.JSON.MaybeDecode()
	g.buf[0] = nil
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Next(_ context.Context) (bool, error) {
	g.nextIndex++
	next, err := g.json.FetchValIdx(g.nextIndex)
	if err != nil || next == nil {
		return false, err
	}
	if g.asText {
		if g.buf[0], err = jsonAsText(next); err != nil {
			return false, err
		}
	} else {
		g.buf[0] = tree.NewDJSON(next)
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Values() tree.Datums {
	return g.buf[:]
}

// jsonObjectKeysImpl is a key generator of a JSON object.
var jsonObjectKeysImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.Jsonb}},
	jsonObjectKeysGeneratorType,
	makeJSONObjectKeysGenerator,
	"Returns sorted set of keys in the outermost JSON object.",
)

var jsonObjectKeysGeneratorType = types.String

type jsonObjectKeysGenerator struct {
	iter *json.ObjectIterator
}

func makeJSONObjectKeysGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	iter, err := target.ObjectIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		switch target.Type() {
		case json.ArrayJSONType:
			return nil, errJSONObjectKeysOnArray
		default:
			return nil, errJSONObjectKeysOnScalar
		}
	}
	return &jsonObjectKeysGenerator{
		iter: iter,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) ResolvedType() *types.T {
	return jsonObjectKeysGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Start(_ context.Context, _ *kv.Txn) error { return nil }

// Close implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Next(_ context.Context) (bool, error) {
	return g.iter.Next(), nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Values() tree.Datums {
	return tree.Datums{tree.NewDString(g.iter.Key())}
}

var jsonEachImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.Jsonb}},
	jsonEachGeneratorType,
	makeJSONEachImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs.",
)

var jsonEachTextImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.Jsonb}},
	jsonEachTextGeneratorType,
	makeJSONEachTextImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs. "+
		"The returned values will be of type text.",
)

var jsonEachGeneratorLabels = []string{"key", "value"}

var jsonEachGeneratorType = types.MakeLabeledTuple(
	[]types.T{*types.String, *types.Jsonb},
	jsonEachGeneratorLabels,
)

var jsonEachTextGeneratorType = types.MakeLabeledTuple(
	[]types.T{*types.String, *types.String},
	jsonEachGeneratorLabels,
)

type jsonEachGenerator struct {
	target tree.DJSON
	iter   *json.ObjectIterator
	key    tree.Datum
	value  tree.Datum
	asText bool
}

func makeJSONEachImplGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	return makeJSONEachGenerator(args, false)
}

func makeJSONEachTextImplGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONEachGenerator(args, true)
}

func makeJSONEachGenerator(args tree.Datums, asText bool) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	return &jsonEachGenerator{
		target: target,
		key:    nil,
		value:  nil,
		asText: asText,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) ResolvedType() *types.T {
	if g.asText {
		return jsonEachTextGeneratorType
	}
	return jsonEachGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Start(_ context.Context, _ *kv.Txn) error {
	iter, err := g.target.ObjectIter()
	if err != nil {
		return err
	}
	if iter == nil {
		switch g.target.Type() {
		case json.ArrayJSONType:
			return errJSONDeconstructArrayAsObject
		default:
			return errJSONDeconstructScalarAsObject
		}
	}
	g.iter = iter
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Next(_ context.Context) (bool, error) {
	if !g.iter.Next() {
		return false, nil
	}
	g.key = tree.NewDString(g.iter.Key())
	if g.asText {
		var err error
		if g.value, err = jsonAsText(g.iter.Value()); err != nil {
			return false, err
		}
	} else {
		g.value = tree.NewDJSON(g.iter.Value())
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Values() tree.Datums {
	return tree.Datums{g.key, g.value}
}

type checkConsistencyGenerator struct {
	ctx      context.Context
	db       *kv.DB
	from, to roachpb.Key
	mode     roachpb.ChecksumMode
	// remainingRows is populated by Start(). Each Next() call peels of the first
	// row and moves it to curRow.
	remainingRows []roachpb.CheckConsistencyResponse_Result
	curRow        roachpb.CheckConsistencyResponse_Result
}

var _ tree.ValueGenerator = &checkConsistencyGenerator{}

func makeCheckConsistencyGenerator(
	ctx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	keyFrom := roachpb.Key(*args[1].(*tree.DBytes))
	keyTo := roachpb.Key(*args[2].(*tree.DBytes))

	if len(keyFrom) == 0 {
		keyFrom = keys.LocalMax
	}
	if len(keyTo) == 0 {
		keyTo = roachpb.KeyMax
	}

	if bytes.Compare(keyFrom, keys.LocalMax) < 0 {
		return nil, errors.Errorf("start key must be >= %q", []byte(keys.LocalMax))
	}
	if bytes.Compare(keyTo, roachpb.KeyMax) > 0 {
		return nil, errors.Errorf("end key must be < %q", []byte(roachpb.KeyMax))
	}
	if bytes.Compare(keyFrom, keyTo) >= 0 {
		return nil, errors.New("start key must be less than end key")
	}

	mode := roachpb.ChecksumMode_CHECK_FULL
	if statsOnly := bool(*args[0].(*tree.DBool)); statsOnly {
		mode = roachpb.ChecksumMode_CHECK_STATS
	}

	return &checkConsistencyGenerator{
		ctx:  ctx.Ctx(),
		db:   ctx.DB,
		from: keyFrom,
		to:   keyTo,
		mode: mode,
	}, nil
}

var checkConsistencyGeneratorType = types.MakeLabeledTuple(
	[]types.T{*types.Int, *types.Bytes, *types.String, *types.String, *types.String},
	[]string{"range_id", "start_key", "start_key_pretty", "status", "detail"},
)

// ResolvedType is part of the tree.ValueGenerator interface.
func (*checkConsistencyGenerator) ResolvedType() *types.T {
	return checkConsistencyGeneratorType
}

// Start is part of the tree.ValueGenerator interface.
func (c *checkConsistencyGenerator) Start(_ context.Context, _ *kv.Txn) error {
	var b kv.Batch
	b.AddRawRequest(&roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    c.from,
			EndKey: c.to,
		},
		Mode: c.mode,
		// No meaningful diff can be created if we're checking the stats only,
		// so request one only if a full check is run.
		WithDiff: c.mode == roachpb.ChecksumMode_CHECK_FULL,
	})
	// NB: DistSender has special code to avoid parallelizing the request if
	// we're requesting CHECK_FULL.
	if err := c.db.Run(c.ctx, &b); err != nil {
		return err
	}
	resp := b.RawResponse().Responses[0].GetInner().(*roachpb.CheckConsistencyResponse)
	c.remainingRows = resp.Result
	return nil
}

// Next is part of the tree.ValueGenerator interface.
func (c *checkConsistencyGenerator) Next(_ context.Context) (bool, error) {
	if len(c.remainingRows) == 0 {
		return false, nil
	}
	c.curRow = c.remainingRows[0]
	c.remainingRows = c.remainingRows[1:]
	return true, nil
}

// Values is part of the tree.ValueGenerator interface.
func (c *checkConsistencyGenerator) Values() tree.Datums {
	return tree.Datums{
		tree.NewDInt(tree.DInt(c.curRow.RangeID)),
		tree.NewDBytes(tree.DBytes(c.curRow.StartKey)),
		tree.NewDString(roachpb.Key(c.curRow.StartKey).String()),
		tree.NewDString(c.curRow.Status.String()),
		tree.NewDString(c.curRow.Detail),
	}
}

// Close is part of the tree.ValueGenerator interface.
func (c *checkConsistencyGenerator) Close() {}
