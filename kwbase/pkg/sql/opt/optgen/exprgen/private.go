// Copyright 2019 The Cockroach Authors.
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

package exprgen

import (
	"context"
	"encoding/json"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/cat"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/memo"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/optgen/lang"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/stats"
)

// evalPrivate evaluates a list of the form
//
//	[ (FieldName <value>) ... ]
//
// into an operation private of the given type (e.g. ScanPrivate, etc).
//
// Various implicit conversions are supported. Examples:
//   - table ID: "table"
//   - index ordinal: "table@index"
//   - column lists or sets: "a,b,c"
//   - orderings and ordering choices: "+a,-b"
//   - operators: "inner-join"
func (eg *exprGen) evalPrivate(privType reflect.Type, expr lang.Expr) interface{} {
	if expr.Op() != lang.ListOp {
		panic(errorf("private must be a list of the form [ (FieldName Value) ... ]"))
	}

	// Special case for FakeRelPrivate; we want to specify the Relational fields
	// directly.
	if privType == reflect.TypeOf(memo.FakeRelPrivate{}) {
		props := eg.evalPrivate(reflect.TypeOf(props.Relational{}), expr).(*props.Relational)
		return &memo.FakeRelPrivate{Props: props}
	}

	items := expr.(*lang.ListExpr).Items

	result := reflect.New(privType)

	for _, item := range items {
		// Each item must be of the form (FieldName Value).
		fn, ok := item.(*lang.FuncExpr)
		if !ok || len(fn.Args) != 1 {
			panic(errorf("private list must contain items of the form (FieldName Value)"))
		}
		fieldName := fn.SingleName()
		field := result.Elem().FieldByName(fieldName)
		if !field.IsValid() {
			panic(errorf("invalid field %s for %s", fieldName, privType))
		}
		val := eg.convertPrivateFieldValue(privType, fieldName, field.Type(), eg.eval(fn.Args[0]))
		field.Set(reflect.ValueOf(val))
	}
	return result.Interface()
}

func (eg *exprGen) convertPrivateFieldValue(
	privType reflect.Type, fieldName string, fieldType reflect.Type, value interface{},
) interface{} {

	// This code handles the conversion of a user-friendly value and the value of
	// the field in the private structure.

	if str, ok := value.(string); ok {
		switch fieldType {
		case reflect.TypeOf(opt.TableID(0)):
			return eg.addTable(str)

		case reflect.TypeOf(0):
			if strings.HasSuffix(fieldName, "Index") {
				return eg.findIndex(str)
			}

		case reflect.TypeOf(opt.Operator(0)):
			return eg.opFromStr(str)

		case reflect.TypeOf(props.Cardinality{}):
			return eg.cardinalityFromStr(str)

		case reflect.TypeOf(props.Statistics{}):
			return eg.statsFromStr(str)
		}
	}

	if res := eg.castToDesiredType(value, fieldType); res != nil {
		return res
	}
	panic(errorf("invalid value for %s.%s: %v", privType, fieldName, value))
}

// addTable resolves the given table name and adds the table to the metadata.
func (eg *exprGen) addTable(name string) opt.TableID {
	tn := tree.MakeUnqualifiedTableName(tree.Name(name))
	ds, _, err := eg.cat.ResolveDataSource(context.Background(), cat.Flags{}, &tn)
	if err != nil {
		panic(exprGenErr{err})
	}
	tab, ok := ds.(cat.Table)
	if !ok {
		panic(errorf("non-table datasource %s not supported", name))
	}
	return eg.mem.Metadata().AddTable(tab, &tn)
}

// findIndex looks for an index specified as "table@idx_name" among the tables
// already added to the metadata.
func (eg *exprGen) findIndex(str string) int {
	a := strings.Split(str, "@")
	if len(a) != 2 {
		panic(errorf("index must be specified as table@index"))
	}
	table, index := a[0], a[1]
	var tab cat.Table
	for _, meta := range eg.mem.Metadata().AllTables() {
		if meta.Alias.Table() == table {
			if tab != nil {
				panic(errorf("ambiguous table name %s", table))
			}
			tab = meta.Table
		}
	}
	if tab == nil {
		panic(errorf("unknown table %s", table))
	}
	for i := 0; i < tab.IndexCount(); i++ {
		if string(tab.Index(i).Name()) == index {
			return i
		}
	}
	panic(errorf("index %s not found for table %s", index, table))
}

// opFromStr converts an operator string like "inner-join" to the corresponding
// operator.
func (eg *exprGen) opFromStr(str string) opt.Operator {
	for i := opt.Operator(1); i < opt.NumOperators; i++ {
		if i.String() == str {
			return i
		}
	}
	panic(errorf("unknown operator %s", str))
}

func (eg *exprGen) cardinalityFromStr(str string) props.Cardinality {
	pieces := strings.SplitN(str, "-", 2)
	if len(pieces) != 2 {
		panic(errorf("cardinality must be of the form \"[<min>] - [<max>]\": %s", str))
	}
	a := strings.Trim(pieces[0], " ")
	b := strings.Trim(pieces[1], " ")
	c := props.AnyCardinality
	if a != "" {
		c.Min = uint32(eg.intFromStr(a))
	}
	if b != "" {
		c.Max = uint32(eg.intFromStr(b))
	}
	return c
}

func (eg *exprGen) intFromStr(str string) int {
	val, err := strconv.Atoi(str)
	if err != nil {
		panic(wrapf(err, "expected number: %s", str))
	}
	return val
}

func (eg *exprGen) statsFromStr(str string) props.Statistics {
	var stats []stats.JSONStatistic
	if err := json.Unmarshal([]byte(str), &stats); err != nil {
		panic(wrapf(err, "error unmarshaling statistics"))
	}
	var result props.Statistics
	if len(stats) == 0 {
		return result
	}
	// Sort the statistics, most-recent first.
	sort.Slice(stats, func(i, j int) bool {
		return stats[i].CreatedAt > stats[j].CreatedAt
	})
	result.RowCount = float64(stats[0].RowCount)
	for i := range stats {
		var cols opt.ColSet
		for _, colStr := range stats[i].Columns {
			cols.Add(eg.LookupColumn(colStr))
		}
		s, added := result.ColStats.Add(cols)
		if !added {
			// The same set was already in a more recent statistic, ignore.
			continue
		}
		s.DistinctCount = float64(stats[i].DistinctCount)
		s.NullCount = float64(stats[i].NullCount)
	}
	return result
}
