//
// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
	"math"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	"github.com/stretchr/testify/assert"
	lua "github.com/yuin/gopher-lua"
)

// TestRegisterLuaUDFs tests the RegisterLuaUDFs function covering main code paths
func TestRegisterLuaUDFs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name    string
		datums  tree.Datums
		wantErr bool
	}{
		{
			name:    "nil datums returns nil",
			datums:  nil,
			wantErr: false,
		},
		{
			name:    "empty datums returns nil",
			datums:  tree.Datums{},
			wantErr: false,
		},
		{
			name: "valid udf descriptor returns function definition",
			datums: tree.Datums{
				tree.NewDBytes(tree.DBytes(createTestUDFDescriptor(
					"test_func",
					[]uint32{uint32(sqlbase.DataType_INT)},
					[]uint32{uint32(sqlbase.DataType_INT)},
					"function test_func(x) return x + 1 end",
				))),
			},
			wantErr: false,
		},
		{
			name: "invalid protobuf returns error",
			datums: tree.Datums{
				tree.NewDBytes(tree.DBytes("invalid protobuf")),
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := RegisterLuaUDFs(tt.datums)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				if len(tt.datums) > 0 && tt.datums[0] != nil {
					assert.NotNil(t, got)
					assert.Equal(t, "test_func", got.Name)
				} else {
					assert.Nil(t, got)
				}
			}
		})
	}
}

// TestParseType tests the parseType function with valid and invalid type identifiers
func TestParseType(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name     string
		typeInt  int32
		expected *types.T
		wantErr  bool
	}{
		{
			name:     "valid timestamp type",
			typeInt:  int32(sqlbase.DataType_TIMESTAMP),
			expected: types.Timestamp,
			wantErr:  false,
		},
		{
			name:     "valid bigint type",
			typeInt:  int32(sqlbase.DataType_BIGINT),
			expected: types.Int,
			wantErr:  false,
		},
		{
			name:     "valid bool type",
			typeInt:  int32(sqlbase.DataType_BOOL),
			expected: types.Bool,
			wantErr:  false,
		},
		{
			name:     "invalid type identifier",
			typeInt:  999,
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseType(tt.typeInt)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, pgcode.FdwInvalidDataType, pgerror.GetPGCode(err))
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// TestParseTypes tests the parseTypes function with multiple type IDs
func TestParseTypes(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		params    []int32
		wantLen   int
		wantFirst *types.T
		wantErr   bool
	}{
		{
			name:      "valid int and varchar types",
			params:    []int32{int32(sqlbase.DataType_INT), int32(sqlbase.DataType_VARCHAR)},
			wantLen:   2,
			wantFirst: types.Int4,
			wantErr:   false,
		},
		{
			name:      "invalid type in parameters",
			params:    []int32{999},
			wantLen:   0,
			wantFirst: nil,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseTypes(tt.params)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			}
		})
	}
}

// TestValidateDatums tests the validateDatums function with valid/invalid datum sets
func TestValidateDatums(t *testing.T) {
	defer leaktest.AfterTest(t)()

	validDatums := tree.Datums{
		tree.NewDString("test_func"),           // funcName
		tree.NewDArray(types.Int),              // argTypes
		tree.NewDArray(types.Int),              // returnType
		tree.NewDArray(types.Int),              // typeLength
		tree.NewDString("function test() end"), // funcBody
	}

	nullNameDatums := tree.Datums{
		nil,                                    // funcName (null - invalid)
		tree.NewDArray(types.Int),              // argTypes
		tree.NewDArray(types.Int),              // returnType
		tree.NewDArray(types.Int),              // typeLength
		tree.NewDString("function test() end"), // funcBody
	}

	wrongTypeDatums := tree.Datums{
		tree.NewDString("test_func"),           // funcName
		tree.NewDInt(1),                        // argTypes (wrong type - not array)
		tree.NewDArray(types.Int),              // returnType
		tree.NewDArray(types.Int),              // typeLength
		tree.NewDString("function test() end"), // funcBody
	}

	tests := []struct {
		name    string
		datums  tree.Datums
		wantErr bool
	}{
		{
			name:    "valid datums pass validation",
			datums:  validDatums,
			wantErr: false,
		},
		{
			name:    "null non-nullable field returns error",
			datums:  nullNameDatums,
			wantErr: true,
		},
		{
			name:    "wrong type field returns error",
			datums:  wrongTypeDatums,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDatums(tt.datums)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, pgcode.DatatypeMismatch, pgerror.GetPGCode(err))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// TestCreateLuaFunction tests the createLuaFunction wrapper and execution
func TestCreateLuaFunction(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name       string
		funcName   string
		funcBody   string
		params     []int32
		returnType int32
		args       tree.Datums
		expected   tree.Datum
		wantErr    bool
	}{
		{
			name:       "simple integer addition",
			funcName:   "add",
			funcBody:   `function add(a, b) return a + b end`,
			params:     []int32{int32(sqlbase.DataType_INT), int32(sqlbase.DataType_INT)},
			returnType: int32(sqlbase.DataType_INT),
			args:       tree.Datums{tree.NewDInt(2), tree.NewDInt(3)},
			expected:   tree.NewDInt(5),
			wantErr:    false,
		},
		{
			name:       "float conversion test",
			funcName:   "float_test",
			funcBody:   `function float_test(x) return x * 1.5 end`,
			params:     []int32{int32(sqlbase.DataType_FLOAT)},
			returnType: int32(sqlbase.DataType_FLOAT),
			args:       tree.Datums{tree.NewDFloat(2.0)},
			expected:   tree.NewDFloat(3.0),
			wantErr:    false,
		},
		{
			name:       "timestamp conversion test",
			funcName:   "time_test",
			funcBody:   `function time_test(ts) return ts end`,
			params:     []int32{int32(sqlbase.DataType_TIMESTAMP)},
			returnType: int32(sqlbase.DataType_TIMESTAMP),
			args:       tree.Datums{tree.MakeDTimestamp(timeutil.Unix(1620000000, 0), 0)},
			expected:   tree.MakeDTimestamp(timeutil.Unix(1620000000, 0), 0),
			wantErr:    false,
		},
		{
			name:       "string manipulation test",
			funcName:   "string_test",
			funcBody:   `function string_test(s) return s .. "_suffix" end`,
			params:     []int32{int32(sqlbase.DataType_VARCHAR)},
			returnType: int32(sqlbase.DataType_VARCHAR),
			args:       tree.Datums{tree.NewDString("test")},
			expected:   tree.NewDString("test_suffix"),
			wantErr:    false,
		},
		{
			name:       "non-existent function returns error",
			funcName:   "missing_func",
			funcBody:   `function add(a,b) return a+b end`,
			params:     []int32{int32(sqlbase.DataType_INT)},
			returnType: int32(sqlbase.DataType_INT),
			args:       tree.Datums{tree.NewDInt(1)},
			expected:   nil,
			wantErr:    true,
		},
		{
			name:       "invalid lua script returns error",
			funcName:   "invalid",
			funcBody:   `invalid lua code`,
			params:     []int32{int32(sqlbase.DataType_INT)},
			returnType: int32(sqlbase.DataType_INT),
			args:       tree.Datums{tree.NewDInt(1)},
			expected:   nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			evalFunc := createLuaFunction(tt.funcName, tt.funcBody, tt.params, tt.returnType)
			got, err := evalFunc(&tree.EvalContext{}, tt.args)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// TestGoValueToLuaValue tests type conversion from Go to Lua
func TestGoValueToLuaValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name      string
		val       tree.Datum
		paramType int32
		expected  lua.LValue
		wantErr   bool
	}{
		{
			name:      "int to lua number",
			val:       tree.NewDInt(42),
			paramType: int32(sqlbase.DataType_INT),
			expected:  lua.LNumber(42),
			wantErr:   false,
		},
		{
			name:      "float to lua number",
			val:       tree.NewDFloat(3.14),
			paramType: int32(sqlbase.DataType_FLOAT),
			expected:  lua.LNumber(3.14),
			wantErr:   false,
		},
		{
			name:      "timestamp to lua unix time",
			val:       tree.MakeDTimestamp(timeutil.Unix(1620000000, 0), 0),
			paramType: int32(sqlbase.DataType_TIMESTAMP),
			expected:  lua.LNumber(1620000000),
			wantErr:   false,
		},
		{
			name:      "string to lua string",
			val:       tree.NewDString("test"),
			paramType: int32(sqlbase.DataType_VARCHAR),
			expected:  lua.LString("test"),
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := goValueToLuaValue(tt.val, tt.paramType)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// TestLuaValueToGoValue tests type conversion from Lua to Go
func TestLuaValueToGoValue(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name       string
		val        lua.LValue
		returnType int32
		expected   tree.Datum
		wantErr    bool
	}{
		{
			name:       "lua number to int",
			val:        lua.LNumber(42),
			returnType: int32(sqlbase.DataType_INT),
			expected:   tree.NewDInt(42),
			wantErr:    false,
		},
		{
			name:       "lua number to smallint overflow",
			val:        lua.LNumber(math.MaxInt16 + 1),
			returnType: int32(sqlbase.DataType_SMALLINT),
			expected:   nil,
			wantErr:    true,
		},
		{
			name:       "lua number to float",
			val:        lua.LNumber(3.14),
			returnType: int32(sqlbase.DataType_FLOAT),
			expected:   tree.NewDFloat(3.14),
			wantErr:    false,
		},
		{
			name:       "lua number to float32 overflow",
			val:        lua.LNumber(math.MaxFloat32 * 2),
			returnType: int32(sqlbase.DataType_FLOAT),
			expected:   tree.NewDFloat(tree.DFloat(math.Inf(+1))),
			wantErr:    false,
		},
		{
			name:       "lua number to timestamp",
			val:        lua.LNumber(1620000000),
			returnType: int32(sqlbase.DataType_TIMESTAMP),
			expected:   tree.MakeDTimestamp(timeutil.Unix(1620000000, 0), 0),
			wantErr:    false,
		},
		{
			name:       "lua string to varchar",
			val:        lua.LString("test"),
			returnType: int32(sqlbase.DataType_VARCHAR),
			expected:   tree.NewDString("test"),
			wantErr:    false,
		},
		{
			name:       "lua nil to DNull",
			val:        lua.LNil,
			returnType: int32(sqlbase.DataType_INT),
			expected:   tree.DNull,
			wantErr:    false,
		},
		{
			name:       "unsupported lua type returns error",
			val:        lua.LBool(true),
			returnType: int32(sqlbase.DataType_INT),
			expected:   nil,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := luaValueToGoValue(tt.val, tt.returnType)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, got)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, got)
			}
		})
	}
}

// TestIntOverflowCheck tests integer overflow detection
func TestIntOverflowCheck(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		name  string
		t     int32
		value float64
		want  bool
	}{
		{
			name:  "smallint valid range",
			t:     int32(sqlbase.DataType_SMALLINT),
			value: float64(math.MaxInt16),
			want:  true,
		},
		{
			name:  "smallint overflow",
			t:     int32(sqlbase.DataType_SMALLINT),
			value: float64(math.MaxInt16) + 1,
			want:  false,
		},
		{
			name:  "int valid range",
			t:     int32(sqlbase.DataType_INT),
			value: float64(math.MaxInt32),
			want:  true,
		},
		{
			name:  "int underflow",
			t:     int32(sqlbase.DataType_INT),
			value: float64(math.MinInt32) - 1,
			want:  false,
		},
		{
			name:  "bigint valid range",
			t:     int32(sqlbase.DataType_BIGINT),
			value: float64(math.MaxInt64),
			want:  true,
		},
		{
			name:  "unknown type returns false",
			t:     999,
			value: 0,
			want:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IntOverflowCheck(tt.t, tt.value)
			assert.Equal(t, tt.want, got)
		})
	}
}

// Helper function to create test UDF descriptor protobuf bytes
func createTestUDFDescriptor(
	name string, argTypes []uint32, returnTypes []uint32, body string,
) []byte {
	desc := sqlbase.FunctionDescriptor{
		Name:          name,
		ArgumentTypes: argTypes,
		ReturnType:    returnTypes,
		FunctionBody:  body,
	}

	// Mock protobuf marshal (simplified for testing)
	// In real tests, use proper protoutil.Marshal
	data, _ := protoutil.Marshal(&desc)
	return data
}
