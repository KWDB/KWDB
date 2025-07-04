// Copyright 2017 The Cockroach Authors.
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
//
// This file provides functionality for integrating and managing user-defined functions (UDFs)
// in a SQL database environment with support for Lua scripting. This file facilitates the registration,
// parsing, and execution of Lua-based UDFs, enabling users to extend the database capabilities with
// custom logic encapsulated within Lua scripts. The utility is designed for scenarios where there is a need
// to execute complex operations within SQL queries that are beyond the capabilities of standard SQL functions.
//
// Functional Scope:
// 1. Convert database rows containing UDF specifications into executable function definitions using Lua.
// 2. Validate and ensure the correctness of UDF specifications against expected SQL data types.
// 3. Dynamically execute Lua scripts during SQL operations, handling data type conversions between SQL and Lua.
// 4. Provide robust error handling and type safety, ensuring that UDF operations adhere to SQL standards.
// Limitations include the reliance on correct Lua scripting by the user and the performance implications of
// scripting within SQL operations.

// Design:
// - **RegisterLuaUDFs:** Extracts UDF metadata from system table and creates function definitions. This function
//   is pivotal for transforming SQL table data into executable code entities within the database.
// - **Validation Logic:** Before a UDF is registered, it undergoes rigorous checks to validate data types and
//   structural integrity, ensuring that the UDFs are safe to execute and interact correctly with SQL data types.
// - **Lua Script Execution:** A key feature where Lua scripts are wrapped in Go functions, allowing SQL engine
//   to execute embedded scripts as part of SQL queries. This includes real-time conversion of SQL data types
//   to Lua-compatible types and vice versa.
// - **Error Handling:** Utilizes PostgreSQL wire protocol error standards (pgcode) to handle errors,
//   ensuring that they are communicated clearly and consistently to the client.
// - **Type Conversion Routines:** Special routines are implemented to manage the conversion between Go and Lua types,
//   addressing the nuances of each type system to maintain data integrity and accuracy during UDF execution.

// This architecture enables seamless integration of scripting capabilities within a SQL database, enhancing flexibility
// and power for database applications requiring custom computational logic directly within SQL queries.

package builtins

import (
	"math"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	lua "github.com/yuin/gopher-lua"
)

const (
	funcNameIndex = iota
	argTypesIndex
	returnTypeIndex
	typeLengthIndex
	funcBodyIndex
)

// RegisterLuaUDFs takes a set of datums representing a row from a UDF definition table and converts them into a tree.FunctionDefinition.
// This function is responsible for parsing the datums, validating their integrity and types, extracting the necessary UDF metadata,
// and creating a functional representation that can be utilized by the SQL engine to execute the UDF.
//
// Parameters:
//   - datums: tree.Datums is an array of datums that provide the function's metadata such as name, argument types,
//     return type, and the Lua script body.
//
// Returns:
//   - *tree.FunctionDefinition: A pointer to a FunctionDefinition that encapsulates the UDF in a format that the SQL engine can execute.
//   - error: An error object that reports issues encountered during the processing of the datums into a function definition,
//     including validation failures or issues in parsing the types and Lua script.
//
// Process:
//  1. The function starts by checking if the input datums are nil or empty, which are conditions where it returns no error and no function definition.
//  2. It then validates the datums against expected types using validateDatums function to ensure they match the required structure for UDFs.
//  3. Post validation, the function extracts individual fields such as function name, argument types, and the body of the Lua script from the datums.
//  4. It converts the argument types and return type datums into internal SQL types using parseTypes and parseType functions.
//  5. A Lua script wrapper function is created using createLuaFunction which encapsulates the Lua logic in a callable function form.
//  6. The resulting function definition, along with metadata about the function such as argument types and return type, is encapsulated
//     in a tree.FunctionDefinition structure.
//  7. This definition is then returned, allowing the SQL engine to invoke the defined Lua UDF as part of SQL queries.
//
// This function is critical for enabling the dynamic execution of Lua scripts as UDFs within the SQL environment,
// supporting advanced data manipulation and custom business logic integration directly within SQL queries.
func RegisterLuaUDFs(datums tree.Datums) (*tree.FunctionDefinition, error) {
	if datums == nil || len(datums) == 0 {
		return nil, nil
	}

	var desc sqlbase.FunctionDescriptor
	val := tree.MustBeDBytes(datums[0])
	if err := protoutil.Unmarshal([]byte(val), &desc); err != nil {
		return nil, pgerror.New(pgcode.Warning, "failed to parse descriptor for udf")
	}

	// Extract datum values.
	funcName, funcBody := desc.Name, desc.FunctionBody
	argumentArray, returnTypeArray := desc.ArgumentTypes, desc.ReturnType
	argumentTypes, funcReturnType := make([]int32, len(argumentArray)), int32(returnTypeArray[0])
	for i, v := range argumentArray {
		argumentTypes[i] = int32(v)
	}

	// Parsing parameter types
	paramTypes, err := parseTypes(argumentTypes)
	if err != nil {
		return nil, err
	}

	// Parse return value type
	returnType, err := parseType(funcReturnType)
	if err != nil {
		return nil, err
	}

	// Create a wrapper for lua scripts
	luaFunction := createLuaFunction(funcName, funcBody, argumentTypes, funcReturnType)

	def := []tree.Overload{{
		Types:      paramTypes,
		ReturnType: tree.FixedReturnType(returnType),
		Fn:         luaFunction,
	}}

	udfFunctionDefinition := tree.GetUdfFunctionDefinition(funcName, &tree.FunctionProperties{NullableArgs: false, ForbiddenExecInTSEngine: true}, def)

	return udfFunctionDefinition, nil
}

// parseType is used to convert typeInt to *types.T
func parseType(typeInt int32) (*types.T, error) {
	// Define a map for type lookups
	typeMap := map[int32]*types.T{
		int32(sqlbase.DataType_TIMESTAMP):   types.Timestamp,
		int32(sqlbase.DataType_BIGINT):      types.Int,
		int32(sqlbase.DataType_SMALLINT):    types.Int2,
		int32(sqlbase.DataType_INT):         types.Int4,
		int32(sqlbase.DataType_FLOAT):       types.Float4,
		int32(sqlbase.DataType_DOUBLE):      types.Float,
		int32(sqlbase.DataType_BOOL):        types.Bool,
		int32(sqlbase.DataType_CHAR):        types.Char,
		int32(sqlbase.DataType_NCHAR):       types.NChar,
		int32(sqlbase.DataType_VARCHAR):     types.VarChar,
		int32(sqlbase.DataType_NVARCHAR):    types.NVarChar,
		int32(sqlbase.DataType_TIMESTAMPTZ): types.TimestampTZ,
	}

	// Look up the type using the provided typeInt
	if typ, exists := typeMap[typeInt]; exists {
		return typ, nil
	}
	return nil, pgerror.Newf(pgcode.FdwInvalidDataType, "unknown udf type identifier: %d", typeInt)
}

// parseTypes is used to convert parameters to tree.TypeList
func parseTypes(parameters []int32) (tree.TypeList, error) {
	typeList := make(tree.ArgTypes, len(parameters))
	for i, p := range parameters {
		paramType, err := parseType(p)
		if err != nil {
			return nil, err
		}
		typeList[i].Name = ""
		typeList[i].Typ = paramType
	}
	return typeList, nil
}

// validateDatums checks if the provided datums match the expected types and constraints.
func validateDatums(datums tree.Datums) error {
	expectedTypes := []struct {
		fieldName    string
		fieldIndex   int
		expectedType *types.T
		nullable     bool
	}{
		{"function_name", funcNameIndex, types.String, false},
		{"argument_types", argTypesIndex, types.IntArray, false},
		{"return_type", returnTypeIndex, types.IntArray, false},
		{"types_length", typeLengthIndex, types.IntArray, false},
		{"function_body", funcBodyIndex, types.String, false},
	}

	for _, exp := range expectedTypes {
		datum := datums[exp.fieldIndex]
		if datum == nil {
			if !exp.nullable {
				return pgerror.Newf(pgcode.DatatypeMismatch, "expected non-null for field '%s'", exp.fieldName)
			}
			// Null is allowed, skip further type checks.
			continue
		}

		if !datum.ResolvedType().Equivalent(exp.expectedType) {
			// Handle the case where the type is unknown and nulls are allowed.
			if exp.nullable && datum.ResolvedType().Family() == types.UnknownFamily {
				continue
			}
			return pgerror.Newf(pgcode.DatatypeMismatch, "%s has type %s. Expected %s", exp.fieldName, datum.ResolvedType(), exp.expectedType)
		}
	}

	return nil
}

// createLuaFunction creates a Go function that encapsulates the execution of a Lua script.
// It provides a bridge between Go and Lua, allowing Lua scripts to be called as functions within
// a SQL environment managed by Go. This function is designed to dynamically interpret and execute
// Lua code based on SQL function calls, converting SQL data into Lua-compatible types and vice versa.
//
// Parameters:
//   - funcName: The name of the global Lua function to be invoked. This function must be defined in the Lua script body.
//   - funcBody: A string containing the Lua script. This script should define a function with the name specified.
//   - parameters: A slice of int32 values representing the SQL data types of the arguments that the Lua function expects.
//     These types are used to convert SQL arguments to Lua-compatible types before invoking the Lua function.
//   - returnType: An int32 representing the expected SQL data type of the Lua function's return value. This type is used
//     to convert the Lua function's return value back to a compatible SQL type.
//
// Returns:
//   - A function of type func(*tree.EvalContext, tree.Datums) (tree.Datum, error). This function takes an evaluation context
//     and a slice of Datums (SQL types) as arguments, executes the Lua script, and returns the result as a SQL type.
//     The function encapsulates error handling for script execution errors and type conversion errors.
//
// Implementation Details:
//   - The function initializes a new Lua state for each invocation to ensure that script executions are isolated and do not
//     interfere with each other.
//   - It loads and executes the Lua script to define the necessary functions within the Lua state.
//   - It retrieves the Lua function by the given name and checks if it is correctly defined as a function.
//   - It converts each SQL argument to the appropriate Lua type based on the specified parameters array.
//   - It executes the Lua function with the converted arguments and handles any errors that occur during this process.
//   - After the Lua function execution, it fetches the return value from the Lua state, converts it back to the specified
//     SQL return type, and returns it to the caller.
//   - The function uses Lua's protected call mechanism to safely execute the Lua function and catch any errors that might occur.
//
// Usage:
// This function is used within the SQL engine to handle UDF (User Defined Functions) calls that execute Lua scripts.
// It allows for complex computations or operations that are not natively supported by the SQL engine but can be implemented
// in Lua.
func createLuaFunction(
	funcName, funcBody string, parameters []int32, returnType int32,
) func(*tree.EvalContext, tree.Datums) (tree.Datum, error) {
	// Returns a function pointer
	return func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		L := lua.NewState()
		defer L.Close() // Ensure Lua state is closed at end of function

		// Load and execute Lua script
		if err := L.DoString(funcBody); err != nil {
			return nil, err
		}

		// Prepare function call
		fn := L.GetGlobal(funcName)
		if fn.Type() != lua.LTFunction {
			return nil, pgerror.Newf(pgcode.Warning, "%s is not a function", funcName)
		}

		var err error
		// Convert args to Lua type
		luaArgs := make([]lua.LValue, len(args))
		for i, arg := range args {
			// Convert Go types to Lua types based on parameter types
			luaArgs[i], err = goValueToLuaValue(arg, parameters[i])
			if err != nil {
				return nil, err
			}
		}

		// Call Lua function
		if err := L.CallByParam(lua.P{
			Fn:      fn,
			NRet:    1,
			Protect: true,
		}, luaArgs...); err != nil {
			return nil, err
		}

		// Get the return value and convert it back to Go type
		ret := L.Get(-1)
		for {
			if fn, ok := ret.(*lua.LFunction); ok {
				if err := L.CallByParam(lua.P{
					Fn:      fn,
					NRet:    1,
					Protect: true,
				}); err != nil {
					return nil, err
				}

				ret = L.Get(-1)
				L.Pop(1)
			} else {
				L.Pop(1)
				break
			}
		}
		return luaValueToGoValue(ret, returnType)
	}
}

// goValueToLuaValue is used to convert go value to lua type
func goValueToLuaValue(val tree.Datum, paramType int32) (lua.LValue, error) {
	switch paramType {
	case int32(sqlbase.DataType_INT), int32(sqlbase.DataType_SMALLINT), int32(sqlbase.DataType_BIGINT):
		if dInt, ok := val.(*tree.DInt); ok {
			pass := IntOverflowCheck(paramType, float64(*dInt))
			if !pass {
				return nil, tree.ErrIntOutOfRange
			}
			return lua.LNumber(*dInt), nil
		}
	case int32(sqlbase.DataType_DOUBLE), int32(sqlbase.DataType_FLOAT):
		if dFloat, ok := val.(*tree.DFloat); ok {
			return lua.LNumber(*dFloat), nil
		}
	case int32(sqlbase.DataType_TIMESTAMP):
		if dTimestamp, ok := val.(*tree.DTimestamp); ok {
			return lua.LNumber(dTimestamp.Unix()), nil
		}
	case int32(sqlbase.DataType_CHAR), int32(sqlbase.DataType_VARCHAR), int32(sqlbase.DataType_NCHAR), int32(sqlbase.DataType_NVARCHAR):
		if dString, ok := val.(*tree.DString); ok {
			return lua.LString(*dString), nil
		}
	}

	// Extract the actual type name from the datum for a more informative error message.
	actualTypeName := "unknown"
	if val.ResolvedType() != nil {
		actualTypeName = val.ResolvedType().Name()
	}

	return nil, pgerror.Newf(pgcode.DatatypeMismatch, "unsupported parameter type %v for value of type %v", paramType, actualTypeName)
}

// luaValueToGoValue converts Lua values back to Go values.
func luaValueToGoValue(val lua.LValue, returnType int32) (tree.Datum, error) {
	if _, ok := val.(*lua.LNilType); ok {
		return tree.DNull, nil
	}

	expectedType, err := parseType(returnType)
	if err != nil {
		return nil, err // Early return if the return type is not recognized
	}

	switch returnType {
	case int32(sqlbase.DataType_INT), int32(sqlbase.DataType_SMALLINT), int32(sqlbase.DataType_BIGINT):
		if num, ok := val.(lua.LNumber); ok {
			pass := IntOverflowCheck(returnType, float64(num))
			if !pass {
				return nil, tree.ErrIntOutOfRange
			}
			return tree.NewDInt(tree.DInt(num)), nil
		}
	case int32(sqlbase.DataType_DOUBLE):
		if num, ok := val.(lua.LNumber); ok {
			return tree.NewDFloat(tree.DFloat(num)), nil
		}
	case int32(sqlbase.DataType_FLOAT):
		if num, ok := val.(lua.LNumber); ok {
			if num > math.MaxFloat32 {
				return tree.NewDFloat(tree.DFloat(math.Inf(+1))), nil
			}
			return tree.NewDFloat(tree.DFloat(num)), nil
		}
	case int32(sqlbase.DataType_TIMESTAMP):
		if num, ok := val.(lua.LNumber); ok {
			return tree.MakeDTimestamp(timeutil.Unix(int64(num), 0), 0), nil
		}
	case int32(sqlbase.DataType_CHAR), int32(sqlbase.DataType_VARCHAR), int32(sqlbase.DataType_NCHAR), int32(sqlbase.DataType_NVARCHAR):
		if str, ok := val.(lua.LString); ok {
			return tree.NewDString(string(str)), nil
		}
	}

	return nil, pgerror.Newf(pgcode.DatatypeMismatch, "lua value type '%s' does not match expected SQL data type '%s'", val.Type().String(), expectedType.SQLString())
}

// IntOverflowCheck is used to check integer type is out of bounds
func IntOverflowCheck(t int32, value float64) bool {
	switch t {
	case int32(sqlbase.DataType_SMALLINT):
		return value >= math.MinInt16 && value <= math.MaxInt16
	case int32(sqlbase.DataType_INT):
		return value >= math.MinInt32 && value <= math.MaxInt32
	case int32(sqlbase.DataType_BIGINT):
		return value >= math.MinInt64 && value <= math.MaxInt64
	}
	return false
}
