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

package sql

import (
	"context"
	"fmt"
	"strings"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sessiondata"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
	"gitee.com/kwbasedb/kwbase/pkg/util/timeutil"
	lua "github.com/yuin/gopher-lua"
)

type createFunctionNode struct {
	n *tree.CreateFunction
	p *planner
}

// CreateFunction creates a function.
func (p *planner) CreateFunction(ctx context.Context, n *tree.CreateFunction) (planNode, error) {
	if !p.extendedEvalCtx.TxnImplicit {
		return nil, pgerror.New(pgcode.FeatureNotSupported, "Create Function statement is not supported in explicit transaction")
	}
	return &createFunctionNode{
		n: n,
		p: p,
	}, nil
}

// startExec is interface implementation, which execute the event of creating function(s).
func (n *createFunctionNode) startExec(params runParams) error {
	if err := n.CheckUdf(params); err != nil {
		return err
	}

	rows := make([]tree.Datums, 0)
	creator := params.p.sessionDataMutator.data.User
	funcName := strings.ToLower(string(n.n.FunctionName))

	// assign unique id
	id, err := GenerateUniqueDescID(params.ctx, params.p.ExecCfg().DB)
	if err != nil {
		return err
	}

	// Generate funcDesc
	desc, err := n.makeFuncDesc(params.SessionData())
	if err != nil {
		return err
	}
	descValue, err := protoutil.Marshal(&desc)
	if err != nil {
		return err
	}

	var ext []byte
	row := tree.Datums{
		tree.NewDString(string(n.n.FunctionName)),
		tree.NewDInt(0),
		tree.NewDInt(0),
		tree.NewDBytes(tree.DBytes(descValue)),
		tree.NewDInt(tree.DInt(id)),
		tree.NewDInt(tree.DInt(sqlbase.Function)),
		tree.NewDString(creator),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.MakeDTimestamp(timeutil.Now(), time.Second),
		tree.NewDInt(tree.DInt(1)),
		tree.DBoolTrue,
		tree.NewDBytes(tree.DBytes(ext)),
	}
	rows = append(rows, row)
	// system.user_defined_routine
	if err := WriteKWDBDesc(params.ctx, params.p.txn, sqlbase.UDRTable, rows, false); err != nil {
		return err
	}
	if err := params.p.txn.Commit(params.ctx); err != nil {
		return err
	}

	if err := GossipUdfAdded(params.p.execCfg.Gossip, funcName); err != nil {
		return err
	}

	// Since gossip execution is asynchronous, a waiting mechanism is added to ensure that functions are registered immediately.
	timeout := time.After(3 * time.Second)
	ticker := time.NewTicker(300 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-timeout:
			return pgerror.Newf(pgcode.Warning, "create %s function hits timeout problem broadcasting across cluster, it might not be available on other nodes", funcName)
		case <-ticker.C:
			if _, ok := tree.ConcurrentFunDefs.LookupFunc(funcName); ok {
				return nil
			}
		}
	}
	return nil
}

// makeFuncDesc constructs and returns a new funcDescriptor
func (n *createFunctionNode) makeFuncDesc(
	sessionData *sessiondata.SessionData,
) (sqlbase.FunctionDescriptor, error) {
	argTypes, returnTypes, typeLens, err := n.getTypesAndLength()
	if err != nil {
		return sqlbase.FunctionDescriptor{}, err
	}
	encodeArgTypes := make([]uint32, len(argTypes.Array))
	encodeReturnTypes := make([]uint32, len(returnTypes.Array))
	encodeTypeLens := make([]uint32, len(typeLens.Array))
	for i, v := range argTypes.Array {
		encodeArgTypes[i] = uint32(*v.(*tree.DInt))
	}
	for i, v := range returnTypes.Array {
		encodeReturnTypes[i] = uint32(*v.(*tree.DInt))
	}
	for i, v := range typeLens.Array {
		encodeTypeLens[i] = uint32(*v.(*tree.DInt))
	}

	return sqlbase.FunctionDescriptor{
		Name:          strings.ToLower(string(n.n.FunctionName)),
		ArgumentTypes: encodeArgTypes,
		ReturnType:    encodeReturnTypes,
		TypesLength:   encodeTypeLens,
		FunctionBody:  n.n.FuncBody,
		FunctionType:  uint32(sqlbase.DefinedFunction),
		Language:      "LUA",
		DbName:        sessionData.Database,
	}, nil
}

// CheckUdf is used to check whether the parameters, return type, function body
// are legal
func (n *createFunctionNode) CheckUdf(params runParams) error {
	L := lua.NewState()
	defer L.Close()

	// check the function name is valid
	if err := n.CheckUdfName(params); err != nil {
		return err
	}

	// Check lua syntax
	if err := L.ParseString(n.n.FuncBody, string(n.n.FunctionName), len(n.n.Arguments)); err != nil {
		return err
	}
	return nil
}

// CheckUdfName is used to check whether the function name is legal
func (n *createFunctionNode) CheckUdfName(params runParams) error {
	funcName := strings.ToLower(string(n.n.FunctionName))
	if funcName == "" {
		return pgerror.New(pgcode.Syntax, "function name cannot be empty when creating a new function")
	}
	// check if there is already a function with the same name
	// by looking up the system table.
	query := fmt.Sprintf("SELECT name from system.user_defined_routine WHERE name = '%s' and routine_type = %d", funcName, sqlbase.Function)
	rows, err := params.p.extendedEvalCtx.ExecCfg.InternalExecutor.Query(params.ctx, "get-functions", params.p.txn, query)
	if err != nil {
		return err
	}

	if len(rows) != 0 {
		return pgerror.Newf(pgcode.DuplicateObject, "function named '%s' already exists. Please choose a different name", funcName)
	}
	// check if there is already a function with the same name
	// by looking up the all builtins function.
	if _, ok := tree.FunDefs[funcName]; ok {
		return pgerror.Newf(pgcode.DuplicateObject, "function named '%s' already exists. Please choose a different name", funcName)
	}
	return nil
}

func (*createFunctionNode) Next(runParams) (bool, error) { return false, nil }
func (*createFunctionNode) Values() tree.Datums          { return tree.Datums{} }
func (*createFunctionNode) Close(context.Context)        {}

// getFuncDataTypeAndLen returns the type and length used by the defined function.
func getFuncDataTypeAndLen(typ *types.T) (sqlbase.DataType, int32) {
	//if typ.InternalType.TypeEngine != 0 && !typ.IsTypeEngineSet(types.TIMESERIES) {
	//	return sqlbase.DataType_UNKNOWN
	//}
	switch typ.Name() {
	case "timestamp":
		return sqlbase.DataType_TIMESTAMP, 0
	case "int2":
		return sqlbase.DataType_SMALLINT, 0
	case "int4":
		return sqlbase.DataType_INT, 0
	case "int":
		return sqlbase.DataType_BIGINT, 0
	case "float4":
		return sqlbase.DataType_FLOAT, 0
	case "float":
		return sqlbase.DataType_DOUBLE, 0
	case "char":
		return sqlbase.DataType_CHAR, typ.Width()
	case "nchar":
		return sqlbase.DataType_NCHAR, typ.Width()
	case "varchar":
		return sqlbase.DataType_VARCHAR, typ.Width()
	case "nvarchar":
		return sqlbase.DataType_NVARCHAR, typ.Width()
	default:
		return sqlbase.DataType_UNKNOWN, 0
	}
}

// getTypesAndLength return defined function's argTypes, returnTypes, typeLens
// Input:   None
// Output:
//
//	1.argTypes     - An array of function's argument type
//	2.returnTypes  - An array of function's return type
//	3.typeLens     - An array of function's argument and return type length
func (n *createFunctionNode) getTypesAndLength() (*tree.DArray, *tree.DArray, *tree.DArray, error) {
	typeLens := tree.NewDArray(types.Int)
	var lengthArray tree.Datums

	argTypes := tree.NewDArray(types.Int)
	var argTypeArray tree.Datums
	for _, val := range n.n.Arguments {
		argType, argLen := getFuncDataTypeAndLen(val.ArgType)
		if argType == sqlbase.DataType_UNKNOWN {
			return nil, nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "argument type %s is not supported", val.ArgType.SQLString())
		}
		argTypeArray = append(argTypeArray, tree.NewDInt(tree.DInt(argType)))
		lengthArray = append(lengthArray, tree.NewDInt(tree.DInt(argLen)))
	}
	argTypes.Array = argTypeArray

	returnTypes := tree.NewDArray(types.Int)
	var returnTypeArray tree.Datums
	returnType, returnLen := getFuncDataTypeAndLen(n.n.ReturnType)
	if returnType == sqlbase.DataType_UNKNOWN {
		return nil, nil, nil, pgerror.Newf(pgcode.DatatypeMismatch, "return type %s is not supported", n.n.ReturnType.SQLString())
	}
	returnTypeArray = append(returnTypeArray, tree.NewDInt(tree.DInt(returnType)))
	lengthArray = append(lengthArray, tree.NewDInt(tree.DInt(returnLen)))
	returnTypes.Array = returnTypeArray
	typeLens.Array = lengthArray

	return argTypes, returnTypes, typeLens, nil
}
