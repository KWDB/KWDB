// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

package procedure

import (
	"context"
	"fmt"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/exec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// SpExecContext execute context
type SpExecContext struct {
	// localVariable local variables
	localVariable []*exec.LocalVariable
	// localVarMap local variables map
	localVarMap map[string]int

	// LocalCurSorMap local cursor map
	localCurSorMap map[string]CursorHelper

	// all sql query result
	results *Result

	// handler for exit and continue, per block exists one, type has not found or sql exception
	handler [tree.ModeMax]*HandlerHelper

	// ret saves return flag and return label name
	ret ReturnHelper

	// Transaction Start or End State.(1: START;2: COMMIT; 3: ROLLBACK)
	procedureTxn *tree.ProcedureTxnState

	// continueExec flags continue execute status
	continueExec *bool

	// Fn saves function that optimize and build
	Fn exec.ProcedurePlanFn

	// TriggerReplaceValues is the interface for trigger replacement.
	TriggerReplaceValues exec.PlaceHolderExecute
}

// Init execute context
func (c *SpExecContext) Init() {
	c.localVariable = nil
	c.localVarMap = make(map[string]int)
	c.localCurSorMap = make(map[string]CursorHelper)
	c.results = &Result{}
	defaultTxt := tree.ProcedureTransactionDefault
	c.procedureTxn = &defaultTxt
	status := false
	c.continueExec = &status
}

// InitResult execute context
func (c *SpExecContext) InitResult() {
	c.results = &Result{}
	*c.continueExec = true
}

// ContinueExec returns execute execute status
func (c *SpExecContext) ContinueExec() bool {
	return *c.continueExec
}

// ResetExecuteStatus resets continue execute status
func (c *SpExecContext) ResetExecuteStatus() {
	*c.continueExec = false
}

// Inherit execute context
func (c *SpExecContext) Inherit(src *SpExecContext) {
	c.localVariable = append(c.localVariable, src.localVariable...)
	for k, v := range src.localVarMap {
		c.localVarMap[k] = v
	}
	for k, v := range src.localCurSorMap {
		c.localCurSorMap[k] = v
	}
	c.handler = src.handler
	c.results = src.results
	c.procedureTxn = src.procedureTxn
	c.continueExec = src.continueExec
	c.Fn = src.Fn
	c.TriggerReplaceValues = src.TriggerReplaceValues
}

// InheritResult execute context
func (c *SpExecContext) InheritResult(src *SpExecContext) {
	c.results = src.results
	c.continueExec = src.continueExec
	c.TriggerReplaceValues = src.TriggerReplaceValues
}

// AddVariable adds  a local variable
func (c *SpExecContext) AddVariable(v *exec.LocalVariable) {
	if idx, ok := c.localVarMap[v.Name]; ok {
		c.localVariable[idx] = v
		return
	}
	c.localVarMap[v.Name] = len(c.localVariable)
	c.localVariable = append(c.localVariable, v)
}

// SetVariable sets a local variable
func (c *SpExecContext) SetVariable(idx int, v *tree.Datum) {
	c.localVariable[idx].Data = *v
}

// SetExternalVariable sets a external variable
func (c *SpExecContext) SetExternalVariable(idx int, v *tree.Datum) {
	c.TriggerReplaceValues.SetValue(tree.PlaceholderIdx(idx), *v)
}

// GetVariableName gets the local variable name.
func (c *SpExecContext) GetVariableName(idx int) string {
	for k, v := range c.localVarMap {
		if v == idx {
			return k
		}
	}
	return ""
}

// SetVariableType sets a local variable type
func (c *SpExecContext) SetVariableType(idx int, typ types.T) {
	c.localVariable[idx].Typ = typ
}

// IntoDeclareVar sets local variable
func (c *SpExecContext) IntoDeclareVar(idx int, vs tree.Datum, columns sqlbase.ResultColumn) error {
	if idx >= len(c.localVariable) {
		return pgerror.Newf(pgcode.StringDataLengthMismatch,
			"idx %v of variable exceeds the maximum of localVariable %v", idx, len(c.localVariable))
	}

	// an error needs to be reported if it is not a null value and the column types are not equal
	if !c.localVariable[idx].Typ.Equivalent(vs.ResolvedType()) && vs != tree.DNull {
		var originTypeStr string
		// the precision of TimestampTZFamily or TimestampFamily needs to be obtained from the original information of the column
		if (vs.ResolvedType().InternalType.Family == types.TimestampTZFamily ||
			vs.ResolvedType().InternalType.Family == types.TimestampFamily) &&
			columns.Typ.InternalType.Precision > 0 {
			originTypeStr = columns.Typ.String()
		} else {
			originTypeStr = vs.ResolvedType().String()
		}
		return pgerror.Newf(pgcode.DatatypeMismatch, "variable %s type %s not save type %s ",
			c.localVariable[idx].Name, c.localVariable[idx].Typ.String(), originTypeStr)
	}

	variableName := c.GetVariableName(idx)
	// check value by type
	value, err := sqlbase.LimitValueWidth(&c.localVariable[idx].Typ, vs, &variableName)
	if err != nil {
		return err
	}
	c.localVariable[idx].Data = value
	return nil
}

// SetVariables sets local variables
func (c *SpExecContext) SetVariables(
	idxs []int, vs tree.Datums, columns sqlbase.ResultColumns,
) error {
	for i, idx := range idxs {
		if err := c.IntoDeclareVar(idx, vs[i], columns[i]); err != nil {
			return err
		}
	}
	return nil
}

// GetVariable gets local variable
func (c *SpExecContext) GetVariable(idx int) *exec.LocalVariable {
	return c.localVariable[idx]
}

// GetLocalVariable gets local variable
func (c *SpExecContext) GetLocalVariable() []*exec.LocalVariable {
	return c.localVariable
}

// GetLocalCurSorMap gets local cursor
func (c *SpExecContext) GetLocalCurSorMap() map[string]CursorHelper {
	return c.localCurSorMap
}

// GetVariableLen gets local variable length
func (c *SpExecContext) GetVariableLen() int {
	return len(c.localVariable)
}

// AddCursor adds  a local cursor
func (c *SpExecContext) AddCursor(name string, v *CursorHelper) {
	c.localCurSorMap[name] = *v
}

// FindCursor finds local cursor
func (c *SpExecContext) FindCursor(name string) *CursorHelper {
	if cursor, ok := c.localCurSorMap[name]; ok {
		return &cursor
	}
	return nil
}

// AddQueryResult adds a sql query result
func (c *SpExecContext) AddQueryResult(result *QueryResult) {
	c.results.AddQueryResult(result)
}

// HasNextResult returns true for has next result
func (c *SpExecContext) HasNextResult() bool {
	return c.results.HasNextResult()
}

// CheckResultExist checks result exist
func (c *SpExecContext) CheckResultExist() bool {
	return c.results != nil && c.results.CheckResultExist()
}

// GetNextResultCols get next result columns
func (c *SpExecContext) GetNextResultCols() sqlbase.ResultColumns {
	return c.results.GetNextResultCols()
}

// GetResultTypeAndAffected get next result query type
func (c *SpExecContext) GetResultTypeAndAffected() (tree.StatementType, int) {
	return c.results.GetResultTypeAndAffected()
}

// Next returns a row data
func (c *SpExecContext) Next() (bool, error) {
	if c.results == nil {
		return false, nil
	}
	return c.results.Next()
}

// Values returns a row data
func (c *SpExecContext) Values() tree.Datums {
	return c.results.Values()
}

// Close for delete row container
func (c *SpExecContext) Close(ctx context.Context) {
	if c.results != nil {
		c.results.Close(ctx)
		*c.results = Result{}
		c.results = nil
	}
	c.ret.labelName = ""
	c.ret.retFlag = false
	c.ret.handler = nil
}

// SetHandler adds a local handler
func (c *SpExecContext) SetHandler(m tree.HandlerMode, h *HandlerHelper) {
	c.handler[m] = h
}

// SetExceptionErr set error
func (c *SpExecContext) SetExceptionErr(err error) {
	c.ret.exceptionErr = err
}

// GetHandlerByMode gets a local handler
func (c *SpExecContext) GetHandlerByMode(m tree.HandlerMode) *HandlerHelper {
	return c.handler[m]
}

// AddReturn adds return flag and label name
func (c *SpExecContext) AddReturn(labelName string, handler *HandlerHelper, exceptionErr error) {
	c.ret.AddReturnLabel(labelName)
	if labelName == HandlerLabel && handler != nil {
		c.ret.AddReturnHandler(handler)
		c.ret.exceptionErr = exceptionErr
	}
}

// CheckAndRemoveReturn removes return flag and label name
func (c *SpExecContext) CheckAndRemoveReturn(label string) {
	if c.ret.labelName == "" || c.ret.labelName == label {
		c.ret.RemoveReturn()
	}
}

// NeedReturn returns return flag
func (c *SpExecContext) NeedReturn() bool {
	return c.ret.retFlag
}

// ContinueStatus returns continue status
func (c *SpExecContext) ContinueStatus() bool {
	return c.ret.retFlag && c.ret.labelName == DefaultLabel
}

// ReturnLabel returns label name
func (c *SpExecContext) ReturnLabel() string {
	return c.ret.labelName
}

// GetReturnHandler returns handler of ReturnHelper
func (c *SpExecContext) GetReturnHandler() *HandlerHelper {
	return c.ret.handler
}

// GetExceptionErr returns exceptionErr of ReturnHelper
func (c *SpExecContext) GetExceptionErr() error {
	return c.ret.exceptionErr
}

// GetProcedureTxn get  c.procedureTxn
func (c *SpExecContext) GetProcedureTxn() tree.ProcedureTxnState {
	return *c.procedureTxn
}

// SetProcedureTxn get  c.procedureTxn
func (c *SpExecContext) SetProcedureTxn(state tree.ProcedureTxnState) {
	c.procedureTxn = &state
}

// IndexedVarEval is part of the tree.IndexedVarContainer interface.
func (c *SpExecContext) IndexedVarEval(idx int, ctx *tree.EvalContext) (tree.Datum, error) {
	return c.localVariable[idx].Data.Eval(ctx)
}

// IndexedVarResolvedType is part of the tree.IndexedVarContainer interface.
func (c *SpExecContext) IndexedVarResolvedType(idx int) *types.T {
	return &c.localVariable[idx].Typ
}

// IndexedVarNodeFormatter is part of the parser.IndexedVarContainer interface.
func (c *SpExecContext) IndexedVarNodeFormatter(idx int) tree.NodeFormatter {
	n := tree.Name(fmt.Sprintf("$%d", idx))
	return &n
}

// IsSetChildStackIndex whether set childStackIndex.
func (c *SpExecContext) IsSetChildStackIndex() bool {
	if c.ReturnLabel() == DefaultLabel || c.ReturnLabel() == HandlerLabel || c.ReturnLabel() == IntoLabel {
		return true
	}
	return false
}
