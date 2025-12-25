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
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgcode"
	"gitee.com/kwbasedb/kwbase/pkg/sql/pgwire/pgerror"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// Cursor creates a new sql for procedure cursor
type Cursor struct {
	//StmtIns for sql plan
	StmtIns

	// resultCols saves result columns
	resultCols sqlbase.ResultColumns

	isOpen bool

	// params for get next row data
	execHelper CursorExec
}

// Start for sql plan
func (c *Cursor) Start(params RunParam, rCtx *SpExecContext) error {
	plan, res, err := c.StmtIns.getPlanAndContainer(rCtx, &c.resultCols, params.EvalContext().Mon)
	if err != nil {
		return err
	}
	//c.plan = plan
	err = rCtx.StartPlanFn(params, c.execHelper, res, c.typ, plan)
	if err != nil {
		// check cancel status
		if errCancel := params.GetCtx().Err(); errCancel != nil {
			return errCancel
		}
		contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
		if errHandler != nil {
			return errHandler
		}
		if contiuneIsExec && !exitIsExec {
			return nil
		}
		return err
	}
	c.isOpen = true
	return nil
}

// Next for Cursor
func (c *Cursor) Next(params RunParam) (tree.Datums, error) {
	result := tree.Datums{}
	for {
		// check cancel status
		if errCancel := params.GetCtx().Err(); errCancel != nil {
			return result, errCancel
		}
		row, meta := c.execHelper.NextRow()
		if row != nil || meta != nil {
			if meta != nil {
				continue
			}

			for i := range row {
				if i < len(c.resultCols) {
					if err := row[i].EnsureDecoded(c.resultCols[i].Typ, c.execHelper.GetDatumAlloc()); err != nil {
						return result, err
					}
					result = append(result, row[i].Datum)
				}
			}

			return result, nil
		}
		break
	}

	// todo consider ConsumerClosed for source
	return result, nil
}

// Close that close resource
func (c *Cursor) Close() {
	c.Clear()
}

// Clear for Cursor
func (c *Cursor) Clear() {
	for {
		row, meta := c.execHelper.NextRow()
		if row != nil || meta != nil {
			continue
		}
		break
	}
	c.execHelper.RunClearUp()
	c.isOpen = false
}

// SetCursorIns creates a new local cursor
type SetCursorIns struct {
	name string
	cur  Cursor
}

// Close that close resource
func (ins *SetCursorIns) Close() {}

// Execute for SetCursorIns
func (ins *SetCursorIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	if ins.cur.typ != tree.Rows {
		return pgerror.Newf(pgcode.InvalidCursorState,
			"cursor %s StatementType %s only support Rows", ins.name, ins.cur.typ.String())
	}
	rCtx.AddCursor(ins.name, &CursorHelper{CursorPtr: &ins.cur})
	return nil
}

// OpenCursorIns saves open cursor name
type OpenCursorIns struct {
	name tree.Name
}

// Close that close resource
func (ins *OpenCursorIns) Close() {}

// Execute for OpenCursorIns
func (ins *OpenCursorIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		c := v.CursorPtr
		if c.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is already open", ins.name)
		}
		return c.Start(params, rCtx)
	}
	return nil
}

// FetchCursorIns creates a fetch cursor ins
type FetchCursorIns struct {
	name      tree.Name
	dstVarIdx []int
}

// Close that close resource
func (ins *FetchCursorIns) Close() {}

// Execute for SetCursorIns
func (ins *FetchCursorIns) Execute(params RunParam, rCtx *SpExecContext) error {
	// check cancel status
	if errCancel := params.GetCtx().Err(); errCancel != nil {
		return errCancel
	}
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		cursor := v.CursorPtr
		if !cursor.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is not open", ins.name)
		}
		if len(cursor.resultCols) != len(ins.dstVarIdx) {
			return pgerror.Newf(pgcode.InvalidCursorState, "the number of columns fetched does not match the number of variables, cols: %v, variables:%v",
				len(cursor.resultCols), len(ins.dstVarIdx))
		}
		res, err := cursor.Next(params)
		if err != nil {
			// check cancel status
			if errCancel := params.GetCtx().Err(); errCancel != nil {
				return errCancel
			}
			contiuneIsExec, exitIsExec, errHandler := dealWithHandleByType(params, rCtx, tree.SQLEXCEPTION)
			if errHandler != nil {
				return errHandler
			}
			if contiuneIsExec && !exitIsExec {
				return nil
			}
			return err
		}

		// check next
		if len(res) == 0 {
			h := rCtx.getHandlerByMode(tree.ModeContinue)
			if h != nil && h.checkFlag(tree.NOTFOUND) {
				return h.Action.Execute(params, rCtx)
			}
			return pgerror.Newf(pgcode.InvalidCursorState, "the fetch cursor has no more data.")
		}
		// get next value into value
		if err := rCtx.SetVariables(ins.dstVarIdx, res, cursor.resultCols); err != nil {
			return err
		}
	}
	return nil
}

// CloseCursorIns creates a close cursor ins
type CloseCursorIns struct {
	name tree.Name
}

// Close that close resource
func (ins *CloseCursorIns) Close() {}

// Execute for SetCursorIns
func (ins *CloseCursorIns) Execute(_ RunParam, rCtx *SpExecContext) error {
	if v := rCtx.FindCursor(string(ins.name)); v != nil {
		c := v.CursorPtr
		if !c.isOpen {
			return pgerror.Newf(pgcode.InvalidCursorState, "cursor %s is already close", ins.name)
		}
		c.Clear()
	}
	return nil
}
