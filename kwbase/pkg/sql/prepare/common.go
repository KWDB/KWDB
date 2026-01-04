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

package prepare

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/sql/parser"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// PreparedHelper records all prepare functions
type PreparedHelper struct {
	Exec  PreparedFn
	Check PreparedCheckFn
	Add   PreparedAddFn
}

// PreparedResult flags prepare PreparedStatement
type PreparedResult interface {
}

// MemoResult flags prepared memo
type MemoResult interface {
}

// PreparedFn compiles sql to generate a PreparedStatement
type PreparedFn func(
	ctx context.Context,
	stmtAST parser.Statement,
	placeholderHints tree.PlaceholderTypes,
	origin int,
	procUserDefinedVars map[string]tree.ProcUdvInfo,
	insidePrepareOfProcFlag uint8,
) (PreparedResult, error)

// PreparedCheckFn checks prepare and gets placeholder types
type PreparedCheckFn func(s *tree.Prepare, numPlaceholders int) (tree.PlaceholderTypes, error)

// PreparedAddFn adds mem for prepare mem acc and saves prepare memo to map
type PreparedAddFn func(
	ctx context.Context, prepareResult PreparedResult, name string,
) error

// ExecuteHelper records all execute functions
type ExecuteHelper struct {
	Check           ExecuteCheckFn
	GetPlaceholder  ExecuteGetPlaceholderFn
	GetReplacedMemo ExecuteGetMemoFn
	GetStatement    ExecuteGetStatementFn
}

// ExecuteCheckFn checks whether the object exists
type ExecuteCheckFn func(s *tree.Execute) (PreparedResult, error)

// ExecuteGetPlaceholderFn gets placeholder
type ExecuteGetPlaceholderFn func(psInterface PreparedResult, name string, params tree.Exprs, semaCtx *tree.SemaContext) (*tree.PlaceholderInfo, error)

// ExecuteGetMemoFn gets the memo with the placeholder replaced
type ExecuteGetMemoFn func(ctx context.Context, s *tree.Execute, plInfo *tree.PlaceholderInfo) (MemoResult, error)

// ExecuteGetStatementFn get statement type of AST
type ExecuteGetStatementFn func(ps PreparedResult) (int, string)

// DeallocateHelper records all deallocate functions
type DeallocateHelper struct {
	Check DeallocateCheckFn
}

// DeallocateCheckFn checks whether the object exists
type DeallocateCheckFn func(s string) bool
