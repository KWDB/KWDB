// Copyright 2018 The Cockroach Authors.
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

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
)

// spoolNode ensures that a child planNode is executed to completion
// during the start phase. The results, if any, are collected. The
// child node is guaranteed to run to completion.
// If hardLimit is set, only that number of rows is collected, but
// the child node is still run to completion.
type spoolNode struct {
	source    planNode
	rows      *rowcontainer.RowContainer
	hardLimit int64
	curRowIdx int
}

func (s *spoolNode) startExec(params runParams) error {
	// If FastPathResults() on the source indicates that the results are
	// already available (2nd value true), then the computation is
	// already done at start time and spooling is unnecessary.
	if f, ok := s.source.(planNodeFastPath); ok {
		_, done := f.FastPathResults()
		if done {
			return nil
		}
	}

	s.rows = rowcontainer.NewRowContainer(
		params.EvalContext().Mon.MakeBoundAccount(),
		sqlbase.ColTypeInfoFromResCols(planColumns(s.source)),
		0, /* rowCapacity */
	)

	// Accumulate all the rows up to the hardLimit, if any.
	// This also guarantees execution of the child node to completion,
	// even if Next() on the spool itself is not called for every row.
	for {
		next, err := s.source.Next(params)
		if err != nil {
			return err
		}
		if !next {
			break
		}
		if s.hardLimit == 0 || int64(s.rows.Len()) < s.hardLimit {
			if _, err := s.rows.AddRow(params.ctx, s.source.Values()); err != nil {
				return err
			}
		}
	}
	s.curRowIdx = -1
	return nil
}

// FastPathResults implements the planNodeFastPath interface.
func (s *spoolNode) FastPathResults() (int, bool) {
	// If the source implements the fast path interface, let it report
	// its status through. This lets e.g. the fast path of a DELETE or
	// an UPSERT report that they have finished its computing already,
	// so the calls to Next() on the spool itself can also be elided.
	// If FastPathResults() on the source says the fast path is unavailable,
	// then startExec() on the spool will also notice that and
	// spooling will occur as expected.
	if f, ok := s.source.(planNodeFastPath); ok {
		return f.FastPathResults()
	}
	return 0, false
}

// spooled implements the planNodeSpooled interface.
func (s *spoolNode) spooled() {}

// Next is part of the planNode interface.
func (s *spoolNode) Next(params runParams) (bool, error) {
	s.curRowIdx++
	return s.curRowIdx < s.rows.Len(), nil
}

// Values is part of the planNode interface.
func (s *spoolNode) Values() tree.Datums {
	return s.rows.At(s.curRowIdx)
}

// Close is part of the planNode interface.
func (s *spoolNode) Close(ctx context.Context) {
	s.source.Close(ctx)
	if s.rows != nil {
		s.rows.Close(ctx)
		s.rows = nil
	}
}
