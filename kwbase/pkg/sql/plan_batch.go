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

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

// batchedPlanNode is an interface that complements planNode to
// indicate that the local execution behavior operates in batches.
// The word "complement" here contrasts with "specializes" as follows:
//
//   - batchedPlanNode specializes planNode for the purpose of logical
//     planning: a node implementing batchedPlanNode behaves in all
//     respects like a planNode from the perspective of the various
//     logical planning transforms.
//
//   - batchedPlanNode *replaces* planNode for the purpose of local
//     execution.
type batchedPlanNode interface {
	// batchedPlanNode specializes planNode for the purpose of the recursions
	// on planNode trees performed during logical planning, so it should "inherit"
	// planNode. However this interface inheritance does not imply that
	// batchedPlanNode *specializes* planNode in all respects; as described
	// in the comment above, it only specializes it for logical planning,
	// and *replaces* it for the semantics of local execution.
	//
	// In particular, nodes implementing batchedPlanNode do not have valid
	// Next() and Values() methods.
	//
	// TODO(knz/andrei): nodes that implement this interface cannot
	// properly implement planNode's Next() and Values() in the way
	// required defined by planNode. This violates the principle that no
	// implementer of a derived interface can change any contract of the
	// base interfaces - or at least not in ways that can break
	// unsuspecting clients of the interface.
	// To fix this wart requires splitting planNode into a planNodeBase
	// interface, which only supports, say, Close(), and then two
	// interfaces that extend planNodeBase; namely serializeNode
	// providing Next/Values and this new interface batchedPlanNode
	// which provides BatchedNext/BatchedCount/BatchedValues.
	// See issue https://gitee.com/kwbasedb/kwbase/issues/23522.
	planNode

	// BatchedNext() performs one batch of work, returning false
	// if an error is encountered or if there is no more work to do.
	// After BatchedNext() returns, BatchedCount() and BatchedValues()
	// provide access to the rows in the last processed batch.
	//
	// Note: Nodes that perform writes (e.g. INSERT) will not return
	// from BatchedNext() before checking foreign key, uniqueness, and
	// other CHECK constraints.
	BatchedNext(params runParams) (bool, error)

	// BatchedCount() returns the number of rows processed in the last
	// processed batch.
	BatchedCount() int

	// BatchedValues exposes one of the rows in the last processed
	// batch, in the range 0 to BatchedCount() exclusive.
	BatchedValues(rowIdx int) tree.Datums
}

var _ batchedPlanNode = &deleteNode{}
var _ batchedPlanNode = &updateNode{}

// serializeNode serializes the results of a batchedPlanNode into a
// plain planNode interface. In other words, it wraps around
// batchedPlanNode's BatchedNext() method which advances full batches
// to provide a Next() method that advances row-by-row.
//
// The FastPathResults behavior of the source plan, if any, is also
// preserved.
type serializeNode struct {
	source batchedPlanNode

	// fastPath is set to true during startExec if the source plan
	// was able to use the fast path and provide a row count.
	fastPath bool

	// rowCount is set either to the total row count if fastPath is true,
	// or to the row count of the current batch otherwise.
	rowCount int

	// rowIdx is the index of the current row in the current batch.
	rowIdx int
}

func (s *serializeNode) startExec(params runParams) error {
	if f, ok := s.source.(planNodeFastPath); ok {
		s.rowCount, s.fastPath = f.FastPathResults()
	}
	return nil
}

func (s *serializeNode) Next(params runParams) (bool, error) {
	if s.fastPath {
		return false, nil
	}
	if s.rowIdx+1 >= s.rowCount {
		// First batch, or finished previous batch; advance one.
		if next, err := s.source.BatchedNext(params); !next {
			return false, err
		}
		s.rowCount = s.source.BatchedCount()
		s.rowIdx = 0
	} else {
		// Advance one position in the current batch.
		s.rowIdx++
	}
	return s.rowCount > 0, nil
}

func (s *serializeNode) Values() tree.Datums       { return s.source.BatchedValues(s.rowIdx) }
func (s *serializeNode) Close(ctx context.Context) { s.source.Close(ctx) }

// FastPathResults implements the planNodeFastPath interface.
func (s *serializeNode) FastPathResults() (int, bool) {
	return s.rowCount, s.fastPath
}

// requireSpool implements the planNodeRequireSpool interface.
func (s *serializeNode) requireSpool() {}

// rowCountNode serializes the results of a batchedPlanNode into a
// plain planNode interface that has guaranteed FastPathResults
// behavior and no result columns (i.e. just the count of rows
// affected).
// All the batches are consumed in startExec().
//
// This is an optimization upon serializeNode when it is known in
// advance that the result rows will be discarded (for example, a
// data-modifying statement with no RETURNING clause or RETURNING
// NOTHING). In that case, we do not need to have individual calls to
// Next() consume the batched rows individually and instead quickly
// accumulate the batch counts themselves.
type rowCountNode struct {
	source   batchedPlanNode
	rowCount int
}

func (r *rowCountNode) startExec(params runParams) error {
	defer func() {
		params.extendedEvalCtx.SessionData.RowCount = r.rowCount
	}()
	done := false
	if f, ok := r.source.(planNodeFastPath); ok {
		r.rowCount, done = f.FastPathResults()
	}
	if !done {
		for {
			if next, err := r.source.BatchedNext(params); !next {
				return err
			}
			r.rowCount += r.source.BatchedCount()
		}
	}
	return nil
}

func (r *rowCountNode) Next(params runParams) (bool, error) { return false, nil }
func (r *rowCountNode) Values() tree.Datums                 { return nil }
func (r *rowCountNode) Close(ctx context.Context)           { r.source.Close(ctx) }

// FastPathResults implements the planNodeFastPath interface.
func (r *rowCountNode) FastPathResults() (int, bool) { return r.rowCount, true }
