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

package sql

import (
	"context"

	"gitee.com/kwbasedb/kwbase/pkg/kv"
	ddl_opts "gitee.com/kwbasedb/kwbase/pkg/sql/ddl_opts"
	"gitee.com/kwbasedb/kwbase/pkg/sql/mutations"
	"gitee.com/kwbasedb/kwbase/pkg/sql/row"
	"gitee.com/kwbasedb/kwbase/pkg/sql/rowcontainer"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// expressionCarrier handles visiting sub-expressions.
type expressionCarrier interface {
	// walkExprs explores all sub-expressions held by this object, if
	// any.
	walkExprs(func(desc string, index int, expr tree.TypedExpr))
}

// tableWriter handles writing kvs and forming table rows.
//
// Usage:
//
//	err := tw.init(txn, evalCtx)
//	// Handle err.
//	for {
//	   values := ...
//	   row, err := tw.row(values)
//	   // Handle err.
//	}
//	err := tw.finalize()
//	// Handle err.
type tableWriter interface {
	expressionCarrier

	// init provides the tableWriter with a Txn and optional monitor to write to
	// and returns an error if it was misconfigured.
	init(context.Context, *kv.Txn, *tree.EvalContext) error

	// row performs a sql row modification (tableInserter performs an insert,
	// etc). It batches up writes to the init'd txn and periodically sends them.
	// The passed Datums is not used after `row` returns.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. We use a separate argument here instead
	// of a Value field on the context because Value access in context.Context
	// is rather expensive and the tableWriter interface is used on the
	// inner loop of table accesses.
	row(context.Context, tree.Datums, bool /* traceKV */) error

	// finalize flushes out any remaining writes. It is called after all calls to
	// row.  It returns a slice of all Datums not yet returned by calls to `row`.
	// The traceKV parameter determines whether the individual K/V operations
	// should be logged to the context. See the comment above for why
	// this a separate parameter as opposed to a Value field on the context.
	finalize(ctx context.Context, traceKV bool) (*rowcontainer.RowContainer, error)

	// tableDesc returns the TableDescriptor for the table that the tableWriter
	// will modify.
	tableDesc() *sqlbase.ImmutableTableDescriptor

	// close frees all resources held by the tableWriter.
	close(context.Context)

	// desc returns a name suitable for describing the table writer in
	// the output of EXPLAIN.
	desc() string

	// enable auto commit in call to finalize().
	enableAutoCommit()

	// atBatchEnd is called at the end of each batch, just before
	// finalize/flush. It can utilize the current KV batch which is
	// still open at that point. It must not run the batch itself; that
	// task is left to tableWriter.finalize() or flushAndStartNewBatch()
	// below.
	atBatchEnd(context.Context, bool /* traceKV */) error

	// flushAndStartNewBatch is called at the end of each batch but the last.
	// This should flush the current batch.
	flushAndStartNewBatch(context.Context) error

	// curBatchSize returns an upper bound for the amount of KV work
	// needed for the current batch. This cannot reflect the actual KV
	// batch size because the actual KV batch will be constructed only
	// during the call to atBatchEnd().
	curBatchSize() int
}

// tableWriterBase is meant to be used to factor common code between
// the other tableWriters.
type tableWriterBase struct {
	// txn is the current KV transaction.
	txn *kv.Txn
	// is autoCommit turned on.
	autoCommit ddl_opts.AutoCommitOpt
	// b is the current batch.
	b *kv.Batch
	// batchSize is the current batch size (when known).
	batchSize int
	// maxBatchSize determines the maximum number of entries in the KV batch
	// for a mutation operation. By default, it will be set to 10k but can be
	// a different value in tests.
	maxBatchSize int
}

func (tb *tableWriterBase) init(txn *kv.Txn) {
	tb.txn = txn
	tb.b = txn.NewBatch()
	tb.maxBatchSize = mutations.MaxBatchSize()
}

// flushAndStartNewBatch shares the common flushAndStartNewBatch() code between
// tableWriters.
func (tb *tableWriterBase) flushAndStartNewBatch(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor,
) error {
	if err := tb.txn.Run(ctx, tb.b); err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	tb.b = tb.txn.NewBatch()
	tb.batchSize = 0
	return nil
}

// curBatchSize shares the common curBatchSize() code between tableWriters.
func (tb *tableWriterBase) curBatchSize() int { return tb.batchSize }

// finalize shares the common finalize() code between tableWriters.
func (tb *tableWriterBase) finalize(
	ctx context.Context, tableDesc *sqlbase.ImmutableTableDescriptor,
) (err error) {
	if tb.autoCommit == ddl_opts.AutoCommitEnabled &&
		// Also, we don't want to try to commit here if the deadline is expired.
		// If we bubble back up to SQL then maybe we can get a fresh deadline
		// before committing.
		!tb.txn.DeadlineLikelySufficient() {
		log.Event(ctx, "autocommit enabled")
		// An auto-txn can commit the transaction with the batch. This is an
		// optimization to avoid an extra round-trip to the transaction
		// coordinator.
		err = tb.txn.CommitInBatch(ctx, tb.b)
	} else {
		err = tb.txn.Run(ctx, tb.b)
	}

	if err != nil {
		return row.ConvertBatchError(ctx, tableDesc, tb.b)
	}
	return nil
}

func (tb *tableWriterBase) enableAutoCommit() {
	tb.autoCommit = ddl_opts.AutoCommitEnabled
}
