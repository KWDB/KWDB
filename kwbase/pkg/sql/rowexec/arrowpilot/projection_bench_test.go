// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package arrowpilot

import (
	"context"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/sql/rowexec"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// BenchmarkArrowProjection measures the throughput of the unified execution
// path for a projection (c = a + b):
//
//	rowexec EncDatumRows --(rowToArrowConverter)--> Arrow Record
//	Arrow Record         --(arrowProjection, arrow/compute "add")--> projected Record
//
// It is a micro-benchmark of the projection operator only; a query-level
// benchmark that wires this into a full SQL plan would additionally exercise
// the planner and the materializer-free row/column handoff.
func BenchmarkArrowProjection(b *testing.B) {
	ctx := context.Background()
	alloc := memory.NewGoAllocator()
	intTyp := types.Int
	mk := func(v tree.Datum) sqlbase.EncDatum { return sqlbase.DatumToEncDatum(intTyp, v) }

	const n = 1024
	rows := make(sqlbase.EncDatumRows, n)
	for i := 0; i < n; i++ {
		rows[i] = sqlbase.EncDatumRow{
			mk(tree.NewDInt(tree.DInt(i))),
			mk(tree.NewDInt(tree.DInt(i * 10))),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		conv := rowexec.NewRowToArrowConverter(alloc, []*types.T{intTyp, intTyp}, rows)
		conv.Init(ctx)
		rec, done, err := conv.Next(ctx)
		if err != nil || done || rec == nil {
			b.Fatalf("rowToArrow: done=%v err=%v rec=%v", done, err, rec)
		}
		src := rowexec.NewArrowRecordSource(alloc, rec)
		proj := rowexec.NewArrowProjection(alloc, src, []rowexec.ArrowProjectionSpec{
			{OutputName: "c", Func: "add", Args: []rowexec.ArrowArg{{ColName: "col0"}, {ColName: "col1"}}},
		})
		proj.Init(ctx)
		out, _, err := proj.Next(ctx)
		if err != nil {
			b.Fatalf("projection: %v", err)
		}
		out.Release()
	}
}
