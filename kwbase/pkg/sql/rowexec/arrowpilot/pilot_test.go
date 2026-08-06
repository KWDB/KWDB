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

// TestArrowProjectionPilot verifies the unification path end-to-end:
//   rowexec EncDatumRows  --(rowToArrowConverter)-->  Arrow Record
//   Arrow Record          --(arrowProjection, arrow/compute "add")-->  projected Record
func TestArrowProjectionPilot(t *testing.T) {
	ctx := context.Background()
	alloc := memory.NewGoAllocator()

	intTyp := types.Int
	mk := func(v tree.Datum) sqlbase.EncDatum { return sqlbase.DatumToEncDatum(intTyp, v) }

	rows := sqlbase.EncDatumRows{
		{mk(tree.NewDInt(tree.DInt(1))), mk(tree.NewDInt(tree.DInt(10)))},
		{mk(tree.NewDInt(tree.DInt(2))), mk(tree.NewDInt(tree.DInt(20)))},
		{mk(tree.NewDInt(tree.DInt(3))), mk(tree.NewDInt(tree.DInt(30)))},
		{mk(tree.NewDInt(tree.DInt(4))), mk(tree.NewDInt(tree.DInt(40)))},
	}

	// rowexec base: convert the row batch into a columnar Arrow Record.
	conv := rowexec.NewRowToArrowConverter(alloc, []*types.T{intTyp, intTyp}, rows)
	conv.Init(ctx)
	rec, done, err := conv.Next(ctx)
	if err != nil {
		t.Fatalf("rowToArrow: %v", err)
	}
	if done || rec == nil {
		t.Fatal("expected one record from converter")
	}
	if rec.NumRows() != int64(len(rows)) || rec.NumCols() != 2 {
		t.Fatalf("unexpected record shape rows=%d cols=%d", rec.NumRows(), rec.NumCols())
	}

	// Feed the same Record (still owned by this test) into a UnifiedProcessor
	// source and project c = a + b. The projection operator releases it.
	src := rowexec.NewArrowRecordSource(alloc, rec)
	proj := rowexec.NewArrowProjection(alloc, src, []rowexec.ArrowProjectionSpec{
		{OutputName: "c", Func: "add", Args: []rowexec.ArrowArg{{ColName: "col0"}, {ColName: "col1"}}},
	}, nil)
	proj.Init(ctx)
	vals, err := rowexec.ArrowProjectionResultInt64(proj, ctx)
	if err != nil {
		t.Fatalf("projection: %v", err)
	}
	want := []int64{11, 22, 33, 44}
	if len(vals) != len(want) {
		t.Fatalf("expected %d rows, got %d", len(want), len(vals))
	}
	for i, w := range want {
		if vals[i] != w {
			t.Fatalf("row %d: want %d, got %d", i, w, vals[i])
		}
	}
}
