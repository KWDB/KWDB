// Copyright (c) 2024-present, Shanghai Yunxi Technology Co, Ltd.
//
// Licensed under the Mulan PSL v2. See the Mulan PSL v2 for details.

package colexec

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"gitee.com/kwbasedb/kwbase/pkg/col/coldata"
	"gitee.com/kwbasedb/kwbase/pkg/col/coltypes"
)

// TestArrowBridgeRoundTrip verifies that a colexec columnar Batch can be
// converted into an Arrow Record and back without loss of data, for every
// type the bridge supports. This guards the BatchToRecord / RecordToBatch
// bridge used to mix the vectorized colexec path with Arrow-based operators.
func TestArrowBridgeRoundTrip(t *testing.T) {
	alloc := memory.NewGoAllocator()
	const n = 5
	typs := []coltypes.T{coltypes.Int64, coltypes.Float64, coltypes.Bool, coltypes.Bytes}
	b := coldata.NewMemBatchWithSize(typs, n)
	b.SetLength(n)

	intSlice := make([]int64, n)
	for i := range intSlice {
		intSlice[i] = int64(i*2 - 1)
	}
	b.ColVec(0).SetCol(intSlice)

	fltSlice := make([]float64, n)
	for i := range fltSlice {
		fltSlice[i] = float64(i) * 1.5
	}
	b.ColVec(1).SetCol(fltSlice)

	boolSlice := make([]bool, n)
	for i := range boolSlice {
		boolSlice[i] = i%2 == 0
	}
	b.ColVec(2).SetCol(boolSlice)

	bs := b.ColVec(3).Bytes()
	wantBytes := make([][]byte, n)
	for i := 0; i < n; i++ {
		wantBytes[i] = []byte(fmt.Sprintf("v%d", i))
		bs.Set(i, wantBytes[i])
	}

	rec, err := BatchToRecord(b, alloc)
	if err != nil {
		t.Fatalf("BatchToRecord: %v", err)
	}
	defer rec.Release()

	if rec.NumRows() != int64(n) || rec.NumCols() != int64(len(typs)) {
		t.Fatalf("record shape mismatch: rows=%d cols=%d", rec.NumRows(), rec.NumCols())
	}

	b2, err := RecordToBatch(rec)
	if err != nil {
		t.Fatalf("RecordToBatch: %v", err)
	}

	gotInt := b2.ColVec(0).Int64()
	for i := 0; i < n; i++ {
		if gotInt[i] != intSlice[i] {
			t.Fatalf("int col row %d: want %d got %d", i, intSlice[i], gotInt[i])
		}
	}
	gotFlt := b2.ColVec(1).Float64()
	for i := 0; i < n; i++ {
		if gotFlt[i] != fltSlice[i] {
			t.Fatalf("float col row %d: want %v got %v", i, fltSlice[i], gotFlt[i])
		}
	}
	gotBool := b2.ColVec(2).Bool()
	for i := 0; i < n; i++ {
		if gotBool[i] != boolSlice[i] {
			t.Fatalf("bool col row %d: want %v got %v", i, boolSlice[i], gotBool[i])
		}
	}
	gotBs := b2.ColVec(3).Bytes()
	for i := 0; i < n; i++ {
		if string(gotBs.Get(i)) != string(wantBytes[i]) {
			t.Fatalf("bytes col row %d: want %s got %s", i, wantBytes[i], gotBs.Get(i))
		}
	}
}

// BenchmarkArrowBridgeRoundTrip measures the cost of the colexec <-> Arrow
// bridge (BatchToRecord then RecordToBatch) for a small fixed-width batch. It
// is a micro-benchmark of the conversion overhead that the unified execution
// path pays when mixing the two execution models in one DAG.
func BenchmarkArrowBridgeRoundTrip(b *testing.B) {
	alloc := memory.NewGoAllocator()
	const n = 1024
	typs := []coltypes.T{coltypes.Int64, coltypes.Float64, coltypes.Bool}
	batch := coldata.NewMemBatchWithSize(typs, n)
	batch.SetLength(n)
	intSl := make([]int64, n)
	fltSl := make([]float64, n)
	boolSl := make([]bool, n)
	for i := 0; i < n; i++ {
		intSl[i] = int64(i)
		fltSl[i] = float64(i)
		boolSl[i] = i%2 == 0
	}
	batch.ColVec(0).SetCol(intSl)
	batch.ColVec(1).SetCol(fltSl)
	batch.ColVec(2).SetCol(boolSl)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec, err := BatchToRecord(batch, alloc)
		if err != nil {
			b.Fatal(err)
		}
		if _, err := RecordToBatch(rec); err != nil {
			b.Fatal(err)
		}
		rec.Release()
	}
}
