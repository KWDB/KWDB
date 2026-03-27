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

package stats

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/encoding"
	"gitee.com/kwbasedb/kwbase/pkg/util/protoutil"
)

// encodeInt encodes an int64 value into a byte slice for the upper bound of a histogram.
func encodeInt(t *testing.T, val int64) []byte {
	datum := tree.NewDInt(tree.DInt(val))
	enc, err := sqlbase.EncodeTableKey(nil, datum, encoding.Ascending)
	if err != nil {
		t.Fatal(err)
	}
	return enc
}

// TestSetHistogram tests the SetHistogram method and converts HistogramData into JSONStatistic.
func TestSetHistogram(t *testing.T) {
	colType := types.Int
	h := &HistogramData{
		ColumnType: *colType,
		Buckets: []HistogramData_Bucket{
			{
				NumEq:         10,
				NumRange:      5,
				DistinctRange: 3.5,
				UpperBound:    encodeInt(t, 100),
			},
			{
				NumEq:         20,
				NumRange:      15,
				DistinctRange: 4.2,
				UpperBound:    encodeInt(t, 200),
			},
		},
	}

	js := &JSONStatistic{}
	err := js.SetHistogram(h)
	if err != nil {
		t.Fatal(err)
	}

	if js.HistogramColumnType != colType.SQLString() {
		t.Errorf("expected column type %s, got %s", colType.SQLString(), js.HistogramColumnType)
	}
	if len(js.HistogramBuckets) != len(h.Buckets) {
		t.Fatalf("expected %d buckets, got %d", len(h.Buckets), len(js.HistogramBuckets))
	}

	for i, b := range h.Buckets {
		jb := js.HistogramBuckets[i]
		if jb.NumEq != b.NumEq {
			t.Errorf("bucket %d: NumEq mismatch", i)
		}
		if jb.NumRange != b.NumRange {
			t.Errorf("bucket %d: NumRange mismatch", i)
		}
		if jb.DistinctRange != b.DistinctRange {
			t.Errorf("bucket %d: DistinctRange mismatch", i)
		}

		datum, _, err := sqlbase.DecodeTableKey(&sqlbase.DatumAlloc{}, colType, b.UpperBound, encoding.Ascending)
		if err != nil {
			t.Fatal(err)
		}
		expectedUpper := tree.AsStringWithFlags(datum, tree.FmtExport)
		if jb.UpperBound != expectedUpper {
			t.Errorf("bucket %d: UpperBound mismatch: got %s, expected %s", i, jb.UpperBound, expectedUpper)
		}
	}
}

// TestDecodeAndSetHistogram tests the DecodeAndSetHistogram method, restoring the histogram from serialized byte data.
func TestDecodeAndSetHistogram(t *testing.T) {
	colType := types.Int
	h := &HistogramData{
		ColumnType: *colType,
		Buckets: []HistogramData_Bucket{
			{
				NumEq:         10,
				NumRange:      5,
				DistinctRange: 3.5,
				UpperBound:    encodeInt(t, 100),
			},
		},
	}
	data, err := protoutil.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	datum := tree.NewDBytes(tree.DBytes(data))

	js := &JSONStatistic{}
	err = js.DecodeAndSetHistogram(datum)
	if err != nil {
		t.Fatal(err)
	}

	if js.HistogramColumnType != colType.SQLString() {
		t.Errorf("expected column type %s, got %s", colType.SQLString(), js.HistogramColumnType)
	}
	if len(js.HistogramBuckets) != len(h.Buckets) {
		t.Fatalf("expected %d buckets, got %d", len(h.Buckets), len(js.HistogramBuckets))
	}

	b := h.Buckets[0]
	jb := js.HistogramBuckets[0]
	if jb.NumEq != b.NumEq || jb.NumRange != b.NumRange || jb.DistinctRange != b.DistinctRange {
		t.Errorf("bucket mismatch")
	}

	datumUpper, _, err := sqlbase.DecodeTableKey(&sqlbase.DatumAlloc{}, colType, b.UpperBound, encoding.Ascending)
	if err != nil {
		t.Fatal(err)
	}
	expectedUpper := tree.AsStringWithFlags(datumUpper, tree.FmtExport)
	if jb.UpperBound != expectedUpper {
		t.Errorf("UpperBound mismatch: got %s, expected %s", jb.UpperBound, expectedUpper)
	}
}

// TestGetHistogram tests the GetHistogram method, converting JSONStatistic back to HistogramData.
func TestGetHistogram(t *testing.T) {
	colType := types.Int
	js := &JSONStatistic{
		HistogramColumnType: colType.SQLString(),
		HistogramBuckets: []JSONHistoBucket{
			{
				NumEq:         10,
				NumRange:      5,
				DistinctRange: 3.5,
				UpperBound:    "100",
			},
			{
				NumEq:         20,
				NumRange:      15,
				DistinctRange: 4.2,
				UpperBound:    "200",
			},
		},
	}

	evalCtx := tree.NewTestingEvalContext(nil)
	h, err := js.GetHistogram(evalCtx)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("expected histogram, got nil")
	}
	if !h.ColumnType.Equal(*colType) {
		t.Errorf("column type mismatch: expected %v, got %v", colType, h.ColumnType)
	}
	if len(h.Buckets) != len(js.HistogramBuckets) {
		t.Fatalf("bucket count mismatch: expected %d, got %d", len(js.HistogramBuckets), len(h.Buckets))
	}

	for i, b := range h.Buckets {
		jb := js.HistogramBuckets[i]
		if b.NumEq != jb.NumEq {
			t.Errorf("bucket %d: NumEq mismatch", i)
		}
		if b.NumRange != jb.NumRange {
			t.Errorf("bucket %d: NumRange mismatch", i)
		}
		if b.DistinctRange != jb.DistinctRange {
			t.Errorf("bucket %d: DistinctRange mismatch", i)
		}

		datum, _, err := sqlbase.DecodeTableKey(&sqlbase.DatumAlloc{}, colType, b.UpperBound, encoding.Ascending)
		if err != nil {
			t.Fatal(err)
		}
		upperStr := tree.AsStringWithFlags(datum, tree.FmtExport)
		if upperStr != jb.UpperBound {
			t.Errorf("bucket %d: UpperBound mismatch: got %s, expected %s", i, upperStr, jb.UpperBound)
		}
	}
}

// TestGetSortHistogram tests the GetSortHistogram method, handling sorted histograms.
func TestGetSortHistogram(t *testing.T) {
	colType := types.Int
	js := &JSONStatistic{
		HistogramColumnType: colType.SQLString(),
		SortHistogramBuckets: []JSONSortHistoBucket{
			{
				RowCount:          100,
				UnorderedRowCount: 50,
				OrderedEntities:   20.5,
				UnorderedEntities: 30.2,
				UpperBound:        "100",
			},
			{
				RowCount:          200,
				UnorderedRowCount: 100,
				OrderedEntities:   40.1,
				UnorderedEntities: 60.3,
				UpperBound:        "200",
			},
		},
	}

	evalCtx := tree.NewTestingEvalContext(nil)
	h, err := js.GetSortHistogram(evalCtx)
	if err != nil {
		t.Fatal(err)
	}
	if h == nil {
		t.Fatal("expected histogram, got nil")
	}
	if !h.ColumnType.Equal(*colType) {
		t.Errorf("column type mismatch: expected %v, got %v", colType, h.ColumnType)
	}
	if len(h.SortedBuckets) != len(js.SortHistogramBuckets) {
		t.Fatalf("bucket count mismatch: expected %d, got %d", len(js.SortHistogramBuckets), len(h.SortedBuckets))
	}

	for i, b := range h.SortedBuckets {
		jb := js.SortHistogramBuckets[i]
		if b.RowCount != jb.RowCount {
			t.Errorf("bucket %d: RowCount mismatch", i)
		}
		if b.UnorderedRowCount != jb.UnorderedRowCount {
			t.Errorf("bucket %d: UnorderedRowCount mismatch", i)
		}
		if b.OrderedEntities != jb.OrderedEntities {
			t.Errorf("bucket %d: OrderedEntities mismatch", i)
		}
		if b.UnorderedEntities != jb.UnorderedEntities {
			t.Errorf("bucket %d: UnorderedEntities mismatch", i)
		}

		datum, _, err := sqlbase.DecodeTableKey(&sqlbase.DatumAlloc{}, colType, b.UpperBound, encoding.Ascending)
		if err != nil {
			t.Fatal(err)
		}
		upperStr := tree.AsStringWithFlags(datum, tree.FmtExport)
		if upperStr != jb.UpperBound {
			t.Errorf("bucket %d: UpperBound mismatch: got %s, expected %s", i, upperStr, jb.UpperBound)
		}
	}
}

// TestDecodeAndSetHistogramNull tests the behaviour when DNull is passed in.
func TestDecodeAndSetHistogramNull(t *testing.T) {
	js := &JSONStatistic{}
	err := js.DecodeAndSetHistogram(tree.DNull)
	if err != nil {
		t.Fatal(err)
	}
	if js.HistogramColumnType != "" || len(js.HistogramBuckets) != 0 {
		t.Errorf("expected empty histogram, got %+v", js)
	}
}

// TestDecodeAndSetHistogramEmptyBytes tests the behaviour when an empty byte slice is passed in.
func TestDecodeAndSetHistogramEmptyBytes(t *testing.T) {
	js := &JSONStatistic{}
	datum := tree.NewDBytes(tree.DBytes(""))
	err := js.DecodeAndSetHistogram(datum)
	if err != nil {
		t.Fatal(err)
	}
	if js.HistogramColumnType != "" || len(js.HistogramBuckets) != 0 {
		t.Errorf("expected empty histogram, got %+v", js)
	}
}
