// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT.
// See the Mulan PSL v2 for more details.

package execbuilder

import (
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func tsPrimaryTagGroupingKey(vals ...[]byte) string {
	key := make([]byte, 0, 16)
	for _, val := range vals {
		key = AppendTSPrimaryTagGroupingKeyBytes(key, val)
	}
	return string(key)
}

func TestTSPrimaryTagGroupingKeyAvoidsBoundaryAmbiguity(t *testing.T) {
	defer leaktest.AfterTest(t)()

	if tsPrimaryTagGroupingKey([]byte("a"), []byte("bc")) ==
		tsPrimaryTagGroupingKey([]byte("ab"), []byte("c")) {
		t.Fatal("expected different keys for ambiguous concatenations")
	}
	if tsPrimaryTagGroupingKey([]byte(""), []byte(":")) ==
		tsPrimaryTagGroupingKey([]byte(":"), []byte("")) {
		t.Fatal("expected empty string and separator-like bytes to keep tuple boundaries")
	}
	if tsPrimaryTagGroupingKey([]byte{'a', 0}, []byte("b")) ==
		tsPrimaryTagGroupingKey([]byte("a"), []byte{0, 'b'}) {
		t.Fatal("expected NUL-containing values to keep tuple boundaries")
	}
	if tsPrimaryTagGroupingKey([]byte{0xff}, []byte{0x00, 0x7f}) ==
		tsPrimaryTagGroupingKey([]byte{0xff, 0x00}, []byte{0x7f}) {
		t.Fatal("expected binary values to keep tuple boundaries")
	}

	key := tsPrimaryTagGroupingKey([]byte("same"), []byte("tuple"))
	if key != tsPrimaryTagGroupingKey([]byte("same"), []byte("tuple")) {
		t.Fatal("expected stable key for identical tuples")
	}

	stringKey := AppendTSPrimaryTagGroupingKeyString(nil, "same")
	stringKey = AppendTSPrimaryTagGroupingKeyString(stringKey, "tuple")
	if string(stringKey) != key {
		t.Fatal("expected string and byte helpers to use the same encoding")
	}
}

func makeTSPrimaryTagGroupingTestColumns() (pTag1, pTag2, tsCol *sqlbase.ColumnDescriptor) {
	pTagTyp := types.MakeVarChar(8, 0)
	pTag1 = &sqlbase.ColumnDescriptor{
		ID:   1,
		Name: "ptag1",
		Type: *pTagTyp,
		TsCol: sqlbase.TSCol{
			ColumnType:         sqlbase.ColumnType_TYPE_PTAG,
			StorageLen:         8,
			VariableLengthType: sqlbase.StorageTuple,
		},
	}
	pTag2 = &sqlbase.ColumnDescriptor{
		ID:   2,
		Name: "ptag2",
		Type: *pTagTyp,
		TsCol: sqlbase.TSCol{
			ColumnType:         sqlbase.ColumnType_TYPE_PTAG,
			StorageLen:         8,
			VariableLengthType: sqlbase.StorageTuple,
		},
	}
	tsCol = &sqlbase.ColumnDescriptor{
		ID:   3,
		Name: "ts",
		Type: *types.Timestamp,
		TsCol: sqlbase.TSCol{
			ColumnType: sqlbase.ColumnType_TYPE_DATA,
			StorageLen: 8,
		},
	}
	return pTag1, pTag2, tsCol
}

func makeTSPrimaryTagGroupingPayloadArgs(
	t *testing.T,
) (PayloadArgs, []*sqlbase.ColumnDescriptor, map[int]int) {
	t.Helper()
	pTag1, pTag2, tsCol := makeTSPrimaryTagGroupingTestColumns()
	dataCols := []*sqlbase.ColumnDescriptor{tsCol}
	pArgs, err := BuildPayloadArgs(
		1,
		&sqlbase.TSIDGenerator{},
		[]*sqlbase.ColumnDescriptor{pTag1, pTag2},
		nil,
		dataCols,
	)
	if err != nil {
		t.Fatal(err)
	}
	v2Info := BuildPayloadV2TupleDataInfo(dataCols)
	pArgs.V2DataHeader = v2Info.DataHeader
	pArgs.V2DataCols = v2Info.DataCols
	return pArgs, dataCols, map[int]int{1: 0, 2: 1, 3: 2}
}

func tsPrimaryTagGroupingPayloadRowCounts(
	payloadNodeMap map[int]*sqlbase.PayloadForDistTSInsert, nodeID int,
) []int {
	payloads := payloadNodeMap[nodeID].PerNodePayloads
	counts := make([]int, 0, len(payloads))
	for _, payload := range payloads {
		counts = append(counts, int(payload.RowNum))
	}
	sort.Ints(counts)
	return counts
}

func TestBuildRowBytesForTsInsertPrimaryTagGroupingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pArgs, dataCols, colIndexs := makeTSPrimaryTagGroupingPayloadArgs(t)
	inputRows := opt.RowsValue{
		{tree.NewDString("a"), tree.NewDString("bc"), tree.NewDInt(1000)},
		{tree.NewDString("ab"), tree.NewDString("c"), tree.NewDInt(2000)},
	}
	inputDatums := make([]tree.Datums, len(inputRows))
	for i := range inputDatums {
		inputDatums[i] = make(tree.Datums, len(inputRows[i]))
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())

	payloadNodeMap, err := BuildRowBytesForTsInsert(
		&evalCtx,
		inputRows,
		inputDatums,
		dataCols,
		colIndexs,
		pArgs,
		1,
		1,
		16,
	)
	if err != nil {
		t.Fatal(err)
	}
	counts := tsPrimaryTagGroupingPayloadRowCounts(payloadNodeMap, int(evalCtx.NodeID))
	if len(counts) != 2 || counts[0] != 1 || counts[1] != 1 {
		t.Fatalf("expected two one-row primary tag groups, got %v", counts)
	}
}

func TestBuildRowBytesForTsImportPrimaryTagGroupingKey(t *testing.T) {
	defer leaktest.AfterTest(t)()

	pArgs, _, colIndexs := makeTSPrimaryTagGroupingPayloadArgs(t)
	inputDatums := []tree.Datums{
		{tree.NewDString("a"), tree.NewDString("bc"), tree.NewDInt(1000)},
		{tree.NewDString("a"), tree.NewDString("bc"), tree.NewDInt(2000)},
		{tree.NewDString("ab"), tree.NewDString("c"), tree.NewDInt(3000)},
	}
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	tsPayload := NewTsPayload()

	payloadNodeMap, _, err := tsPayload.BuildRowBytesForTsImport(
		&evalCtx,
		evalCtx.Txn,
		inputDatums,
		len(inputDatums),
		pArgs.PrettyCols,
		colIndexs,
		pArgs,
		1,
		1,
		16,
		false,
	)
	if err != nil {
		t.Fatal(err)
	}
	counts := tsPrimaryTagGroupingPayloadRowCounts(payloadNodeMap, int(evalCtx.NodeID))
	if len(counts) != 2 || counts[0] != 1 || counts[1] != 2 {
		t.Fatalf("expected one two-row group and one one-row group, got %v", counts)
	}
}
