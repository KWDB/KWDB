// Copyright 2017 The Cockroach Authors.
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

package stats

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/settings/cluster"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util"
	"gitee.com/kwbasedb/kwbase/pkg/util/randutil"
)

// runSampleTest feeds rows with the given ranks through a reservoir
// of a given size and verifies the results are correct.
func runSampleTest(t *testing.T, evalCtx *tree.EvalContext, numSamples int, ranks []int) {
	ctx := context.Background()
	var sr SampleReservoir
	sr.Init(numSamples, []types.T{*types.Int}, nil /* memAcc */, util.MakeFastIntSet(0))
	for _, r := range ranks {
		d := sqlbase.DatumToEncDatum(types.Int, tree.NewDInt(tree.DInt(r)))
		if err := sr.SampleRow(ctx, evalCtx, sqlbase.EncDatumRow{d}, uint64(r)); err != nil {
			t.Errorf("%v", err)
		}
	}
	samples := sr.Get()
	sampledRanks := make([]int, len(samples))

	// Verify that the row and the ranks weren't mishandled.
	for i, s := range samples {
		if *s.Row[0].Datum.(*tree.DInt) != tree.DInt(s.Rank) {
			t.Fatalf(
				"mismatch between row %s and rank %d",
				s.Row.String([]types.T{*types.Int}), s.Rank,
			)
		}
		sampledRanks[i] = int(s.Rank)
	}

	// Verify the top ranks made it.
	sort.Ints(sampledRanks)
	expected := append([]int(nil), ranks...)
	sort.Ints(expected)
	if len(expected) > numSamples {
		expected = expected[:numSamples]
	}
	if !reflect.DeepEqual(expected, sampledRanks) {
		t.Errorf("invalid ranks: %v vs %v", sampledRanks, expected)
	}
}

func TestSampleReservoir(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	for _, n := range []int{10, 100, 1000, 10000} {
		rng, _ := randutil.NewPseudoRand()
		ranks := make([]int, n)
		for i := range ranks {
			ranks[i] = rng.Int()
		}
		for _, k := range []int{1, 5, 10, 100} {
			t.Run(fmt.Sprintf("%d/%d", n, k), func(t *testing.T) {
				runSampleTest(t, &evalCtx, k, ranks)
			})
		}
	}
}

func TestTruncateDatum(t *testing.T) {
	evalCtx := tree.MakeTestingEvalContext(cluster.MakeTestingClusterSettings())
	runTest := func(d, expected tree.Datum) {
		actual := truncateDatum(&evalCtx, d, 10 /* maxBytes */)
		if actual.Compare(&evalCtx, expected) != 0 {
			t.Fatalf("expected %s but found %s", expected.String(), actual.String())
		}
	}

	original1, err := tree.ParseDBitArray("0110110101111100001100110110101111100001100110110101111" +
		"10000110011011010111110000110011011010111110000110011011010111110000110")
	if err != nil {
		t.Fatal(err)
	}
	expected1, err := tree.ParseDBitArray("0110110101111100001100110110101111100001100110110101111" +
		"1000011001101101011111000")
	if err != nil {
		t.Fatal(err)
	}
	runTest(original1, expected1)

	original2 := tree.DBytes("deadbeef1234567890")
	expected2 := tree.DBytes("deadbeef12")
	runTest(&original2, &expected2)

	original3 := tree.DString("Hello 世界")
	expected3 := tree.DString("Hello 世")
	runTest(&original3, &expected3)

	original4, err := tree.NewDCollatedString(`IT was lovely summer weather in the country, and the golden
corn, the green oats, and the haystacks piled up in the meadows looked beautiful`,
		"en_US", &tree.CollationEnvironment{})
	if err != nil {
		t.Fatal(err)
	}
	expected4, err := tree.NewDCollatedString("IT was lov", "en_US", &tree.CollationEnvironment{})
	if err != nil {
		t.Fatal(err)
	}
	runTest(original4, expected4)
}
