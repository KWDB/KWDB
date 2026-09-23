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

package ts

import (
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/ts/tspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/kr/pretty"
)

func TestComputeRollupFromData(t *testing.T) {
	defer leaktest.AfterTest(t)()

	for _, tc := range []struct {
		input    tspb.TimeSeriesData
		expected []roachpb.InternalTimeSeriesData
	}{
		{
			input: tsd("test.metric", "",
				tsdp(10, 200),
				tsdp(20, 300),
				tsdp(40, 400),
				tsdp(80, 400),
				tsdp(97, 400),
				tsdp(201, 41234),
				tsdp(249, 423),
				tsdp(424, 123),
				tsdp(425, 342),
				tsdp(426, 643),
				tsdp(427, 835),
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
				tsdp(1323, 999),
				tsdp(1348, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(0, 50, []tspb.TimeSeriesDatapoint{
					tsdp(10, 200),
					tsdp(20, 300),
					tsdp(40, 400),
					tsdp(80, 400),
					tsdp(97, 400),
					tsdp(201, 41234),
					tsdp(249, 423),
					tsdp(424, 123),
					tsdp(425, 342),
					tsdp(426, 643),
					tsdp(427, 835),
				}),
				makeInternalColumnData(1000, 50, []tspb.TimeSeriesDatapoint{
					tsdp(1023, 999),
					tsdp(1048, 888),
					tsdp(1123, 999),
					tsdp(1248, 888),
					tsdp(1323, 999),
					tsdp(1348, 888),
				}),
			},
		},
		{
			input: tsd("test.metric", "",
				tsdp(1023, 999),
				tsdp(1048, 888),
				tsdp(1123, 999),
				tsdp(1248, 888),
			),
			expected: []roachpb.InternalTimeSeriesData{
				makeInternalColumnData(1000, 50, []tspb.TimeSeriesDatapoint{
					tsdp(1023, 999),
					tsdp(1048, 888),
					tsdp(1123, 999),
					tsdp(1248, 888),
				}),
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			rollups := computeRollupsFromData(tc.input, 50)
			internal, err := rollups.toInternal(1000, 50)
			if err != nil {
				t.Fatal(err)
			}
			if a, e := internal, tc.expected; !reflect.DeepEqual(a, e) {
				for _, diff := range pretty.Diff(a, e) {
					t.Error(diff)
				}
			}
		})
	}
}
