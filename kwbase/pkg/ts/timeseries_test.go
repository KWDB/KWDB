// Copyright 2015 The Cockroach Authors.
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
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/roachpb"
	"gitee.com/kwbasedb/kwbase/pkg/storage"
	"gitee.com/kwbasedb/kwbase/pkg/testutils"
	"gitee.com/kwbasedb/kwbase/pkg/ts/tspb"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func tsd(name, source string, dps ...tspb.TimeSeriesDatapoint) tspb.TimeSeriesData {
	return tspb.TimeSeriesData{
		Name:       name,
		Source:     source,
		Datapoints: dps,
	}
}

func tsdp(ts time.Duration, val float64) tspb.TimeSeriesDatapoint {
	return tspb.TimeSeriesDatapoint{
		TimestampNanos: ts.Nanoseconds(),
		Value:          val,
	}
}

// TestToInternal verifies the conversion of tspb.TimeSeriesData to internal storage
// format is correct.
func TestToInternal(t *testing.T) {
	defer leaktest.AfterTest(t)()
	tcases := []struct {
		keyDuration    int64
		sampleDuration int64
		columnar       bool
		expectedError  string
		input          tspb.TimeSeriesData
		expected       []roachpb.InternalTimeSeriesData
	}{
		{
			time.Minute.Nanoseconds(),
			101,
			false,
			"does not evenly divide key duration",
			tsd("error.series", ""),
			nil,
		},
		{
			time.Minute.Nanoseconds(),
			time.Hour.Nanoseconds(),
			false,
			"does not evenly divide key duration",
			tsd("error.series", ""),
			nil,
		},
		{
			(24 * time.Hour).Nanoseconds(),
			(20 * time.Minute).Nanoseconds(),
			false,
			"",
			tsd("test.series", "",
				tsdp(5*time.Hour+5*time.Minute, 1.0),
				tsdp(24*time.Hour+39*time.Minute, 2.0),
				tsdp(10*time.Hour+10*time.Minute, 3.0),
				tsdp(48*time.Hour, 4.0),
				tsdp(15*time.Hour+22*time.Minute+1, 5.0),
				tsdp(52*time.Hour+15*time.Minute, 0.0),
			),
			[]roachpb.InternalTimeSeriesData{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 15,
							Count:  1,
							Sum:    1.0,
						},
						{
							Offset: 30,
							Count:  1,
							Sum:    3.0,
						},
						{
							Offset: 46,
							Count:  1,
							Sum:    5.0,
						},
					},
				},
				{
					StartTimestampNanos: 24 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 1,
							Count:  1,
							Sum:    2.0,
						},
					},
				},
				{
					StartTimestampNanos: 48 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Samples: []roachpb.InternalTimeSeriesSample{
						{
							Offset: 0,
							Count:  1,
							Sum:    4.0,
						},
						{
							Offset: 12,
							Count:  1,
							Sum:    0.0,
						},
					},
				},
			},
		},
		{
			(24 * time.Hour).Nanoseconds(),
			(20 * time.Minute).Nanoseconds(),
			true,
			"",
			tsd("test.series", "",
				tsdp(5*time.Hour+5*time.Minute, 1.0),
				tsdp(24*time.Hour+39*time.Minute, 2.0),
				tsdp(10*time.Hour+10*time.Minute, 3.0),
				tsdp(48*time.Hour, 4.0),
				tsdp(15*time.Hour+22*time.Minute+1, 5.0),
				tsdp(52*time.Hour+15*time.Minute, 0.0),
			),
			[]roachpb.InternalTimeSeriesData{
				{
					StartTimestampNanos: 0,
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Offset:              []int32{15, 30, 46},
					Last:                []float64{1.0, 3.0, 5.0},
				},
				{
					StartTimestampNanos: 24 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Offset:              []int32{1},
					Last:                []float64{2.0},
				},
				{
					StartTimestampNanos: 48 * time.Hour.Nanoseconds(),
					SampleDurationNanos: 20 * time.Minute.Nanoseconds(),
					Offset:              []int32{0, 12},
					Last:                []float64{4.0, 0.0},
				},
			},
		},
	}

	for i, tc := range tcases {
		actual, err := tc.input.ToInternal(tc.keyDuration, tc.sampleDuration, tc.columnar)
		if !testutils.IsError(err, tc.expectedError) {
			t.Errorf("expected error %q from case %d, got %v", tc.expectedError, i, err)
		}

		if !reflect.DeepEqual(actual, tc.expected) {
			t.Errorf("case %d fails: ToInternal result was %v, expected %v", i, actual, tc.expected)
		}
	}
}

// TestDiscardEarlierSamples verifies that only a single sample is kept for each
// sample period; earlier samples are discarded.
func TestDiscardEarlierSamples(t *testing.T) {
	defer leaktest.AfterTest(t)()
	// TODO(ajkr): this should also run on Pebble. Maybe combine it into the merge_test.go tests or prove
	// it is redundant with the tests there.
	ts := tsd("test.series", "",
		tsdp(5*time.Hour+5*time.Minute, -1.0),
		tsdp(5*time.Hour+5*time.Minute, -2.0),
	)
	internal, err := ts.ToInternal(Resolution10s.SlabDuration(), Resolution10s.SampleDuration(), false)
	if err != nil {
		t.Fatal(err)
	}

	out, err := storage.MergeInternalTimeSeriesData(true /* mergeIntoNil */, false /* usePartialMerge */, internal...)
	if err != nil {
		t.Fatal(err)
	}

	if len(out.Samples) > 0 {
		if maxVal := out.Samples[0].Max; maxVal != nil {
			t.Fatal("Expected maximum of sample 0 to be nil; samples are no longer merged")
		}
		if a, e := out.Samples[0].Sum, -2.0; a != e {
			t.Fatalf("Expected sum of sample 0 to be %f after initial merge, was %f", e, a)
		}
	} else {
		t.Fatal("All samples unexpectedly discarded")
	}
}
