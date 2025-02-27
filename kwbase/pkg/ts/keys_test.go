// Copyright 2014 The Cockroach Authors.
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
	"bytes"
	"reflect"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/keys"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestDataKeys(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		name           string
		source         string
		timestamp      int64
		resolution     Resolution
		expectedLen    int
		expectedPretty string
	}{
		{
			"test.metric",
			"testsource",
			0,
			Resolution10s,
			30,
			"/System/tsd/test.metric/testsource/10s/1970-01-01T00:00:00Z",
		},
		{
			"test.no.source",
			"",
			1429114700000000000,
			Resolution10s,
			26,
			"/System/tsd/test.no.source//10s/2015-04-15T16:00:00Z",
		},
		{
			"",
			"",
			-1429114700000000000,
			Resolution10s,
			12,
			"/System/tsd///10s/1924-09-18T08:00:00Z",
		},
	}

	for i, tc := range testCases {
		encoded := MakeDataKey(tc.name, tc.source, tc.resolution, tc.timestamp)
		if !bytes.HasPrefix(encoded, keys.TimeseriesPrefix) {
			t.Errorf("%d: encoded key %v did not have time series data prefix", i, encoded)
		}
		if a, e := len(encoded), tc.expectedLen; a != e {
			t.Errorf("%d: encoded length %d did not match expected %d", i, a, e)
		}

		// Normalize timestamp of test case; we expect MakeDataKey to
		// automatically truncate it to an exact multiple of the Resolution's
		// KeyDuration
		tc.timestamp = (tc.timestamp / tc.resolution.SlabDuration()) * tc.resolution.SlabDuration()

		d := tc
		var err error
		d.name, d.source, d.resolution, d.timestamp, err = DecodeDataKey(encoded)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(d, tc) {
			t.Errorf("%d: decoded values %v did not match expected %v", i, d, tc)
		}
		if pretty := keys.PrettyPrint(nil /* valDirs */, encoded); tc.expectedPretty != pretty {
			t.Errorf("%d: expected %s, but got %s", i, tc.expectedPretty, pretty)
		}
	}
}
