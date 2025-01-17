// Copyright 2019 The Cockroach Authors.
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

package kvserver

import (
	"fmt"
	"math"
	"strings"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"gitee.com/kwbasedb/kwbase/pkg/util/log"
)

// TestWrapNumbersAsSafe tests the wrapNumbersAsSafe through ReportablesToSafeError.
func TestWrapNumbersAsSafe(t *testing.T) {
	defer leaktest.AfterTest(t)()

	reportables := []interface{}{
		uint(math.MaxUint32),
		uint8(math.MaxUint8),
		uint16(math.MaxUint16),
		uint32(math.MaxUint32),
		uint64(math.MaxUint64),

		int(math.MaxInt32),
		int8(math.MaxInt8),
		int16(math.MaxInt16),
		int32(math.MaxInt32),
		int64(math.MaxInt64),

		float32(math.MaxFloat32),
		float64(math.MaxFloat64),

		"unsafe-string",
		"123",
	}

	wrapNumbersAsSafe(reportables...)
	format := "some reportables"
	err := log.ReportablesToSafeError(0, format, reportables)

	expectedReportables := []string{
		fmt.Sprint(uint(math.MaxUint32)),
		fmt.Sprint(uint8(math.MaxUint8)),
		fmt.Sprint(uint16(math.MaxUint16)),
		fmt.Sprint(uint32(math.MaxUint32)),
		fmt.Sprint(uint64(math.MaxUint64)),

		fmt.Sprint(int(math.MaxInt32)),
		fmt.Sprint(int8(math.MaxInt8)),
		fmt.Sprint(int16(math.MaxInt16)),
		fmt.Sprint(int32(math.MaxInt32)),
		fmt.Sprint(int64(math.MaxInt64)),

		fmt.Sprint(float32(math.MaxFloat32)),
		fmt.Sprint(float64(math.MaxFloat64)),

		"string",
		"string",
	}

	expectedErrorStr := fmt.Sprintf("%s:%d: %s%s%s", "?", 0, format, " | ", strings.Join(expectedReportables, "; "))

	if expectedErrorStr != err.Error() {
		t.Fatalf("expected error to be %s but was %s", expectedErrorStr, err.Error())
	}

}
