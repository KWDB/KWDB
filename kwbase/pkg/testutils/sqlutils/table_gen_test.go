// Copyright 2016 The Cockroach Authors.
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

package sqlutils

import (
	"bytes"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestIntToEnglish(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		val int
		exp string
	}{
		{0, "zero"},
		{1, "one"},
		{2, "two"},
		{3, "three"},
		{456, "four-five-six"},
		{70, "seven-zero"},
		{108, "one-zero-eight"},
		{9901, "nine-nine-zero-one"},
	}
	for _, c := range testCases {
		if res := IntToEnglish(c.val); res != c.exp {
			t.Errorf("expected %s, got %s", c.exp, res)
		}
	}
}

func TestGenValues(t *testing.T) {
	defer leaktest.AfterTest(t)()
	var buf bytes.Buffer
	genValues(&buf, 7, 11, ToRowFn(RowIdxFn, RowModuloFn(3), RowEnglishFn), false /* shouldPrint */)
	expected := `(7:::INT8,1:::INT8,'seven':::STRING),(8:::INT8,2:::INT8,'eight':::STRING),(9:::INT8,0:::INT8,'nine':::STRING),(10:::INT8,1:::INT8,'one-zero':::STRING),(11:::INT8,2:::INT8,'one-one':::STRING)`
	if buf.String() != expected {
		t.Errorf("expected '%s', got '%s'", expected, buf.String())
	}
}
