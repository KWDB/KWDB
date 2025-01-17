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

package ordering

import (
	"fmt"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/opt"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props"
	"gitee.com/kwbasedb/kwbase/pkg/sql/opt/props/physical"
)

func TestTrimProvided(t *testing.T) {
	emptyFD, equivFD, constFD := testFDs()
	testCases := []struct {
		req, prov string
		fds       props.FuncDepSet
		exp       string
	}{
		{ // case 1
			req:  "+1 opt(2)",
			prov: "+1,+2,+3",
			fds:  emptyFD,
			exp:  "+1",
		},
		{ // case 2
			req:  "+1,+3 opt(2)",
			prov: "+1,+2,+3",
			fds:  emptyFD,
			exp:  "+1,+2,+3",
		},
		{ // case 3
			req:  "+4,-5 opt(1,2,3)",
			prov: "-2,+4,-5,+7",
			fds:  constFD,
			exp:  "-2,+4,-5",
		},
		{ // case 4
			req:  "+(1|2),-(3|4) opt(5)",
			prov: "+2,-5,-3,+4",
			fds:  equivFD,
			exp:  "+2,-5,-3",
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			req := physical.ParseOrderingChoice(tc.req)
			prov := physical.ParseOrdering(tc.prov)
			res := trimProvided(prov, &req, &tc.fds).String()
			if res != tc.exp {
				t.Errorf("expected %s, got %s", tc.exp, res)
			}
		})
	}
}

func TestRemapProvided(t *testing.T) {
	emptyFD, equivFD, constFD := testFDs()
	c := func(cols ...opt.ColumnID) opt.ColSet {
		return opt.MakeColSet(cols...)
	}
	testCases := []struct {
		prov string
		fds  props.FuncDepSet
		cols opt.ColSet
		exp  string
	}{
		{ // case 1
			prov: "+1,+2,+3",
			fds:  emptyFD,
			cols: c(1, 2, 3),
			exp:  "+1,+2,+3",
		},
		{ // case 2
			prov: "-1,+2,+3",
			fds:  equivFD,
			cols: c(1, 2, 3),
			exp:  "-1,+3",
		},
		{ // case 3
			prov: "+1,-2,+3",
			fds:  equivFD,
			cols: c(1, 3),
			exp:  "+1,+3",
		},
		{ // case 4
			prov: "-1,+2,+3",
			fds:  equivFD,
			cols: c(2, 4),
			exp:  "-2,+4",
		},
		{ // case 5
			prov: "+4,-1,-5,+2",
			fds:  constFD,
			cols: c(1, 2, 3, 4, 5),
			exp:  "+4,-5",
		},
	}
	for tcIdx, tc := range testCases {
		t.Run(fmt.Sprintf("case%d", tcIdx+1), func(t *testing.T) {
			prov := physical.ParseOrdering(tc.prov)
			res := remapProvided(prov, &tc.fds, tc.cols).String()
			if res != tc.exp {
				t.Errorf("expected %s, got %s", tc.exp, res)
			}
		})
	}
}

// testFDs returns FDs that can be used for testing:
//   - emptyFD
//   - equivFD: (1)==(2), (2)==(1), (3)==(4), (4)==(3)
//   - constFD: ()-->(1,2)
func testFDs() (emptyFD, equivFD, constFD props.FuncDepSet) {
	equivFD.AddEquivalency(1, 2)
	equivFD.AddEquivalency(3, 4)

	constFD.AddConstants(opt.MakeColSet(1, 2))

	return emptyFD, equivFD, constFD
}
