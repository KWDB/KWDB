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

package physicalplan

import (
	"math"
	"strings"

	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
)

// parseResultTypes converts a comma-separated list like "A,B,C" into a
// slice of placeholder `types.T` values used by tests.
func parseResultTypes(s string) []types.T {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	res := make([]types.T, len(parts))
	for i, p := range parts {
		res[i] = *types.MakeCollatedString(types.String, p)
	}
	return res
}

// floatAlmostEqual returns true when two floats are within eps relative
// error; useful for comparing aggregation floats in tests.
func floatAlmostEqual(a, b float64, eps float64) bool {
	if math.IsNaN(a) && math.IsNaN(b) {
		return true
	}
	if a == b {
		return true
	}
	d := math.Abs(a - b)
	if d <= eps {
		return true
	}
	// relative error
	if math.Abs(b) > eps {
		return d/math.Abs(b) <= eps
	}
	return false
}
