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

package util

import (
	"regexp"
	"strconv"
	"strings"

	"github.com/cockroachdb/errors"
)

var memorySizeRegex = regexp.MustCompile(`^(\d+(?:\.\d+)?)\s*([KMGTP]?B)$`)

// ParseMemorySize 将形如 "1GB", "2.5MB", "3 KB" 的字符串转换为字节数
// 支持单位：B, KB, MB, GB, TB, PB（不区分大小写）
func ParseMemorySize(sizeStr string) (int64, error) {
	sizeStr = strings.TrimSpace(sizeStr)

	// 尝试直接解析纯数字（默认为字节）
	if memory, err := strconv.ParseInt(sizeStr, 10, 64); err == nil {
		return memory, nil
	}

	matches := memorySizeRegex.FindStringSubmatch(strings.ToUpper(sizeStr))
	if len(matches) != 3 {
		return 0, errors.Errorf("invalid memory size format: %s, expected format like '1GB', '2.5MB', '1024'", sizeStr)
	}

	num, err := strconv.ParseFloat(matches[1], 64)
	if err != nil {
		return 0, errors.Errorf("invalid number in memory size: %s", matches[1])
	}

	unit := matches[2]
	multipliers := map[string]int64{
		"B":  1,
		"KB": 1 << 10,
		"MB": 1 << 20,
		"GB": 1 << 30,
		"TB": 1 << 40,
		"PB": 1 << 50,
	}

	multiplier, exists := multipliers[unit]
	if !exists {
		return 0, errors.Errorf("invalid memory unit: %s, supported units: B, KB, MB, GB, TB, PB", unit)
	}

	result := int64(num * float64(multiplier))
	if result < 0 {
		return 0, errors.Errorf("memory size cannot be negative: %s", sizeStr)
	}

	return result, nil
}
