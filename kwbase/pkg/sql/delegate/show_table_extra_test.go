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

package delegate

import (
	"testing"
	"time"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

func TestConvertTagValToString(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// Test string
	dStr := tree.NewDString("test_str")
	res := ConvertTagValToString(dStr)
	require.Equal(t, "\\'test_str\\'", res)

	// Test bytes
	dBytes := tree.NewDBytes("test_bytes")
	res = ConvertTagValToString(dBytes)
	require.Contains(t, res, "746573745f6279746573") // Hex of "test_bytes"

	// Test timestamp
	dTs := tree.MakeDTimestamp(time.Date(2023, 1, 1, 0, 0, 0, 0, time.UTC), time.Microsecond)
	res = ConvertTagValToString(dTs)
	require.Equal(t, "2023-01-01 00:00:00+00:00", res) // Might have quotes depending on format, wait

	// Test other (int)
	dInt := tree.NewDInt(123)
	res = ConvertTagValToString(dInt)
	require.Equal(t, "123", res)
}

func TestShowCreateInstanceTable(t *testing.T) {
	defer leaktest.AfterTest(t)()

	sTable := tree.Name("stable_name")
	cTable := "inst_name"
	attributeName := []string{"tag1", "tag2"}
	attributeValue := []string{"'val1'", "123"}
	sde := false
	typ := []types.T{*types.String, *types.Int}

	res := ShowCreateInstanceTable(sTable, cTable, attributeName, attributeValue, sde, typ)
	require.Contains(t, res, "CREATE TABLE inst_name USING stable_name")
	require.Contains(t, res, "tag1")
	require.Contains(t, res, "'val1'")
	require.Contains(t, res, "123")
}
