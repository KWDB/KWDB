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

package sql

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sqlbase"
	"gitee.com/kwbasedb/kwbase/pkg/sql/types"
	"gitee.com/kwbasedb/kwbase/pkg/util/leaktest"
)

func TestFormatColumns(t *testing.T) {
	defer leaktest.AfterTest(t)()

	emptyCols := sqlbase.ResultColumns{}
	result := formatColumns(emptyCols, false)
	if result != "()" {
		t.Errorf("formatColumns(empty, false) = %q, want %q", result, "()")
	}

	result = formatColumns(emptyCols, true)
	if result != "()" {
		t.Errorf("formatColumns(empty, true) = %q, want %q", result, "()")
	}

	singleCol := sqlbase.ResultColumns{
		{Name: "a", Typ: types.Int},
	}
	result = formatColumns(singleCol, false)
	if result != "(a)" {
		t.Errorf("formatColumns(singleCol, false) = %q, want %q", result, "(a)")
	}

	result = formatColumns(singleCol, true)
	if result != "(a int)" {
		t.Errorf("formatColumns(singleCol, true) = %q, want %q", result, "(a int)")
	}

	multiCol := sqlbase.ResultColumns{
		{Name: "a", Typ: types.Int},
		{Name: "b", Typ: types.String},
		{Name: "c", Typ: types.Float},
	}
	result = formatColumns(multiCol, false)
	if result != "(a, b, c)" {
		t.Errorf("formatColumns(multiCol, false) = %q, want %q", result, "(a, b, c)")
	}

	result = formatColumns(multiCol, true)
	if result != "(a int, b string, c float)" {
		t.Errorf("formatColumns(multiCol, true) = %q, want %q", result, "(a int, b string, c float)")
	}

	hiddenCol := sqlbase.ResultColumns{
		{Name: "a", Typ: types.Int, Hidden: true},
	}
	result = formatColumns(hiddenCol, false)
	if result != "(a[hidden])" {
		t.Errorf("formatColumns(hiddenCol, false) = %q, want %q", result, "(a[hidden])")
	}

	result = formatColumns(hiddenCol, true)
	if result != "(a[hidden] int)" {
		t.Errorf("formatColumns(hiddenCol, true) = %q, want %q", result, "(a[hidden] int)")
	}

	mixedHiddenCol := sqlbase.ResultColumns{
		{Name: "a", Typ: types.Int},
		{Name: "b", Typ: types.String, Hidden: true},
		{Name: "c", Typ: types.Float},
	}
	result = formatColumns(mixedHiddenCol, false)
	if result != "(a, b[hidden], c)" {
		t.Errorf("formatColumns(mixedHiddenCol, false) = %q, want %q", result, "(a, b[hidden], c)")
	}
}
