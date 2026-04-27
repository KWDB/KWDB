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

package execinfrapb

import "testing"

func TestPostProcessSpecRAI(t *testing.T) {
	obj1 := &PostProcessSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestColumnsRAI(t *testing.T) {
	obj1 := &Columns{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTableReaderSpanRAI(t *testing.T) {
	obj1 := &TableReaderSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}
