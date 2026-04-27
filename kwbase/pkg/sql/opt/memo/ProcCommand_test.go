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

package memo

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestProcType(t *testing.T) {
	var result ProcCommand
	result = &ArrayCommand{}
	result.Type()
	result = &BlockCommand{}
	result.Type()
	result = &StmtCommand{}
	result.Type()
	result = &PrepareCommand{}
	result.Type()
	result = &ExecuteCommand{}
	result.Type()
	result = &DeallocateCommand{}
	result.Type()
	result = &IfCommand{}
	result.Type()
	result = &DeclareVarCommand{}
	result.Type()
	result = &WhileCommand{}
	result.Type()
	result = &LeaveCommand{}
	result.Type()
	result = &ProcedureTxnCommand{}
	result.Type()
	result = &OpenCursorCommand{}
	result.Type()
	result = &DeclCursorCommand{}
	result.Type()
	result = &FetchCursorCommand{}
	result.Type()
	result = &CloseCursorCommand{}
	result.Type()
	result = &DeclHandlerCommand{}
	result.Type()
	result = &IntoCommand{}
	result.Type()

	var l LeaveCommand
	l.Label = "test"
	var l1 LeaveCommand
	l.Label = "test1"

	require.Equal(t, l.Equal(&l1), false)
	require.Equal(t, l.Equal(&l), true)
	require.Equal(t, l.Equal(result), false)

	var o OpenCursorCommand
	o.Name = "test"
	var o1 OpenCursorCommand
	o1.Name = "test1"

	require.Equal(t, o.Equal(&o1), false)
	require.Equal(t, o.Equal(&o), true)

	var closeCurser CloseCursorCommand
	closeCurser.Name = "test"
	var closeCurser1 CloseCursorCommand
	closeCurser1.Name = "test1"

	require.Equal(t, closeCurser.Equal(&closeCurser1), false)
	require.Equal(t, closeCurser.Equal(&closeCurser), true)
}
