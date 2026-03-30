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

package procedure

import (
	"context"
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/prepare"
	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
	"github.com/stretchr/testify/require"
)

func TestPrepareIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
	}

	// Create PrepareIns
	prepareName := tree.Name("testPrepare")
	var res prepare.PreparedResult

	addFnCalled := false
	addFn := func(ctx context.Context, res prepare.PreparedResult, name string) error {
		addFnCalled = true
		require.Equal(t, "testPrepare", name)
		return nil
	}

	prepareIns := &PrepareIns{
		prepareName: prepareName,
		res:         res,
		addFn:       addFn,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test Execute
	err := prepareIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.True(t, addFnCalled)
}

func TestPrepareIns_Close(t *testing.T) {
	prepareIns := &PrepareIns{}
	prepareIns.Close()
	// Just ensure no panic
}

func TestExecuteIns_Execute(t *testing.T) {
	ctx := context.Background()
	runParam := &MockRunParam{
		ctx: ctx,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Create ExecuteIns with createDefault=true
	executeIns := &ExecuteIns{
		createDefault: true,
	}

	// Test execute with createDefault=true
	err := executeIns.Execute(runParam, rCtx)
	require.NoError(t, err)
	require.False(t, executeIns.createDefault)

	// TODO: Test with createDefault=false (requires more complex setup)
}

func TestExecuteIns_Close(t *testing.T) {
	executeIns := &ExecuteIns{}
	executeIns.Close()
	// Just ensure no panic
}

func TestDeallocateIns_Execute(t *testing.T) {
	runParam := &MockRunParam{}

	// Create DeallocateIns
	name := "testPrepare"
	deallocateIns := &DeallocateIns{
		name: name,
	}

	rCtx := &SpExecContext{}
	rCtx.Init()

	// Test Execute
	err := deallocateIns.Execute(runParam, rCtx)
	require.NoError(t, err)
}

func TestDeallocateIns_Close(t *testing.T) {
	deallocateIns := &DeallocateIns{}
	deallocateIns.Close()
	// Just ensure no panic
}