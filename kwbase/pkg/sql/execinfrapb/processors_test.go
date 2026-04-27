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

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/sem/tree"
)

func TestProcessorSpecRAI(t *testing.T) {
	obj1 := &ProcessorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestProcessorCoreUnionRAI(t *testing.T) {
	obj1 := &ProcessorCoreUnion{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestNoopCoreSpecRAI(t *testing.T) {
	obj1 := &NoopCoreSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestLocalPlanNodeSpecRAI(t *testing.T) {
	obj1 := &LocalPlanNodeSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemotePlanNodeSpecRAI(t *testing.T) {
	obj1 := &RemotePlanNodeSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestMetadataTestSenderSpecRAI(t *testing.T) {
	obj1 := &MetadataTestSenderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestMetadataTestReceiverSpecRAI(t *testing.T) {
	obj1 := &MetadataTestReceiverSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

// TestEquals tests the Equals method of AggregatorSpec_Aggregation
func TestEquals(t *testing.T) {
	// Test equal aggregations
	a1 := AggregatorSpec_Aggregation{
		Func:     AggregatorSpec_COUNT_ROWS,
		Distinct: false,
		ColIdx:   []uint32{1, 2, 3},
	}
	a2 := AggregatorSpec_Aggregation{
		Func:     AggregatorSpec_COUNT_ROWS,
		Distinct: false,
		ColIdx:   []uint32{1, 2, 3},
	}
	if !a1.Equals(a2) {
		t.Error("AggregatorSpec_Aggregation.Equals() returned false for equal aggregations")
	}

	// Test unequal aggregations
	a3 := AggregatorSpec_Aggregation{
		Func:     AggregatorSpec_SUM,
		Distinct: false,
		ColIdx:   []uint32{1, 2, 3},
	}
	if a1.Equals(a3) {
		t.Error("AggregatorSpec_Aggregation.Equals() returned true for unequal aggregations")
	}
}

// TestIsScalar tests the IsScalar method of AggregatorSpec
func TestIsScalar(t *testing.T) {
	// Test scalar aggregator
	spec1 := &AggregatorSpec{
		Type: AggregatorSpec_SCALAR,
	}
	if !spec1.IsScalar() {
		t.Error("AggregatorSpec.IsScalar() returned false for scalar aggregator")
	}

	// Test non-scalar aggregator
	spec2 := &AggregatorSpec{
		Type: AggregatorSpec_NON_SCALAR,
	}
	if spec2.IsScalar() {
		t.Error("AggregatorSpec.IsScalar() returned true for non-scalar aggregator")
	}

	// Test backward compatibility
	spec3 := &AggregatorSpec{
		GroupCols: []uint32{},
	}
	if !spec3.IsScalar() {
		t.Error("AggregatorSpec.IsScalar() returned false for backward compatibility case")
	}

	spec4 := &AggregatorSpec{
		GroupCols: []uint32{1},
	}
	if spec4.IsScalar() {
		t.Error("AggregatorSpec.IsScalar() returned true for backward compatibility case with group cols")
	}
}

// TestIsRowCount tests the IsRowCount method of AggregatorSpec
func TestIsRowCount(t *testing.T) {
	// Test row count aggregator
	spec1 := &AggregatorSpec{
		Type: AggregatorSpec_SCALAR,
		Aggregations: []AggregatorSpec_Aggregation{
			{
				Func:     AggregatorSpec_COUNT_ROWS,
				Distinct: false,
			},
		},
	}
	if !spec1.IsRowCount() {
		t.Error("AggregatorSpec.IsRowCount() returned false for row count aggregator")
	}

	// Test non-row count aggregator
	spec2 := &AggregatorSpec{
		Type: AggregatorSpec_SCALAR,
		Aggregations: []AggregatorSpec_Aggregation{
			{
				Func:     AggregatorSpec_SUM,
				Distinct: false,
			},
		},
	}
	if spec2.IsRowCount() {
		t.Error("AggregatorSpec.IsRowCount() returned true for non-row count aggregator")
	}
}

// TestGetAggregateInfo tests the GetAggregateInfo function
func TestGetAggregateInfo(t *testing.T) {
	// Test COUNT_ROWS aggregate
	constructor, returnType, err := GetAggregateInfo(AggregatorSpec_COUNT_ROWS)
	if err != nil {
		t.Errorf("GetAggregateInfo() failed: %v", err)
	}
	if constructor == nil {
		t.Error("GetAggregateInfo() returned nil constructor")
	}
	if returnType == nil {
		t.Error("GetAggregateInfo() returned nil returnType")
	}
}

// TestGetWindowFunctionInfo tests the GetWindowFunctionInfo function
func TestGetWindowFunctionInfo(t *testing.T) {
	// Test window function with aggregate
	fn := WindowerSpec_Func{
		WindowFunc: func() *WindowerSpec_WindowFunc { f := WindowerSpec_ROW_NUMBER; return &f }(),
	}
	constructor, returnType, err := GetWindowFunctionInfo(fn)
	if err != nil {
		t.Errorf("GetWindowFunctionInfo() failed: %v", err)
	}
	if constructor == nil {
		t.Error("GetWindowFunctionInfo() returned nil constructor")
	}
	if returnType == nil {
		t.Error("GetWindowFunctionInfo() returned nil returnType")
	}
}

// TestWindowerSpecFrameModeInitFromAST tests the initFromAST method of WindowerSpec_Frame_Mode
func TestWindowerSpecFrameModeInitFromAST(t *testing.T) {
	// Test RANGE mode
	var mode WindowerSpec_Frame_Mode
	err := mode.initFromAST(tree.RANGE)
	if err != nil {
		t.Errorf("initFromAST() failed for RANGE mode: %v", err)
	}
	if mode != WindowerSpec_Frame_RANGE {
		t.Errorf("initFromAST() returned %v for RANGE mode, expected %v", mode, WindowerSpec_Frame_RANGE)
	}

	// Test ROWS mode
	err = mode.initFromAST(tree.ROWS)
	if err != nil {
		t.Errorf("initFromAST() failed for ROWS mode: %v", err)
	}
	if mode != WindowerSpec_Frame_ROWS {
		t.Errorf("initFromAST() returned %v for ROWS mode, expected %v", mode, WindowerSpec_Frame_ROWS)
	}

	// Test GROUPS mode
	err = mode.initFromAST(tree.GROUPS)
	if err != nil {
		t.Errorf("initFromAST() failed for GROUPS mode: %v", err)
	}
	if mode != WindowerSpec_Frame_GROUPS {
		t.Errorf("initFromAST() returned %v for GROUPS mode, expected %v", mode, WindowerSpec_Frame_GROUPS)
	}
}

// TestWindowerSpecFrameBoundTypeInitFromAST tests the initFromAST method of WindowerSpec_Frame_BoundType
func TestWindowerSpecFrameBoundTypeInitFromAST(t *testing.T) {
	// Test UnboundedPreceding
	var boundType WindowerSpec_Frame_BoundType
	err := boundType.initFromAST(tree.UnboundedPreceding)
	if err != nil {
		t.Errorf("initFromAST() failed for UnboundedPreceding: %v", err)
	}
	if boundType != WindowerSpec_Frame_UNBOUNDED_PRECEDING {
		t.Errorf("initFromAST() returned %v for UnboundedPreceding, expected %v", boundType, WindowerSpec_Frame_UNBOUNDED_PRECEDING)
	}

	// Test CurrentRow
	err = boundType.initFromAST(tree.CurrentRow)
	if err != nil {
		t.Errorf("initFromAST() failed for CurrentRow: %v", err)
	}
	if boundType != WindowerSpec_Frame_CURRENT_ROW {
		t.Errorf("initFromAST() returned %v for CurrentRow, expected %v", boundType, WindowerSpec_Frame_CURRENT_ROW)
	}

	// Test UnboundedFollowing
	err = boundType.initFromAST(tree.UnboundedFollowing)
	if err != nil {
		t.Errorf("initFromAST() failed for UnboundedFollowing: %v", err)
	}
	if boundType != WindowerSpec_Frame_UNBOUNDED_FOLLOWING {
		t.Errorf("initFromAST() returned %v for UnboundedFollowing, expected %v", boundType, WindowerSpec_Frame_UNBOUNDED_FOLLOWING)
	}
}

// TestWindowerSpecFrameExclusionInitFromAST tests the initFromAST method of WindowerSpec_Frame_Exclusion
func TestWindowerSpecFrameExclusionInitFromAST(t *testing.T) {
	// Test NoExclusion
	var exclusion WindowerSpec_Frame_Exclusion
	err := exclusion.initFromAST(tree.NoExclusion)
	if err != nil {
		t.Errorf("initFromAST() failed for NoExclusion: %v", err)
	}
	if exclusion != WindowerSpec_Frame_NO_EXCLUSION {
		t.Errorf("initFromAST() returned %v for NoExclusion, expected %v", exclusion, WindowerSpec_Frame_NO_EXCLUSION)
	}

	// Test ExcludeCurrentRow
	err = exclusion.initFromAST(tree.ExcludeCurrentRow)
	if err != nil {
		t.Errorf("initFromAST() failed for ExcludeCurrentRow: %v", err)
	}
	if exclusion != WindowerSpec_Frame_EXCLUDE_CURRENT_ROW {
		t.Errorf("initFromAST() returned %v for ExcludeCurrentRow, expected %v", exclusion, WindowerSpec_Frame_EXCLUDE_CURRENT_ROW)
	}

	// Test ExcludeGroup
	err = exclusion.initFromAST(tree.ExcludeGroup)
	if err != nil {
		t.Errorf("initFromAST() failed for ExcludeGroup: %v", err)
	}
	if exclusion != WindowerSpec_Frame_EXCLUDE_GROUP {
		t.Errorf("initFromAST() returned %v for ExcludeGroup, expected %v", exclusion, WindowerSpec_Frame_EXCLUDE_GROUP)
	}

	// Test ExcludeTies
	err = exclusion.initFromAST(tree.ExcludeTies)
	if err != nil {
		t.Errorf("initFromAST() failed for ExcludeTies: %v", err)
	}
	if exclusion != WindowerSpec_Frame_EXCLUDE_TIES {
		t.Errorf("initFromAST() returned %v for ExcludeTies, expected %v", exclusion, WindowerSpec_Frame_EXCLUDE_TIES)
	}
}

// TestWindowerSpecFrameBoundsInitFromAST tests the initFromAST method of WindowerSpec_Frame_Bounds
func TestWindowerSpecFrameBoundsInitFromAST(t *testing.T) {
	// Create window frame bounds
	bounds := tree.WindowFrameBounds{
		StartBound: &tree.WindowFrameBound{
			BoundType: tree.UnboundedPreceding,
		},
		EndBound: &tree.WindowFrameBound{
			BoundType: tree.CurrentRow,
		},
	}

	// Create WindowerSpec_Frame_Bounds
	var spec WindowerSpec_Frame_Bounds

	// Create eval context
	evalCtx := &tree.EvalContext{}

	// Test with ROWS mode
	err := spec.initFromAST(bounds, tree.ROWS, evalCtx)
	if err != nil {
		t.Errorf("initFromAST() failed for ROWS mode: %v", err)
	}

	// Test with RANGE mode
	err = spec.initFromAST(bounds, tree.RANGE, evalCtx)
	if err != nil {
		t.Errorf("initFromAST() failed for RANGE mode: %v", err)
	}

	// Test with GROUPS mode
	err = spec.initFromAST(bounds, tree.GROUPS, evalCtx)
	if err != nil {
		t.Errorf("initFromAST() failed for GROUPS mode: %v", err)
	}
}

// TestWindowerSpecFrameModeConvertToAST tests the convertToAST method of WindowerSpec_Frame_Mode
func TestWindowerSpecFrameModeConvertToAST(t *testing.T) {
	// Test RANGE mode
	mode := WindowerSpec_Frame_RANGE
	astMode, err := mode.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for RANGE mode: %v", err)
	}
	if astMode != tree.RANGE {
		t.Errorf("convertToAST() returned %v for RANGE mode, expected %v", astMode, tree.RANGE)
	}

	// Test ROWS mode
	mode = WindowerSpec_Frame_ROWS
	astMode, err = mode.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for ROWS mode: %v", err)
	}
	if astMode != tree.ROWS {
		t.Errorf("convertToAST() returned %v for ROWS mode, expected %v", astMode, tree.ROWS)
	}

	// Test GROUPS mode
	mode = WindowerSpec_Frame_GROUPS
	astMode, err = mode.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for GROUPS mode: %v", err)
	}
	if astMode != tree.GROUPS {
		t.Errorf("convertToAST() returned %v for GROUPS mode, expected %v", astMode, tree.GROUPS)
	}
}

// TestWindowerSpecFrameBoundTypeConvertToAST tests the convertToAST method of WindowerSpec_Frame_BoundType
func TestWindowerSpecFrameBoundTypeConvertToAST(t *testing.T) {
	// Test UNBOUNDED_PRECEDING
	boundType := WindowerSpec_Frame_UNBOUNDED_PRECEDING
	astBoundType, err := boundType.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for UNBOUNDED_PRECEDING: %v", err)
	}
	if astBoundType != tree.UnboundedPreceding {
		t.Errorf("convertToAST() returned %v for UNBOUNDED_PRECEDING, expected %v", astBoundType, tree.UnboundedPreceding)
	}

	// Test CURRENT_ROW
	boundType = WindowerSpec_Frame_CURRENT_ROW
	astBoundType, err = boundType.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for CURRENT_ROW: %v", err)
	}
	if astBoundType != tree.CurrentRow {
		t.Errorf("convertToAST() returned %v for CURRENT_ROW, expected %v", astBoundType, tree.CurrentRow)
	}

	// Test UNBOUNDED_FOLLOWING
	boundType = WindowerSpec_Frame_UNBOUNDED_FOLLOWING
	astBoundType, err = boundType.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for UNBOUNDED_FOLLOWING: %v", err)
	}
	if astBoundType != tree.UnboundedFollowing {
		t.Errorf("convertToAST() returned %v for UNBOUNDED_FOLLOWING, expected %v", astBoundType, tree.UnboundedFollowing)
	}
}

// TestWindowerSpecFrameExclusionConvertToAST tests the convertToAST method of WindowerSpec_Frame_Exclusion
func TestWindowerSpecFrameExclusionConvertToAST(t *testing.T) {
	// Test NO_EXCLUSION
	exclusion := WindowerSpec_Frame_NO_EXCLUSION
	astExclusion, err := exclusion.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for NO_EXCLUSION: %v", err)
	}
	if astExclusion != tree.NoExclusion {
		t.Errorf("convertToAST() returned %v for NO_EXCLUSION, expected %v", astExclusion, tree.NoExclusion)
	}

	// Test EXCLUDE_CURRENT_ROW
	exclusion = WindowerSpec_Frame_EXCLUDE_CURRENT_ROW
	astExclusion, err = exclusion.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for EXCLUDE_CURRENT_ROW: %v", err)
	}
	if astExclusion != tree.ExcludeCurrentRow {
		t.Errorf("convertToAST() returned %v for EXCLUDE_CURRENT_ROW, expected %v", astExclusion, tree.ExcludeCurrentRow)
	}

	// Test EXCLUDE_GROUP
	exclusion = WindowerSpec_Frame_EXCLUDE_GROUP
	astExclusion, err = exclusion.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for EXCLUDE_GROUP: %v", err)
	}
	if astExclusion != tree.ExcludeGroup {
		t.Errorf("convertToAST() returned %v for EXCLUDE_GROUP, expected %v", astExclusion, tree.ExcludeGroup)
	}

	// Test EXCLUDE_TIES
	exclusion = WindowerSpec_Frame_EXCLUDE_TIES
	astExclusion, err = exclusion.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed for EXCLUDE_TIES: %v", err)
	}
	if astExclusion != tree.ExcludeTies {
		t.Errorf("convertToAST() returned %v for EXCLUDE_TIES, expected %v", astExclusion, tree.ExcludeTies)
	}
}

// TestWindowerSpecFrameBoundsConvertToAST tests the convertToAST method of WindowerSpec_Frame_Bounds
func TestWindowerSpecFrameBoundsConvertToAST(t *testing.T) {
	// Create WindowerSpec_Frame_Bounds
	spec := WindowerSpec_Frame_Bounds{
		Start: WindowerSpec_Frame_Bound{
			BoundType: WindowerSpec_Frame_UNBOUNDED_PRECEDING,
		},
		End: &WindowerSpec_Frame_Bound{
			BoundType: WindowerSpec_Frame_CURRENT_ROW,
		},
	}

	// Test convertToAST
	bounds, err := spec.convertToAST()
	if err != nil {
		t.Errorf("convertToAST() failed: %v", err)
	}
	if bounds.StartBound == nil {
		t.Error("convertToAST() returned nil StartBound")
	}
	if bounds.EndBound == nil {
		t.Error("convertToAST() returned nil EndBound")
	}
}

// TestWindowerSpecFrameInitFromAST tests the InitFromAST method of WindowerSpec_Frame
func TestWindowerSpecFrameInitFromAST(t *testing.T) {
	// Create window frame
	frame := &tree.WindowFrame{
		Mode: tree.ROWS,
		Bounds: tree.WindowFrameBounds{
			StartBound: &tree.WindowFrameBound{
				BoundType: tree.UnboundedPreceding,
			},
			EndBound: &tree.WindowFrameBound{
				BoundType: tree.CurrentRow,
			},
		},
		Exclusion: tree.NoExclusion,
	}

	// Create WindowerSpec_Frame
	spec := &WindowerSpec_Frame{}

	// Create eval context
	evalCtx := &tree.EvalContext{}

	// Test InitFromAST
	err := spec.InitFromAST(frame, evalCtx)
	if err != nil {
		t.Errorf("InitFromAST() failed: %v", err)
	}
}

// TestWindowerSpecFrameConvertToAST tests the ConvertToAST method of WindowerSpec_Frame
func TestWindowerSpecFrameConvertToAST(t *testing.T) {
	// Create WindowerSpec_Frame
	spec := &WindowerSpec_Frame{
		Mode: WindowerSpec_Frame_ROWS,
		Bounds: WindowerSpec_Frame_Bounds{
			Start: WindowerSpec_Frame_Bound{
				BoundType: WindowerSpec_Frame_UNBOUNDED_PRECEDING,
			},
			End: &WindowerSpec_Frame_Bound{
				BoundType: WindowerSpec_Frame_CURRENT_ROW,
			},
		},
		Exclusion: WindowerSpec_Frame_NO_EXCLUSION,
	}

	// Test ConvertToAST
	frame, err := spec.ConvertToAST()
	if err != nil {
		t.Errorf("ConvertToAST() failed: %v", err)
	}
	if frame == nil {
		t.Error("ConvertToAST() returned nil frame")
	}
}
