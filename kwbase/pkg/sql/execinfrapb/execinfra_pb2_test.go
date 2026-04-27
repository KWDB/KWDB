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

package execinfrapb_test

import (
	"testing"

	"gitee.com/kwbasedb/kwbase/pkg/sql/execinfrapb"
)

func TestBytesEncodeFormatRAI(t *testing.T) {
	obj1 := execinfrapb.BytesEncodeFormat(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestStreamEndpointTypeRAI(t *testing.T) {
	obj1 := execinfrapb.StreamEndpointType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestOrdering_Column_DirectionRAI(t *testing.T) {
	obj1 := execinfrapb.Ordering_Column_Direction(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestInputSyncSpec_TypeRAI(t *testing.T) {
	obj1 := execinfrapb.InputSyncSpec_Type(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestOutputRouterSpec_TypeRAI(t *testing.T) {
	obj1 := execinfrapb.OutputRouterSpec_Type(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTSTableReadModeRAI(t *testing.T) {
	obj1 := execinfrapb.TSTableReadMode(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestSketchMethodRAI(t *testing.T) {
	obj1 := execinfrapb.SketchMethod(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTSBlockFilterTypeRAI(t *testing.T) {
	obj1 := execinfrapb.TSBlockFilterType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTSBlockFilter_Span_SpanBoundaryRAI(t *testing.T) {
	obj1 := execinfrapb.TSBlockFilter_Span_SpanBoundary(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTSFill_TSFillTypeRAI(t *testing.T) {
	obj1 := execinfrapb.TSFill_TSFillType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestTSStatisticReaderSpec_ParamInfoTypeRAI(t *testing.T) {
	obj1 := execinfrapb.TSStatisticReaderSpec_ParamInfoType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestBackfillerSpec_TypeRAI(t *testing.T) {
	obj1 := execinfrapb.BackfillerSpec_Type(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestProcessorSpecEngineTypeRAI(t *testing.T) {
	obj1 := execinfrapb.ProcessorSpecEngineType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestScanVisibilityRAI(t *testing.T) {
	obj1 := execinfrapb.ScanVisibility(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestDedupRuleRAI(t *testing.T) {
	obj1 := execinfrapb.DedupRule(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestOperatorTypeRAI(t *testing.T) {
	obj1 := execinfrapb.OperatorType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestAggregatorSpec_FuncRAI(t *testing.T) {
	obj1 := execinfrapb.AggregatorSpec_Func(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestAggregatorSpec_TypeRAI(t *testing.T) {
	obj1 := execinfrapb.AggregatorSpec_Type(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestWindowerSpec_WindowFuncRAI(t *testing.T) {
	obj1 := execinfrapb.WindowerSpec_WindowFunc(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestWindowerSpec_Frame_ModeRAI(t *testing.T) {
	obj1 := execinfrapb.WindowerSpec_Frame_Mode(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestWindowerSpec_Frame_BoundTypeRAI(t *testing.T) {
	obj1 := execinfrapb.WindowerSpec_Frame_BoundType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestWindowerSpec_Frame_ExclusionRAI(t *testing.T) {
	obj1 := execinfrapb.WindowerSpec_Frame_Exclusion(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}

func TestSketchTypeRAI(t *testing.T) {
	obj1 := execinfrapb.SketchType(0)
	_ = obj1.Enum()
	_ = obj1.String()
	_, _ = obj1.EnumDescriptor()

	empty := []byte{'"', '"'}
	_ = obj1.UnmarshalJSON(empty)
}
