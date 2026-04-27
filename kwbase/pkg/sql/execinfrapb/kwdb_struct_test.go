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

func TestTableDescArrayRAI(t *testing.T) {
	obj1 := &TableDescArray{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTableDescRAI(t *testing.T) {
	obj1 := &TableDesc{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSSynchronizerSpecRAI(t *testing.T) {
	obj1 := &TSSynchronizerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTsSpanRAI(t *testing.T) {
	obj1 := &TsSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSBlockFilterRAI(t *testing.T) {
	obj1 := &TSBlockFilter{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSBlockFilter_SpanRAI(t *testing.T) {
	obj1 := &TSBlockFilter_Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSFillRAI(t *testing.T) {
	obj1 := &TSFill{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSReaderSpecRAI(t *testing.T) {
	obj1 := &TSReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSStatisticReaderSpecRAI(t *testing.T) {
	obj1 := &TSStatisticReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSStatisticReaderSpec_ParamInfoRAI(t *testing.T) {
	obj1 := &TSStatisticReaderSpec_ParamInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSStatisticReaderSpec_ParamsRAI(t *testing.T) {
	obj1 := &TSStatisticReaderSpec_Params{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestHashpointSpanRAI(t *testing.T) {
	obj1 := &HashpointSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSTagReaderSpecRAI(t *testing.T) {
	obj1 := &TSTagReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSTagReaderSpec_TagValueArrayRAI(t *testing.T) {
	obj1 := &TSTagReaderSpec_TagValueArray{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSTagReaderSpec_TagIndexInfoRAI(t *testing.T) {
	obj1 := &TSTagReaderSpec_TagIndexInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestTSSamplerSpecRAI(t *testing.T) {
	obj1 := &TSSamplerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestSketchInfoRAI(t *testing.T) {
	obj1 := &SketchInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestStreamReaderSpecRAI(t *testing.T) {
	obj1 := &StreamReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestStreamAggregatorSpecRAI(t *testing.T) {
	obj1 := &StreamAggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}
