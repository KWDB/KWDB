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

func TestSetupFlowRequestRAI(t *testing.T) {
	obj1 := &execinfrapb.SetupFlowRequest{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SetupFlowRequest{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestFlowSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.FlowSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.FlowSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSFlowSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSFlowSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSFlowSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.TsInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestEvalContextRAI(t *testing.T) {
	obj1 := &execinfrapb.EvalContext{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.EvalContext{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestUserDefinedVarRAI(t *testing.T) {
	obj1 := &execinfrapb.UserDefinedVar{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.UserDefinedVar{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSequenceStateRAI(t *testing.T) {
	obj1 := &execinfrapb.SequenceState{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SequenceState{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSequenceState_SeqRAI(t *testing.T) {
	obj1 := &execinfrapb.SequenceState_Seq{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SequenceState_Seq{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSimpleResponseRAI(t *testing.T) {
	obj1 := &execinfrapb.SimpleResponse{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SimpleResponse{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestConsumerSignalRAI(t *testing.T) {
	obj1 := &execinfrapb.ConsumerSignal{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ConsumerSignal{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDrainRequestRAI(t *testing.T) {
	obj1 := &execinfrapb.DrainRequest{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DrainRequest{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestConsumerHandshakeRAI(t *testing.T) {
	obj1 := &execinfrapb.ConsumerHandshake{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ConsumerHandshake{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestErrorRAI(t *testing.T) {
	obj1 := &execinfrapb.Error{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Error{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestExpressionRAI(t *testing.T) {
	obj1 := &execinfrapb.Expression{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Expression{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOrderingRAI(t *testing.T) {
	obj1 := &execinfrapb.Ordering{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Ordering{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOrdering_ColumnRAI(t *testing.T) {
	obj1 := &execinfrapb.Ordering_Column{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Ordering_Column{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestStreamEndpointSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.StreamEndpointSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.StreamEndpointSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestInputSyncSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.InputSyncSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.InputSyncSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOutputRouterSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.OutputRouterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.OutputRouterSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOutputRouterSpec_RangeRouterSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.OutputRouterSpec_RangeRouterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.OutputRouterSpec_RangeRouterSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOutputRouterSpec_RangeRouterSpec_ColumnEncodingRAI(t *testing.T) {
	obj1 := &execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.OutputRouterSpec_RangeRouterSpec_ColumnEncoding{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOutputRouterSpec_RangeRouterSpec_SpanRAI(t *testing.T) {
	obj1 := &execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.OutputRouterSpec_RangeRouterSpec_Span{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDatumInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.DatumInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DatumInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProducerHeaderRAI(t *testing.T) {
	obj1 := &execinfrapb.ProducerHeader{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProducerHeader{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProducerDataRAI(t *testing.T) {
	obj1 := &execinfrapb.ProducerData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProducerData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProducerMessageRAI(t *testing.T) {
	obj1 := &execinfrapb.ProducerMessage{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProducerMessage{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadataRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_RangeInfosRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_RangeInfos{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_RangeInfos{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TraceDataRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TraceData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TraceData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_RowNumRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_RowNum{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_RowNum{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_SamplerProgressRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_SamplerProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_SamplerProgress{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_BulkProcessorProgressRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_BulkProcessorProgress{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_MetricsRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_Metrics{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_Metrics{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSInsertRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSInsert{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSInsert{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSDeleteRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSDelete{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSDelete{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSTagUpdateRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSTagUpdate{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSTagUpdate{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSCreateTableRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSCreateTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSCreateTable{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSProRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSPro{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSPro{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemoteProducerMetadata_TSAlterColumnRAI(t *testing.T) {
	obj1 := &execinfrapb.RemoteProducerMetadata_TSAlterColumn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemoteProducerMetadata_TSAlterColumn{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDistSQLVersionGossipInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.DistSQLVersionGossipInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DistSQLVersionGossipInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDistSQLDrainingInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.DistSQLDrainingInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DistSQLDrainingInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescArrayRAI(t *testing.T) {
	obj1 := &execinfrapb.TableDescArray{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TableDescArray{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableDescRAI(t *testing.T) {
	obj1 := &execinfrapb.TableDesc{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TableDesc{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSSynchronizerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSSynchronizerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSSynchronizerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsSpanRAI(t *testing.T) {
	obj1 := &execinfrapb.TsSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsSpan{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSBlockFilterRAI(t *testing.T) {
	obj1 := &execinfrapb.TSBlockFilter{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSBlockFilter{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSBlockFilter_SpanRAI(t *testing.T) {
	obj1 := &execinfrapb.TSBlockFilter_Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSBlockFilter_Span{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSFillRAI(t *testing.T) {
	obj1 := &execinfrapb.TSFill{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSFill{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSStatisticReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSStatisticReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSStatisticReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSStatisticReaderSpec_ParamInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.TSStatisticReaderSpec_ParamInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSStatisticReaderSpec_ParamInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSStatisticReaderSpec_ParamsRAI(t *testing.T) {
	obj1 := &execinfrapb.TSStatisticReaderSpec_Params{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSStatisticReaderSpec_Params{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestHashpointSpanRAI(t *testing.T) {
	obj1 := &execinfrapb.HashpointSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.HashpointSpan{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSTagReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSTagReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSTagReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSTagReaderSpec_TagValueArrayRAI(t *testing.T) {
	obj1 := &execinfrapb.TSTagReaderSpec_TagValueArray{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSTagReaderSpec_TagValueArray{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSTagReaderSpec_TagIndexInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.TSTagReaderSpec_TagIndexInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSTagReaderSpec_TagIndexInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTSSamplerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TSSamplerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TSSamplerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSketchInfoRAI(t *testing.T) {
	obj1 := &execinfrapb.SketchInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SketchInfo{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestStreamReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.StreamReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.StreamReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestStreamAggregatorSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.StreamAggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.StreamAggregatorSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPostProcessSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.PostProcessSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.PostProcessSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestColumnsRAI(t *testing.T) {
	obj1 := &execinfrapb.Columns{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Columns{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableReaderSpanRAI(t *testing.T) {
	obj1 := &execinfrapb.TableReaderSpan{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TableReaderSpan{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestBackfillerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.BackfillerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.BackfillerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestJobProgressRAI(t *testing.T) {
	obj1 := &execinfrapb.JobProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.JobProgress{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReadImportDataSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReadImportDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReadImportDataSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCSVWriterSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.CSVWriterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.CSVWriterSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestBulkRowWriterSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.BulkRowWriterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.BulkRowWriterSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationIngestionPartitionSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationIngestionPartitionSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationIngestionPartitionSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationIngestionDataSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationIngestionDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationIngestionDataSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationIngestionFrontierSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationIngestionFrontierSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationIngestionFrontierSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationRecvPartitionSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationRecvPartitionSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationRecvPartitionSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationRecvDataSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationRecvDataSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationRecvDataSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationRecvFrontierSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationRecvFrontierSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationRecvFrontierSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestChangeAggregatorSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ChangeAggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ChangeAggregatorSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestChangeAggregatorSpec_WatchRAI(t *testing.T) {
	obj1 := &execinfrapb.ChangeAggregatorSpec_Watch{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ChangeAggregatorSpec_Watch{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestChangeFrontierSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ChangeFrontierSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ChangeFrontierSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProcessorSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ProcessorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProcessorSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProcessorCoreUnionRAI(t *testing.T) {
	obj1 := &execinfrapb.ProcessorCoreUnion{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProcessorCoreUnion{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestNoopCoreSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.NoopCoreSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.NoopCoreSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestLocalPlanNodeSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.LocalPlanNodeSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.LocalPlanNodeSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestRemotePlanNodeSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.RemotePlanNodeSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.RemotePlanNodeSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestMetadataTestSenderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.MetadataTestSenderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.MetadataTestSenderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestMetadataTestReceiverSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.MetadataTestReceiverSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.MetadataTestReceiverSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestValuesCoreSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ValuesCoreSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ValuesCoreSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTableReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TableReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TableReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsInsertSelSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsInsertSelSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsInsertSelSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestPayloadForDistributeModeRAI(t *testing.T) {
	obj1 := &execinfrapb.PayloadForDistributeMode{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.PayloadForDistributeMode{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsInsertProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsInsertProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsInsertProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSpanRAI(t *testing.T) {
	obj1 := &execinfrapb.Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.Span{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsDeleteProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsDeleteProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsDeleteProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDeleteEntityGroupRAI(t *testing.T) {
	obj1 := &execinfrapb.DeleteEntityGroup{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DeleteEntityGroup{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsTagUpdateProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsTagUpdateProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsTagUpdateProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsCreateTableProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsCreateTableProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsCreateTableProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsAlterProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsAlterProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsAlterProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestIndexSkipTableReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.IndexSkipTableReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.IndexSkipTableReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestJoinReaderSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.JoinReaderSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.JoinReaderSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSorterSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.SorterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SorterSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestDistinctSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.DistinctSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.DistinctSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestOrdinalitySpecRAI(t *testing.T) {
	obj1 := &execinfrapb.OrdinalitySpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.OrdinalitySpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestZigzagJoinerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ZigzagJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ZigzagJoinerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestMergeJoinerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.MergeJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.MergeJoinerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestHashJoinerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.HashJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.HashJoinerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestBatchLookupJoinerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.BatchLookupJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.BatchLookupJoinerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestAggregatorSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.AggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.AggregatorSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestAggregatorSpec_AggregationRAI(t *testing.T) {
	obj1 := &execinfrapb.AggregatorSpec_Aggregation{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.AggregatorSpec_Aggregation{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestInterleavedReaderJoinerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.InterleavedReaderJoinerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.InterleavedReaderJoinerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestInterleavedReaderJoinerSpec_TableRAI(t *testing.T) {
	obj1 := &execinfrapb.InterleavedReaderJoinerSpec_Table{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.InterleavedReaderJoinerSpec_Table{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestProjectSetSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ProjectSetSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ProjectSetSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpec_FuncRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec_Func{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec_Func{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpec_FrameRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec_Frame{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec_Frame{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpec_Frame_BoundRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec_Frame_Bound{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec_Frame_Bound{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpec_Frame_BoundsRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec_Frame_Bounds{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec_Frame_Bounds{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestWindowerSpec_WindowFnRAI(t *testing.T) {
	obj1 := &execinfrapb.WindowerSpec_WindowFn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.WindowerSpec_WindowFn{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationServiceCallerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationServiceCallerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationServiceCallerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestReplicationServiceCallerSpec_DisasterNodeRAI(t *testing.T) {
	obj1 := &execinfrapb.ReplicationServiceCallerSpec_DisasterNode{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.ReplicationServiceCallerSpec_DisasterNode{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestTsInsertWithCDCProSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.TsInsertWithCDCProSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.TsInsertWithCDCProSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCDCDataRAI(t *testing.T) {
	obj1 := &execinfrapb.CDCData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.CDCData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestCDCPushDataRAI(t *testing.T) {
	obj1 := &execinfrapb.CDCPushData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.CDCPushData{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSketchSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.SketchSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SketchSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSamplerSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.SamplerSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SamplerSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}

func TestSampleAggregatorSpecRAI(t *testing.T) {
	obj1 := &execinfrapb.SampleAggregatorSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()

	holder := make([]byte, 1024)
	_, _ = obj1.MarshalTo(holder)
	_, _ = obj1.XXX_Marshal(holder, true)
	empty := []byte{'"', '"'}

	_ = obj1.XXX_Unmarshal(empty)
	obj1.ProtoMessage()
	_, _ = obj1.Descriptor()

	obj2 := &execinfrapb.SampleAggregatorSpec{}
	obj2.ProtoMessage()
	obj2.Descriptor()
}
