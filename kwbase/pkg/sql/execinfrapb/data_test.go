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

func TestErrorRAI(t *testing.T) {
	obj1 := &Error{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestExpressionRAI(t *testing.T) {
	obj1 := &Expression{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOrderingRAI(t *testing.T) {
	obj1 := &Ordering{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOrdering_ColumnRAI(t *testing.T) {
	obj1 := &Ordering_Column{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestInputSyncSpecRAI(t *testing.T) {
	obj1 := &InputSyncSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOutputRouterSpecRAI(t *testing.T) {
	obj1 := &OutputRouterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOutputRouterSpec_RangeRouterSpecRAI(t *testing.T) {
	obj1 := &OutputRouterSpec_RangeRouterSpec{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOutputRouterSpec_RangeRouterSpec_ColumnEncodingRAI(t *testing.T) {
	obj1 := &OutputRouterSpec_RangeRouterSpec_ColumnEncoding{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestOutputRouterSpec_RangeRouterSpec_SpanRAI(t *testing.T) {
	obj1 := &OutputRouterSpec_RangeRouterSpec_Span{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestDatumInfoRAI(t *testing.T) {
	obj1 := &DatumInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestProducerHeaderRAI(t *testing.T) {
	obj1 := &ProducerHeader{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestProducerDataRAI(t *testing.T) {
	obj1 := &ProducerData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestProducerMessageRAI(t *testing.T) {
	obj1 := &ProducerMessage{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadataRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_RangeInfosRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_RangeInfos{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TraceDataRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TraceData{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_RowNumRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_RowNum{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_SamplerProgressRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_SamplerProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_BulkProcessorProgressRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_BulkProcessorProgress{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_MetricsRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_Metrics{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSInsertRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSInsert{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSDeleteRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSDelete{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSTagUpdateRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSTagUpdate{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSCreateTableRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSCreateTable{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSProRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSPro{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestRemoteProducerMetadata_TSAlterColumnRAI(t *testing.T) {
	obj1 := &RemoteProducerMetadata_TSAlterColumn{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestDistSQLVersionGossipInfoRAI(t *testing.T) {
	obj1 := &DistSQLVersionGossipInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}

func TestDistSQLDrainingInfoRAI(t *testing.T) {
	obj1 := &DistSQLDrainingInfo{}
	obj1.Reset()
	_ = obj1.String()
	_ = obj1.XXX_Size()
	_ = obj1.Size()
}
