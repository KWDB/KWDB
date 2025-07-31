// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.
#ifndef KWDBTS2_EXEC_TESTS_EE_ITERATOR_DATA_TEST_H_
#define KWDBTS2_EXEC_TESTS_EE_ITERATOR_DATA_TEST_H_
#include "ee_field.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

void CreateTagReaderSpec(TSTagReaderSpec **spec, k_uint64 table_id) {
  *spec = KNEW TSTagReaderSpec();
  (*spec)->set_tableversion(1);
  (*spec)->set_only_tag(false);
  TSCol *col0 = (*spec)->add_colmetas();
  col0->set_storage_type(roachpb::DataType::BIGINT);
  col0->set_storage_len(8);
  TSCol *col1 = (*spec)->add_colmetas();
  col1->set_storage_type(roachpb::DataType::INT);
  col1->set_storage_len(4);
  (*spec)->set_tableid(table_id);
  (*spec)->set_accessmode(TSTableReadMode::metaTable);
}

void CreateReaderSpec(TSReaderSpec **spec, k_uint64 objid) {
  *spec = KNEW TSReaderSpec();

  TsSpan *span = (*spec)->add_ts_spans();
  span->set_fromtimestamp(0);
  span->set_totimestamp(100000);
  // (*spec)->set_usestatistic(false);
  (*spec)->set_tableid(objid);
  (*spec)->set_tableversion(1);
}

void CreateTSStatisticReaderSpec(TSStatisticReaderSpec **spec, k_uint64 objid) {
  *spec = KNEW TSStatisticReaderSpec();

  TsSpan *span = (*spec)->add_tsspans();
  span->set_fromtimestamp(0);
  span->set_totimestamp(100000);
  (*spec)->set_tableid(objid);
  for (int i = 0; i < 2; i++) {
    TSStatisticReaderSpec_Params *parm = (*spec)->add_paramidx();
    TSStatisticReaderSpec_ParamInfo *pa = parm->add_param();
    pa->set_value(0);
    pa->set_typ(0);
  }
}

void CreateReaderTSPostProcessSpec(TSPostProcessSpec **post) {
  *post = KNEW TSPostProcessSpec();
  (*post)->set_limit(3);
  (*post)->set_offset(1);
  (*post)->add_renders("@1");
  (*post)->add_renders("@2");
  (*post)->add_outputcols(0);
  (*post)->add_outputcols(1);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}
void CreateMergeSpec(TSSynchronizerSpec **spec) {
  *spec = KNEW TSSynchronizerSpec();
}
void CreateMergeTSPostProcessSpec(TSPostProcessSpec **post) {
  *post = KNEW TSPostProcessSpec();
  // (*post)->add_primarytags();
  // (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  // (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}
void CreateSortSpec(TSSorterSpec **spec) {
  *spec = KNEW TSSorterSpec();
  TSOrdering *ordering = KNEW TSOrdering();
  (*spec)->set_allocated_output_ordering(ordering);
  TSOrdering_Column *col = ordering->add_columns();
  col->set_col_idx(0);
}

void CreateSortTSPostProcessSpec(TSPostProcessSpec **post) {
  *post = KNEW TSPostProcessSpec();
  // (*post)->add_primarytags();
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}

void CreateNoopSpec(TSNoopSpec **spec) {
  *spec = KNEW TSNoopSpec();
}
void CreateNoopTSPostProcessSpec(TSPostProcessSpec **post) {
  *post = KNEW TSPostProcessSpec();
  // (*post)->add_primarytags();
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}

void CreateDistinctSpec(DistinctSpec **spec) {
  (*spec) = KNEW DistinctSpec();
  (*spec)->add_distinct_columns(0);
}

void CreateAggregatorSpec(TSAggregatorSpec **spec) {
  (*spec) = KNEW TSAggregatorSpec();
  (*spec)->add_group_cols(0);
  TSAggregatorSpec_Aggregation *item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::ANY_NOT_NULL));
  item->add_col_idx(0);
  item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::COUNT));
  item->add_col_idx(0);

  item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::MAX));
  item->add_col_idx(1);

  item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::MIN));
  item->add_col_idx(0);

  item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::SUM));
  item->add_col_idx(1);

  // item = (*spec)->add_aggregations();
  // item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::STDDEV));
  // item->add_col_idx(0);

  item = (*spec)->add_aggregations();
  item->set_func(static_cast<TSAggregatorSpec_Func>(Sumfunctype::AVG));
  item->add_col_idx(1);
}

void CreateAggPostProcessSpec(TSPostProcessSpec **post) {
  (*post) = KNEW TSPostProcessSpec();
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  // (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}

void CreateDistinctPostProcessSpec(TSPostProcessSpec **post) {
  (*post) = KNEW TSPostProcessSpec();
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
  (*post)->add_outputtypes(KWDBTypeFamily::IntFamily);
}

void CreateScanFlowSpec(kwdbContext_p ctx, TSFlowSpec **flow, k_uint64 objid) {
  TSTagReaderSpec *spec{nullptr};
  TSPostProcessSpec *post{nullptr};
  CreateTagReaderSpec(&spec, objid);
  CreateReaderTSPostProcessSpec(&post);
  *flow = KNEW TSFlowSpec();
  TSProcessorSpec *processor = (*flow)->add_processors();
  // TSInputSyncSpec* input = processor->add_input();
  // TSStreamEndpointSpec* stream = input->add_streams();
  // stream->set_stream_id(0);
  TSProcessorCoreUnion *core = KNEW TSProcessorCoreUnion();
  processor->set_allocated_core(core);
  core->set_allocated_tagreader(spec);
  processor->set_allocated_post(post);
  processor->set_processor_id(0);
}

void CreateDistinctSpecs(DistinctSpec **spec, TSPostProcessSpec **post) {
  CreateDistinctSpec(spec);
  CreateDistinctPostProcessSpec(post);
}

void CreateAggSpecs(TSAggregatorSpec **spec, TSPostProcessSpec **post) {
  CreateAggregatorSpec(spec);
  CreateAggPostProcessSpec(post);
}

void CreateMergeSpecs(TSSynchronizerSpec **spec, TSPostProcessSpec **post) {
  CreateMergeSpec(spec);
  CreateMergeTSPostProcessSpec(post);
}

void CreateSortSpecs(TSSorterSpec **spec, TSPostProcessSpec **post) {
  CreateSortSpec(spec);
  CreateSortTSPostProcessSpec(post);
}
void CreateNoopSpecs(TSNoopSpec **spec, TSPostProcessSpec **post) {
  CreateNoopSpec(spec);
  CreateNoopTSPostProcessSpec(post);
}
void CreateSourceSpec(TSInputSyncSpec **spec) {
  *spec = KNEW TSInputSyncSpec();
  (*spec)->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);
  TSStreamEndpointSpec *stream = (*spec)->add_streams();
  stream->set_stream_id(0);
  stream->set_type(StreamEndpointType::REMOTE);
  stream->set_target_node_id(1);
  stream->set_dest_processor(1);

  TSOrdering *ordering = KNEW TSOrdering();
  (*spec)->set_allocated_ordering(ordering);
  TSOrdering_Column *col = ordering->add_columns();
  col->set_col_idx(0);
}

void CreateSourceMergeSpec(TSInputSyncSpec **spec) {
  *spec = KNEW TSInputSyncSpec();
  (*spec)->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);
  TSStreamEndpointSpec *stream = (*spec)->add_streams();
  stream->set_stream_id(0);
  stream->set_type(StreamEndpointType::REMOTE);
  stream->set_target_node_id(1);
  stream->set_dest_processor(1);

  TSOrdering *ordering = KNEW TSOrdering();
  (*spec)->set_allocated_ordering(ordering);
  TSOrdering_Column *col = ordering->add_columns();
  col->set_col_idx(0);
  col->set_direction(kwdbts::TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);
  TSOrdering_Column *col1 = ordering->add_columns();
  col1->set_col_idx(1);
  col1->set_direction(kwdbts::TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);
}

void CreateSinkSpec(TSOutputRouterSpec **spec) {
  *spec = KNEW TSOutputRouterSpec();
  // (*spec)->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_HASH);
  // (*spec)->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_MIRROR);
  // (*spec)->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
  (*spec)->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_BY_GATHER);
  TSStreamEndpointSpec *stream = (*spec)->add_streams();
  stream->set_stream_id(0);
  stream->set_type(StreamEndpointType::REMOTE);
  // stream->set_type(StreamEndpointType::LOCAL);
  stream->set_target_node_id(0);
  stream->set_dest_processor(1);
  (*spec)->add_hash_columns(0);

  stream = (*spec)->add_streams();
  stream->set_stream_id(1);
  stream->set_type(StreamEndpointType::LOCAL);
  // stream->set_type(StreamEndpointType::LOCAL);
  stream->set_target_node_id(1);
  stream->set_dest_processor(1);
  (*spec)->add_hash_columns(0);
}
void CreateSinkSpecs(TSOutputRouterSpec **spec, TSPostProcessSpec **post) {
  CreateSinkSpec(spec);
  CreateNoopTSPostProcessSpec(post);
}

void CreateSourceSpecs(TSInputSyncSpec **spec, TSPostProcessSpec **post) {
  CreateSourceSpec(spec);
  CreateNoopTSPostProcessSpec(post);
}

void CreateSourceMergeSpecs(TSInputSyncSpec **spec, TSPostProcessSpec **post) {
  CreateSourceMergeSpec(spec);
  CreateNoopTSPostProcessSpec(post);
}

/**
 * Create a FlowSpec that contains all operators
 * readerIter、MergeIter、SortIter、AggIter、TsSamplerIter、NoopIter、
 * merge
 *   |
 * reader
 */
void CreateScanFlowSpecAllCases(kwdbContext_p ctx, TSFlowSpec **flow,
                                k_uint64 objid) {
  *flow = KNEW TSFlowSpec();
  // create Merge
  TSSynchronizerSpec *TSSynchronizerSpec{nullptr};
  TSPostProcessSpec *mergepost{nullptr};
  CreateMergeSpecs(&TSSynchronizerSpec, &mergepost);
  // add child
  TSReaderSpec *spec{nullptr};
  TSPostProcessSpec *post{nullptr};
  CreateReaderSpec(&spec, objid);
  CreateReaderTSPostProcessSpec(&post);
  // add child
  TSAggregatorSpec *aggspec{nullptr};
  TSPostProcessSpec *aggpost{nullptr};
  CreateAggSpecs(&aggspec, &aggpost);

  TSTagReaderSpec *tagspec{nullptr};
  TSPostProcessSpec *tagpost{nullptr};
  CreateTagReaderSpec(&tagspec, objid);
  tagpost = KNEW TSPostProcessSpec();

  TSProcessorSpec *tagprocessor = (*flow)->add_processors();
  TSProcessorCoreUnion *tagcore = KNEW TSProcessorCoreUnion();
  tagprocessor->set_allocated_core(tagcore);
  tagcore->set_allocated_tagreader(tagspec);
  tagprocessor->set_allocated_post(tagpost);
  tagprocessor->set_processor_id(0);

  TSProcessorSpec *processor = (*flow)->add_processors();
  TSInputSyncSpec *input = processor->add_input();
  TSStreamEndpointSpec *stream = input->add_streams();
  stream->set_stream_id(0);
  TSProcessorCoreUnion *core = KNEW TSProcessorCoreUnion();
  processor->set_allocated_core(core);
  core->set_allocated_tablereader(spec);
  processor->set_allocated_post(post);
  processor->set_processor_id(1);

  TSProcessorSpec *aggprocessor = (*flow)->add_processors();
  TSProcessorCoreUnion *aggcore = KNEW TSProcessorCoreUnion();
  aggprocessor->set_allocated_core(aggcore);
  TSInputSyncSpec *agginput = aggprocessor->add_input();
  TSStreamEndpointSpec *aggstream = agginput->add_streams();
  aggstream->set_stream_id(1);
  aggcore->set_allocated_aggregator(aggspec);
  aggprocessor->set_allocated_post(aggpost);
  aggprocessor->set_processor_id(0);

  TSProcessorSpec *mergeprocessor = (*flow)->add_processors();
  TSInputSyncSpec *mergeinput = mergeprocessor->add_input();
  TSStreamEndpointSpec *mergestream = mergeinput->add_streams();
  mergestream->set_stream_id(0);
  TSProcessorCoreUnion *mergecore = KNEW TSProcessorCoreUnion();
  mergecore->set_allocated_synchronizer(TSSynchronizerSpec);
  mergeprocessor->set_allocated_core(mergecore);
  mergeprocessor->set_allocated_post(mergepost);
  mergeprocessor->set_processor_id(2);
}
}  // namespace kwdbts
#endif  // KWDBTS2_EXEC_TESTS_EE_ITERATOR_DATA_TEST_H_
