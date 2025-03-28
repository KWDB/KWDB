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

#pragma once

#include "ee_field.h"
#include "ee_pb_plan.pb.h"

namespace kwdbts {

class SpecBase {
 public:
  explicit SpecBase(k_uint64 table_id) : table_id_(table_id) {}

  virtual ~SpecBase() = default;

  virtual void PrepareFlowSpec(TSFlowSpec& flow) = 0;

 protected:
  void PrepareTagReaderProcessor(TSFlowSpec& flow, int processor_id) {
    auto* tag_read_processor = flow.add_processors();
    tag_read_processor->set_processor_id(processor_id);

    auto tag_reader_core = KNEW TSProcessorCoreUnion();
    tag_read_processor->set_allocated_core(tag_reader_core);

    auto tag_reader = KNEW TSTagReaderSpec();
    initTagReaderSpec(*tag_reader);
    tag_reader->set_accessmode(TSTableReadMode::tableTableMeta);
    tag_reader_core->set_allocated_tagreader(tag_reader);
    tag_reader->set_uniontype(0);
    
    auto tag_post = KNEW TSPostProcessSpec();
    initTagReaderPostSpec(*tag_post);
    tag_read_processor->set_allocated_post(tag_post);

    auto* tag_reader_output = tag_read_processor->add_output();
    tag_reader_output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);

    auto* tag_reader_stream = tag_reader_output->add_streams();
    tag_reader_stream->set_type(StreamEndpointType::LOCAL);
    tag_reader_stream->set_s_type(StreamEndpointType::LOCAL);
    tag_reader_stream->set_stream_id(processor_id);
    tag_reader_stream->set_target_node_id(0);
  }

  void PrepareTableReaderProcessor(TSFlowSpec& flow, int processor_id) {
    auto* table_read_processor = flow.add_processors();
    table_read_processor->set_processor_id(processor_id);

    auto table_reader_input = table_read_processor->add_input();
    table_reader_input->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);

    auto table_reader_stream = table_reader_input->add_streams();
    table_reader_stream->set_stream_id(processor_id - 1);
    table_reader_stream->set_type(StreamEndpointType::LOCAL);
    table_reader_stream->set_s_type(StreamEndpointType::LOCAL);
    table_reader_stream->set_target_node_id(0);

    auto table_reader_core = KNEW TSProcessorCoreUnion();
    table_read_processor->set_allocated_core(table_reader_core);

    table_reader_ = KNEW TSReaderSpec();
    table_reader_->set_tableid(table_id_);
    table_reader_->set_offsetopt(false);
    table_reader_->set_orderedscan(false);
    table_reader_->set_aggpushdown(agg_push_down_);
    table_reader_->set_tableversion(1);
    table_reader_->set_tstablereaderid(1);

    table_reader_core->set_allocated_tablereader(table_reader_);
    table_reader_->set_aggpushdown(agg_push_down_);

    auto reader_post = KNEW TSPostProcessSpec();
    initTableReaderPostSpec(*reader_post);
    table_read_processor->set_allocated_post(reader_post);

    auto* reader_output = table_read_processor->add_output();
    reader_output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);

    auto* reader_stream = reader_output->add_streams();
    reader_stream->set_stream_id(processor_id);
    reader_stream->set_type(StreamEndpointType::LOCAL);
    reader_stream->set_s_type(StreamEndpointType::LOCAL);
    reader_stream->set_target_node_id(0);
  }

  void PrepareSynchronizerProcessor(TSFlowSpec& flow, int processor_id) {
    auto* synchronizer = flow.add_processors();
    synchronizer->set_processor_id(processor_id);

    auto sync_input = synchronizer->add_input();
    sync_input->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);

    auto sync_stream = sync_input->add_streams();
    sync_stream->set_type(StreamEndpointType::LOCAL);
    sync_stream->set_s_type(StreamEndpointType::LOCAL);
    sync_stream->set_stream_id(processor_id - 1);
    sync_stream->set_target_node_id(0);

    auto sync_core = KNEW TSProcessorCoreUnion();
    synchronizer->set_allocated_core(sync_core);

    auto sync = KNEW TSSynchronizerSpec();
    sync_core->set_allocated_synchronizer(sync);


    auto sync_post = KNEW TSPostProcessSpec();
    initOutputTypes(*sync_post);

    synchronizer->set_allocated_post(sync_post);

    auto sync_output = synchronizer->add_output();
    auto sync_output_stream = sync_output->add_streams();
    sync_output_stream->set_stream_id(processor_id);
    sync_output_stream->set_type(StreamEndpointType::LOCAL);
    sync_output_stream->set_s_type(StreamEndpointType::LOCAL);
    sync_output_stream->set_target_node_id(0);
  }

  void PrepareSorterProcessor(TSFlowSpec& flow, int processor_id) {
    auto* sorter_processor = flow.add_processors();
    sorter_processor->set_processor_id(processor_id);

    auto sorter_input = sorter_processor->add_input();
    sorter_input->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);

    auto sorter_stream = sorter_input->add_streams();
    sorter_stream->set_type(StreamEndpointType::LOCAL);
    sorter_stream->set_s_type(StreamEndpointType::LOCAL);
    sorter_stream->set_stream_id(processor_id - 1);
    sorter_stream->set_target_node_id(0);

    auto sorter_core = KNEW TSProcessorCoreUnion();
    sorter_processor->set_allocated_core(sorter_core);

    auto sorter = KNEW TSSorterSpec();
    sorter_core->set_allocated_sorter(sorter);

    auto ordering = KNEW TSOrdering();
    auto col1 = ordering->add_columns();
    col1->set_col_idx(2);
    col1->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC);

    auto col2 = ordering->add_columns();
    col2->set_col_idx(11);
    col2->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);

    sorter->set_allocated_output_ordering(ordering);

    auto sorter_post = KNEW TSPostProcessSpec();
    initOutputTypes(*sorter_post);

    sorter_processor->set_allocated_post(sorter_post);

    auto sorter_output = sorter_processor->add_output();
    auto sorter_output_stream = sorter_output->add_streams();
    sorter_output_stream->set_stream_id(processor_id);
    sorter_output_stream->set_type(StreamEndpointType::LOCAL);
    sorter_output_stream->set_s_type(StreamEndpointType::LOCAL);
    sorter_output_stream->set_target_node_id(0);
  }

  void PrepareAggProcessor(TSFlowSpec& flow, int processor_id) {
    auto* agg_processor = flow.add_processors();
    agg_processor->set_processor_id(processor_id);

    auto agg_input = agg_processor->add_input();
    agg_input->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);

    auto agg_stream = agg_input->add_streams();
    agg_stream->set_stream_id(processor_id - 1);
    agg_stream->set_type(StreamEndpointType::LOCAL);
    agg_stream->set_s_type(StreamEndpointType::LOCAL);
    agg_stream->set_target_node_id(0);

    auto agg_core = KNEW TSProcessorCoreUnion();
    agg_processor->set_allocated_core(agg_core);

    agg_spec_ = KNEW TSAggregatorSpec();
    agg_core->set_allocated_aggregator(agg_spec_);
    agg_spec_->set_agg_push_down(agg_push_down_);

    initAggFuncs(*agg_spec_);

    agg_post_ = KNEW TSPostProcessSpec();
    initAggOutputTypes(*agg_post_);
    agg_processor->set_allocated_post(agg_post_);

    auto* reader_output = agg_processor->add_output();
    reader_output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);

    auto* reader_stream = reader_output->add_streams();
    reader_stream->set_stream_id(processor_id);
    reader_stream->set_type(StreamEndpointType::LOCAL);
    reader_stream->set_s_type(StreamEndpointType::LOCAL);
    reader_stream->set_target_node_id(0);
  }

  // include all columns of TSBS in the Tag Reader Spec.
  virtual void initTagReaderSpec(TSTagReaderSpec& spec) {
    // timestamp column
    spec.set_tableversion(1);
    spec.set_only_tag(false);
    auto ts_col = spec.add_colmetas();
    ts_col->set_storage_type(roachpb::DataType::TIMESTAMP);
    ts_col->set_storage_len(8);
    ts_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    // metrics column
    for (int i = 0; i < 10; i++) {
      auto metrics_col = spec.add_colmetas();
      metrics_col->set_storage_type(roachpb::DataType::BIGINT);
      metrics_col->set_storage_len(8);
      metrics_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
    }

    // primary tag
    auto p_tag = spec.add_colmetas();
    p_tag->set_storage_type(roachpb::DataType::CHAR);
    p_tag->set_storage_len(30);
    p_tag->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);

    // normal tags
    for (int i = 0; i < 9; i++) {
      auto tag_col = spec.add_colmetas();
      tag_col->set_storage_type(roachpb::DataType::CHAR);
      tag_col->set_storage_len(30);
      tag_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }

    spec.set_tableid(table_id_);
    spec.set_accessmode(TSTableReadMode::metaTable);
  }

  // include all columns of TSBS in the Tag Reader Post Spec.
  virtual void initTagReaderPostSpec(TSPostProcessSpec& post) {
    // primary tag
    post.add_outputcols(11);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);

    // normal tag
    for (int i = 0; i < 9; i++) {
      post.add_outputcols(i + 12);
      post.add_outputtypes(KWDBTypeFamily::StringFamily);
    }
    post.set_projection(true);
  }

  // include all columns of TSBS in the Table Reader Post Spec.
  virtual void initTableReaderPostSpec(TSPostProcessSpec& post) {
    // timestamp columns
    post.add_outputcols(0);
    post.add_outputtypes(KWDBTypeFamily::TimestampTZFamily);

    // metrics columns
    for (int i = 0; i < 10; i++) {
      post.add_outputcols(i + 1);
      post.add_outputtypes(KWDBTypeFamily::IntFamily);
    }
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // include all columns of TSBS in the Output list.
  virtual void initOutputTypes(TSPostProcessSpec& post) {
    // timestamp columns
    post.add_outputtypes(TimestampTZFamily);

    // metrics columns
    for (int i = 0; i < 10; i++) {
      post.add_outputtypes(IntFamily);
    }

    // primary tag
    post.add_outputtypes(StringFamily);

    // normal tag
    for (int i = 0; i < 9; i++) {
      post.add_outputtypes(StringFamily);
    }
  }

  // subclass needs to provide the Agg Function list, refer to class SpecAgg.
  virtual void initAggFuncs(TSAggregatorSpec& agg) = 0;

  // subclass needs to define the Output types, refer to class SpecAgg.
  virtual void initAggOutputTypes(TSPostProcessSpec& post) = 0;

 protected:
  k_uint64 table_id_;

  TSReaderSpec* table_reader_{nullptr};
  TSAggregatorSpec* agg_spec_{nullptr};
  TSPostProcessSpec* agg_post_{nullptr};
  bool agg_push_down_{false};
};

// select * from benchmark.cpu order by usage_system desc,hostname;
// only scan primary tag to simplify the spec
class SpecSelectWithSort : public SpecBase {

 public:
  explicit SpecSelectWithSort(k_uint64 table_id) : SpecBase(table_id) {}

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    //tag reader
    PrepareTagReaderProcessor(flow, 0);

    //table reader
    PrepareTableReaderProcessor(flow, 1);

    // synchronizer
    PrepareSynchronizerProcessor(flow, 2);

    // order by
    PrepareSorterProcessor(flow, 3);
  }

 protected:
  void initAggFuncs(TSAggregatorSpec& agg) override {};

  void initAggOutputTypes(TSPostProcessSpec& post) override {};
};

// select hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
// from benchmark.cpu group by hostname  order by min_usage;
class SpecAgg : public SpecBase {
 public:
  explicit SpecAgg(k_uint64 table_id) : SpecBase(table_id) {}

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    //tag reader
    PrepareTagReaderProcessor(flow, 0);

    //table reader
    PrepareTableReaderProcessor(flow, 1);

    //agg
    PrepareAggProcessor(flow, 2);

    // synchronizer
    PrepareSynchronizerProcessor(flow, 3);
  }

 protected:
  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(TSPostProcessSpec& post) override {
    // primary tag
    post.add_outputcols(11);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(TSPostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 4; i++) {
      post.add_outputcols(i);
      post.add_outputtypes(KWDBTypeFamily::IntFamily);
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(TSPostProcessSpec& post) override {
    // metrics columns
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    // primary tag
    post.add_outputtypes(StringFamily);
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {
    agg.add_group_cols(4);

    auto agg1 = agg.add_aggregations();
    agg1->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
    agg1->add_col_idx(4);

    // MIN min(usage_user)
    auto agg2 = agg.add_aggregations();
    agg2->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN);
    agg2->add_col_idx(0);

    // MAX max(usage_system)
    auto agg3 = agg.add_aggregations();
    agg3->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
    agg3->add_col_idx(1);

    // SUM sum(usage_idle)
    auto agg4 = agg.add_aggregations();
    agg4->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
    agg4->add_col_idx(2);

    // COUNT count(usage_nice)
    auto agg5 = agg.add_aggregations();
    agg5->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
    agg5->add_col_idx(3);
  }

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(TSPostProcessSpec& post) override {
    // primary tag
    post.add_outputtypes(StringFamily);

    // metrics columns
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
  }
};

//SELECT time_bucket(k_timestamp, '1s') as k_timestamp, hostname, max(usage_user)
//FROM benchmark.cpu
//    WHERE k_timestamp >= '2023-01-01 00:00:00'
//AND k_timestamp < '2024-01-01 00:00:00'
//GROUP BY hostname, time_bucket(k_timestamp, '1s')
//ORDER BY hostname, time_bucket(k_timestamp, '1s');

class AggScanSpec : public SpecAgg {
 public:
  explicit AggScanSpec(k_uint64 table_id) : SpecAgg(table_id) {
    agg_push_down_ = true;
  }

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    SpecAgg::PrepareFlowSpec(flow);

    auto agg_spec = KNEW TSAggregatorSpec();
    agg_spec->CopyFrom(*agg_spec_);
    table_reader_->set_allocated_aggregator(agg_spec);

    auto agg_post = KNEW TSPostProcessSpec();
    agg_post->CopyFrom(*agg_post_);
    table_reader_->set_allocated_aggregatorpost(agg_post);
  }

 protected:
  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(TSPostProcessSpec& post) override {
    // primary tag
    post.add_outputcols(11);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(TSPostProcessSpec& post) override {
    // metrics columns
    post.add_outputcols(0);
    post.add_outputtypes(KWDBTypeFamily::TimestampTZFamily);

    post.add_outputcols(1);
    post.add_outputtypes(KWDBTypeFamily::IntFamily);

    post.add_outputcols(11);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);

//    post.set_filter(
//        "(@1 >= '2023-01-01 00:00:00+00:00':::TIMESTAMPTZ) AND (@1 < '2025-01-01 00:00:00+00:00':::TIMESTAMPTZ)");
    post.add_renders("Function:::time_bucket(@1, '2s':::STRING)");
    post.add_renders("@2");
    post.add_renders("@3");

    post.set_projection(false);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(TSPostProcessSpec& post) override {
    // metrics columns
    post.add_outputtypes(TimestampTZFamily);
    post.add_outputtypes(IntFamily);
    // primary tag
    post.add_outputtypes(StringFamily);
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {
    agg.add_group_cols(2);
    agg.add_group_cols(0);

    auto agg1 = agg.add_aggregations();
    agg1->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
    agg1->add_col_idx(2);

    auto agg2 = agg.add_aggregations();
    agg2->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
    agg2->add_col_idx(0);

    // MAX max(usage_system)
    auto agg3 = agg.add_aggregations();
    agg3->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
    agg3->add_col_idx(1);
  }

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(TSPostProcessSpec& post) override {
    // primary tag
    post.add_outputtypes(StringFamily);

    // metrics columns
    post.add_outputtypes(TimestampTZFamily);
    post.add_outputtypes(IntFamily);
  }
};

// select hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
// from benchmark.cpu group by hostname;
class HashTagScanSpec : public SpecBase {
 public:
  HashTagScanSpec(k_uint64 table_id, TSTableReadMode access_mode) : SpecBase(table_id), access_mode_(access_mode) {}

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    //tag reader
    PrepareTagReaderProcessor(flow, 0);

    //table reader
    PrepareTableReaderProcessor(flow, 1);

    // synchronizer
    PrepareSynchronizerProcessor(flow, 3);
  }

 private:
  TSTableReadMode access_mode_;

 protected:
  // prepare HashTagScan with primary tags
  void PrepareTagReaderProcessor(TSFlowSpec& flow, int processor_id) {
    auto* tag_read_processor = flow.add_processors();
    tag_read_processor->set_processor_id(processor_id);

    auto tag_reader_core = KNEW TSProcessorCoreUnion();
    tag_read_processor->set_allocated_core(tag_reader_core);

    auto tag_reader = KNEW TSTagReaderSpec();
    initTagReaderSpec(*tag_reader);
    tag_reader->set_accessmode(access_mode_);
    tag_reader->set_uniontype(0);
    tag_reader_core->set_allocated_tagreader(tag_reader);

    auto tag_post = KNEW TSPostProcessSpec();
    initTagReaderPostSpec(*tag_post);
    tag_read_processor->set_allocated_post(tag_post);

    auto* tag_reader_output = tag_read_processor->add_output();
    tag_reader_output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);

    auto* tag_reader_stream = tag_reader_output->add_streams();
    tag_reader_stream->set_type(StreamEndpointType::LOCAL);
    tag_reader_stream->set_s_type(StreamEndpointType::LOCAL);
    tag_reader_stream->set_stream_id(processor_id);
    tag_reader_stream->set_target_node_id(0);
  }

  // include all columns of TSBS in the Tag Reader Spec for HashTagScan with primary tags.
  void initTagReaderSpec(TSTagReaderSpec& spec) {
    // timestamp column
    spec.set_tableversion(1);
    spec.set_only_tag(false);
    auto ts_col = spec.add_colmetas();
    ts_col->set_storage_type(roachpb::DataType::TIMESTAMP);
    ts_col->set_storage_len(8);
    ts_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    // metrics column
    for (int i = 0; i < 10; i++) {
      auto metrics_col = spec.add_colmetas();
      metrics_col->set_storage_type(roachpb::DataType::BIGINT);
      metrics_col->set_storage_len(8);
      metrics_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
    }

    // primary tag
    auto p_tag = spec.add_colmetas();
    p_tag->set_storage_type(roachpb::DataType::CHAR);
    p_tag->set_storage_len(30);
    p_tag->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);

    // normal tags
    for (int i = 0; i < 9; i++) {
      auto tag_col = spec.add_colmetas();
      tag_col->set_storage_type(roachpb::DataType::CHAR);
      tag_col->set_storage_len(30);
      tag_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);
    }

    // rel columns
    for (int i = 0; i < 3; ++i) {
      auto rel_col = spec.add_relationalcols();
      rel_col->set_storage_type(roachpb::DataType::CHAR);
      rel_col->set_storage_len(30);
      rel_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);
    }

    // join columns
    spec.add_probecolids(0);
    spec.add_hashcolids(11);  // prob的0对应bt中的11
    spec.add_probecolids(1);
    spec.add_hashcolids(12);  // prob的1对应bt(tag表)中的12

    spec.set_tableid(table_id_);
  }

  // include hostname, secondary tag and last relational column in the Tag Reader Spec.
  void initTagReaderPostSpec(TSPostProcessSpec& post) override {
    // primary tag
    post.add_outputcols(11);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);

    // secondary tag
    post.add_outputcols(12);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);

    // rel column
    post.add_outputcols(13);
    post.add_outputtypes(KWDBTypeFamily::StringFamily);
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(TSPostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 4; i++) {
      post.add_outputcols(i);
      post.add_outputtypes(KWDBTypeFamily::IntFamily);
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(TSPostProcessSpec& post) override {
    // metrics columns
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    post.add_outputtypes(IntFamily);
    // primary tag
    post.add_outputtypes(StringFamily);
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {}

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(TSPostProcessSpec& post) override {}
};

}  // namespace kwdbts
