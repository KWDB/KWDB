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
#include "ee_internal_type.h"

namespace kwdbts {

class SpecBase {
 public:
  explicit SpecBase(k_uint64 table_id) : table_id_(table_id) {}

  virtual ~SpecBase() = default;

  virtual void PrepareFlowSpec(TSFlowSpec& flow) = 0;

  void PrepareInputOutputSpec(TSFlowSpec& flow) {
    k_uint32 operator_size = flow.processors_size();
    for (k_uint32 i = 0; i < operator_size; ++i) {
      auto* processor = flow.mutable_processors(i);
      // input
      if (i != 0) {
        TSInputSyncSpec *input_spec = processor->add_input();
        input_spec->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);
        TSStreamEndpointSpec *stream_spec = input_spec->add_streams();
        stream_spec->set_type(StreamEndpointType::LOCAL);
        stream_spec->set_stream_id(i - 1);
        stream_spec->set_target_node_id(0);
        stream_spec->set_dest_processor(processor->processor_id());
      }

      // output
      TSOutputRouterSpec *output_spec = processor->add_output();
      output_spec->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);
      TSStreamEndpointSpec *output_stream_spec = output_spec->add_streams();
      output_stream_spec->set_type(StreamEndpointType::LOCAL);
      output_stream_spec->set_stream_id(i);
      output_stream_spec->set_target_node_id(0);
      if (i != operator_size - 1) {
        output_stream_spec->set_dest_processor(flow.mutable_processors(i + 1)->processor_id());
      } else {
        output_stream_spec->set_dest_processor(processor->processor_id() + 1);
        processor->set_final_ts_processor(true);
      }
    }
  }

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

    auto tag_post = KNEW PostProcessSpec();
    initTagReaderPostSpec(*tag_post);
    tag_read_processor->set_allocated_post(tag_post);
  }

  void PrepareTableReaderProcessor(TSFlowSpec& flow, int processor_id) {
    auto* table_read_processor = flow.add_processors();
    table_read_processor->set_processor_id(processor_id);

    auto table_reader_input = table_read_processor->add_input();
    table_reader_input->set_type(TSInputSyncSpec_Type::TSInputSyncSpec_Type_UNORDERED);

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

    auto reader_post = KNEW PostProcessSpec();
    initTableReaderPostSpec(*reader_post);
    table_read_processor->set_allocated_post(reader_post);
  }

  void PrepareSynchronizerProcessor(TSFlowSpec& flow, int processor_id) {
    auto* synchronizer = flow.add_processors();
    synchronizer->set_processor_id(processor_id);

    auto sync_core = KNEW TSProcessorCoreUnion();
    synchronizer->set_allocated_core(sync_core);

    auto sync = KNEW TSSynchronizerSpec();
    sync_core->set_allocated_synchronizer(sync);


    auto sync_post = KNEW PostProcessSpec();
    initOutputTypes(*sync_post);

    synchronizer->set_allocated_post(sync_post);
  }

  void PrepareDistinctProcessor(TSFlowSpec& flow, int processor_id) {
    auto* distinct_processor = flow.add_processors();
    distinct_processor->set_processor_id(processor_id);

    auto distinct_core = KNEW TSProcessorCoreUnion();
    distinct_processor->set_allocated_core(distinct_core);

    auto *spec = KNEW DistinctSpec();
    spec->add_distinct_columns(1);
    distinct_core->set_allocated_distinct(spec);

    auto *post = KNEW PostProcessSpec();
    post->add_output_columns(1);
    post->add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
    distinct_processor->set_allocated_post(post);
  }

  void PrepareNoopProcessor(TSFlowSpec& flow, int processor_id) {
    auto* noop_processor = flow.add_processors();
    noop_processor->set_processor_id(processor_id);

    auto noop_core = KNEW TSProcessorCoreUnion();
    noop_processor->set_allocated_core(noop_core);

    auto noop = KNEW NoopCoreSpec();
    noop_core->set_allocated_noop(noop);

    auto *post = KNEW PostProcessSpec();
    noop_processor->set_allocated_post(post);
  }

  void PrepareSorterProcessor(TSFlowSpec& flow, int processor_id) {
    auto* sorter_processor = flow.add_processors();
    sorter_processor->set_processor_id(processor_id);

    auto sorter_core = KNEW TSProcessorCoreUnion();
    sorter_processor->set_allocated_core(sorter_core);

    auto sorter = KNEW TSSorterSpec();
    sorter_core->set_allocated_sorter(sorter);

    auto ordering = KNEW TSOrdering();
    auto col1 = ordering->add_columns();
    col1->set_col_idx(2);
    col1->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC);

    auto col2 = ordering->add_columns();
    col2->set_col_idx(4);
    col2->set_direction(TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC);

    sorter->set_allocated_output_ordering(ordering);

    auto sorter_post = KNEW PostProcessSpec();
    initOutputTypes(*sorter_post);

    sorter_processor->set_allocated_post(sorter_post);
  }

  void PrepareAggProcessor(TSFlowSpec& flow, int processor_id) {
    auto* agg_processor = flow.add_processors();
    agg_processor->set_processor_id(processor_id);

    auto agg_core = KNEW TSProcessorCoreUnion();
    agg_processor->set_allocated_core(agg_core);

    agg_spec_ = KNEW TSAggregatorSpec();
    agg_core->set_allocated_aggregator(agg_spec_);
    agg_spec_->set_agg_push_down(agg_push_down_);

    initAggFuncs(*agg_spec_);

    agg_post_ = KNEW PostProcessSpec();
    initAggOutputTypes(*agg_post_);
    agg_processor->set_allocated_post(agg_post_);
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

    ts_col = spec.add_colmetas();
    ts_col->set_storage_type(roachpb::DataType::SMALLINT);
    ts_col->set_storage_len(2);
    ts_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    ts_col = spec.add_colmetas();
    ts_col->set_storage_type(roachpb::DataType::INT);
    ts_col->set_storage_len(4);
    ts_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    ts_col = spec.add_colmetas();
    ts_col->set_storage_type(roachpb::DataType::VARCHAR);
    ts_col->set_storage_len(30);
    ts_col->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_DATA);

    // primary tag
    auto p_tag = spec.add_colmetas();
    p_tag->set_storage_type(roachpb::DataType::TIMESTAMP);
    p_tag->set_storage_len(8);
    p_tag->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_PTAG);

    // normal tags
    p_tag = spec.add_colmetas();
    p_tag->set_storage_type(roachpb::DataType::INT);
    p_tag->set_storage_len(4);
    p_tag->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);

    p_tag = spec.add_colmetas();
    p_tag->set_storage_type(roachpb::DataType::VARCHAR);
    p_tag->set_storage_len(30);
    p_tag->set_column_type(roachpb::KWDBKTSColumn::ColumnType::KWDBKTSColumn_ColumnType_TYPE_TAG);

    spec.set_tableid(table_id_);
    spec.set_accessmode(TSTableReadMode::metaTable);
  }

  // include all columns of TSBS in the Tag Reader Post Spec.
  virtual void initTagReaderPostSpec(PostProcessSpec& post) {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));

    // normal tag
    post.add_output_columns(5);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(6);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    post.set_projection(true);
  }

  // include all columns of TSBS in the Table Reader Post Spec.
  virtual void initTableReaderPostSpec(PostProcessSpec& post) {
    // timestamp columns
    post.add_output_columns(0);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampTZFamily, 8));

    // metrics columns
    post.add_output_columns(1);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(2);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(3);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // include all columns of TSBS in the Output list.
  virtual void initOutputTypes(PostProcessSpec& post) {
    // timestamp columns
    post.add_output_types(MarshalToOutputType(TimestampTZFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(StringFamily, 10));

    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(StringFamily, 10));
  }

  // subclass needs to provide the Agg Function list, refer to class SpecAgg.
  virtual void initAggFuncs(TSAggregatorSpec& agg) = 0;

  // subclass needs to define the Output types, refer to class SpecAgg.
  virtual void initAggOutputTypes(PostProcessSpec& post) = 0;

 protected:
  k_uint64 table_id_;

  TSReaderSpec* table_reader_{nullptr};
  TSAggregatorSpec* agg_spec_{nullptr};
  PostProcessSpec* agg_post_{nullptr};
  bool agg_push_down_{false};
};

// select * from benchmark.cpu order by usage_system desc,hostname;
// only scan primary tag to simplify the spec
class SpecSelectWithSort : public SpecBase {
 public:
  explicit SpecSelectWithSort(k_uint64 table_id) : SpecBase(table_id) {}

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    // tag reader
    PrepareTagReaderProcessor(flow, 0);

    // table reader
    PrepareTableReaderProcessor(flow, 1);

    // synchronizer
    PrepareSynchronizerProcessor(flow, 2);

    // order by
    PrepareSorterProcessor(flow, 3);
  }

 protected:
  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(PostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 2; i++) {
      post.add_output_columns(i);
      post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(PostProcessSpec& post) override {
     // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 4));
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {};

  void initAggOutputTypes(PostProcessSpec& post) override {};
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
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(PostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 2; i++) {
      post.add_output_columns(i);
      post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(PostProcessSpec& post) override {
    // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {
    agg.add_group_cols(4);

    auto agg1 = agg.add_aggregations();
    agg1->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
    agg1->add_col_idx(2);

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
    agg4->add_col_idx(0);

    // COUNT count(usage_nice)
    auto agg5 = agg.add_aggregations();
    agg5->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
    agg5->add_col_idx(1);
  }

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(PostProcessSpec& post) override {
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));

    // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
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

    auto agg_post = KNEW PostProcessSpec();
    agg_post->CopyFrom(*agg_post_);
    table_reader_->set_allocated_aggregatorpost(agg_post);
  }

 protected:
  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(PostProcessSpec& post) override {
    // metrics columns
    post.add_output_columns(0);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampTZFamily, 8));

    post.add_output_columns(1);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));

    post.add_output_columns(3);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    kwdbts::Expression *expr1 = post.add_render_exprs();
    expr1->set_expr("Function:::time_bucket(@1, '2s':::STRING)");
    kwdbts::Expression *expr2 = post.add_render_exprs();
    expr2->set_expr("@2");
    kwdbts::Expression *expr3 = post.add_render_exprs();
    expr3->set_expr("@3");
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(PostProcessSpec& post) override {
    // metrics columns
    post.add_output_types(MarshalToOutputType(TimestampTZFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));
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
  void initAggOutputTypes(PostProcessSpec& post) override {
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));

    // metrics columns
    post.add_output_types(MarshalToOutputType(TimestampTZFamily, 8));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
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

    auto tag_post = KNEW PostProcessSpec();
    initTagReaderPostSpec(*tag_post);
    tag_read_processor->set_allocated_post(tag_post);

    auto* tag_reader_output = tag_read_processor->add_output();
    tag_reader_output->set_type(TSOutputRouterSpec_Type::TSOutputRouterSpec_Type_PASS_THROUGH);

    auto* tag_reader_stream = tag_reader_output->add_streams();
    tag_reader_stream->set_type(StreamEndpointType::LOCAL);
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
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(11);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    // secondary tag
    post.add_output_columns(12);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));

    // rel column
    post.add_output_columns(13);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::StringFamily, 10));
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(PostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 4; i++) {
      post.add_output_columns(i);
      post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(PostProcessSpec& post) override {
    // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    // primary tag
    post.add_output_types(MarshalToOutputType(StringFamily, 10));
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {}

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(PostProcessSpec& post) override {}
};


class TestDistinctSpec : public SpecBase {
 public:
  using SpecBase::SpecBase;

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    PrepareTagReaderProcessor(flow, 0);

    PrepareTableReaderProcessor(flow, 1);

    PrepareSynchronizerProcessor(flow, 2);

    PrepareDistinctProcessor(flow, 3);
  }

  // include hostname in the Tag Reader Spec.
  void initTagReaderPostSpec(PostProcessSpec& post) override {
    // primary tag
    post.add_output_columns(4);
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  }

  // output columns: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initTableReaderPostSpec(PostProcessSpec& post) override {
    // metrics columns
    for (int i = 1; i <= 2; i++) {
      post.add_output_columns(i);
      post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
    }
    // primary tag column
    initTagReaderPostSpec(post);
    post.set_projection(true);
  }

  // output column types: usage_user,usage_system,usage_idle,usage_nice,hostname
  void initOutputTypes(PostProcessSpec& post) override {
     // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));
  }

  void initAggFuncs(TSAggregatorSpec& agg) override {
    agg.add_group_cols(4);

    auto agg1 = agg.add_aggregations();
    agg1->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
    agg1->add_col_idx(2);

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
    agg4->add_col_idx(0);

    // COUNT count(usage_nice)
    auto agg5 = agg.add_aggregations();
    agg5->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
    agg5->add_col_idx(1);
  }

  // output order: hostname, min(usage_user) as min_usage, max(usage_system), sum(usage_idle), count(usage_nice)
  void initAggOutputTypes(PostProcessSpec& post) override {
    // primary tag
    post.add_output_types(MarshalToOutputType(TimestampFamily, 8));

    // metrics columns
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
    post.add_output_types(MarshalToOutputType(IntFamily, 4));
  }
};

class TestNoopSpec : public AggScanSpec {
 public:
  using AggScanSpec::AggScanSpec;

  void PrepareFlowSpec(TSFlowSpec& flow) override {
    AggScanSpec::PrepareFlowSpec(flow);

    PrepareNoopProcessor(flow, 4);
  }
};

}  // namespace kwdbts
