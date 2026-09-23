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

#include "ee_aggregate_op.h"
#include "ee_post_agg_scan_op.h"

#include <algorithm>
#include <memory>
#include <deque>
#include <string>
#include <vector>

#include "ee_aggregate_func.h"
#include "ee_common.h"
#include "ee_field.h"
#include "ee_field_agg.h"
#include "ee_field_const.h"
#include "ee_global.h"
#include "ee_internal_type.h"
#include "ee_kwthd_context.h"
#include "ee_mempool.h"
#include "gtest/gtest.h"

extern "C" {
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}

namespace kwdbts {
namespace {

class OwnedColumnDataChunk : public DataChunk {
 public:
  OwnedColumnDataChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedColumnDataChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

class DummyInputOperator : public BaseOperator {
 public:
  explicit DummyInputOperator(std::vector<Field*> fields = {})
      : BaseOperator(nullptr, nullptr, nullptr, 0) {
    output_fields_ = std::move(fields);
  }

  ~DummyInputOperator() override {
    for (auto*& field : output_fields_) {
      SafeDeletePointer(field);
    }
    output_fields_.clear();
  }

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }
};

class ChunkInputOperator : public BaseOperator {
 public:
  ChunkInputOperator(std::vector<Field*> fields, std::vector<DataChunkPtr> chunks)
      : BaseOperator(nullptr, nullptr, nullptr, 0),
        chunks_(std::move(chunks)) {
    output_fields_ = std::move(fields);
  }

  ~ChunkInputOperator() override {
    for (auto*& field : output_fields_) {
      SafeDeletePointer(field);
    }
    output_fields_.clear();
  }

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    next_chunk_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Start(kwdbContext_p) override {
    next_chunk_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr& chunk) override {
    if (next_chunk_ >= chunks_.size()) {
      chunk = nullptr;
      return EEIteratorErrCode::EE_END_OF_RECORD;
    }
    chunk = std::move(chunks_[next_chunk_++]);
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override {
    next_chunk_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }

 private:
  std::vector<DataChunkPtr> chunks_;
  size_t next_chunk_{0};
};

class CloneableChunkInputOperator : public ChunkInputOperator {
 public:
  using ChunkInputOperator::ChunkInputOperator;

  BaseOperator* Clone() override {
    if (clone_returns_null_) {
      return nullptr;
    }
    return new CloneableChunkInputOperator(std::vector<Field*>{},
                                           std::vector<DataChunkPtr>{});
  }

  bool clone_returns_null_{false};
};

class ScriptedChunkInputOperator : public BaseOperator {
 public:
  struct Step {
    EEIteratorErrCode code;
    DataChunkPtr chunk;
  };

  ScriptedChunkInputOperator(std::vector<Field*> fields, std::deque<Step> steps)
      : BaseOperator(nullptr, nullptr, nullptr, 0), steps_(std::move(steps)) {
    output_fields_ = std::move(fields);
  }

  ~ScriptedChunkInputOperator() override {
    for (auto*& field : output_fields_) {
      SafeDeletePointer(field);
    }
    output_fields_.clear();
  }

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    next_step_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Start(kwdbContext_p) override {
    next_step_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr& chunk) override {
    if (next_step_ >= steps_.size()) {
      chunk = nullptr;
      return EEIteratorErrCode::EE_END_OF_RECORD;
    }
    auto& step = steps_[next_step_++];
    chunk = std::move(step.chunk);
    return step.code;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override {
    next_step_ = 0;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    return EEIteratorErrCode::EE_OK;
  }

 private:
  std::deque<Step> steps_;
  size_t next_step_{0};
};

class CloneablePostAggInputOperator : public DummyInputOperator {
 public:
  using DummyInputOperator::DummyInputOperator;

  BaseOperator* Clone() override {
    if (clone_returns_null_) {
      return nullptr;
    }
    return new CloneablePostAggInputOperator();
  }

  bool clone_returns_null_{false};
};

class CountingInputOperator : public DummyInputOperator {
 public:
  using DummyInputOperator::DummyInputOperator;

  EEIteratorErrCode Reset(kwdbContext_p) override {
    ++reset_calls_;
    return EEIteratorErrCode::EE_OK;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return EEIteratorErrCode::EE_OK;
  }

  int reset_calls_{0};
  int close_calls_{0};
};

class InspectableAggregator : public BaseAggregator {
 public:
  InspectableAggregator(TSAggregatorSpec* spec, PostProcessSpec* post)
      : BaseAggregator(nullptr, spec, post, nullptr, 0) {}

  ~InspectableAggregator() override {
    if (param_.aggs_ != nullptr) {
      for (size_t i = 0; i < param_.aggs_size_; ++i) {
        SafeDeletePointer(param_.aggs_[i]);
      }
      free(param_.aggs_);
      param_.aggs_ = nullptr;
      param_.aggs_size_ = 0;
    }
  }

  OperatorType Type() override { return OperatorType::OPERATOR_HASH_GROUP_BY; }

  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  BaseOperator* Clone() override { return nullptr; }

  void SetAggFields(const std::vector<FieldAggNum*>& fields) {
    param_.aggs_size_ = fields.size();
    param_.aggs_ = static_cast<Field**>(malloc(fields.size() * sizeof(Field*)));
    memset(param_.aggs_, 0, fields.size() * sizeof(Field*));
    for (size_t i = 0; i < fields.size(); ++i) {
      param_.aggs_[i] = fields[i];
    }
  }

  void CallCalculateAggOffsets() { CalculateAggOffsets(); }

  k_uint32 CallCalculateAggEffectiveWidth(DatumRowPtr bucket) const {
    return CalculateAggEffectiveWidth(bucket);
  }

  void CallResolveGroupByCols(kwdbContext_p ctx) { ResolveGroupByCols(ctx); }

  void CallInitFirstLastTimeStamp(DatumRowPtr ptr) { InitFirstLastTimeStamp(ptr); }

  void SetAggNullOffset(k_uint32 offset) { agg_null_offset_ = offset; }

  std::vector<k_uint32>& MutableFuncOffsets() { return func_offsets_; }

  std::vector<k_uint32>& MutableGroupCols() { return group_cols_; }

  std::vector<AggregateFunc*>& MutableFuncs() { return funcs_; }

  std::vector<roachpb::DataType>& MutableColTypes() { return col_types_; }

  std::vector<k_uint32>& MutableColLens() { return col_lens_; }

  std::vector<bool>& MutableAllowNull() { return col_allow_null_; }

  KStatus CallAccumulateRowIntoBucket(kwdbContext_p ctx, DatumRowPtr bucket,
                                      k_uint32 agg_null_offset, IChunk* chunk,
                                      k_uint32 line) {
    return accumulateRowIntoBucket(ctx, bucket, agg_null_offset, chunk, line);
  }
};

class InspectablePostAggOperator : public PostAggScanOperator {
 public:
  InspectablePostAggOperator(TSAggregatorSpec* spec, PostProcessSpec* post)
      : PostAggScanOperator(nullptr, spec, post, nullptr, 0) {}

  using PostAggScanOperator::AddDependency;
  using PostAggScanOperator::CalculateAggOffsets;
  using PostAggScanOperator::constructAggResults;
  using PostAggScanOperator::accumulateRows;
  using PostAggScanOperator::Init;
  using PostAggScanOperator::ResolveAggFuncs;
  using PostAggScanOperator::ResolveGroupByCols;
  using PostAggScanOperator::getAggResults;

  std::vector<Field*>& MutableOutputFields() { return output_fields_; }
  std::vector<TSAggregatorSpec_Aggregation>& MutableAggregations() {
    return aggregations_;
  }
  std::vector<AggregateFunc*>& MutableFuncs() { return funcs_; }
  std::vector<k_uint32>& MutableFuncOffsets() { return func_offsets_; }
  std::vector<k_uint32>& MutableGroupCols() { return group_cols_; }
  std::queue<DataChunkPtr>& MutableProcessedChunks() { return processed_chunks_; }
  std::map<k_uint32, k_uint32>& MutableSourceTargetMap() {
    return agg_source_target_col_map_;
  }
  ColumnInfo*& MutableAggOutputColInfo() { return agg_output_col_info_; }
  k_int32& MutableAggOutputColNum() { return agg_output_col_num_; }
  bool& MutablePassAgg() { return pass_agg_; }
  k_uint32& MutableLimit() { return limit_; }
  k_uint32& MutableCurOffset() { return cur_offset_; }
  k_uint32& MutableExaminedRows() { return examined_rows_; }
  k_uint32& MutableTotalReadRow() { return total_read_row_; }
  k_uint64& MutableAggResultCounter() { return agg_result_counter_; }
  k_uint32& MutableAggNullOffset() { return agg_null_offset_; }
  k_uint32& MutableAggRowSize() { return agg_row_size_; }
  std::unique_ptr<BaseHashTable>& MutableHashTable() { return ht_; }
  bool IsDone() const { return is_done_; }
  std::vector<roachpb::DataType>& MutableColTypes() { return col_types_; }
  std::vector<k_uint32>& MutableColLens() { return col_lens_; }
  std::vector<bool>& MutableAllowNull() { return col_allow_null_; }
};

class InspectableHashAggregateOperator : public HashAggregateOperator {
 public:
  InspectableHashAggregateOperator(TSAggregatorSpec* spec, PostProcessSpec* post)
      : HashAggregateOperator(nullptr, spec, post, nullptr, 0) {}

  using HashAggregateOperator::Clone;
  using HashAggregateOperator::Init;
  using HashAggregateOperator::Next;
  using HashAggregateOperator::Start;
  using HashAggregateOperator::accumulateBatch;
  using HashAggregateOperator::accumulateRows;
  using HashAggregateOperator::getAggResults;

  Field*& MutableHavingFilter() { return having_filter_; }
  k_uint32& MutableCurOffset() { return cur_offset_; }
  k_uint32& MutableLimit() { return limit_; }
  k_uint32 ExaminedRows() const { return examined_rows_; }
  k_uint32 TotalReadRows() const { return total_read_row_; }
  k_uint32 AggRowSize() const { return agg_row_size_; }
  k_uint32 AggNullOffset() const { return agg_null_offset_; }
  std::unique_ptr<BaseHashTable>& MutableHashTable() { return ht_; }
};

class InspectableOrderedAggregateOperator : public OrderedAggregateOperator {
 public:
  InspectableOrderedAggregateOperator(TSAggregatorSpec* spec, PostProcessSpec* post)
      : OrderedAggregateOperator(nullptr, spec, post, nullptr, 0) {}

  using OrderedAggregateOperator::Clone;
  using OrderedAggregateOperator::FinishProcess;
  using OrderedAggregateOperator::Init;
  using OrderedAggregateOperator::Next;
  using OrderedAggregateOperator::ProcessData;
  using OrderedAggregateOperator::Start;
  using OrderedAggregateOperator::getAggResult;

  Field*& MutableHavingFilter() { return having_filter_; }
  k_uint32& MutableCurOffset() { return cur_offset_; }
  k_uint32& MutableLimit() { return limit_; }
  k_uint32 ExaminedRows() const { return examined_rows_; }
  k_uint32 AggRowSize() const { return agg_row_size_; }
  k_uint32 AggNullOffset() const { return agg_null_offset_; }
};

class AggregateOpWhiteBoxTest : public ::testing::Test {
 protected:
  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
    EEPgErrorInfo::ResetPgErrorInfo();
  }

  void TearDown() override {
    EEPgErrorInfo::ResetPgErrorInfo();
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
};

DataChunkPtr MakeIntChunk(const std::vector<k_int32>& values) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT, KWDBTypeFamily::IntFamily);

  DataChunkPtr chunk =
      std::make_unique<OwnedColumnDataChunk>(col_info, 1, values.size());
  EXPECT_TRUE(chunk->Initialize());
  for (size_t row = 0; row < values.size(); ++row) {
    chunk->AddCount();
    k_int32 value = values[row];
    EXPECT_EQ(chunk->InsertData(row, 0, reinterpret_cast<char*>(&value),
                                sizeof(value)),
              KStatus::SUCCESS);
  }
  return chunk;
}

DataChunkPtr MakeEmptyIntChunk() {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT, KWDBTypeFamily::IntFamily);

  DataChunkPtr chunk = std::make_unique<OwnedColumnDataChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  return chunk;
}

std::vector<k_int64> ReadBigIntColumnValues(const DataChunkPtr& chunk,
                                            k_uint32 column = 0) {
  std::vector<k_int64> values;
  if (chunk == nullptr) {
    return values;
  }
  for (k_uint32 row = 0; row < chunk->Count(); ++row) {
    values.push_back(
        *reinterpret_cast<k_int64*>(chunk->GetData(row, column)));
  }
  return values;
}

TEST_F(AggregateOpWhiteBoxTest,
       CalculateAggOffsetsAndEffectiveWidthHandleSpecialStorageLayouts) {
  TSAggregatorSpec spec;
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.SetAggFields({
      new FieldAggString(0, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggDecimal(1, roachpb::DataType::DECIMAL, sizeof(k_double64),
                          &aggregator),
      new FieldAggAvg(2, roachpb::DataType::DOUBLE, sizeof(k_double64),
                      &aggregator),
      new FieldAggDouble(3, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggDouble(4, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggLonglong(5, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
  });

  aggregator.CallCalculateAggOffsets();
  const auto& offsets = aggregator.MutableFuncOffsets();
  ASSERT_EQ(offsets.size(), 6U);
  EXPECT_EQ(offsets[0], 0U);
  EXPECT_EQ(offsets[1], 16U + STRING_WIDE);
  EXPECT_EQ(offsets[2], 16U + STRING_WIDE + sizeof(k_double64) + BOOL_WIDE);
  EXPECT_EQ(offsets[3], offsets[2] + sizeof(k_double64) + sizeof(k_int64));
  EXPECT_EQ(offsets[4], offsets[3] + sizeof(k_double64) + sizeof(ElapsedInfo));
  EXPECT_EQ(offsets[5], offsets[4] + sizeof(k_double64) + sizeof(TwaInfo));

  const k_uint32 total_value_width =
      offsets[5] + sizeof(k_int64) + sizeof(KTimestamp);
  aggregator.SetAggNullOffset(total_value_width);

  std::vector<char> bucket(total_value_width + 1, 0);
  char* bitmap = bucket.data() + total_value_width;
  for (int i = 0; i < 6; ++i) {
    AggregateFunc::SetNotNull(bitmap, i);
  }
  *reinterpret_cast<k_uint16*>(bucket.data() + offsets[0]) = 5;
  *reinterpret_cast<k_bool*>(bucket.data() + offsets[1]) = KFALSE;

  EXPECT_EQ(
      aggregator.CallCalculateAggEffectiveWidth(bucket.data()),
      static_cast<k_uint32>(1 + (STRING_WIDE + 5) + (BOOL_WIDE + sizeof(k_double64)) +
                            (sizeof(k_int64) + sizeof(k_double64)) +
                            (sizeof(ElapsedInfo) + sizeof(k_double64)) +
                            (sizeof(TwaInfo) + sizeof(k_double64)) +
                            (sizeof(KTimestamp) + sizeof(k_int64))));
  EXPECT_EQ(aggregator.CallCalculateAggEffectiveWidth(nullptr), 0U);
}

TEST_F(AggregateOpWhiteBoxTest,
       ResolveGroupByColsAndInitFirstLastTimeStampCoverInternalState) {
  TSAggregatorSpec spec;
  spec.add_group_cols(3);
  spec.add_group_cols(1);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.CallResolveGroupByCols(ctx_);
  ASSERT_EQ(aggregator.MutableGroupCols().size(), 2U);
  EXPECT_EQ(aggregator.MutableGroupCols()[0], 3U);
  EXPECT_EQ(aggregator.MutableGroupCols()[1], 1U);

  std::unique_ptr<AggregateFunc> first_row(AggregateFuncFactory::CreateFirstRow(
      roachpb::DataType::BIGINT, 0, 0, sizeof(k_int64), 1));
  std::unique_ptr<AggregateFunc> last_row_ts(
      AggregateFuncFactory::CreateLastRowTS(1, 1, sizeof(k_int64), 2));
  std::unique_ptr<AggregateFunc> count_row(
      AggregateFuncFactory::CreateCountRow(2, sizeof(k_int64)));
  ASSERT_NE(first_row, nullptr);
  ASSERT_NE(last_row_ts, nullptr);
  ASSERT_NE(count_row, nullptr);
  first_row->SetOffset(0);
  last_row_ts->SetOffset(16);
  count_row->SetOffset(24);

  aggregator.MutableFuncs().push_back(first_row.release());
  aggregator.MutableFuncs().push_back(last_row_ts.release());
  aggregator.MutableFuncs().push_back(count_row.release());

  std::vector<char> bucket(32, 0);
  aggregator.CallInitFirstLastTimeStamp(bucket.data());
  EXPECT_EQ(*reinterpret_cast<KTimestamp*>(bucket.data()), INT64_MAX);
  EXPECT_EQ(*reinterpret_cast<KTimestamp*>(bucket.data() + 16), INT64_MIN);
}

TEST_F(AggregateOpWhiteBoxTest,
       ResolveAggFuncsCreatesRepresentativeFunctionsAndRejectsInvalidType) {
  DummyInputOperator input({
      new FieldLonglong(0, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldVarchar(2, roachpb::DataType::VARCHAR, 16),
      new FieldDecimal(3, roachpb::DataType::DECIMAL, sizeof(k_double64)),
  });

  TSAggregatorSpec spec;
  auto* agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST);
  agg->add_col_idx(2);
  agg->add_col_idx(0);
  agg->add_timestampconstant(1000);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS);
  agg->add_col_idx(0);
  agg->add_timestampconstant(2000);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  agg->add_col_idx(0);
  agg->add_arguments()->set_expr("1.5:::FLOAT8");

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED);
  agg->add_col_idx(0);
  agg->add_arguments()->set_expr("'00:00:01':::INTERVAL");

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(2);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
  agg->add_col_idx(2);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM_INT);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_STDDEV);
  agg->add_col_idx(1);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.AddDependency(&input);
  aggregator.SetAggFields({
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &aggregator),
      new FieldAggLonglong(1, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
      new FieldAggString(2, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggLonglong(3, roachpb::DataType::TIMESTAMP, sizeof(k_int64),
                           &aggregator),
      new FieldAggAvg(4, roachpb::DataType::DOUBLE, sizeof(k_double64),
                      &aggregator),
      new FieldAggDouble(5, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggDouble(6, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggString(7, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggDecimal(8, roachpb::DataType::DECIMAL, sizeof(k_double64),
                          &aggregator),
      new FieldAggString(9, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggDecimal(10, roachpb::DataType::DECIMAL, sizeof(k_double64),
                          &aggregator),
      new FieldAggLonglong(11, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
      new FieldAggLonglong(12, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
  });
  aggregator.CallCalculateAggOffsets();

  EXPECT_EQ(aggregator.ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  ASSERT_EQ(aggregator.MutableFuncs().size(), 13U);
  for (size_t i = 0; i < aggregator.MutableFuncs().size(); ++i) {
    EXPECT_EQ(aggregator.MutableFuncs()[i]->GetOffset(),
              aggregator.MutableFuncOffsets()[i]);
  }
}

TEST_F(AggregateOpWhiteBoxTest,
       ResolveAggFuncsCoversRemainingFirstLastTwaAndExtendBranches) {
  DummyInputOperator input({
      new FieldLonglong(0, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldVarchar(2, roachpb::DataType::VARCHAR, 16),
      new FieldDecimal(3, roachpb::DataType::DECIMAL, sizeof(k_double64)),
  });

  TSAggregatorSpec spec;
  auto* agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW);
  agg->add_col_idx(2);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST);
  agg->add_col_idx(2);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS);
  agg->add_col_idx(1);
  agg->add_col_idx(0);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  agg->add_col_idx(0);
  agg->add_col_idx(1);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  agg->add_col_idx(0);
  agg->add_arguments()->set_expr("const(2.5):::FLOAT8");

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(3);

  agg = spec.add_aggregations();
  agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN_EXTEND);
  agg->add_col_idx(1);
  agg->add_col_idx(2);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.AddDependency(&input);
  aggregator.SetAggFields({
      new FieldAggString(0, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggLonglong(1, roachpb::DataType::TIMESTAMP, sizeof(k_int64),
                           &aggregator),
      new FieldAggString(2, roachpb::DataType::VARCHAR, 16, &aggregator),
      new FieldAggLonglong(3, roachpb::DataType::TIMESTAMP, sizeof(k_int64),
                           &aggregator),
      new FieldAggInt(4, roachpb::DataType::INT, sizeof(k_int32), &aggregator),
      new FieldAggLonglong(5, roachpb::DataType::TIMESTAMP, sizeof(k_int64),
                           &aggregator),
      new FieldAggDouble(6, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggDouble(7, roachpb::DataType::DOUBLE, sizeof(k_double64),
                         &aggregator),
      new FieldAggDecimal(8, roachpb::DataType::DECIMAL, sizeof(k_double64),
                          &aggregator),
      new FieldAggString(9, roachpb::DataType::VARCHAR, 16, &aggregator),
  });
  aggregator.CallCalculateAggOffsets();

  EXPECT_EQ(aggregator.ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  ASSERT_EQ(aggregator.MutableFuncs().size(), 10U);
}

TEST_F(AggregateOpWhiteBoxTest,
       AccumulateRowIntoBucketHandlesDistinctAndNonDistinctUpdates) {
  TSAggregatorSpec spec;
  auto* distinct_count = spec.add_aggregations();
  distinct_count->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
  distinct_count->set_distinct(true);
  distinct_count->add_col_idx(0);

  auto* count_rows = spec.add_aggregations();
  count_rows->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.SetAggFields({
      new FieldAggLonglong(0, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
      new FieldAggLonglong(1, roachpb::DataType::BIGINT, sizeof(k_int64),
                           &aggregator),
  });
  aggregator.CallCalculateAggOffsets();
  aggregator.SetAggNullOffset(sizeof(k_int64) * 2);
  auto* count_func = AggregateFuncFactory::CreateCount(roachpb::DataType::INT, 0,
                                                       0, sizeof(k_int64));
  auto* count_row_func = AggregateFuncFactory::CreateCountRow(1,
                                                              sizeof(k_int64));
  ASSERT_NE(count_func, nullptr);
  ASSERT_NE(count_row_func, nullptr);
  count_func->SetOffset(aggregator.MutableFuncOffsets()[0]);
  count_row_func->SetOffset(aggregator.MutableFuncOffsets()[1]);
  aggregator.MutableFuncs().push_back(count_func);
  aggregator.MutableFuncs().push_back(count_row_func);
  aggregator.MutableColTypes().push_back(roachpb::DataType::INT);
  aggregator.MutableColLens().push_back(sizeof(k_int32));
  aggregator.MutableAllowNull().push_back(false);

  DataChunkPtr chunk = MakeIntChunk({7});
  std::vector<char> bucket(sizeof(k_int64) * 2 + 1, 0);

  EXPECT_EQ(aggregator.CallAccumulateRowIntoBucket(
                ctx_, bucket.data(), sizeof(k_int64) * 2, chunk.get(), 0),
            KStatus::SUCCESS);
  EXPECT_EQ(aggregator.CallAccumulateRowIntoBucket(
                ctx_, bucket.data(), sizeof(k_int64) * 2, chunk.get(), 0),
            KStatus::SUCCESS);

  EXPECT_EQ(*reinterpret_cast<k_int64*>(bucket.data()), 1);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(bucket.data() + sizeof(k_int64)), 2);
}

TEST_F(AggregateOpWhiteBoxTest,
       CalculateAggEffectiveWidthCoversFixedStringNullSkipAndResetClose) {
  TSAggregatorSpec spec;
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);

  PostProcessSpec post;
  InspectableAggregator aggregator(&spec, &post);
  aggregator.SetAggFields({
      new FieldAggString(0, roachpb::DataType::CHAR, 8, &aggregator),
      new FieldAggInt(1, roachpb::DataType::INT, sizeof(k_int32), &aggregator),
  });
  aggregator.CallCalculateAggOffsets();
  aggregator.SetAggNullOffset(8 + STRING_WIDE + sizeof(k_int32));

  std::vector<char> bucket(8 + STRING_WIDE + sizeof(k_int32) + 1, 0);
  char* bitmap = bucket.data() + 8 + STRING_WIDE + sizeof(k_int32);
  AggregateFunc::SetNotNull(bitmap, 0);
  AggregateFunc::SetNull(bitmap, 1);
  *reinterpret_cast<k_uint16*>(bucket.data()) = 3;

  EXPECT_EQ(aggregator.CallCalculateAggEffectiveWidth(bucket.data()),
            static_cast<k_uint32>(1 + 8 + STRING_WIDE));

  CountingInputOperator input;
  InspectableAggregator delegating_aggregator(&spec, &post);
  delegating_aggregator.AddDependency(&input);
  EXPECT_EQ(delegating_aggregator.Reset(ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(delegating_aggregator.Close(ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(input.reset_calls_, 2);
  EXPECT_EQ(input.close_calls_, 1);
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggResolveAggFuncsAndGroupColsCoverExtendedBranches) {
  TSAggregatorSpec spec;
  spec.add_group_cols(7);
  spec.add_group_cols(3);

  auto* max_agg = spec.add_aggregations();
  max_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
  auto* max_extend = spec.add_aggregations();
  max_extend->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX_EXTEND);
  auto* any_agg = spec.add_aggregations();
  any_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL);
  any_agg->add_col_idx(7);
  auto* lastts_agg = spec.add_aggregations();
  lastts_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS);
  lastts_agg->add_timestampconstant(100);
  auto* firstrowts_agg = spec.add_aggregations();
  firstrowts_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS);
  auto* stddev_agg = spec.add_aggregations();
  stddev_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_STDDEV);
  auto* avg_agg = spec.add_aggregations();
  avg_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &op),
      new FieldAggString(1, roachpb::DataType::VARCHAR, 16, &op),
      new FieldAggString(2, roachpb::DataType::VARCHAR, 16, &op),
      new FieldAggLonglong(3, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggLonglong(4, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggDouble(5, roachpb::DataType::DOUBLE, sizeof(k_double64), &op),
      new FieldAggDouble(6, roachpb::DataType::DOUBLE, sizeof(k_double64), &op),
  };
  op.MutableAggregations().assign(spec.aggregations().begin(),
                                  spec.aggregations().end());
  op.CalculateAggOffsets();

  EXPECT_EQ(op.ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  EXPECT_EQ(op.MutableFuncs().size(), 7U);
  EXPECT_EQ(op.MutableSourceTargetMap()[7], 2U);
  EXPECT_EQ(op.MutableOutputFields()[3]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);
  EXPECT_EQ(op.MutableOutputFields()[4]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);

  op.MutableSourceTargetMap()[7] = 1;
  op.MutableSourceTargetMap()[3] = 4;
  op.ResolveGroupByCols(ctx_);
  ASSERT_EQ(op.MutableGroupCols().size(), 2U);
  EXPECT_EQ(op.MutableGroupCols()[0], 1U);
  EXPECT_EQ(op.MutableGroupCols()[1], 4U);
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggResolveAggFuncsRejectsUnknownFunctionAndCloneRequiresChildClone) {
  TSAggregatorSpec spec;
  auto* unknown = spec.add_aggregations();
  unknown->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &op),
  };
  op.MutableAggregations().assign(spec.aggregations().begin(),
                                  spec.aggregations().end());
  op.CalculateAggOffsets();
  EXPECT_EQ(op.ResolveAggFuncs(ctx_), KStatus::FAIL);

  CloneablePostAggInputOperator child;
  op.AddDependency(&child);
  BaseOperator* cloned = op.Clone();
  ASSERT_NE(cloned, nullptr);
  delete cloned;

  InspectablePostAggOperator no_clone(&spec, &post);
  CloneablePostAggInputOperator non_clone_child;
  non_clone_child.clone_returns_null_ = true;
  no_clone.AddDependency(&non_clone_child);
  EXPECT_EQ(no_clone.Clone(), nullptr);
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggGetAggResultsHandlesPassThroughQueueAndCompletion) {
  TSAggregatorSpec spec;
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &op),
  };
  op.MutableAggOutputColNum() = 1;
  op.MutableAggOutputColInfo() = new ColumnInfo[1];
  op.MutableAggOutputColInfo()[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily);
  op.MutablePassAgg() = true;
  op.MutableProcessedChunks().push(MakeIntChunk({7, 8}));

  DataChunkPtr result = nullptr;
  EXPECT_EQ(op.getAggResults(ctx_, result), KStatus::SUCCESS);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 2U);

  result = nullptr;
  EXPECT_EQ(op.getAggResults(ctx_, result), KStatus::SUCCESS);
  EXPECT_TRUE(op.IsDone());

  InspectablePostAggOperator limited(&spec, &post);
  limited.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &limited),
  };
  limited.MutableAggOutputColNum() = 1;
  limited.MutableAggOutputColInfo() = new ColumnInfo[1];
  limited.MutableAggOutputColInfo()[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily);
  limited.MutablePassAgg() = true;
  limited.MutableLimit() = 1;
  limited.MutableExaminedRows() = 1;
  limited.MutableProcessedChunks().push(MakeIntChunk({9}));

  result = nullptr;
  EXPECT_EQ(limited.getAggResults(ctx_, result), KStatus::SUCCESS);
  EXPECT_TRUE(limited.IsDone());
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggGetAggResultsMaterializesHashTableRowsAndNullScalarCount) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
  };
  op.MutableAggregations().assign(spec.aggregations().begin(),
                                  spec.aggregations().end());
  op.CalculateAggOffsets();
  ASSERT_EQ(op.ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  op.MutableAggOutputColNum() = 1;
  op.MutableAggOutputColInfo() = new ColumnInfo[1];
  op.MutableAggOutputColInfo()[0] =
      ColumnInfo(sizeof(k_int64), roachpb::DataType::BIGINT,
                 KWDBTypeFamily::IntFamily);
  op.MutableAggRowSize() = sizeof(k_int64) + 1;
  op.MutableAggNullOffset() = sizeof(k_int64);
  op.MutablePassAgg() = false;
  op.MutableHashTable() = std::make_unique<LinearProbingHashTable>(
      std::vector<roachpb::DataType>{}, std::vector<k_uint32>{},
      op.MutableAggRowSize(), std::vector<bool>{}, true);
  ASSERT_NE(op.MutableHashTable(), nullptr);
  ASSERT_EQ(op.MutableHashTable()->Initialize(), KStatus::SUCCESS);

  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  std::vector<char> tuple(op.MutableAggRowSize(), 0);
  ASSERT_EQ(op.MutableHashTable()->FindOrCreateGroupsAndAddTuple(
                tuple.data(), &loc, &hash_val, &is_used, &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_FALSE(is_used);
  ASSERT_FALSE(is_abandoned);
  DatumRowPtr agg_ptr = op.MutableHashTable()->GetAggResult(loc);
  ASSERT_NE(agg_ptr, nullptr);
  k_int64 value = 9;
  std::memcpy(agg_ptr + op.MutableFuncOffsets()[0], &value, sizeof(value));
  AggregateFunc::SetNotNull(agg_ptr + op.MutableAggNullOffset(), 0);

  DataChunkPtr result = op.constructAggResults(4);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(op.getAggResults(ctx_, result), KStatus::SUCCESS);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 1U);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(result->GetData(0, 0)), value);
  EXPECT_TRUE(op.IsDone());
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggGetAggResultsSkipsWholeQueuedChunkForOffset) {
  TSAggregatorSpec spec;
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &op),
  };
  op.MutableAggOutputColNum() = 1;
  op.MutableAggOutputColInfo() = new ColumnInfo[1];
  op.MutableAggOutputColInfo()[0] =
      ColumnInfo(sizeof(k_int32), roachpb::DataType::INT,
                 KWDBTypeFamily::IntFamily);
  op.MutablePassAgg() = true;
  op.MutableCurOffset() = 2;
  op.MutableLimit() = 10;
  op.MutableProcessedChunks().push(MakeIntChunk({1, 2}));
  op.MutableProcessedChunks().push(MakeIntChunk({3, 4}));

  DataChunkPtr result = nullptr;
  EXPECT_EQ(op.getAggResults(ctx_, result), KStatus::SUCCESS);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->Count(), 2U);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(result->GetData(0, 0)), 3);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(result->GetData(1, 0)), 4);
  EXPECT_EQ(op.MutableTotalReadRow(), 2U);
}

TEST_F(AggregateOpWhiteBoxTest,
       PostAggResolveAggFuncsCoversRemainingCasesAndEmptyOffsets) {
  TSAggregatorSpec spec;

  auto* min_agg = spec.add_aggregations();
  min_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN);
  auto* min_extend = spec.add_aggregations();
  min_extend->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN_EXTEND);
  auto* sum_agg = spec.add_aggregations();
  sum_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM);
  auto* count_agg = spec.add_aggregations();
  count_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT);
  auto* count_rows_agg = spec.add_aggregations();
  count_rows_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);
  auto* last_agg = spec.add_aggregations();
  last_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST);
  last_agg->add_timestampconstant(123);
  auto* last_row_agg = spec.add_aggregations();
  last_row_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW);
  auto* lastrowts_agg = spec.add_aggregations();
  lastrowts_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS);
  auto* first_agg = spec.add_aggregations();
  first_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST);
  auto* firstts_agg = spec.add_aggregations();
  firstts_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTTS);
  auto* first_row_agg = spec.add_aggregations();
  first_row_agg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);

  PostProcessSpec post;
  InspectablePostAggOperator op(&spec, &post);
  op.MutableOutputFields() = {
      new FieldAggInt(0, roachpb::DataType::INT, sizeof(k_int32), &op),
      new FieldAggDecimal(1, roachpb::DataType::DECIMAL, sizeof(k_double64), &op),
      new FieldAggLonglong(2, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggLonglong(3, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggLonglong(4, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggString(5, roachpb::DataType::VARCHAR, 16, &op),
      new FieldAggString(6, roachpb::DataType::VARCHAR, 16, &op),
      new FieldAggLonglong(7, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggInt(8, roachpb::DataType::INT, sizeof(k_int32), &op),
      new FieldAggLonglong(9, roachpb::DataType::BIGINT, sizeof(k_int64), &op),
      new FieldAggInt(10, roachpb::DataType::INT, sizeof(k_int32), &op),
  };
  op.MutableAggregations().assign(spec.aggregations().begin(),
                                  spec.aggregations().end());
  op.CalculateAggOffsets();

  EXPECT_EQ(op.ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  EXPECT_EQ(op.MutableFuncs().size(), 11U);
  EXPECT_EQ(op.MutableOutputFields()[7]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);
  EXPECT_EQ(op.MutableOutputFields()[9]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);

  InspectablePostAggOperator empty(&spec, &post);
  empty.CalculateAggOffsets();
  EXPECT_TRUE(empty.MutableFuncOffsets().empty());
}

TEST_F(AggregateOpWhiteBoxTest,
       HashAggregateOperatorInitCoversExtraStorageLayouts) {
  DummyInputOperator input({
      new FieldLonglong(0, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
  });

  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR);

  auto* avg = spec.add_aggregations();
  avg->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG);
  avg->add_col_idx(1);

  auto* elapsed = spec.add_aggregations();
  elapsed->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ELAPSED);
  elapsed->add_col_idx(0);
  elapsed->add_arguments()->set_expr("'00:00:01':::INTERVAL");

  auto* twa = spec.add_aggregations();
  twa->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_TWA);
  twa->add_col_idx(0);
  twa->add_col_idx(1);

  auto* first_row = spec.add_aggregations();
  first_row->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW);
  first_row->add_col_idx(1);
  first_row->add_col_idx(0);

  auto* last_ts = spec.add_aggregations();
  last_ts->set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS);
  last_ts->add_col_idx(0);
  last_ts->add_col_idx(0);
  last_ts->add_timestampconstant(7);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::FloatFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::FloatFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::FloatFamily, 8));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 4));
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::TimestampFamily, 8));
  InspectableHashAggregateOperator op(&spec, &post);
  op.AddDependency(&input);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_GT(op.AggNullOffset(), 0U);
  EXPECT_GT(op.AggRowSize(), op.AggNullOffset());
  ASSERT_NE(op.MutableHashTable(), nullptr);
}

TEST_F(AggregateOpWhiteBoxTest,
       HashAggregateOperatorGroupedCountRowsRunsEndToEndAndClones) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  std::vector<DataChunkPtr> chunks;
  chunks.push_back(MakeIntChunk({1, 1, 2}));
  CloneableChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(chunks));

  InspectableHashAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.accumulateBatch(ctx_, MakeEmptyIntChunk().get()),
            KStatus::SUCCESS);

  BaseOperator* cloned = op.Clone();
  ASSERT_NE(cloned, nullptr);
  delete cloned;

  ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);
  std::vector<k_int64> counts;
  for (;;) {
    DataChunkPtr result = nullptr;
    EEIteratorErrCode code = op.Next(ctx_, result);
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      break;
    }
    ASSERT_EQ(code, EEIteratorErrCode::EE_OK);
    auto values = ReadBigIntColumnValues(result);
    counts.insert(counts.end(), values.begin(), values.end());
  }

  std::sort(counts.begin(), counts.end());
  EXPECT_EQ(counts, std::vector<k_int64>({1, 2}));
}

TEST_F(AggregateOpWhiteBoxTest,
       HashAggregateOperatorHavingFilterCanSkipAllResults) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  std::vector<DataChunkPtr> chunks;
  chunks.push_back(MakeIntChunk({1, 2}));
  ChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(chunks));
  InspectableHashAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);
  op.MutableHavingFilter() =
      new FieldConstInt(roachpb::DataType::INT, 0, sizeof(k_int64));

  DataChunkPtr result = nullptr;
  EXPECT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(result, nullptr);
  SafeDeletePointer(op.MutableHavingFilter());
}

TEST_F(AggregateOpWhiteBoxTest,
       HashAggregateOperatorHonorsOffsetAndLimit) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  post.set_offset(1);
  post.set_limit(1);
  std::vector<DataChunkPtr> chunks;
  chunks.push_back(MakeIntChunk({1, 1, 2, 3, 3, 3}));
  ChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(chunks));
  InspectableHashAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);

  DataChunkPtr result = nullptr;
  ASSERT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_OK);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 1U);
  result = nullptr;
  EXPECT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(op.ExaminedRows(), 1U);
}

TEST_F(AggregateOpWhiteBoxTest,
       HashAggregateOperatorAccumulateRowsFailsOnChildError) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  std::deque<ScriptedChunkInputOperator::Step> steps;
  steps.push_back({EEIteratorErrCode::EE_ERROR, nullptr});

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  ScriptedChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(steps));
  InspectableHashAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(op.accumulateRows(ctx_), KStatus::FAIL);
}

TEST_F(AggregateOpWhiteBoxTest,
       OrderedAggregateOperatorGroupedCountRowsRunsEndToEndAndClones) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  std::vector<DataChunkPtr> chunks;
  chunks.push_back(MakeIntChunk({1, 1}));
  chunks.push_back(MakeIntChunk({2}));
  CloneableChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(chunks));

  InspectableOrderedAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);

  BaseOperator* cloned = op.Clone();
  ASSERT_NE(cloned, nullptr);
  delete cloned;

  ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);
  std::vector<k_int64> counts;
  for (;;) {
    DataChunkPtr result = nullptr;
    EEIteratorErrCode code = op.Next(ctx_, result);
    if (code == EEIteratorErrCode::EE_END_OF_RECORD) {
      break;
    }
    ASSERT_EQ(code, EEIteratorErrCode::EE_OK);
    auto values = ReadBigIntColumnValues(result);
    counts.insert(counts.end(), values.begin(), values.end());
  }

  EXPECT_EQ(counts, std::vector<k_int64>({2, 1}));
}

TEST_F(AggregateOpWhiteBoxTest,
       OrderedAggregateOperatorHandlesNullProcessDataScalarEmptyAndCloneNull) {
  {
    TSAggregatorSpec spec;
    spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
    spec.add_group_cols(0);
    spec.add_aggregations()->set_func(
        TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

    PostProcessSpec post;
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
    ChunkInputOperator child(
        {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))}, {});
    InspectableOrderedAggregateOperator op(&spec, &post);
    op.AddDependency(&child);

    ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
    DataChunkPtr null_chunk = nullptr;
    EXPECT_EQ(op.ProcessData(ctx_, null_chunk), KStatus::FAIL);
  }

  {
    TSAggregatorSpec spec;
    spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_SCALAR);
    spec.add_aggregations()->set_func(
        TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

    std::deque<ScriptedChunkInputOperator::Step> steps;
    steps.push_back({EEIteratorErrCode::EE_OK, MakeEmptyIntChunk()});
    PostProcessSpec post;
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
    ScriptedChunkInputOperator child(
        {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
        std::move(steps));
    InspectableOrderedAggregateOperator op(&spec, &post);
    op.AddDependency(&child);

    ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
    ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);

    DataChunkPtr result = nullptr;
    ASSERT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_OK);
    ASSERT_NE(result, nullptr);
    ASSERT_EQ(result->Count(), 1U);
    EXPECT_EQ(*reinterpret_cast<k_int64*>(result->GetData(0, 0)), 0);
    result = nullptr;
    EXPECT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
  }

  {
    TSAggregatorSpec spec;
    spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
    spec.add_group_cols(0);
    spec.add_aggregations()->set_func(
        TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

    PostProcessSpec post;
    post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
    CloneableChunkInputOperator child(
        {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))}, {});
    child.clone_returns_null_ = true;
    InspectableOrderedAggregateOperator op(&spec, &post);
    op.AddDependency(&child);

    EXPECT_EQ(op.Clone(), nullptr);
  }
}

TEST_F(AggregateOpWhiteBoxTest,
       OrderedAggregateOperatorHonorsOffsetAndLimit) {
  TSAggregatorSpec spec;
  spec.set_type(TSAggregatorSpec_Type::TSAggregatorSpec_Type_NON_SCALAR);
  spec.add_group_cols(0);
  spec.add_aggregations()->set_func(
      TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS);

  PostProcessSpec post;
  post.add_output_types(MarshalToOutputType(KWDBTypeFamily::IntFamily, 8));
  post.set_offset(1);
  post.set_limit(1);
  std::vector<DataChunkPtr> chunks;
  chunks.push_back(MakeIntChunk({1, 1, 2, 2, 3}));
  ChunkInputOperator child(
      {new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32))},
      std::move(chunks));

  InspectableOrderedAggregateOperator op(&spec, &post);
  op.AddDependency(&child);

  ASSERT_EQ(op.Init(ctx_), EEIteratorErrCode::EE_OK);
  ASSERT_EQ(op.Start(ctx_), EEIteratorErrCode::EE_OK);

  DataChunkPtr result = nullptr;
  ASSERT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_OK);
  ASSERT_NE(result, nullptr);
  ASSERT_EQ(result->Count(), 1U);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(result->GetData(0, 0)), 2);
  result = nullptr;
  EXPECT_EQ(op.Next(ctx_, result), EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(op.ExaminedRows(), 1U);
}

}  // namespace
}  // namespace kwdbts
