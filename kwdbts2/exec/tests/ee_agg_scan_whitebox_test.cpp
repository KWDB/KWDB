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

#include <cstring>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#define private public
#define protected public
#include "ee_agg_scan_op.h"
#undef protected
#undef private

#include "ee_field_const.h"
#include "ee_field_func.h"
#include "ee_global.h"
#include "ee_kwthd_context.h"
#include "ee_mempool.h"

extern "C" {
bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr) { return false; }
}

namespace kwdbts {
namespace {

class DummyChildOperator : public BaseOperator {
 public:
  DummyChildOperator() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Start(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  EEIteratorErrCode Close(kwdbContext_p) override { return EEIteratorErrCode::EE_OK; }

  BaseOperator* Clone() override { return nullptr; }
};

class MockRowBatch : public RowBatch {
 public:
  explicit MockRowBatch(k_uint32 col_num)
      : col_num_(col_num) {
    typ_ = RowBatchTypeScan;
  }

  void AddRow() {
    rows_.emplace_back(col_num_);
    count_ = rows_.size();
  }

  template <typename T>
  void SetValue(k_uint32 row, k_uint32 col, const T& value) {
    rows_[row][col].resize(sizeof(T));
    std::memcpy(rows_[row][col].data(), &value, sizeof(T));
    rows_[row][col].push_back('\0');
  }

  void SetString(k_uint32 row, k_uint32 col, const std::string& value) {
    rows_[row][col].assign(value.begin(), value.end());
    rows_[row][col].push_back('\0');
  }

  char* GetData(k_uint32 col, k_uint32, roachpb::KWDBKTSColumn::ColumnType,
                roachpb::DataType) override {
    return rows_[cursor_][col].empty() ? nullptr : rows_[cursor_][col].data();
  }

  bool IsNull(k_uint32, roachpb::KWDBKTSColumn::ColumnType) override {
    return false;
  }

  k_uint32 Count() override { return count_; }

  k_int32 NextLine() override {
    if (cursor_ + 1 < count_) {
      ++cursor_;
    }
    return cursor_;
  }

  void ResetLine() override { cursor_ = 0; }

  KStatus Sort(Field**, const std::vector<k_uint32>&,
               const std::vector<k_int32>&) override {
    return KStatus::SUCCESS;
  }

  void SetLimitOffset(k_uint32, k_uint32) override {}

  void SetCount(k_uint32 count) override { count_ = count; }

 private:
  k_uint32 col_num_;
  k_uint32 count_{0};
  k_uint32 cursor_{0};
  std::vector<std::vector<std::vector<char>>> rows_;
};

class AggScanWhiteBoxTest : public ::testing::Test {
 protected:
  static void SetUpTestCase() {
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
  }

  static void TearDownTestCase() {
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }

  void SetUp() override {
    InitServerKWDBContext(&ctx_storage_);
    EEPgErrorInfo::ResetPgErrorInfo();
    spec_.Clear();
    post_.Clear();
  }

  void TearDown() override { EEPgErrorInfo::ResetPgErrorInfo(); }

  std::unique_ptr<AggTableScanOperator> MakeOperator() {
    return std::make_unique<AggTableScanOperator>(nullptr, &spec_, &post_,
                                                  nullptr, 0);
  }

  void FreeOperator(AggTableScanOperator* op) {
    SafeFreePointer(op->group_cols_);
    SafeFreePointer(op->agg_source_target_col_map_);
    SafeFreePointer(op->agg_renders_);
    SafeFreePointer(op->renders_);
  }

  ColumnInfo MakeColumn(roachpb::DataType type, k_uint32 len,
                        KWDBTypeFamily family) {
    ColumnInfo info(len, type, family);
    info.allow_null = true;
    return info;
  }

  kwdbContext_t ctx_storage_{};
  kwdbContext_p ctx_{&ctx_storage_};
  TSReaderSpec spec_;
  PostProcessSpec post_;
};

TEST_F(AggScanWhiteBoxTest,
       ProcessGroupColsCoversTypeSwitchesAndTimeBucketExtraction) {
  auto op = MakeOperator();

  auto* bucket_input = new FieldLonglong(0, roachpb::DataType::TIMESTAMPTZ,
                                         sizeof(k_int64));
  auto* interval = new FieldConstString(roachpb::DataType::CHAR, "300s");
  std::list<Field*> time_bucket_args{bucket_input, interval};
  auto* time_bucket = new FieldFuncTimeBucket(time_bucket_args, 0);
  auto* int_field =
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32));
  auto* short_field =
      new FieldShort(2, roachpb::DataType::SMALLINT, sizeof(k_int16));
  auto* float_field =
      new FieldFloat(3, roachpb::DataType::FLOAT, sizeof(k_float32));
  auto* double_field =
      new FieldDouble(4, roachpb::DataType::DOUBLE, sizeof(k_double64));
  auto* string_field =
      new FieldChar(5, roachpb::DataType::CHAR, 16);
  auto* bool_field =
      new FieldBool(6, roachpb::DataType::BOOL, sizeof(k_bool));
  auto* decimal_field =
      new FieldDecimal(7, roachpb::DataType::DECIMAL, sizeof(k_double64));

  op->num_ = 8;
  op->renders_ = static_cast<Field**>(malloc(op->num_ * sizeof(Field*)));
  ASSERT_NE(op->renders_, nullptr);
  op->output_fields_ = {time_bucket, int_field,   short_field, float_field,
                        double_field, string_field, bool_field, decimal_field};
  op->renders_[0] = time_bucket;
  op->renders_[1] = int_field;
  op->renders_[2] = short_field;
  op->renders_[3] = float_field;
  op->renders_[4] = double_field;
  op->renders_[5] = string_field;
  op->renders_[6] = bool_field;
  op->renders_[7] = decimal_field;

  op->extractTimeBucket(op->renders_, op->num_);
  EXPECT_TRUE(op->hasTimeBucket());
  EXPECT_EQ(op->col_idx_, 0U);
  EXPECT_EQ(op->interval_seconds_, 300000);

  op->group_cols_size_ = 8;
  op->group_cols_ = static_cast<k_uint32*>(
      malloc(op->group_cols_size_ * sizeof(k_uint32)));
  op->agg_source_target_col_map_ = static_cast<k_uint32*>(
      malloc(op->num_ * sizeof(k_uint32)));
  ASSERT_NE(op->group_cols_, nullptr);
  ASSERT_NE(op->agg_source_target_col_map_, nullptr);
  for (k_uint32 i = 0; i < op->group_cols_size_; ++i) {
    op->group_cols_[i] = i;
    op->agg_source_target_col_map_[i] = i;
  }

  std::vector<ColumnInfo> columns = {
      MakeColumn(roachpb::DataType::TIMESTAMPTZ, sizeof(k_int64),
                 KWDBTypeFamily::TimestampTZFamily),
      MakeColumn(roachpb::DataType::INT, sizeof(k_int32),
                 KWDBTypeFamily::IntFamily),
      MakeColumn(roachpb::DataType::SMALLINT, sizeof(k_int16),
                 KWDBTypeFamily::IntFamily),
      MakeColumn(roachpb::DataType::FLOAT, sizeof(k_float32),
                 KWDBTypeFamily::FloatFamily),
      MakeColumn(roachpb::DataType::DOUBLE, sizeof(k_double64),
                 KWDBTypeFamily::FloatFamily),
      MakeColumn(roachpb::DataType::CHAR, 16, KWDBTypeFamily::StringFamily),
      MakeColumn(roachpb::DataType::BOOL, sizeof(k_bool),
                 KWDBTypeFamily::BoolFamily),
      MakeColumn(roachpb::DataType::DECIMAL, sizeof(k_double64),
                 KWDBTypeFamily::DecimalFamily),
  };
  DataChunkPtr chunk =
      std::make_unique<DataChunk>(columns.data(), columns.size(), 2);
  ASSERT_TRUE(chunk->Initialize());
  chunk->SetAllNull();

  MockRowBatch batch(8);
  batch.AddRow();
  batch.AddRow();
  const k_int64 first_bucket_source = 1690600319688;
  const k_int64 second_bucket_source = 1690600919688;
  const k_int32 int_value = 42;
  const k_int16 short_value = 7;
  const k_float32 float_value = 1.25f;
  const k_double64 double_value = 2.5;
  const k_bool bool_value = KTRUE;
  const k_double64 decimal_value = 9.5;
  batch.SetValue(0, 0, first_bucket_source);
  batch.SetValue(0, 1, int_value);
  batch.SetValue(0, 2, short_value);
  batch.SetValue(0, 3, float_value);
  batch.SetValue(0, 4, double_value);
  batch.SetString(0, 5, "host_a");
  batch.SetValue(0, 6, bool_value);
  batch.SetValue(0, 7, decimal_value);
  batch.SetValue(1, 0, second_bucket_source);
  batch.SetValue(1, 1, int_value);
  batch.SetValue(1, 2, short_value);
  batch.SetValue(1, 3, float_value);
  batch.SetValue(1, 4, double_value);
  batch.SetString(1, 5, "host_a");
  batch.SetValue(1, 6, bool_value);
  batch.SetValue(1, 7, decimal_value);

  GroupByColumnInfo group_by_cols[8] = {};
  k_int32 target_row = -1;
  KTimestampTz time_bucket_value = 0;
  EXPECT_TRUE(op->ProcessGroupCols(target_row, &batch, group_by_cols,
                                   time_bucket_value, chunk.get(), 0));
  EXPECT_EQ(time_bucket_value, 1690600200000);
  EXPECT_EQ(group_by_cols[5].len, 6U);
  EXPECT_EQ(group_by_cols[6].len, sizeof(k_bool));

  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&time_bucket_value),
                    sizeof(time_bucket_value));
  batch.ResetLine();
  batch.NextLine();
  target_row = 0;
  time_bucket_value = 0;
  EXPECT_TRUE(op->ProcessGroupCols(target_row, &batch, group_by_cols,
                                   time_bucket_value, chunk.get(), 1));
  EXPECT_EQ(time_bucket_value, 1690600800000);

  SafeDeletePointer(bucket_input);
  SafeDeletePointer(interval);
}

TEST_F(AggScanWhiteBoxTest, ResolveAggFuncsCoversSuccessAndSkipBranches) {
  auto op = MakeOperator();

  op->output_fields_ = {
      new FieldLonglong(0, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldVarchar(2, roachpb::DataType::VARCHAR, 16),
      new FieldDecimal(3, roachpb::DataType::DECIMAL, sizeof(k_double64)),
  };
  op->agg_output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldInt(1, roachpb::DataType::INT, sizeof(k_int32)),
      new FieldVarchar(2, roachpb::DataType::VARCHAR, 16),
      new FieldVarchar(3, roachpb::DataType::VARCHAR, 16),
      new FieldVarchar(4, roachpb::DataType::VARCHAR, 16),
      new FieldDouble(5, roachpb::DataType::DOUBLE, sizeof(k_double64)),
      new FieldLonglong(6, roachpb::DataType::BIGINT, sizeof(k_int64)),
      new FieldLonglong(7, roachpb::DataType::BIGINT, sizeof(k_int64)),
      new FieldVarchar(8, roachpb::DataType::VARCHAR, 16),
      new FieldLonglong(9, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldVarchar(10, roachpb::DataType::VARCHAR, 16),
      new FieldLonglong(11, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldVarchar(12, roachpb::DataType::VARCHAR, 16),
      new FieldLonglong(13, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldVarchar(14, roachpb::DataType::VARCHAR, 16),
      new FieldLonglong(15, roachpb::DataType::TIMESTAMP, sizeof(k_int64)),
      new FieldDouble(16, roachpb::DataType::DOUBLE, sizeof(k_double64)),
      new FieldVarchar(17, roachpb::DataType::VARCHAR, 16),
      new FieldDecimal(18, roachpb::DataType::DECIMAL, sizeof(k_double64)),
  };

  op->group_cols_size_ = 2;
  op->group_cols_ = static_cast<k_uint32*>(malloc(2 * sizeof(k_uint32)));
  op->group_cols_[0] = 0;
  op->group_cols_[1] = 1;
  op->col_idx_ = 0;
  op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(op->output_fields_.size(), sizeof(k_uint32)));
  ASSERT_NE(op->agg_source_target_col_map_, nullptr);

  auto add_agg = [&](TSAggregatorSpec_Func func,
                     std::initializer_list<k_uint32> cols,
                     k_int64 time = 0) {
    TSAggregatorSpec_Aggregation agg;
    agg.set_func(func);
    for (auto col : cols) {
      agg.add_col_idx(col);
    }
    if (time != 0) {
      agg.add_timestampconstant(time);
    }
    op->aggregations_.push_back(agg);
  };

  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL, {0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_ANY_NOT_NULL, {2});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_SUM, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_COUNT_ROWS, {});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST, {2, 0}, 1000);
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTTS, {2, 0}, 2000);
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LAST_ROW, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_LASTROWTS, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTTS, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRST_ROW, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_FIRSTROWTS, {2, 0});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_AVG, {1});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX_EXTEND, {1, 2});
  add_agg(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MIN_EXTEND, {1, 3});

  ASSERT_EQ(op->ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  EXPECT_EQ(op->funcs_.size(), 17U);
  EXPECT_EQ(op->agg_source_target_col_map_[0], 2U);
  EXPECT_EQ(op->agg_source_target_col_map_[1], 3U);
  EXPECT_EQ(op->agg_source_target_col_map_[2], 4U);
  EXPECT_EQ(op->agg_output_fields_[9]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);
  EXPECT_EQ(op->agg_output_fields_[11]->get_storage_type(),
            roachpb::DataType::TIMESTAMP);
  EXPECT_EQ(op->agg_output_fields_[8]->get_storage_type(),
            roachpb::DataType::VARCHAR);
  EXPECT_EQ(op->agg_output_fields_[10]->get_storage_type(),
            roachpb::DataType::VARCHAR);
  EXPECT_EQ(op->funcs_[15]->GetLen(), 16U + STRING_WIDE);
  EXPECT_EQ(op->funcs_[16]->GetLen(), sizeof(k_double64) + BOOL_WIDE);
}

TEST_F(AggScanWhiteBoxTest, ResolveAggFuncsCoversFailureBranches) {
  auto op = MakeOperator();
  op->output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  op->agg_output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  ASSERT_NE(op->agg_source_target_col_map_, nullptr);

  TSAggregatorSpec_Aggregation stddev_agg;
  stddev_agg.set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_STDDEV);
  stddev_agg.add_col_idx(0);
  op->aggregations_.push_back(stddev_agg);
  EXPECT_EQ(op->ResolveAggFuncs(ctx_), KStatus::FAIL);

  auto distinct_op = MakeOperator();
  distinct_op->output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  distinct_op->agg_output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  distinct_op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  TSAggregatorSpec_Aggregation distinct_agg;
  distinct_agg.set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
  distinct_agg.add_col_idx(0);
  distinct_agg.set_distinct(true);
  distinct_op->aggregations_.push_back(distinct_agg);
  EXPECT_EQ(distinct_op->ResolveAggFuncs(ctx_), KStatus::FAIL);
  EXPECT_TRUE(distinct_op->funcs_.empty());

  auto unknown_op = MakeOperator();
  unknown_op->output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  unknown_op->agg_output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  unknown_op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  TSAggregatorSpec_Aggregation unknown_agg;
  unknown_agg.func_ = 999;
  unknown_agg._has_bits_[0] |= 0x00000001u;
  unknown_agg.add_col_idx(0);
  unknown_op->aggregations_.push_back(unknown_agg);
  EXPECT_EQ(unknown_op->ResolveAggFuncs(ctx_), KStatus::FAIL);

  auto oom_flag_op = MakeOperator();
  oom_flag_op->output_fields_ = {
      new FieldInt(0, roachpb::DataType::UNKNOWN, sizeof(k_int32)),
  };
  oom_flag_op->agg_output_fields_ = {
      new FieldInt(0, roachpb::DataType::INT, sizeof(k_int32)),
  };
  oom_flag_op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  TSAggregatorSpec_Aggregation max_agg;
  max_agg.set_func(TSAggregatorSpec_Func::TSAggregatorSpec_Func_MAX);
  max_agg.add_col_idx(0);
  oom_flag_op->aggregations_.push_back(max_agg);
  EEPgErrorInfo::ResetPgErrorInfo();
  EXPECT_EQ(oom_flag_op->ResolveAggFuncs(ctx_), KStatus::SUCCESS);
  EXPECT_TRUE(oom_flag_op->funcs_.empty());
  EXPECT_NE(EEPgErrorInfo::GetPgErrorInfo().code, 0);
  FreeOperator(oom_flag_op.get());
  FreeOperator(op.get());
  FreeOperator(distinct_op.get());
  FreeOperator(unknown_op.get());
}

TEST_F(AggScanWhiteBoxTest, AddRowBatchDataHandlesEdgeCasesAndChunkRotation) {
  auto op = MakeOperator();
  EXPECT_EQ(op->AddRowBatchData(ctx_, nullptr), KStatus::FAIL);

  MockRowBatch row_batch(1);
  row_batch.AddRow();
  const k_int64 group_value = 200;
  row_batch.SetValue(0, 0, group_value);
  EXPECT_EQ(op->AddRowBatchData(ctx_, &row_batch), KStatus::FAIL);

  op->output_fields_ = {
      new FieldLonglong(0, roachpb::DataType::BIGINT, sizeof(k_int64)),
  };
  op->num_ = 1;
  op->renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
  ASSERT_NE(op->renders_, nullptr);
  op->renders_[0] = op->output_fields_[0];
  op->group_cols_size_ = 1;
  op->group_cols_ = static_cast<k_uint32*>(malloc(sizeof(k_uint32)));
  op->group_cols_[0] = 0;
  op->col_idx_ = 0;
  op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  op->agg_output_col_num_ = 2;
  op->agg_output_col_info_ = KNEW ColumnInfo[2];
  op->agg_output_col_info_[0] = MakeColumn(roachpb::DataType::BIGINT,
                                           sizeof(k_int64),
                                           KWDBTypeFamily::IntFamily);
  op->agg_output_col_info_[1] = MakeColumn(roachpb::DataType::BIGINT,
                                           sizeof(k_int64),
                                           KWDBTypeFamily::IntFamily);
  op->current_data_chunk_ = std::make_unique<DataChunk>(
      op->agg_output_col_info_, op->agg_output_col_num_, 1);
  ASSERT_TRUE(op->current_data_chunk_->Initialize());
  op->current_data_chunk_->SetAllNull();
  ASSERT_NE(op->current_data_chunk_, nullptr);
  op->current_data_chunk_->SetCount(1);
  const k_int64 existing_group = 100;
  op->current_data_chunk_->InsertData(0, 0,
                                      reinterpret_cast<char*>(
                                          const_cast<k_int64*>(&existing_group)),
                                      sizeof(existing_group));
  op->funcs_.push_back(AggregateFuncFactory::CreateCountRow(1, sizeof(k_int64)));
  ASSERT_TRUE(op->group_by_metadata_.initialize());

  EXPECT_EQ(op->AddRowBatchData(ctx_, &row_batch), KStatus::SUCCESS);
  ASSERT_EQ(op->output_queue_.size(), 1U);
  EXPECT_EQ(op->current_data_chunk_->Count(), 1U);
  k_int64 stored_group = *reinterpret_cast<k_int64*>(
      op->current_data_chunk_->GetData(0, 0));
  k_int64 stored_count = *reinterpret_cast<k_int64*>(
      op->current_data_chunk_->GetData(0, 1));
  EXPECT_EQ(stored_group, group_value);
  EXPECT_EQ(stored_count, 1);
}

TEST_F(AggScanWhiteBoxTest, AddRowBatchDataRespectsAggLimitAndFinishProcess) {
  auto op = MakeOperator();
  op->output_fields_ = {
      new FieldLonglong(0, roachpb::DataType::BIGINT, sizeof(k_int64)),
  };
  op->num_ = 1;
  op->renders_ = static_cast<Field**>(malloc(sizeof(Field*)));
  ASSERT_NE(op->renders_, nullptr);
  op->renders_[0] = op->output_fields_[0];
  op->group_cols_size_ = 1;
  op->group_cols_ = static_cast<k_uint32*>(malloc(sizeof(k_uint32)));
  op->group_cols_[0] = 0;
  op->col_idx_ = 0;
  op->agg_limit_ = 1;
  op->agg_source_target_col_map_ =
      static_cast<k_uint32*>(calloc(1, sizeof(k_uint32)));
  op->agg_output_col_num_ = 2;
  op->agg_output_col_info_ = KNEW ColumnInfo[2];
  op->agg_output_col_info_[0] = MakeColumn(roachpb::DataType::BIGINT,
                                           sizeof(k_int64),
                                           KWDBTypeFamily::IntFamily);
  op->agg_output_col_info_[1] = MakeColumn(roachpb::DataType::BIGINT,
                                           sizeof(k_int64),
                                           KWDBTypeFamily::IntFamily);
  op->constructAggResults();
  ASSERT_NE(op->current_data_chunk_, nullptr);
  op->funcs_.push_back(AggregateFuncFactory::CreateCountRow(1, sizeof(k_int64)));
  ASSERT_TRUE(op->group_by_metadata_.initialize());

  MockRowBatch batch(1);
  batch.AddRow();
  batch.AddRow();
  const k_int64 first_group = 100;
  const k_int64 second_group = 200;
  batch.SetValue(0, 0, first_group);
  batch.SetValue(1, 0, second_group);

  EXPECT_EQ(op->AddRowBatchData(ctx_, &batch), KStatus::SUCCESS);
  EXPECT_EQ(batch.Count(), 1U);
  EXPECT_EQ(op->max_ts_, second_group);
  EXPECT_EQ(op->current_data_chunk_->Count(), 1U);
  EXPECT_EQ(op->FinishProcess(ctx_), KStatus::SUCCESS);
}

TEST_F(AggScanWhiteBoxTest, GetAggResultAllocatesChunkAndCloneKeepsChild) {
  auto op = MakeOperator();
  op->output_col_num_ = 2;
  op->output_col_info_ = KNEW ColumnInfo[2];
  op->output_col_info_[0] = MakeColumn(roachpb::DataType::BIGINT,
                                       sizeof(k_int64),
                                       KWDBTypeFamily::IntFamily);
  op->output_col_info_[1] = MakeColumn(roachpb::DataType::VARCHAR, 16,
                                       KWDBTypeFamily::StringFamily);
  op->temporary_data_chunk_ =
      std::make_unique<DataChunk>(op->output_col_info_, op->output_col_num_, 2);
  ASSERT_TRUE(op->temporary_data_chunk_->Initialize());
  op->temporary_data_chunk_->SetCount(2);

  op->agg_num_ = 2;
  op->agg_renders_ = static_cast<Field**>(malloc(2 * sizeof(Field*)));
  ASSERT_NE(op->agg_renders_, nullptr);
  op->agg_renders_[0] =
      new FieldConstInt(roachpb::DataType::BIGINT, 7, sizeof(k_int64));
  op->agg_renders_[1] =
      new FieldConstString(roachpb::DataType::VARCHAR, "ok");

  DataChunkPtr result;
  ASSERT_EQ(op->getAggResult(ctx_, result), KStatus::SUCCESS);
  ASSERT_NE(result, nullptr);
  EXPECT_EQ(result->Count(), 2U);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(result->GetData(0, 0)), 7);
  k_uint16 len = 0;
  DatumPtr str = result->GetData(0, 1, len);
  EXPECT_EQ(std::string(str, str + len), "ok");

  DummyChildOperator child;
  op->AddDependency(&child);
  BaseOperator* clone = op->Clone();
  ASSERT_NE(clone, nullptr);
  EXPECT_EQ(clone->GetChildren().size(), 1U);
  EXPECT_EQ(clone->GetChildren()[0], &child);
  SafeDeletePointer(op->agg_renders_[0]);
  SafeDeletePointer(op->agg_renders_[1]);
  SafeDeletePointer(clone);
}

}  // namespace
}  // namespace kwdbts
