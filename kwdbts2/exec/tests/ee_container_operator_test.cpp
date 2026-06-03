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

#include <chrono>
#include <cstring>
#include <queue>
#include <thread>
#include <string>
#include <utility>
#include <vector>

#include "gtest/gtest.h"

#define private public
#define protected public
#include "ee_local_merge_inbound_op.h"
#include "ee_memory_data_container.h"
#undef protected
#undef private

#include "ee_kwthd_context.h"
#include "ee_mempool.h"
#include "ee_pipeline_group.h"
#include "ee_result_collector_op.h"

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

std::unique_ptr<DataChunk> MakeIntChunk(const std::vector<k_int32>& values) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  auto chunk =
      std::make_unique<OwnedColumnDataChunk>(col_info, 1,
                                             static_cast<k_uint32>(values.size()));
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < values.size(); ++i) {
    chunk->AddCount();
    auto value = values[i];
    chunk->InsertData(i, 0, reinterpret_cast<char*>(&value), sizeof(value));
  }
  return chunk;
}

std::unique_ptr<DataChunk> MakeIntVarcharChunk(
    const std::vector<std::pair<k_int32, std::string>>& rows) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  col_info[1] =
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  auto chunk =
      std::make_unique<OwnedColumnDataChunk>(col_info, 2,
                                             static_cast<k_uint32>(rows.size()));
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < rows.size(); ++i) {
    chunk->AddCount();
    auto value = rows[i].first;
    chunk->InsertData(i, 0, reinterpret_cast<char*>(&value), sizeof(value));
    chunk->InsertData(i, 1, const_cast<char*>(rows[i].second.data()),
                      rows[i].second.size());
  }
  return chunk;
}

class CountingChildOperator : public BaseOperator {
 public:
  CountingChildOperator() : BaseOperator(nullptr, nullptr, nullptr, 0) {}

  OperatorType Type() override { return OperatorType::OPERATOR_NOOP; }

  EEIteratorErrCode Init(kwdbContext_p) override {
    ++init_calls_;
    return init_code_;
  }

  EEIteratorErrCode Start(kwdbContext_p) override {
    ++start_calls_;
    return start_code_;
  }

  EEIteratorErrCode Next(kwdbContext_p, DataChunkPtr&) override {
    return EEIteratorErrCode::EE_END_OF_RECORD;
  }

  EEIteratorErrCode Reset(kwdbContext_p) override {
    ++reset_calls_;
    return reset_code_;
  }

  EEIteratorErrCode Close(kwdbContext_p) override {
    ++close_calls_;
    return close_code_;
  }

  KStatus BuildPipeline(PipelineGroup*, Processors*) override {
    ++build_calls_;
    return build_code_;
  }

  int init_calls_{0};
  int start_calls_{0};
  int reset_calls_{0};
  int close_calls_{0};
  int build_calls_{0};
  EEIteratorErrCode init_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode start_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode reset_code_{EEIteratorErrCode::EE_OK};
  EEIteratorErrCode close_code_{EEIteratorErrCode::EE_OK};
  KStatus build_code_{KStatus::SUCCESS};
};

class ExposedLocalMergeInboundOperator : public LocalMergeInboundOperator {
 public:
  using LocalMergeInboundOperator::LocalMergeInboundOperator;
  using LocalMergeInboundOperator::CreateSortContainer;
  using LocalMergeInboundOperator::PullChunk;
};

class ContainerOperatorTest : public ::testing::Test {
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
    InitServerKWDBContext(&context_);
    current_thd = KNEW KWThdContext();
  }

  void TearDown() override { SafeDeletePointer(current_thd); }

  kwdbContext_t context_{};
};

TEST_F(ContainerOperatorTest, ResultCollectorDelegatesLifecycleAndPublishesFinishState) {
  CountingChildOperator child;
  ResultCollectorOperator collector;
  collector.AddDependency(&child);

  EXPECT_EQ(collector.Init(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.init_calls_, 1);
  EXPECT_EQ(collector.Start(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(collector.Reset(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.reset_calls_, 1);
  EXPECT_EQ(collector.Close(&context_), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(child.close_calls_, 1);

  EEPgErrorInfo pg_info;
  pg_info.code = 1234;
  std::snprintf(pg_info.msg, sizeof(pg_info.msg), "%s", "collector failed");
  collector.PushFinish(EEIteratorErrCode::EE_ERROR, 0, pg_info);
  DataChunkPtr chunk = nullptr;
  EXPECT_EQ(collector.Next(&context_, chunk), EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, 1234);

  ResultCollectorOperator eos_collector;
  eos_collector.PushFinish(EEIteratorErrCode::EE_END_OF_RECORD, 0,
                           EEPgErrorInfo{});
  EXPECT_EQ(eos_collector.Next(&context_, chunk),
            EEIteratorErrCode::EE_END_OF_RECORD);
}

TEST_F(ContainerOperatorTest, ResultCollectorBuildPipelineUsesChildResult) {
  CountingChildOperator child;
  ResultCollectorOperator collector;
  collector.AddDependency(&child);
  PipelineGroup pipeline(nullptr);

  EXPECT_EQ(collector.BuildPipeline(&pipeline, nullptr), KStatus::SUCCESS);
  EXPECT_EQ(pipeline.operator_, &collector);
  EXPECT_EQ(child.build_calls_, 1);
}

TEST_F(ContainerOperatorTest, ResultCollectorWaitsUntilFinishIsPushed) {
  ResultCollectorOperator collector;
  EEIteratorErrCode next_code = EEIteratorErrCode::EE_ERROR;

  std::thread waiter([&]() {
    kwdbContext_t worker_ctx{};
    InitServerKWDBContext(&worker_ctx);
    DataChunkPtr chunk = nullptr;
    next_code = collector.Next(&worker_ctx, chunk);
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  collector.PushFinish(EEIteratorErrCode::EE_END_OF_RECORD, 0, EEPgErrorInfo{});
  waiter.join();

  EXPECT_EQ(next_code, EEIteratorErrCode::EE_END_OF_RECORD);
}

TEST_F(ContainerOperatorTest, PgErrorInfoHandlesNullAndLongMessages) {
  EEPgErrorInfo::ResetPgErrorInfo();
  EXPECT_FALSE(EEPgErrorInfo::IsError());

  EEPgErrorInfo::SetPgErrorInfo(99, nullptr);
  EXPECT_TRUE(EEPgErrorInfo::IsError());
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, 99);
  EXPECT_STREQ(EEPgErrorInfo::GetPgErrorInfo().msg, "");

  EEPgErrorInfo::ResetPgErrorInfo();
  std::string long_msg(MAX_PG_ERROR_MSG_LEN + 8, 'x');
  EEPgErrorInfo::SetPgErrorInfo(100, long_msg.c_str());
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, 100);
  EXPECT_EQ(std::strlen(EEPgErrorInfo::GetPgErrorInfo().msg),
            MAX_PG_ERROR_MSG_LEN - 1);

  EEPgErrorInfo::SetPgErrorInfo(101, "ignored");
  EXPECT_EQ(EEPgErrorInfo::GetPgErrorInfo().code, 100);
}

TEST_F(ContainerOperatorTest, MemRowContainerHandlesDisorderSortingAndSelectors) {
  ColumnInfo simple_col_info[1] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily)};
  MemRowContainer disorder_container(simple_col_info, 1);
  ASSERT_EQ(disorder_container.Init(), KStatus::SUCCESS);

  auto empty_chunk = MakeIntChunk({});
  EXPECT_EQ(disorder_container.Append(empty_chunk), KStatus::SUCCESS);

  DataChunkPtr next_chunk = nullptr;
  EXPECT_EQ(disorder_container.NextChunk(next_chunk),
            EEIteratorErrCode::EE_END_OF_RECORD);

  std::queue<DataChunkPtr> buffer;
  buffer.push(MakeIntChunk({1, 2}));
  buffer.push(MakeIntChunk({3}));
  ASSERT_EQ(disorder_container.Append(buffer), KStatus::SUCCESS);
  EXPECT_EQ(disorder_container.Count(), 3U);

  ASSERT_EQ(disorder_container.NextChunk(next_chunk), EEIteratorErrCode::EE_OK);
  ASSERT_NE(next_chunk, nullptr);
  EXPECT_EQ(next_chunk->Count(), 1U);
  ASSERT_EQ(disorder_container.NextChunk(next_chunk), EEIteratorErrCode::EE_OK);
  ASSERT_NE(next_chunk, nullptr);
  EXPECT_EQ(next_chunk->Count(), 2U);
  EXPECT_EQ(disorder_container.NextChunk(next_chunk),
            EEIteratorErrCode::EE_END_OF_RECORD);

  std::vector<ColumnOrderInfo> order_info{
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC}};
  MemRowContainer sorted_container(order_info, simple_col_info, 1);
  ASSERT_EQ(sorted_container.Init(), KStatus::SUCCESS);
  auto sort_chunk = MakeIntChunk({3, 1});
  ASSERT_EQ(sorted_container.Append(sort_chunk), KStatus::SUCCESS);
  ASSERT_EQ(sorted_container.Sort(), KStatus::SUCCESS);
  EXPECT_EQ(sorted_container.NextChunk(next_chunk), EEIteratorErrCode::EE_ERROR);

  EXPECT_EQ(sorted_container.NextLine(), 1);
  auto* first_value =
      reinterpret_cast<k_int32*>(sorted_container.GetData(1, 0));
  ASSERT_NE(first_value, nullptr);
  EXPECT_EQ(*first_value, 1);
  EXPECT_FALSE(sorted_container.IsNull(1, 0));
  EXPECT_FALSE(sorted_container.IsNull(0));
  auto* current_value =
      reinterpret_cast<k_int32*>(sorted_container.GetData(0));
  ASSERT_NE(current_value, nullptr);
  EXPECT_EQ(*current_value, 1);
  EXPECT_NE(sorted_container.GetRawData(0), nullptr);

  ColumnInfo wide_col_info[1] = {
      ColumnInfo(131072, roachpb::DataType::CHAR, KWDBTypeFamily::StringFamily)};
  MemRowContainer wide_container(wide_col_info, 1);
  EXPECT_EQ(wide_container.capacity_, 1U);

  ColumnInfo mixed_col_info[2] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};
  std::vector<ColumnOrderInfo> mixed_order_info{
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC}};
  MemRowContainer mixed_container(mixed_order_info, mixed_col_info, 2);
  ASSERT_EQ(mixed_container.Init(), KStatus::SUCCESS);
  auto mixed_chunk = MakeIntVarcharChunk({{2, "b"}, {1, "a"}});
  ASSERT_EQ(mixed_container.Append(mixed_chunk), KStatus::SUCCESS);
  ASSERT_EQ(mixed_container.Sort(), KStatus::SUCCESS);
  EXPECT_EQ(mixed_container.NextLine(), 1);
  k_uint16 str_len = 0;
  auto* str_ptr = mixed_container.GetData(1, 1, str_len);
  ASSERT_NE(str_ptr, nullptr);
  EXPECT_EQ(std::string(str_ptr, str_len), "a");
}

TEST_F(ContainerOperatorTest, LocalMergeInboundCreatesSortContainersAndPullsSortedChunks) {
  TSInputSyncSpec spec;
  auto sort_col_info = std::make_unique<ColumnInfo[]>(1);
  sort_col_info[0] =
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  ExposedLocalMergeInboundOperator op(nullptr, &spec, nullptr);

  auto buffer_chunk = MakeIntChunk({2, 1});
  std::deque<DataChunkPtr> mem_buffer;
  mem_buffer.push_back(std::move(buffer_chunk));
  ASSERT_EQ(op.CreateSortContainer(0, mem_buffer, sort_col_info.get(), 1, true),
            KStatus::SUCCESS);
  ASSERT_NE(op.container_, nullptr);
  EXPECT_EQ(op.container_->Count(), 2U);

  std::deque<DataChunkPtr> empty_disk_buffer;
  ASSERT_EQ(op.CreateSortContainer(BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE + 1,
                                   empty_disk_buffer, sort_col_info.get(), 1,
                                   false),
            KStatus::SUCCESS);
  ASSERT_NE(op.container_, nullptr);

  auto disk_chunk = MakeIntChunk({4, 3});
  std::deque<DataChunkPtr> disk_buffer;
  disk_buffer.push_back(std::move(disk_chunk));
  EXPECT_EQ(op.CreateSortContainer(BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE + 1,
                                   disk_buffer, sort_col_info.get(), 1, false),
            KStatus::FAIL);

  op.order_info_.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  op.chunks_.push_back(MakeIntChunk({3, 1}));
  op.is_finished_ = true;
  auto* output_col_info = new ColumnInfo[1];
  output_col_info[0] = sort_col_info[0];
  DataChunkPtr output = std::make_unique<OwnedColumnDataChunk>(output_col_info, 1, 2);
  ASSERT_TRUE(output->Initialize());
  ASSERT_EQ(op.PullChunk(&context_, output), KStatus::SUCCESS);
  ASSERT_NE(output, nullptr);
  ASSERT_EQ(output->Count(), 2U);
  ASSERT_EQ(output->NextLine(), 0);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(output->GetData(0, 0)), 1);
  ASSERT_EQ(output->NextLine(), 1);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(output->GetData(1, 0)), 3);

  ExposedLocalMergeInboundOperator empty_op(nullptr, &spec, nullptr);
  empty_op.is_finished_ = true;
  DataChunkPtr empty_output = nullptr;
  EXPECT_EQ(empty_op.PullChunk(&context_, empty_output), KStatus::FAIL);
  EXPECT_EQ(empty_output, nullptr);
}

}  // namespace
}  // namespace kwdbts
