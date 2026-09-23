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


#include <climits>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>
#include <vector>

#include "ee_batch_data_container.h"
#include "ee_check_overflow.h"
#include "ee_chunk_cursor.h"
#include "ee_combined_group_key.h"
#include "ee_dynamic_hash_index.h"
#include "ee_field.h"
#include "ee_internal_type.h"
#include "ee_merge_cursor.h"
#include "ee_mempool.h"
#include "ee_protobuf_serde.h"
#include "gtest/gtest.h"

namespace kwdbts {
namespace {

DataChunkPtr MakeSingleColumnChunk(roachpb::DataType type, KWDBTypeFamily family,
                                   k_uint32 length, bool allow_null = false) {
  static ColumnInfo column;
  column = ColumnInfo(length, type, family);
  column.allow_null = allow_null;

  DataChunkPtr chunk = std::make_unique<DataChunk>(&column, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  return chunk;
}

template <typename T>
DataChunkPtr MakeNumericChunk(roachpb::DataType type, KWDBTypeFamily family,
                              T value) {
  auto chunk = MakeSingleColumnChunk(type, family, sizeof(T));
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&value), sizeof(T));
  return chunk;
}

DataChunkPtr MakeStringChunk(roachpb::DataType type, KWDBTypeFamily family,
                             const char* value, bool allow_null = false) {
  auto chunk = MakeSingleColumnChunk(type, family, 16, allow_null);
  chunk->InsertData(0, 0, const_cast<char*>(value), std::strlen(value));
  return chunk;
}

DataChunkPtr MakeIntStringChunk() {
  static ColumnInfo columns[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};
  columns[0].allow_null = false;
  columns[1].allow_null = true;

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 2, 2);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount(2);

  const k_int32 id = 7;
  chunk->InsertData(0, 0, reinterpret_cast<char*>(const_cast<k_int32*>(&id)),
                    sizeof(id));
  chunk->InsertData(0, 1, const_cast<char*>("alpha"), 5);

  chunk->InsertData(1, 0, reinterpret_cast<char*>(const_cast<k_int32*>(&id)),
                    sizeof(id));
  chunk->InsertData(1, 1, const_cast<char*>("beta"), 4);
  return chunk;
}

DataChunkPtr MakeOrderedIntChunk(const std::vector<k_int32>& values) {
  static ColumnInfo column(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  column.allow_null = false;

  DataChunkPtr chunk = std::make_unique<DataChunk>(&column, 1, values.size());
  EXPECT_TRUE(chunk->Initialize());
  for (size_t i = 0; i < values.size(); ++i) {
    chunk->AddCount();
    auto value = values[i];
    chunk->InsertData(static_cast<k_uint32>(i), 0,
                      reinterpret_cast<char*>(&value), sizeof(value));
  }
  return chunk;
}

std::vector<k_int32> ReadIntValues(DataChunk* chunk) {
  std::vector<k_int32> values;
  values.reserve(chunk->Count());
  for (k_uint32 i = 0; i < chunk->Count(); ++i) {
    values.push_back(*reinterpret_cast<k_int32*>(chunk->GetData(i, 0)));
  }
  return values;
}

DataChunkPtr MakeIntVarcharChunkForSerde() {
  static ColumnInfo columns[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily),
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};
  columns[0].allow_null = false;
  columns[1].allow_null = true;

  DataChunkPtr chunk = std::make_unique<DataChunk>(columns, 2, 2);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount(2);

  const k_int32 left = 11;
  const k_int32 right = 22;
  chunk->InsertData(0, 0, reinterpret_cast<char*>(const_cast<k_int32*>(&left)),
                    sizeof(left));
  chunk->InsertData(0, 1, const_cast<char*>("alpha"), 5);
  chunk->InsertData(1, 0,
                    reinterpret_cast<char*>(const_cast<k_int32*>(&right)),
                    sizeof(right));
  chunk->InsertData(1, 1, const_cast<char*>("beta"), 4);
  return chunk;
}

const std::vector<k_uint32> kSortedIntColumns{0};

struct CursorState {
  std::vector<std::vector<k_int32>> batches;
  size_t next_batch{0};
};

std::unique_ptr<SortedChunkCursor> MakeSortedIntCursor(
    std::vector<std::vector<k_int32>> batches) {
  auto state = std::make_shared<CursorState>();
  state->batches = std::move(batches);

  DataChunkProvider provider =
      [state](DataChunkPtr* chunk, k_bool* is_end) -> k_bool {
    if (chunk == nullptr && is_end == nullptr) {
      return true;
    }

    if (state->next_batch >= state->batches.size()) {
      *chunk = nullptr;
      *is_end = true;
      return true;
    }

    *chunk = MakeOrderedIntChunk(state->batches[state->next_batch]);
    ++state->next_batch;
    *is_end = state->next_batch >= state->batches.size();
    return true;
  };

  return std::make_unique<SortedChunkCursor>(provider, &kSortedIntColumns);
}

class ExecUtilsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
  }

  void TearDown() override {
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
};

TEST_F(ExecUtilsTest, CheckOverflowHelpersHandleBoundaryCases) {
  EXPECT_DOUBLE_EQ(check_float_overflow(3.25), 3.25);
  EXPECT_DOUBLE_EQ(check_float_overflow(std::numeric_limits<double>::infinity()),
                   0.0);
  EXPECT_DOUBLE_EQ(
      check_float_overflow(-std::numeric_limits<double>::infinity()), 0.0);

  EXPECT_EQ(check_integer_overflow(10, false, false), 10);
  EXPECT_EQ(check_integer_overflow(10, true, true), 10);
  EXPECT_EQ(check_integer_overflow(-1, true, false), 0);
  EXPECT_EQ(check_integer_overflow(-1, false, true), 0);
}

TEST_F(ExecUtilsTest, SortedChunkCursorTracksReadinessAndEndState) {
  const std::vector<k_uint32> sorted_columns{0, 1};
  int ready_calls = 0;
  int fetch_calls = 0;
  DataChunkProvider provider =
      [&](DataChunkPtr* chunk, k_bool* is_end) -> k_bool {
    if (chunk == nullptr && is_end == nullptr) {
      ready_calls++;
      return ready_calls > 1;
    }

    fetch_calls++;
    if (fetch_calls == 1) {
      *chunk = MakeIntStringChunk();
      *is_end = false;
      return true;
    }

    *chunk = nullptr;
    *is_end = true;
    return true;
  };

  SortedChunkCursor cursor(provider, &sorted_columns);
  EXPECT_FALSE(cursor.IsDataReady());
  EXPECT_TRUE(cursor.IsDataReady());
  EXPECT_FALSE(cursor.IsAtEnd());

  auto first_chunk = cursor.FetchNextSortedChunk();
  ASSERT_NE(first_chunk.first, nullptr);
  EXPECT_EQ(first_chunk.first->Count(), 2U);
  EXPECT_EQ(first_chunk.second, sorted_columns);
  EXPECT_FALSE(cursor.IsAtEnd());

  auto second_chunk = cursor.FetchNextSortedChunk();
  EXPECT_EQ(second_chunk.first, nullptr);
  EXPECT_TRUE(second_chunk.second.empty());
  EXPECT_TRUE(cursor.IsAtEnd());

  auto already_end = cursor.FetchNextSortedChunk();
  EXPECT_EQ(already_end.first, nullptr);
  EXPECT_TRUE(already_end.second.empty());
}

TEST_F(ExecUtilsTest, CombinedGroupKeyFormatsAndDetectsGroupChanges) {
  auto chunk = MakeIntStringChunk();
  std::vector<k_uint32> group_cols{0, 1};

  CombinedGroupKey key;
  ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), group_cols));
  key.AddGroupKey(chunk->GetData(0, 0), 0);
  key.AddGroupKey(chunk->GetData(0, 1), 1);

  EXPECT_EQ(key.to_string(), "7,alpha");
  EXPECT_FALSE(key.IsNewGroup(chunk.get(), 0, group_cols));
  EXPECT_TRUE(key.IsNewGroup(chunk.get(), 1, group_cols));

  CombinedGroupKey same_key;
  ASSERT_TRUE(same_key.Init(chunk->GetColumnInfo(), group_cols));
  same_key.AddGroupKey(chunk->GetData(0, 0), 0);
  same_key.AddGroupKey(chunk->GetData(0, 1), 1);
  EXPECT_TRUE(key == same_key);

  chunk->SetNull(1, 1);
  CombinedGroupKey null_key;
  ASSERT_TRUE(null_key.Init(chunk->GetColumnInfo(), group_cols));
  null_key.AddGroupKey(chunk->GetData(1, 0), 0);
  null_key.AddGroupKey(nullptr, 1);
  EXPECT_EQ(null_key.to_string(), "7,NULL");
  EXPECT_FALSE(key == null_key);
  EXPECT_FALSE(null_key.IsNewGroup(chunk.get(), 1, group_cols));
}

TEST_F(ExecUtilsTest, CombinedGroupKeyCoversTypeAliasesAndMismatchBranches) {
  {
    auto chunk = MakeNumericChunk(roachpb::DataType::BOOL,
                                  KWDBTypeFamily::BoolFamily, k_bool{1});
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_EQ(key.to_string(), "1");
  }
  {
    auto chunk = MakeNumericChunk(roachpb::DataType::SMALLINT,
                                  KWDBTypeFamily::IntFamily, k_int16{12});
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_EQ(key.to_string(), "12");
  }
  for (auto type : {roachpb::DataType::TIMESTAMP,
                    roachpb::DataType::TIMESTAMPTZ,
                    roachpb::DataType::TIMESTAMP_MICRO,
                    roachpb::DataType::TIMESTAMP_NANO,
                    roachpb::DataType::TIMESTAMPTZ_MICRO,
                    roachpb::DataType::TIMESTAMPTZ_NANO,
                    roachpb::DataType::DATE,
                    roachpb::DataType::BIGINT}) {
    auto chunk = MakeNumericChunk(type, KWDBTypeFamily::TimestampTZFamily,
                                  static_cast<k_int64>(99));
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_EQ(key.to_string(), "99");
  }
  {
    auto chunk = MakeNumericChunk(roachpb::DataType::FLOAT,
                                  KWDBTypeFamily::FloatFamily, k_float32{1.5F});
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_FALSE(key.to_string().empty());
  }
  {
    auto chunk = MakeNumericChunk(roachpb::DataType::DOUBLE,
                                  KWDBTypeFamily::FloatFamily, k_double64{2.5});
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_FALSE(key.to_string().empty());
  }
  for (auto type : {roachpb::DataType::CHAR, roachpb::DataType::VARCHAR,
                    roachpb::DataType::NCHAR, roachpb::DataType::NVARCHAR,
                    roachpb::DataType::BINARY,
                    roachpb::DataType::VARBINARY}) {
    auto chunk = MakeStringChunk(type, KWDBTypeFamily::StringFamily, "tag");
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_EQ(key.to_string(), "tag");
  }
  {
    auto chunk = MakeNumericChunk(roachpb::DataType::UNKNOWN,
                                  KWDBTypeFamily::AnyFamily, k_int32{7});
    CombinedGroupKey key;
    ASSERT_TRUE(key.Init(chunk->GetColumnInfo(), {0}));
    key.AddGroupKey(chunk->GetData(0, 0), 0);
    EXPECT_EQ(key.to_string(), "");
  }

  auto int_chunk = MakeNumericChunk(roachpb::DataType::INT, KWDBTypeFamily::IntFamily,
                                    k_int32{8});
  CombinedGroupKey short_key;
  ASSERT_TRUE(short_key.Init(int_chunk->GetColumnInfo(), {0}));
  short_key.AddGroupKey(int_chunk->GetData(0, 0), 0);

  CombinedGroupKey larger_key;
  ASSERT_TRUE(larger_key.Init(int_chunk->GetColumnInfo(), {0, 0}));
  larger_key.AddGroupKey(int_chunk->GetData(0, 0), 0);
  larger_key.AddGroupKey(int_chunk->GetData(0, 0), 1);
  EXPECT_FALSE(short_key == larger_key);

  auto bigint_chunk = MakeNumericChunk(roachpb::DataType::BIGINT,
                                       KWDBTypeFamily::IntFamily, k_int64{8});
  CombinedGroupKey bigint_key;
  ASSERT_TRUE(bigint_key.Init(bigint_chunk->GetColumnInfo(), {0}));
  bigint_key.AddGroupKey(bigint_chunk->GetData(0, 0), 0);
  EXPECT_FALSE(short_key == bigint_key);

  auto string_chunk = MakeStringChunk(roachpb::DataType::VARCHAR,
                                      KWDBTypeFamily::StringFamily, "tag");
  auto string_chunk_other = MakeStringChunk(roachpb::DataType::VARCHAR,
                                            KWDBTypeFamily::StringFamily,
                                            "tag2");
  CombinedGroupKey string_key;
  ASSERT_TRUE(string_key.Init(string_chunk->GetColumnInfo(), {0}));
  string_key.AddGroupKey(string_chunk->GetData(0, 0), 0);
  CombinedGroupKey other_string_key;
  ASSERT_TRUE(other_string_key.Init(string_chunk_other->GetColumnInfo(), {0}));
  other_string_key.AddGroupKey(string_chunk_other->GetData(0, 0), 0);
  EXPECT_FALSE(string_key == other_string_key);

  auto nullable_chunk = MakeStringChunk(roachpb::DataType::VARCHAR,
                                        KWDBTypeFamily::StringFamily, "same",
                                        true);
  CombinedGroupKey null_data_key;
  ASSERT_TRUE(null_data_key.Init(nullable_chunk->GetColumnInfo(), {0}));
  null_data_key.AddGroupKey(nullptr, 0);
  EXPECT_TRUE(null_data_key.IsNewGroup(nullable_chunk.get(), 0, {0}));

  nullable_chunk->SetNull(0, 0);
  CombinedGroupKey null_group_key;
  ASSERT_TRUE(null_group_key.Init(nullable_chunk->GetColumnInfo(), {0}));
  null_group_key.AddGroupKey(nullptr, 0);
  EXPECT_FALSE(null_group_key.IsNewGroup(nullable_chunk.get(), 0, {0}));
}

TEST_F(ExecUtilsTest, BatchDataLinkedListResetAndContainerBuildLinkedList) {
  BatchDataLinkedList linked_list;
  ASSERT_EQ(linked_list.init(2), 0);
  RowIndice* external_rows = static_cast<RowIndice*>(malloc(2 * sizeof(RowIndice)));
  ASSERT_NE(external_rows, nullptr);
  external_rows[0] = {9, 1};
  external_rows[1] = {9, 2};
  linked_list.ResetDataPtr(external_rows);
  EXPECT_EQ(linked_list.row_indice_list_[0].batch_no, 9);
  linked_list.row_indice_list_ = nullptr;
  free(external_rows);

  BatchDataContainer container;
  static ColumnInfo rel_columns[] = {
      ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily)};
  rel_columns[0].allow_null = false;

  DataChunkPtr rel_chunk = std::make_unique<DataChunk>(rel_columns, 1, 2);
  ASSERT_TRUE(rel_chunk->Initialize());
  rel_chunk->AddCount(2);

  const k_int32 join_key = 42;
  rel_chunk->InsertData(
      0, 0, reinterpret_cast<char*>(const_cast<k_int32*>(&join_key)),
      sizeof(join_key));
  rel_chunk->InsertData(
      1, 0, reinterpret_cast<char*>(const_cast<k_int32*>(&join_key)),
      sizeof(join_key));

  DynamicHashIndex rel_hash_index;
  ASSERT_EQ(rel_hash_index.init(sizeof(k_int32)), 0);

  std::vector<k_uint32> rel_key_cols{0};
  std::vector<k_uint32> join_column_lengths{sizeof(k_int32)};
  char join_buffer[sizeof(k_int32)] = {0};
  EXPECT_EQ(container.AddRelDataChunkAndBuildLinkedList(
                &rel_hash_index, rel_chunk, rel_key_cols, join_column_lengths,
                join_buffer, sizeof(join_key)),
            KStatus::SUCCESS);

  ASSERT_EQ(container.rel_data_container_.GetBatchCount(), 1);
  ASSERT_EQ(container.GetRelDataChunk(0)->Count(), 2U);

  LinkedListPtr& stored_list = container.rel_data_linked_lists_.GetLinkedList(0);
  ASSERT_NE(stored_list, nullptr);
  EXPECT_EQ(stored_list->row_indice_list_[0].batch_no, 0);
  EXPECT_EQ(stored_list->row_indice_list_[0].offset_in_batch, 0);
  EXPECT_EQ(stored_list->row_indice_list_[1].batch_no, 1);
  EXPECT_EQ(stored_list->row_indice_list_[1].offset_in_batch, 1);

  ResultSet result(1);
  container.AddPhysicalTagData(result);
  ASSERT_EQ(container.physical_tag_data_batches_.size(), 1U);
  EXPECT_EQ(container.physical_tag_data_batches_[0].size(), 1U);
}

TEST_F(ExecUtilsTest, BatchDataContainerHandlesStringJoinKeysAndNullRows) {
  BatchDataContainer container;
  static ColumnInfo rel_columns[] = {
      ColumnInfo(16, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily)};
  rel_columns[0].allow_null = true;

  DataChunkPtr rel_chunk = std::make_unique<DataChunk>(rel_columns, 1, 3);
  ASSERT_TRUE(rel_chunk->Initialize());
  rel_chunk->AddCount(3);
  rel_chunk->InsertData(0, 0, const_cast<char*>("alpha"), 5);
  rel_chunk->SetNull(1, 0);
  rel_chunk->InsertData(2, 0, const_cast<char*>("alpha"), 5);

  DynamicHashIndex rel_hash_index;
  ASSERT_EQ(rel_hash_index.init(8), 0);

  std::vector<k_uint32> rel_key_cols{0};
  std::vector<k_uint32> join_column_lengths{8};
  char join_buffer[8] = {0};
  EXPECT_EQ(container.AddRelDataChunkAndBuildLinkedList(
                &rel_hash_index, rel_chunk, rel_key_cols, join_column_lengths,
                join_buffer, sizeof(join_buffer)),
            KStatus::SUCCESS);

  LinkedListPtr& stored_list = container.rel_data_linked_lists_.GetLinkedList(0);
  ASSERT_NE(stored_list, nullptr);
  EXPECT_EQ(stored_list->row_indice_list_[0].batch_no, 0);
  EXPECT_EQ(stored_list->row_indice_list_[1].batch_no, 0);
  EXPECT_EQ(stored_list->row_indice_list_[2].batch_no, 1);
  EXPECT_EQ(stored_list->row_indice_list_[2].offset_in_batch, 1);
}

TEST_F(ExecUtilsTest, InternalTypeEncodingAndFieldCreationCoverFamilies) {
  EXPECT_EQ(GetInternalOutputType(MarshalToOutputType(KWDBTypeFamily::IntFamily, 32)),
            KWDBTypeFamily::IntFamily);
  EXPECT_EQ(GetInternalOutputType(std::string("\x08", 1)),
            KWDBTypeFamily::AnyFamily);

  Field* field = nullptr;
  auto expect_field = [&](KWDBTypeFamily family, k_uint32 width,
                          roachpb::DataType storage_type) {
    field = nullptr;
    const std::string encoded = MarshalToOutputType(family, width);
    ASSERT_EQ(GetInternalField(encoded.data(), encoded.size(), &field, 3),
              KStatus::SUCCESS);
    ASSERT_NE(field, nullptr);
    EXPECT_EQ(field->get_storage_type(), storage_type);
    EXPECT_TRUE(field->is_chunk_);
    EXPECT_EQ(field->get_return_type(), family);
    EXPECT_EQ(field->get_column_type(),
              roachpb::KWDBKTSColumn::ColumnType::
                  KWDBKTSColumn_ColumnType_TYPE_DATA);
    delete field;
    field = nullptr;
  };

  expect_field(KWDBTypeFamily::BoolFamily, 1, roachpb::DataType::BOOL);
  expect_field(KWDBTypeFamily::IntFamily, 16, roachpb::DataType::SMALLINT);
  expect_field(KWDBTypeFamily::IntFamily, 32, roachpb::DataType::INT);
  expect_field(KWDBTypeFamily::IntFamily, 64, roachpb::DataType::BIGINT);
  expect_field(KWDBTypeFamily::FloatFamily, 32, roachpb::DataType::FLOAT);
  expect_field(KWDBTypeFamily::FloatFamily, 64, roachpb::DataType::DOUBLE);
  expect_field(KWDBTypeFamily::DecimalFamily, 0, roachpb::DataType::DECIMAL);
  expect_field(KWDBTypeFamily::DateFamily, 0, roachpb::DataType::DATE);
  expect_field(KWDBTypeFamily::TimestampFamily, 8,
               roachpb::DataType::TIMESTAMP);
  expect_field(KWDBTypeFamily::IntervalFamily, 8,
               roachpb::DataType::TIMESTAMPTZ);
  expect_field(KWDBTypeFamily::StringFamily, 0, roachpb::DataType::VARCHAR);
  expect_field(KWDBTypeFamily::BytesFamily, 0, roachpb::DataType::VARBINARY);
  expect_field(KWDBTypeFamily::TimestampTZFamily, 8,
               roachpb::DataType::TIMESTAMPTZ);

  const std::string bad_int = MarshalToOutputType(KWDBTypeFamily::IntFamily, 8);
  EXPECT_EQ(GetInternalField(bad_int.data(), bad_int.size(), &field, 1),
            KStatus::FAIL);

  const std::string bad_float =
      MarshalToOutputType(KWDBTypeFamily::FloatFamily, 16);
  EXPECT_EQ(GetInternalField(bad_float.data(), bad_float.size(), &field, 1),
            KStatus::FAIL);

  const std::string bad_family =
      MarshalToOutputType(KWDBTypeFamily::AnyFamily, 0);
  EXPECT_EQ(GetInternalField(bad_family.data(), bad_family.size(), &field, 1),
            KStatus::FAIL);

  const std::string truncated("\x08", 1);
  EXPECT_EQ(GetInternalField(truncated.data(), truncated.size(), &field, 1),
            KStatus::FAIL);
}

TEST_F(ExecUtilsTest, MergeCursorUtilitiesCoverRunMergeAndCascadePaths) {
  SortingRules rules = SortingRules::AscNullLast(1);

  DataChunkPtr left_unique = MakeOrderedIntChunk({1, 3});
  DataChunkPtr right_unique = MakeOrderedIntChunk({2, 4});
  DataChunkSPtr left_chunk(left_unique.release());
  DataChunkSPtr right_chunk(right_unique.release());

  SortedRun left_run(left_chunk, &kSortedIntColumns);
  SortedRun right_run(right_chunk, &kSortedIntColumns);
  EXPECT_EQ(left_run.Intersect(rules, right_run), 0);
  EXPECT_EQ(right_run.Intersect(rules, left_run), 0);
  EXPECT_EQ(left_run.GetRangeLength(), 2U);
  EXPECT_TRUE(left_run.HasOrderByColumn(0));
  EXPECT_FALSE(left_run.HasOrderByColumn(1));
  EXPECT_EQ(left_run.GetColumnIdx(0), 0U);

  SortedRun left_slice(left_run, 0, 1);
  DataChunkPtr slice_chunk = left_slice.CloneDataSlice();
  ASSERT_NE(slice_chunk, nullptr);
  EXPECT_EQ(ReadIntValues(slice_chunk.get()), std::vector<k_int32>({1}));
  EXPECT_LT(left_run.CompareRow(rules, right_run, 0, 0), 0);
  EXPECT_GT(CursorOperationAlgorithm::CompareTailRow(rules, right_run, left_run),
            0);

  Permutation permutation;
  ASSERT_EQ(MergeTwoSortedRuns(rules, left_run, right_run, &permutation),
            KStatus::SUCCESS);
  ASSERT_EQ(permutation.size(), 4U);
  EXPECT_EQ(permutation[0].chunk_index, 0U);
  EXPECT_EQ(permutation[0].index_in_chunk, 0U);
  EXPECT_EQ(permutation[1].chunk_index, 1U);
  EXPECT_EQ(permutation[1].index_in_chunk, 0U);
  EXPECT_EQ(permutation[2].chunk_index, 0U);
  EXPECT_EQ(permutation[2].index_in_chunk, 1U);
  EXPECT_EQ(permutation[3].chunk_index, 1U);
  EXPECT_EQ(permutation[3].index_in_chunk, 1U);

  SortedRuns runs(left_run);
  runs.chunks.push_back(right_run);
  EXPECT_EQ(runs.Count(), 4U);
  runs.Clear();
  EXPECT_EQ(runs.Chunksize(), 0U);

  TwoCursorMerger merger(rules, MakeSortedIntCursor({{1, 3}}),
                         MakeSortedIntCursor({{2, 4}}));
  EXPECT_TRUE(merger.IsDataReady());
  EXPECT_FALSE(merger.IsAtEnd());

  DataChunkPtr merged = nullptr;
  ASSERT_EQ(merger.GetNextMergedChunk(merged), KStatus::SUCCESS);
  ASSERT_NE(merged, nullptr);
  EXPECT_EQ(ReadIntValues(merged.get()), std::vector<k_int32>({1, 2}));

  merged = nullptr;
  ASSERT_EQ(merger.GetNextMergedChunk(merged), KStatus::SUCCESS);
  ASSERT_NE(merged, nullptr);
  EXPECT_EQ(ReadIntValues(merged.get()), std::vector<k_int32>({3, 4}));
  EXPECT_TRUE(merger.IsAtEnd());

  TwoCursorMerger cursor_merger(rules, MakeSortedIntCursor({{1, 3}}),
                                MakeSortedIntCursor({{2, 4}}));
  auto merged_cursor = cursor_merger.AsChunkCursor();
  auto merged_pair = merged_cursor->FetchNextSortedChunk();
  ASSERT_NE(merged_pair.first, nullptr);
  EXPECT_EQ(ReadIntValues(merged_pair.first.get()),
            std::vector<k_int32>({1, 2}));

  std::vector<std::unique_ptr<SortedChunkCursor>> cursors;
  cursors.push_back(MakeSortedIntCursor({{1, 4}}));
  cursors.push_back(MakeSortedIntCursor({{2, 5}}));
  cursors.push_back(MakeSortedIntCursor({{3, 6}}));

  CascadingCursorMerger cascading_merger;
  ASSERT_EQ(cascading_merger.Init(rules, std::move(cursors)), KStatus::SUCCESS);
  EXPECT_TRUE(cascading_merger.IsDataReady());
  EXPECT_EQ(ReadIntValues(cascading_merger.FetchNextSortedChunk().get()),
            std::vector<k_int32>({1, 2}));
  EXPECT_EQ(ReadIntValues(cascading_merger.FetchNextSortedChunk().get()),
            std::vector<k_int32>({3, 4}));
  EXPECT_EQ(ReadIntValues(cascading_merger.FetchNextSortedChunk().get()),
            std::vector<k_int32>({5, 6}));
  EXPECT_EQ(cascading_merger.FetchNextSortedChunk(), nullptr);
  EXPECT_TRUE(cascading_merger.IsAtEnd());
}

TEST_F(ExecUtilsTest, ProtobufSerdeRoundTripsChunkAndColumnMetadata) {
  auto chunk = MakeIntVarcharChunkForSerde();
  auto* columns = chunk->GetColumnInfo();

  ProtobufChunkSerrialde serde;
  k_bool first_chunk = true;
  ChunkPB serialized = serde.SerializeChunk(chunk.get(), &first_chunk);
  EXPECT_EQ(serialized.column_info_size(), 2);
  EXPECT_EQ(serialized.data_size(), chunk->Size());
  EXPECT_FALSE(first_chunk);

  ChunkPB serialized_again = serde.SerializeChunk(chunk.get(), &first_chunk);
  EXPECT_EQ(serialized_again.column_info_size(), 0);

  ChunkPB serialized_column = serde.SerializeColumn(chunk.get(), &first_chunk);
  EXPECT_EQ(serialized_column.data_size(), chunk->Size());
  EXPECT_EQ(serialized_column.column_info_size(), 0);

  ChunkPB column_data = serde.SerializeColumnData(chunk.get());
  ASSERT_EQ(column_data.columns_size(), 2);
  EXPECT_GT(column_data.columns(0).uncompressed_size(), 0);
  EXPECT_GT(column_data.columns(1).uncompressed_size(), 0);

  DataChunkPtr restored = nullptr;
  ASSERT_TRUE(serde.Deserialize(restored, serialized.data(),
                                serialized.serialized_size(),
                                serialized.is_encoding(), columns, 2));
  ASSERT_NE(restored, nullptr);
  EXPECT_EQ(restored->Count(), 2U);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(restored->GetData(0, 0)), 11);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(restored->GetData(1, 0)), 22);

  k_uint16 len = 0;
  auto* first_str = restored->GetData(0, 1, len);
  ASSERT_NE(first_str, nullptr);
  EXPECT_EQ(std::string(first_str, len), "alpha");
  auto* second_str = restored->GetData(1, 1, len);
  ASSERT_NE(second_str, nullptr);
  EXPECT_EQ(std::string(second_str, len), "beta");

  serde.DeserializeColumn(restored, serialized.data(), columns, 2);
}

TEST_F(ExecUtilsTest, ProtobufSerdeHandlesEncodingAndInvalidVersions) {
  static ColumnInfo column(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  DataChunk chunk(&column, 1, 1);
  const unsigned char encoding_buffer[] = {1, 2, 3, 4, 5};
  ASSERT_TRUE(chunk.SetEncodingBuf(encoding_buffer, sizeof(encoding_buffer)));
  chunk.SetCount(1);

  ProtobufChunkSerrialde serde;
  k_bool first_chunk = true;
  ChunkPB serialized = serde.SerializeChunk(&chunk, &first_chunk);
  EXPECT_TRUE(serialized.is_encoding());
  EXPECT_EQ(serialized.data_size(), sizeof(encoding_buffer));

  DataChunkPtr restored = nullptr;
  ASSERT_TRUE(serde.Deserialize(restored, serialized.data(),
                                serialized.serialized_size(),
                                serialized.is_encoding(), &column, 1));
  ASSERT_NE(restored, nullptr);
  EXPECT_TRUE(restored->IsEncoding());
  EXPECT_EQ(restored->GetEncodingBufferLength(), sizeof(encoding_buffer));

  std::string invalid = serialized.data();
  invalid[0] = 0;
  invalid[1] = 0;
  invalid[2] = 0;
  invalid[3] = 0;
  DataChunkPtr invalid_chunk = nullptr;
  EXPECT_FALSE(serde.Deserialize(invalid_chunk, invalid,
                                 serialized.serialized_size(),
                                 serialized.is_encoding(), &column, 1));
}

}  // namespace
}  // namespace kwdbts
