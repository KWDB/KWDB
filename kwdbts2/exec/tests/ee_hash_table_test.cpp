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
#include <new>
#include <string>
#include <vector>

#include "ee_data_chunk.h"
#include "ee_global.h"
#define private public
#define protected public
#include "ee_aggregate_func.h"
#include "ee_hash_table.h"
#undef protected
#undef private
#include "ee_hash_table_tuple_data.h"
#include "ee_exec_pool.h"
#include "ee_mempool.h"
#include "gtest/gtest.h"

namespace kwdbts {
namespace {

constexpr k_uint32 kLargeFixedStringLen = 9 * 1024 * 1024;
constexpr k_uint64 kOverflowInitCapacity = 64;

void SetVarchar(DatumPtr col, const char* s) {
  const k_uint16 len = static_cast<k_uint16>(std::strlen(s));
  *reinterpret_cast<k_uint16*>(col) = len;
  std::memcpy(col + STRING_WIDE, s, len);
}

void SetFixedStringLenOne(DatumPtr col, char v) {
  *reinterpret_cast<k_uint16*>(col) = 1;
  *(col + STRING_WIDE) = v;
}

void SetDecimalInt(DatumPtr col, k_int64 v) {
  *reinterpret_cast<k_bool*>(col) = false;
  *reinterpret_cast<k_int64*>(col + BOOL_WIDE) = v;
}

void SetDecimalDouble(DatumPtr col, k_double64 v) {
  *reinterpret_cast<k_bool*>(col) = true;
  *reinterpret_cast<k_double64*>(col + BOOL_WIDE) = v;
}

class OwnedColumnInfoChunk : public DataChunk {
 public:
  OwnedColumnInfoChunk(ColumnInfo* col_info, k_uint32 col_num, k_uint32 capacity)
      : DataChunk(col_info, col_num, capacity), owned_col_info_(col_info) {}

  ~OwnedColumnInfoChunk() override { delete[] owned_col_info_; }

 private:
  ColumnInfo* owned_col_info_;
};

std::unique_ptr<DataChunk> MakeIntVarcharChunk(k_int32 int_value,
                                               const std::string& str_value,
                                               bool str_is_null = false) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] = ColumnInfo(4, roachpb::DataType::INT, KWDBTypeFamily::IntFamily);
  col_info[1] =
      ColumnInfo(32, roachpb::DataType::VARCHAR, KWDBTypeFamily::StringFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 2, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&int_value), sizeof(int_value));
  if (str_is_null) {
    chunk->SetNull(0, 1);
  } else {
    chunk->InsertData(0, 1, const_cast<char*>(str_value.data()),
                      str_value.size());
  }
  return chunk;
}

std::unique_ptr<DataChunk> MakeBigIntChunk(k_int64 value) {
  auto* col_info = new ColumnInfo[1];
  col_info[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 1, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&value), sizeof(value));
  return chunk;
}

std::vector<char> MakePackedBigIntSpillTuple(k_int64 key, k_int64 agg) {
  std::vector<char> packed(1 + sizeof(key) + sizeof(agg), 0);
  AggregateFunc::SetNotNull(packed.data(), 0);
  std::memcpy(packed.data() + 1, &key, sizeof(key));
  std::memcpy(packed.data() + 1 + sizeof(key), &agg, sizeof(agg));
  return packed;
}

void BuildTupleData(const LinearProbingHashTable& ht, k_int32 int_value,
                    const std::string& varchar_value, k_int64 agg_value,
                    std::vector<char>* tuple, bool str_is_null = false) {
  tuple->assign(ht.TupleStorageSize(), 0);
  DatumPtr int_col = tuple->data() + ht.group_offsets_[0];
  DatumPtr str_col = tuple->data() + ht.group_offsets_[1];
  DatumPtr null_bitmap = tuple->data() + ht.group_null_offset_;
  AggregateFunc::SetNotNull(null_bitmap, 0);
  if (str_is_null) {
    AggregateFunc::SetNull(null_bitmap, 1);
  } else {
    AggregateFunc::SetNotNull(null_bitmap, 1);
    SetVarchar(str_col, varchar_value.c_str());
  }
  *reinterpret_cast<k_int32*>(int_col) = int_value;
  if (ht.AggStorageWidth() >= sizeof(agg_value)) {
    *reinterpret_cast<k_int64*>(tuple->data() + ht.group_width_) = agg_value;
  } else if (ht.AggStorageWidth() > 0) {
    std::memcpy(tuple->data() + ht.group_width_, &agg_value,
                ht.AggStorageWidth());
  }
}

void BuildSingleBigIntTuple(const LinearProbingHashTable& ht, k_int64 key,
                            std::vector<char>* tuple) {
  tuple->assign(ht.TupleStorageSize(), 0);
  DatumPtr null_bitmap = tuple->data() + ht.group_null_offset_;
  AggregateFunc::SetNotNull(null_bitmap, 0);
  *reinterpret_cast<k_int64*>(tuple->data() + ht.group_offsets_[0]) = key;
}

void BuildSingleDecimalTuple(const LinearProbingHashTable& ht, k_int64 key,
                             std::vector<char>* tuple) {
  tuple->assign(ht.TupleStorageSize(), 0);
  DatumPtr null_bitmap = tuple->data() + ht.group_null_offset_;
  AggregateFunc::SetNotNull(null_bitmap, 0);
  SetDecimalInt(tuple->data() + ht.group_offsets_[0], key);
}

k_uint64 FindFirstUsedSlot(LinearProbingHashTable* ht) {
  for (k_uint64 i = 0; i < ht->Capacity(); ++i) {
    if (ht->IsUsed(i)) {
      return i;
    }
  }
  return ht->Capacity();
}

class ExposedHashTable : public LinearProbingHashTable {
 public:
  using LinearProbingHashTable::LinearProbingHashTable;
  using BaseHashTable::UpdateAggEffectiveWidth;
  using BaseHashTable::UpdateGroupEffectiveWidth;

  k_uint32 GroupEffectiveWidth() const { return group_effective_width_; }
};

class SumAggFunc : public AggregateFunc {
 public:
  SumAggFunc() : AggregateFunc(0, 0, sizeof(k_int64)) {}

  void combine(DatumRowPtr dest, DatumPtr bitmap, DatumRowPtr src,
               DatumPtr src_bitmap) override {
    (void)bitmap;
    (void)src_bitmap;
    *reinterpret_cast<k_int64*>(dest) += *reinterpret_cast<k_int64*>(src);
  }
};

std::unique_ptr<DataChunk> MakeBigIntFixedCharChunk(k_int64 value, char ch) {
  auto* col_info = new ColumnInfo[2];
  col_info[0] =
      ColumnInfo(8, roachpb::DataType::BIGINT, KWDBTypeFamily::IntFamily);
  col_info[1] =
      ColumnInfo(kLargeFixedStringLen, roachpb::DataType::CHAR,
                 KWDBTypeFamily::StringFamily);
  auto chunk = std::make_unique<OwnedColumnInfoChunk>(col_info, 2, 1);
  EXPECT_TRUE(chunk->Initialize());
  chunk->AddCount();
  chunk->InsertData(0, 0, reinterpret_cast<char*>(&value), sizeof(value));
  chunk->InsertData(0, 1, &ch, 1);
  return chunk;
}

std::pair<k_int64, k_int64> FindBigIntCollisionPair(LinearProbingHashTable* ht) {
  for (k_int64 left = 1; left < 512; ++left) {
    std::vector<char> left_tuple;
    BuildSingleBigIntTuple(*ht, left, &left_tuple);
    const k_uint64 left_slot = ht->HashGroups(left_tuple.data()) & ht->mask_;
    for (k_int64 right = left + 1; right < 1024; ++right) {
      std::vector<char> right_tuple;
      BuildSingleBigIntTuple(*ht, right, &right_tuple);
      const k_uint64 right_slot = ht->HashGroups(right_tuple.data()) & ht->mask_;
      if (left_slot == right_slot) {
        return {left, right};
      }
    }
  }
  return {0, 0};
}

}  // namespace

class HashTableTest : public ::testing::Test {
 protected:
  void SetUp() override {
    ExecPool::GetInstance().db_path_ = "./";
    g_pstBufferPoolInfo = EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    ASSERT_NE(g_pstBufferPoolInfo, nullptr);
  }

  void TearDown() override {
    ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
};

TEST_F(HashTableTest, FindOrCreateGroupsNormalAndNullFlow) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::INT, roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{4, 64};
  std::vector<bool> group_allow_null{false, true};
  LinearProbingHashTable ht(group_types, group_lens, 0, group_allow_null, false);
  ASSERT_EQ(ht.Initialize(8), KStatus::SUCCESS);

  std::vector<char> tuple_data(ht.TupleStorageSize(), 0);
  DatumPtr int_col = tuple_data.data() + ht.group_offsets_[0];
  DatumPtr str_col = tuple_data.data() + ht.group_offsets_[1];
  DatumPtr null_bitmap = tuple_data.data() + ht.group_null_offset_;

  AggregateFunc::SetNotNull(null_bitmap, 0);
  AggregateFunc::SetNotNull(null_bitmap, 1);
  *reinterpret_cast<k_int32*>(int_col) = 7;
  SetVarchar(str_col, "alpha");

  k_uint64 loc1 = 0;
  size_t hash1 = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc1, &hash1,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
  EXPECT_FALSE(is_abandoned);

  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc1, &hash1,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);
  EXPECT_FALSE(is_abandoned);
  EXPECT_EQ(ht.Size(), 1);

  *reinterpret_cast<k_int32*>(int_col) = 8;
  AggregateFunc::SetNull(null_bitmap, 1);
  k_uint64 loc2 = 0;
  size_t hash2 = 0;
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc2, &hash2,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
  EXPECT_FALSE(is_abandoned);

  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc2, &hash2,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);
  EXPECT_EQ(ht.Size(), 2);

  DatumPtr stored = ht.GetTuple(loc2);
  ASSERT_NE(stored, nullptr);
  EXPECT_TRUE(
      AggregateFunc::IsNull(stored + ht.group_null_offset_, 1));
}

TEST_F(HashTableTest, SpillAndCombineAfterAbandonedTuple) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::BIGINT, roachpb::DataType::CHAR};
  std::vector<k_uint32> group_lens{8, kLargeFixedStringLen};
  std::vector<bool> group_allow_null{false, false};
  LinearProbingHashTable ht(group_types, group_lens, 0, group_allow_null, true);
  ASSERT_EQ(ht.Initialize(kOverflowInitCapacity), KStatus::SUCCESS);

  std::vector<char> tuple_data(ht.TupleStorageSize(), 0);
  DatumPtr int_col = tuple_data.data() + ht.group_offsets_[0];
  DatumPtr fixed_col = tuple_data.data() + ht.group_offsets_[1];
  DatumPtr null_bitmap = tuple_data.data() + ht.group_null_offset_;
  AggregateFunc::SetNotNull(null_bitmap, 0);
  AggregateFunc::SetNotNull(null_bitmap, 1);
  SetFixedStringLenOne(fixed_col, 'x');

  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;

  // Fill in-memory tuple slots until the next insert is marked as abandoned.
  for (k_int64 i = 0; i < 32; ++i) {
    *reinterpret_cast<k_int64*>(int_col) = i;
    ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc, &hash_val,
                                               &is_used, &is_abandoned),
              KStatus::SUCCESS);
    EXPECT_FALSE(is_abandoned);
  }

  *reinterpret_cast<k_int64*>(int_col) = 32;
  const KStatus spill_status =
      ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc, &hash_val,
                                       &is_used, &is_abandoned);
  ASSERT_TRUE(spill_status == KStatus::SUCCESS || spill_status == KStatus::FAIL);
  if (spill_status == KStatus::FAIL && ht.AbandonedSize() == 0) {
    SUCCEED();
    return;
  }
  EXPECT_FALSE(is_used);
  EXPECT_TRUE(is_abandoned || ht.AbandonedSize() > 0);
  EXPECT_EQ(ht.AbandonedSize(), 1);

  std::vector<AggregateFunc*> funcs;
  int round = 0;
  while (ht.AbandonedSize() > 0 && round < 64) {
    ASSERT_EQ(ht.Combine(&funcs, 0), KStatus::SUCCESS);
    round++;
  }
  EXPECT_LT(round, 64);
  EXPECT_EQ(ht.AbandonedSize(), 0);

  // The spilled tuple should be queryable from rebuilt hash table state.
  k_uint64 check_loc = 0;
  size_t check_hash = 0;
  k_bool check_used = false;
  ASSERT_EQ(ht.FindOrCreateGroups(tuple_data.data(), &check_loc, &check_hash,
                                  &check_used),
            KStatus::SUCCESS);
  EXPECT_TRUE(check_used);
}

TEST_F(HashTableTest, MemoryTupleDataDestructorHandlesPartialInitializeFailure) {
  ASSERT_EQ(EE_MemPoolCleanUp(g_pstBufferPoolInfo), KStatus::SUCCESS);
  g_pstBufferPoolInfo = EE_MemPoolInit(1, ROW_BUFFER_SIZE);
  ASSERT_NE(g_pstBufferPoolInfo, nullptr);

  alignas(MemoryTupleData) unsigned char storage[sizeof(MemoryTupleData)];
  std::memset(storage, 0x7f, sizeof(storage));

  auto* tuple_data = new (storage) MemoryTupleData(8, 1, true);
  EXPECT_EQ(tuple_data->Initialize(), KStatus::FAIL);
  EXPECT_EQ(g_pstBufferPoolInfo->iNumOfFreeBlock, 0U);

  tuple_data->~MemoryTupleData();

  EXPECT_EQ(g_pstBufferPoolInfo->iNumOfFreeBlock, 1U);
}

TEST_F(HashTableTest, CompareColumnSupportsCommonTypes) {
  k_bool bool_left = true;
  k_bool bool_right = true;
  EXPECT_TRUE(CompareColumn(reinterpret_cast<DatumPtr>(&bool_left),
                            reinterpret_cast<DatumPtr>(&bool_right),
                            roachpb::DataType::BOOL));

  k_int32 int_left = 123;
  k_int32 int_right = 123;
  EXPECT_TRUE(CompareColumn(reinterpret_cast<DatumPtr>(&int_left),
                            reinterpret_cast<DatumPtr>(&int_right),
                            roachpb::DataType::INT));

  k_int16 short_left = 9;
  k_int16 short_right = 9;
  EXPECT_TRUE(CompareColumn(reinterpret_cast<DatumPtr>(&short_left),
                            reinterpret_cast<DatumPtr>(&short_right),
                            roachpb::DataType::SMALLINT));

  k_int64 bigint_left = 456;
  k_int64 bigint_right = 789;
  EXPECT_FALSE(CompareColumn(reinterpret_cast<DatumPtr>(&bigint_left),
                             reinterpret_cast<DatumPtr>(&bigint_right),
                             roachpb::DataType::BIGINT));
  EXPECT_TRUE(CompareColumn(reinterpret_cast<DatumPtr>(&bigint_left),
                            reinterpret_cast<DatumPtr>(&bigint_left),
                            roachpb::DataType::TIMESTAMP));

  k_float32 float_left = 1.5F;
  k_float32 float_right = 1.5F;
  EXPECT_TRUE(CompareColumn(reinterpret_cast<DatumPtr>(&float_left),
                            reinterpret_cast<DatumPtr>(&float_right),
                            roachpb::DataType::FLOAT));

  k_double64 double_left = 2.5;
  k_double64 double_right = 3.5;
  EXPECT_FALSE(CompareColumn(reinterpret_cast<DatumPtr>(&double_left),
                             reinterpret_cast<DatumPtr>(&double_right),
                             roachpb::DataType::DOUBLE));

  char varchar_left[STRING_WIDE + 8] = {0};
  char varchar_right[STRING_WIDE + 8] = {0};
  SetVarchar(varchar_left, "abc");
  SetVarchar(varchar_right, "abc");
  EXPECT_TRUE(CompareColumn(varchar_left, varchar_right,
                            roachpb::DataType::VARCHAR));
  SetVarchar(varchar_right, "abd");
  EXPECT_FALSE(CompareColumn(varchar_left, varchar_right,
                             roachpb::DataType::VARCHAR));

  char decimal_left[BOOL_WIDE + sizeof(k_double64)] = {0};
  char decimal_right[BOOL_WIDE + sizeof(k_double64)] = {0};
  SetDecimalInt(decimal_left, 88);
  SetDecimalInt(decimal_right, 88);
  EXPECT_TRUE(CompareColumn(decimal_left, decimal_right,
                            roachpb::DataType::DECIMAL));
  SetDecimalDouble(decimal_left, 88.5);
  SetDecimalDouble(decimal_right, 88.5);
  EXPECT_TRUE(CompareColumn(decimal_left, decimal_right,
                            roachpb::DataType::DECIMAL));
  SetDecimalDouble(decimal_right, 88.0);
  EXPECT_FALSE(CompareColumn(decimal_left, decimal_right,
                             roachpb::DataType::DECIMAL));

  EXPECT_FALSE(CompareColumn(reinterpret_cast<DatumPtr>(&int_left),
                             reinterpret_cast<DatumPtr>(&int_right),
                             roachpb::DataType::UNKNOWN));
}

TEST_F(HashTableTest, CompareGroupsAndEffectiveWidthsTrackTupleShape) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::INT, roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{4, 32};
  std::vector<bool> group_allow_null{false, true};
  ExposedHashTable left(group_types, group_lens, 8, group_allow_null, false);
  ExposedHashTable right(group_types, group_lens, 8, group_allow_null, false);
  ASSERT_EQ(left.Initialize(8), KStatus::SUCCESS);
  ASSERT_EQ(right.Initialize(8), KStatus::SUCCESS);

  std::vector<char> tuple_data(left.TupleStorageSize(), 0);
  DatumPtr left_int = tuple_data.data() + left.group_offsets_[0];
  DatumPtr left_str = tuple_data.data() + left.group_offsets_[1];
  DatumPtr left_null_bitmap = tuple_data.data() + left.group_null_offset_;
  AggregateFunc::SetNotNull(left_null_bitmap, 0);
  AggregateFunc::SetNotNull(left_null_bitmap, 1);
  *reinterpret_cast<k_int32*>(left_int) = 7;
  SetVarchar(left_str, "hello");

  k_uint64 left_loc = 0;
  size_t left_hash = 0;
  k_bool left_used = false;
  k_bool left_abandoned = false;
  ASSERT_EQ(left.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &left_loc,
                                               &left_hash, &left_used,
                                               &left_abandoned),
            KStatus::SUCCESS);

  k_uint64 right_loc = 0;
  size_t right_hash = 0;
  k_bool right_used = false;
  k_bool right_abandoned = false;
  ASSERT_EQ(right.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &right_loc,
                                                &right_hash, &right_used,
                                                &right_abandoned),
            KStatus::SUCCESS);

  EXPECT_TRUE(CompareGroups(left, left_loc, right, right_loc));

  std::vector<char> different_tuple(left.TupleStorageSize(), 0);
  DatumPtr diff_int = different_tuple.data() + left.group_offsets_[0];
  DatumPtr diff_str = different_tuple.data() + left.group_offsets_[1];
  DatumPtr diff_null_bitmap = different_tuple.data() + left.group_null_offset_;
  AggregateFunc::SetNotNull(diff_null_bitmap, 0);
  AggregateFunc::SetNull(diff_null_bitmap, 1);
  *reinterpret_cast<k_int32*>(diff_int) = 7;
  SetVarchar(diff_str, "hello");

  k_uint64 different_loc = 0;
  ASSERT_EQ(right.FindOrCreateGroupsAndAddTuple(different_tuple.data(),
                                                &different_loc, &right_hash,
                                                &right_used, &right_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(CompareGroups(left, left_loc, right, different_loc));

  const k_uint32 initial_agg_width = left.AggWidth();
  left.UpdateAggEffectiveWidth(initial_agg_width + 16);
  EXPECT_EQ(left.AggWidth(), initial_agg_width + 16);
  left.UpdateAggEffectiveWidth(1);
  EXPECT_EQ(left.AggWidth(), initial_agg_width + 16);

  ExposedHashTable width_table(group_types, group_lens, 8, group_allow_null,
                               false);
  ASSERT_EQ(width_table.Initialize(8), KStatus::SUCCESS);
  const k_uint32 initial_group_width = width_table.GroupEffectiveWidth();
  width_table.UpdateGroupEffectiveWidth(nullptr);
  EXPECT_EQ(width_table.GroupEffectiveWidth(), initial_group_width);
  width_table.UpdateGroupEffectiveWidth(tuple_data.data());
  EXPECT_GT(width_table.GroupEffectiveWidth(), initial_group_width);

  std::vector<roachpb::DataType> decimal_types{roachpb::DataType::DECIMAL};
  std::vector<k_uint32> decimal_lens{sizeof(k_int64)};
  std::vector<bool> decimal_allow_null{false};
  ExposedHashTable decimal_table(decimal_types, decimal_lens, 8,
                                 decimal_allow_null, false);
  ASSERT_EQ(decimal_table.Initialize(4), KStatus::SUCCESS);

  std::vector<char> decimal_tuple(decimal_table.TupleStorageSize(), 0);
  DatumPtr decimal_group = decimal_tuple.data() + decimal_table.group_offsets_[0];
  DatumPtr decimal_null_bitmap =
      decimal_tuple.data() + decimal_table.group_null_offset_;
  AggregateFunc::SetNotNull(decimal_null_bitmap, 0);
  SetDecimalDouble(decimal_group, 66.5);
  const k_uint32 decimal_group_width = decimal_table.GroupEffectiveWidth();
  decimal_table.UpdateGroupEffectiveWidth(decimal_tuple.data());
  EXPECT_EQ(decimal_table.GroupEffectiveWidth(), decimal_group_width);
}

TEST_F(HashTableTest, MemoryTupleDataResizesIteratesAndResets) {
  MemoryTupleData tuple_data(sizeof(k_int32), 2, false);
  EXPECT_EQ(tuple_data.Initialize(), KStatus::SUCCESS);
  DatumPtr slot = nullptr;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  *reinterpret_cast<k_int32*>(slot) = 11;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  *reinterpret_cast<k_int32*>(slot) = 22;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::REHASH);
  EXPECT_EQ(tuple_data.GetCapacity(), 4U);
  EXPECT_EQ(tuple_data.GetCount(), 2U);

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  *reinterpret_cast<k_int32*>(slot) = 33;

  ASSERT_NE(tuple_data.GetTupleData(0), nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(tuple_data.GetTupleData(0)), 11);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(tuple_data.GetTupleData(1)), 22);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(tuple_data.GetTupleData(2)), 33);
  EXPECT_EQ(tuple_data.GetTupleData(99), nullptr);

  DatumPtr iter = nullptr;
  EXPECT_EQ(tuple_data.NextTuple(iter), EEIteratorErrCode::EE_OK);
  ASSERT_NE(iter, nullptr);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(iter), 11);
  EXPECT_EQ(tuple_data.CurrentTuple(), iter);

  EXPECT_EQ(tuple_data.NextTuple(iter), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(iter), 22);
  EXPECT_EQ(tuple_data.NextTuple(iter), EEIteratorErrCode::EE_OK);
  EXPECT_EQ(*reinterpret_cast<k_int32*>(iter), 33);
  EXPECT_EQ(tuple_data.NextTuple(iter), EEIteratorErrCode::EE_END_OF_RECORD);

  EXPECT_EQ(tuple_data.Resize(2), KStatus::SUCCESS);
  EXPECT_EQ(tuple_data.Reset(), KStatus::SUCCESS);
  EXPECT_EQ(tuple_data.GetCount(), 0U);
  EXPECT_EQ(tuple_data.CurrentTuple(), nullptr);
}

TEST_F(HashTableTest, MemoryTupleDataSupportsAbandonedAndFinishRepartition) {
  const k_uint32 tuple_size =
      static_cast<k_uint32>(BaseTupleData::MAX_MEMORY_SIZE / 2 + 1);
  MemoryTupleData tuple_data(tuple_size, 2, true);
  EXPECT_EQ(tuple_data.Initialize(), KStatus::SUCCESS);
  DatumPtr slot = nullptr;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  slot[0] = 7;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  slot[0] = 8;

  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::ABANDONED);
  ASSERT_NE(slot, nullptr);
  EXPECT_EQ(slot[0], 0);

  tuple_data.FinishRepartition();
  EXPECT_EQ(tuple_data.GetCapacity(), 0U);
  EXPECT_EQ(tuple_data.GetCount(), 0U);
  EXPECT_EQ(tuple_data.GetTupleData(0), nullptr);

  DatumPtr iter = nullptr;
  EXPECT_EQ(tuple_data.NextTuple(iter), EEIteratorErrCode::EE_END_OF_RECORD);
}

TEST_F(HashTableTest, MemoryTupleDataReportsFailuresWhenResizeCannotAllocate) {
  MemoryTupleData tuple_data(sizeof(k_int32), 1, false);
  EXPECT_EQ(tuple_data.Initialize(), KStatus::SUCCESS);
  DatumPtr slot = nullptr;

  ASSERT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::DO_NOTHING);
  ASSERT_NE(slot, nullptr);
  *reinterpret_cast<k_int32*>(slot) = 123;

  auto* saved_pool = g_pstBufferPoolInfo;
  g_pstBufferPoolInfo = nullptr;

  EXPECT_EQ(tuple_data.Resize(2), KStatus::FAIL);
  slot = nullptr;
  EXPECT_EQ(tuple_data.GetNextTuplePtr(0, slot), PTDFeedBack::FAIL);
  EXPECT_EQ(slot, nullptr);

  g_pstBufferPoolInfo = saved_pool;
}

TEST_F(HashTableTest, InitializeAndSpillBufferFailuresAreReported) {
  auto* saved_pool = g_pstBufferPoolInfo;
  LinearProbingHashTable spill_buf_fail_ht({roachpb::DataType::INT}, {4}, 0,
                                           {false}, false);
  ASSERT_EQ(spill_buf_fail_ht.Initialize(4), KStatus::SUCCESS);
  spill_buf_fail_ht.spill_serialize_buf_ = nullptr;
  spill_buf_fail_ht.spill_serialize_buf_size_ = 0;
  g_pstBufferPoolInfo = nullptr;
  EXPECT_EQ(spill_buf_fail_ht.EnsureSpillSerializeBuffer(8), KStatus::FAIL);
  EXPECT_EQ(spill_buf_fail_ht.spill_serialize_buf_size_, 0U);
  g_pstBufferPoolInfo = saved_pool;
}

TEST_F(HashTableTest, InitializeFailsWithoutMempool) {
  auto* saved_pool = g_pstBufferPoolInfo;
  g_pstBufferPoolInfo = nullptr;

  LinearProbingHashTable init_fail_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                      {false}, false);
  EXPECT_EQ(init_fail_ht.Initialize(4), KStatus::FAIL);

  g_pstBufferPoolInfo = saved_pool;
}

TEST_F(HashTableTest, HashGroupsOnStoredNullTupleSkipsNullColumns) {
  LinearProbingHashTable null_hash_ht(
      {roachpb::DataType::INT, roachpb::DataType::VARCHAR}, {4, 32}, 0,
      {false, true}, false);
  ASSERT_EQ(null_hash_ht.Initialize(4), KStatus::SUCCESS);

  std::vector<char> tuple_data;
  BuildTupleData(null_hash_ht, 9, "", 0, &tuple_data, true);

  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  ASSERT_EQ(null_hash_ht.FindOrCreateGroupsAndAddTuple(
                tuple_data.data(), &loc, &hash_val, &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_EQ(null_hash_ht.HashGroups(loc), null_hash_ht.HashGroups(tuple_data.data()));
}

TEST_F(HashTableTest, DecimalTupleDataPathCoversGroupValueSize) {
  LinearProbingHashTable decimal_ht({roachpb::DataType::DECIMAL},
                                    {static_cast<k_uint32>(sizeof(k_int64))}, 0,
                                    {false}, false);
  ASSERT_EQ(decimal_ht.Initialize(4), KStatus::SUCCESS);
  std::vector<char> decimal_tuple;
  BuildSingleDecimalTuple(decimal_ht, 11, &decimal_tuple);
  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  EXPECT_EQ(decimal_ht.FindOrCreateGroupsAndAddTuple(decimal_tuple.data(), &loc,
                                                     &hash_val, &is_used,
                                                     &is_abandoned),
            KStatus::SUCCESS);
}

TEST_F(HashTableTest, TupleDataCollisionPathAdvancesProbe) {
  LinearProbingHashTable tuple_collision_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                            {false}, false);
  ASSERT_EQ(tuple_collision_ht.Initialize(4), KStatus::SUCCESS);
  const auto collision_pair = FindBigIntCollisionPair(&tuple_collision_ht);
  ASSERT_NE(collision_pair.first, 0);
  ASSERT_NE(collision_pair.second, 0);

  std::vector<char> tuple_left;
  std::vector<char> tuple_right;
  BuildSingleBigIntTuple(tuple_collision_ht, collision_pair.first, &tuple_left);
  BuildSingleBigIntTuple(tuple_collision_ht, collision_pair.second,
                         &tuple_right);
  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  EXPECT_EQ(tuple_collision_ht.FindOrCreateGroupsAndAddTuple(
                tuple_left.data(), &loc, &hash_val, &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_EQ(tuple_collision_ht.FindOrCreateGroups(tuple_right.data(), &loc,
                                                  &hash_val, &is_used),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
}

TEST_F(HashTableTest, CompareGroupsMismatchBranchesReturnFalse) {
  LinearProbingHashTable compare_ht(
      {roachpb::DataType::INT, roachpb::DataType::VARCHAR}, {4, 32}, 0,
      {false, true}, false);
  ASSERT_EQ(compare_ht.Initialize(4), KStatus::SUCCESS);
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  DatumPtr agg_ptr = nullptr;
  auto hello_chunk = MakeIntVarcharChunk(5, "hello");
  EXPECT_EQ(compare_ht.FindOrCreateGroupsAndAddTuple(
                hello_chunk.get(), 0, {0, 1}, agg_ptr, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  const k_uint64 used_loc = FindFirstUsedSlot(&compare_ht);
  ASSERT_LT(used_loc, compare_ht.Capacity());

  auto null_chunk = MakeIntVarcharChunk(5, "", true);
  compare_ht.HashGroups(null_chunk.get(), 0, {0, 1});
  EXPECT_FALSE(compare_ht.CompareGroups({0, 1}, used_loc));

  auto world_chunk = MakeIntVarcharChunk(5, "world");
  compare_ht.HashGroups(world_chunk.get(), 0, {0, 1});
  EXPECT_FALSE(compare_ht.CompareGroups({0, 1}, used_loc));

  std::vector<char> null_tuple;
  BuildTupleData(compare_ht, 5, "", 0, &null_tuple, true);
  EXPECT_FALSE(compare_ht.CompareGroups(null_tuple.data(), used_loc));
}

TEST_F(HashTableTest, TupleDataRehashFailAndSpillPhaseElseBranch) {
  LinearProbingHashTable rehash_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                   false);
  ASSERT_EQ(rehash_ht.Initialize(4), KStatus::SUCCESS);
  std::vector<char> tuple_one;
  std::vector<char> tuple_two;
  std::vector<char> tuple_three;
  BuildSingleBigIntTuple(rehash_ht, 1, &tuple_one);
  BuildSingleBigIntTuple(rehash_ht, 2, &tuple_two);
  BuildSingleBigIntTuple(rehash_ht, 3, &tuple_three);
  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  EXPECT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(tuple_one.data(), &loc,
                                                    &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(tuple_two.data(), &loc,
                                                    &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(tuple_three.data(), &loc,
                                                    &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);

  LinearProbingHashTable fail_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                 false);
  ASSERT_EQ(fail_ht.Initialize(4), KStatus::SUCCESS);
  BuildSingleBigIntTuple(fail_ht, 10, &tuple_one);
  BuildSingleBigIntTuple(fail_ht, 20, &tuple_two);
  BuildSingleBigIntTuple(fail_ht, 30, &tuple_three);
  EXPECT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(tuple_one.data(), &loc,
                                                  &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(tuple_two.data(), &loc,
                                                  &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::SUCCESS);
  auto* saved_pool = g_pstBufferPoolInfo;
  g_pstBufferPoolInfo = nullptr;
  EXPECT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(tuple_three.data(), &loc,
                                                  &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::FAIL);
  EXPECT_EQ(fail_ht.Resize(8, PTDFeedBack::REHASH), KStatus::FAIL);
  g_pstBufferPoolInfo = saved_pool;

  LinearProbingHashTable phase_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                  false);
  ASSERT_EQ(phase_ht.Initialize(4), KStatus::SUCCESS);
  phase_ht.spill_partitions_1_ =
      std::make_unique<LinearProbingHashTable::SpillPartitionState[]>(
          LinearProbingHashTable::kSpillPartitionNum);
  phase_ht.spill_partitions_2_ =
      std::make_unique<LinearProbingHashTable::SpillPartitionState[]>(
          LinearProbingHashTable::kSpillPartitionNum);
  phase_ht.write_spill_partitions_ = &phase_ht.spill_partitions_2_;
  phase_ht.read_spill_partitions_ = &phase_ht.spill_partitions_1_;
  phase_ht.StartSpillReadPhase();
  EXPECT_EQ(phase_ht.read_spill_partitions_, &phase_ht.spill_partitions_2_);
  EXPECT_EQ(phase_ht.write_spill_partitions_, &phase_ht.spill_partitions_1_);
  EXPECT_TRUE(phase_ht.spill_read_phase_);
}

TEST_F(HashTableTest, SpillRoundTripAndCombineCoverSuccessPaths) {
  LinearProbingHashTable spill_ht({roachpb::DataType::BIGINT}, {8},
                                  sizeof(k_int64), {false}, false);
  ASSERT_EQ(spill_ht.Initialize(4), KStatus::SUCCESS);

  auto* saved_pool = g_pstBufferPoolInfo;
  spill_ht.spill_serialize_buf_ = nullptr;
  spill_ht.spill_serialize_buf_size_ = 0;
  g_pstBufferPoolInfo = nullptr;
  std::vector<char> tuple_data;
  BuildSingleBigIntTuple(spill_ht, 7, &tuple_data);
  EXPECT_EQ(spill_ht.SaveAggTupleToDisk(tuple_data.data() + spill_ht.group_width_),
            KStatus::FAIL);
  g_pstBufferPoolInfo = saved_pool;

  LinearProbingHashTable no_part_ht({roachpb::DataType::BIGINT}, {8},
                                    sizeof(k_int64), {false}, false);
  ASSERT_EQ(no_part_ht.Initialize(4), KStatus::SUCCESS);
  no_part_ht.abandoned_count_ = 1;
  no_part_ht.spill_read_phase_ = true;
  no_part_ht.read_partition_idx_ = LinearProbingHashTable::kSpillPartitionNum;
  std::vector<AggregateFunc*> empty_funcs;
  EXPECT_EQ(no_part_ht.Combine(&empty_funcs, 0), KStatus::SUCCESS);
  EXPECT_FALSE(no_part_ht.spill_read_phase_);
}

TEST_F(HashTableTest, CombineDuplicateSpillRecordsMergesAggAtLastPartition) {
  LinearProbingHashTable spill_ht({roachpb::DataType::BIGINT}, {8},
                                  sizeof(k_int64), {false}, false);
  ASSERT_EQ(spill_ht.Initialize(4), KStatus::SUCCESS);

  auto packed_left = MakePackedBigIntSpillTuple(17, 3);
  auto packed_right = MakePackedBigIntSpillTuple(17, 4);
  constexpr k_uint32 part_id = LinearProbingHashTable::kSpillPartitionNum - 1;
  ASSERT_EQ(spill_ht.SaveToSpillPartition(part_id, packed_left.data(),
                                          packed_left.size()),
            KStatus::SUCCESS);
  ASSERT_EQ(spill_ht.SaveToSpillPartition(part_id, packed_right.data(),
                                          packed_right.size()),
            KStatus::SUCCESS);

  spill_ht.abandoned_count_ = 2;
  SumAggFunc sum_func;
  std::vector<AggregateFunc*> funcs{&sum_func};
  ASSERT_EQ(spill_ht.Combine(&funcs, 0), KStatus::SUCCESS);
  EXPECT_FALSE(spill_ht.spill_read_phase_);
  EXPECT_EQ(spill_ht.read_partition_idx_,
            LinearProbingHashTable::kSpillPartitionNum);

  std::vector<char> probe_tuple;
  BuildSingleBigIntTuple(spill_ht, 17, &probe_tuple);
  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  ASSERT_EQ(spill_ht.FindOrCreateGroups(probe_tuple.data(), &loc, &hash_val,
                                        &is_used),
            KStatus::SUCCESS);
  ASSERT_TRUE(is_used);
  EXPECT_EQ(*reinterpret_cast<k_int64*>(spill_ht.GetAggResult(loc)), 7);
}

TEST_F(HashTableTest, CombineReportsEnsureBufferAndDiskTupleAllocFailures) {
  std::vector<AggregateFunc*> funcs;
  auto* saved_pool = g_pstBufferPoolInfo;

  LinearProbingHashTable ensure_fail_ht({roachpb::DataType::BIGINT}, {8},
                                        sizeof(k_int64), {false}, false);
  ASSERT_EQ(ensure_fail_ht.Initialize(4), KStatus::SUCCESS);
  ensure_fail_ht.abandoned_count_ = 1;
  ensure_fail_ht.spill_serialize_buf_ = nullptr;
  ensure_fail_ht.spill_serialize_buf_size_ = 0;
  g_pstBufferPoolInfo = nullptr;
  EXPECT_EQ(ensure_fail_ht.Combine(&funcs, 0), KStatus::FAIL);
  g_pstBufferPoolInfo = saved_pool;

  LinearProbingHashTable malloc_fail_ht({roachpb::DataType::BIGINT}, {8},
                                        sizeof(k_int64), {false}, false);
  ASSERT_EQ(malloc_fail_ht.Initialize(4), KStatus::SUCCESS);
  auto packed_tuple = MakePackedBigIntSpillTuple(9, 1);
  ASSERT_EQ(malloc_fail_ht.SaveToSpillPartition(0, packed_tuple.data(),
                                                packed_tuple.size()),
            KStatus::SUCCESS);
  malloc_fail_ht.abandoned_count_ = 1;
  g_pstBufferPoolInfo = nullptr;
  EXPECT_EQ(malloc_fail_ht.Combine(&funcs, 0), KStatus::FAIL);
  g_pstBufferPoolInfo = saved_pool;
}

TEST_F(HashTableTest, ChunkPathAndSpillHelpersCoverHashTableBranches) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::INT, roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{4, 32};
  std::vector<bool> group_allow_null{false, true};
  LinearProbingHashTable ht(group_types, group_lens, sizeof(k_int64),
                            group_allow_null, false);
  ASSERT_EQ(ht.Initialize(4), KStatus::SUCCESS);

  EXPECT_EQ(ht.Resize(2), KStatus::SUCCESS);

  auto chunk = MakeIntVarcharChunk(42, "hello");
  DatumPtr agg_ptr = nullptr;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(chunk.get(), 0, {0, 1}, agg_ptr,
                                             &hash_val, &is_used,
                                             &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
  EXPECT_FALSE(is_abandoned);
  ASSERT_NE(agg_ptr, nullptr);
  *reinterpret_cast<k_int64*>(agg_ptr) = 99;

  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(chunk.get(), 0, {0, 1}, agg_ptr,
                                             &hash_val, &is_used,
                                             &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);

  const k_uint64 loc = FindFirstUsedSlot(&ht);
  ASSERT_LT(loc, ht.Capacity());
  EXPECT_EQ(ht.GetHashVal(loc), hash_table_entry_t::ExtractSalt(hash_val));
  EXPECT_EQ(ht.HashGroups(chunk.get(), 0, {0, 1}), ht.HashGroups(loc));

  std::vector<char> tuple_data;
  BuildTupleData(ht, 42, "hello", 99, &tuple_data);
  k_uint64 tuple_loc = 0;
  size_t tuple_hash = 0;
  ASSERT_EQ(ht.FindOrCreateGroups(tuple_data.data(), &tuple_loc, &tuple_hash,
                                  &is_used),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);
  EXPECT_EQ(tuple_loc, loc);
  EXPECT_TRUE(ht.CompareGroups(tuple_data.data(), loc));

  std::vector<char> other_tuple_data;
  BuildTupleData(ht, 42, "world", 99, &other_tuple_data);
  EXPECT_FALSE(ht.CompareGroups(other_tuple_data.data(), loc));

  EXPECT_EQ(*reinterpret_cast<k_int64*>(ht.NextLine()), 99);
  EXPECT_EQ(ht.NextLine(), nullptr);

  std::vector<roachpb::DataType> single_group_type{roachpb::DataType::BIGINT};
  std::vector<k_uint32> single_group_len{8};
  std::vector<bool> single_group_null{true};
  LinearProbingHashTable rehash_src(single_group_type, single_group_len, 0,
                                    single_group_null, false);
  LinearProbingHashTable rehash_dest(single_group_type, single_group_len, 0,
                                     single_group_null, false);
  ASSERT_EQ(rehash_src.Initialize(4), KStatus::SUCCESS);
  ASSERT_EQ(rehash_dest.Initialize(4), KStatus::SUCCESS);

  std::vector<char> one_bigint_tuple;
  BuildSingleBigIntTuple(rehash_src, 12345, &one_bigint_tuple);
  k_uint64 src_loc = 0;
  ASSERT_EQ(rehash_src.FindOrCreateGroupsAndAddTuple(one_bigint_tuple.data(),
                                                     &src_loc, &tuple_hash,
                                                     &is_used,
                                                     &is_abandoned),
            KStatus::SUCCESS);

  k_uint64 dest_loc = 0;
  ASSERT_EQ(rehash_dest.FindOrCreateGroups(rehash_src, src_loc, &dest_loc,
                                           &is_used),
            0);
  EXPECT_FALSE(is_used);
  rehash_dest.hash_entry_[dest_loc].SetSalt(rehash_src.GetHashVal(src_loc));
  rehash_dest.hash_entry_[dest_loc].SetPointer(rehash_src.GetTuple(src_loc));
  ASSERT_EQ(rehash_dest.FindOrCreateGroups(rehash_src, src_loc, &dest_loc,
                                           &is_used),
            0);
  EXPECT_TRUE(is_used);

  LinearProbingHashTable null_group_ht({roachpb::DataType::INT}, {4}, 0, {true},
                                       false);
  ASSERT_EQ(null_group_ht.Initialize(4), KStatus::SUCCESS);
  k_uint64 null_loc = 0;
  ASSERT_EQ(null_group_ht.CreateNullGroups(&null_loc, &is_used), 0);
  EXPECT_FALSE(is_used);
  null_group_ht.hash_entry_[null_loc].SetPointer(
      reinterpret_cast<DatumPtr>(&null_group_ht));
  ASSERT_EQ(null_group_ht.CreateNullGroups(&null_loc, &is_used), 0);
  EXPECT_TRUE(is_used);

  EEPgErrorInfo::ResetPgErrorInfo();
  std::size_t bad_hash = 17;
  ht.HashColumn(reinterpret_cast<DatumPtr>(&bad_hash), roachpb::DataType::UNKNOWN,
                &bad_hash);
  EXPECT_TRUE(EEPgErrorInfo::IsError());
}

TEST_F(HashTableTest, SpillPartitionsRoundTripAndResizePreserveRows) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::INT, roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{4, 32};
  std::vector<bool> group_allow_null{false, true};
  LinearProbingHashTable ht(group_types, group_lens, sizeof(k_int64),
                            group_allow_null, false);
  ASSERT_EQ(ht.Initialize(4), KStatus::SUCCESS);

  std::vector<char> tuple_data;
  BuildTupleData(ht, 7, "spill", 999, &tuple_data);

  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc, &hash_val,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_FALSE(is_used);
  ASSERT_FALSE(is_abandoned);
  *reinterpret_cast<k_int64*>(ht.GetAggResult(loc)) = 999;

  std::vector<AggregateFunc*> funcs;
  EXPECT_EQ(ht.Combine(&funcs, 0), KStatus::SUCCESS);

  EXPECT_EQ(ht.SaveAggTupleToDisk(nullptr), KStatus::FAIL);
  EXPECT_EQ(ht.SaveToSpillPartition(LinearProbingHashTable::kSpillPartitionNum,
                                    tuple_data.data(), tuple_data.size()),
            KStatus::FAIL);
  EXPECT_EQ(ht.SaveToSpillPartition(0, nullptr, tuple_data.size()),
            KStatus::FAIL);
  EXPECT_EQ(ht.SaveToSpillPartition(0, tuple_data.data(), 0), KStatus::FAIL);

  ASSERT_EQ(ht.EnsureSpillSerializeBuffer(1), KStatus::SUCCESS);
  DatumPtr original_buffer = ht.spill_serialize_buf_;
  ASSERT_EQ(ht.EnsureSpillSerializeBuffer(1), KStatus::SUCCESS);
  EXPECT_EQ(ht.spill_serialize_buf_, original_buffer);

  ASSERT_EQ(ht.SaveAggTupleToDisk(ht.GetAggResult(loc)), KStatus::SUCCESS);
  ASSERT_NE(ht.spill_partitions_1_, nullptr);
  ASSERT_NE(ht.spill_partitions_2_, nullptr);

  LinearProbingHashTable spill_ht(group_types, group_lens, sizeof(k_int64),
                                  group_allow_null, false);
  ASSERT_EQ(spill_ht.Initialize(4), KStatus::SUCCESS);
  constexpr k_uint32 part_id = 3;
  const char spill_payload[] = {'o', 'k', '!'};
  ASSERT_EQ(spill_ht.SaveToSpillPartition(part_id,
                                          const_cast<char*>(spill_payload),
                                          sizeof(spill_payload)),
            KStatus::SUCCESS);

  spill_ht.StartSpillReadPhase();
  std::vector<char> packed_tuple(sizeof(spill_payload), 0);
  k_uint32 packed_size = 0;
  EXPECT_GT((*spill_ht.read_spill_partitions_)[part_id].count_, 0U);
  EXPECT_NE(spill_ht.LoadNextFromSpillPartition(part_id, packed_tuple.data(),
                                                packed_tuple.size(),
                                                &packed_size),
            EEIteratorErrCode::EE_END_OF_RECORD);
  EXPECT_EQ(spill_ht.LoadNextFromSpillPartition(
                LinearProbingHashTable::kSpillPartitionNum, packed_tuple.data(),
                packed_tuple.size(), &packed_size),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(spill_ht.LoadNextFromSpillPartition(part_id, nullptr,
                                                packed_tuple.size(),
                                                &packed_size),
            EEIteratorErrCode::EE_ERROR);
  EXPECT_EQ(spill_ht.LoadNextFromSpillPartition(part_id, packed_tuple.data(),
                                                packed_tuple.size(), nullptr),
            EEIteratorErrCode::EE_ERROR);

  spill_ht.ResetSpillPartitions(*spill_ht.read_spill_partitions_);
  EXPECT_EQ((*spill_ht.read_spill_partitions_)[part_id].count_, 0U);
  EXPECT_EQ((*spill_ht.read_spill_partitions_)[part_id].read_count_, 0U);
  EXPECT_EQ(spill_ht.LoadNextFromSpillPartition(part_id, packed_tuple.data(),
                                                packed_tuple.size(),
                                                &packed_size),
            EEIteratorErrCode::EE_END_OF_RECORD);

  auto smaller_spill = MakePackedBigIntSpillTuple(5, 6);
  std::vector<roachpb::DataType> short_read_types{roachpb::DataType::BIGINT};
  std::vector<k_uint32> short_read_lens{8};
  std::vector<bool> short_read_allow_null{false};
  LinearProbingHashTable short_read_ht(short_read_types, short_read_lens,
                                       sizeof(k_int64), short_read_allow_null,
                                       false);
  ASSERT_EQ(short_read_ht.Initialize(4), KStatus::SUCCESS);
  ASSERT_EQ(short_read_ht.SaveToSpillPartition(part_id, smaller_spill.data(),
                                               smaller_spill.size()),
            KStatus::SUCCESS);
  short_read_ht.StartSpillReadPhase();
  std::vector<char> too_small(smaller_spill.size() - 1, 0);
  EXPECT_EQ(short_read_ht.LoadNextFromSpillPartition(part_id, too_small.data(),
                                                     too_small.size(),
                                                     &packed_size),
            EEIteratorErrCode::EE_ERROR);
  auto& short_part = (*short_read_ht.read_spill_partitions_)[part_id];
  EXPECT_EQ(short_part.offset_, sizeof(k_uint32));
  EXPECT_EQ(short_part.read_count_, 0U);

  auto rehash_chunk = MakeBigIntChunk(55);
  std::vector<roachpb::DataType> rehash_types{roachpb::DataType::BIGINT};
  std::vector<k_uint32> rehash_lens{8};
  std::vector<bool> rehash_allow_null{false};
  LinearProbingHashTable rehash_ht(rehash_types, rehash_lens, 0,
                                   rehash_allow_null, false);
  ASSERT_EQ(rehash_ht.Initialize(4), KStatus::SUCCESS);

  DatumPtr rehash_agg = nullptr;
  ASSERT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(rehash_chunk.get(), 0, {0},
                                                    rehash_agg, &hash_val,
                                                    &is_used, &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(rehash_ht.Resize(8, PTDFeedBack::REHASH), KStatus::SUCCESS);

  std::vector<char> rehash_tuple(rehash_ht.TupleStorageSize(), 0);
  DatumPtr null_bitmap = rehash_tuple.data() + rehash_ht.group_null_offset_;
  AggregateFunc::SetNotNull(null_bitmap, 0);
  *reinterpret_cast<k_int64*>(rehash_tuple.data() + rehash_ht.group_offsets_[0]) =
      55;
  k_uint64 rehash_loc = 0;
  ASSERT_EQ(rehash_ht.FindOrCreateGroups(rehash_tuple.data(), &rehash_loc,
                                         &hash_val, &is_used),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);
}

TEST_F(HashTableTest, HashColumnAndChunkFeedbackPathsCoverAdditionalBranches) {
  LinearProbingHashTable scalar_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                   false);
  ASSERT_EQ(scalar_ht.Initialize(4), KStatus::SUCCESS);

  constexpr std::size_t kInitHashValue = 13;
  std::size_t hash_seed = kInitHashValue;
  k_bool bool_value = true;
  scalar_ht.HashColumn(reinterpret_cast<DatumPtr>(&bool_value),
                       roachpb::DataType::BOOL, &hash_seed);

  k_int16 smallint_value = 12;
  scalar_ht.HashColumn(reinterpret_cast<DatumPtr>(&smallint_value),
                       roachpb::DataType::SMALLINT, &hash_seed);

  k_float32 float_value = 1.25f;
  scalar_ht.HashColumn(reinterpret_cast<DatumPtr>(&float_value),
                       roachpb::DataType::FLOAT, &hash_seed);

  k_double64 double_value = 9.5;
  scalar_ht.HashColumn(reinterpret_cast<DatumPtr>(&double_value),
                       roachpb::DataType::DOUBLE, &hash_seed);

  char decimal_int[BOOL_WIDE + sizeof(k_int64)] = {0};
  SetDecimalInt(decimal_int, 77);
  scalar_ht.HashColumn(decimal_int, roachpb::DataType::DECIMAL, &hash_seed);

  char decimal_double[BOOL_WIDE + sizeof(k_double64)] = {0};
  SetDecimalDouble(decimal_double, 3.5);
  scalar_ht.HashColumn(decimal_double, roachpb::DataType::DECIMAL, &hash_seed);
  EXPECT_NE(hash_seed, kInitHashValue);

  LinearProbingHashTable chunk_collision_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                            {false}, false);
  ASSERT_EQ(chunk_collision_ht.Initialize(4), KStatus::SUCCESS);
  const auto collision_pair = FindBigIntCollisionPair(&chunk_collision_ht);
  ASSERT_NE(collision_pair.first, 0);
  ASSERT_NE(collision_pair.second, 0);

  DatumPtr agg_ptr = nullptr;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  auto chunk_first = MakeBigIntChunk(collision_pair.first);
  auto chunk_second = MakeBigIntChunk(collision_pair.second);
  ASSERT_EQ(chunk_collision_ht.FindOrCreateGroupsAndAddTuple(
                chunk_first.get(), 0, {0}, agg_ptr, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
  ASSERT_EQ(chunk_collision_ht.FindOrCreateGroupsAndAddTuple(
                chunk_second.get(), 0, {0}, agg_ptr, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);

  k_uint64 first_loc = 0;
  size_t first_hash = 0;
  std::vector<char> first_tuple;
  BuildSingleBigIntTuple(chunk_collision_ht, collision_pair.first, &first_tuple);
  ASSERT_EQ(chunk_collision_ht.FindOrCreateGroups(first_tuple.data(), &first_loc,
                                                  &first_hash, &is_used),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_used);
  chunk_collision_ht.CopyGroups(chunk_first.get(), 0, {0}, first_loc, first_hash);

  LinearProbingHashTable tuple_collision_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                            {false}, false);
  ASSERT_EQ(tuple_collision_ht.Initialize(4), KStatus::SUCCESS);
  std::vector<char> tuple_left;
  std::vector<char> tuple_right;
  BuildSingleBigIntTuple(tuple_collision_ht, collision_pair.first, &tuple_left);
  BuildSingleBigIntTuple(tuple_collision_ht, collision_pair.second, &tuple_right);
  k_uint64 tuple_loc = 0;
  ASSERT_EQ(tuple_collision_ht.FindOrCreateGroupsAndAddTuple(
                tuple_left.data(), &tuple_loc, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(tuple_collision_ht.FindOrCreateGroupsAndAddTuple(
                tuple_right.data(), &tuple_loc, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);

  LinearProbingHashTable src_collision_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                          {false}, false);
  LinearProbingHashTable dest_collision_ht({roachpb::DataType::BIGINT}, {8}, 0,
                                           {false}, false);
  ASSERT_EQ(src_collision_ht.Initialize(4), KStatus::SUCCESS);
  ASSERT_EQ(dest_collision_ht.Initialize(4), KStatus::SUCCESS);
  k_uint64 src_loc = 0;
  ASSERT_EQ(src_collision_ht.FindOrCreateGroupsAndAddTuple(
                tuple_right.data(), &src_loc, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  k_uint64 dest_loc = 0;
  ASSERT_EQ(dest_collision_ht.FindOrCreateGroupsAndAddTuple(
                tuple_left.data(), &dest_loc, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(dest_collision_ht.FindOrCreateGroups(src_collision_ht, src_loc,
                                                 &dest_loc, &is_used),
            0);
  EXPECT_FALSE(is_used);

  LinearProbingHashTable rehash_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                   false);
  ASSERT_EQ(rehash_ht.Initialize(4), KStatus::SUCCESS);
  auto chunk_one = MakeBigIntChunk(11);
  auto chunk_two = MakeBigIntChunk(22);
  auto chunk_three = MakeBigIntChunk(33);
  ASSERT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(chunk_one.get(), 0, {0},
                                                    agg_ptr, &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(chunk_two.get(), 0, {0},
                                                    agg_ptr, &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(rehash_ht.FindOrCreateGroupsAndAddTuple(chunk_three.get(), 0, {0},
                                                    agg_ptr, &hash_val, &is_used,
                                                    &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_GE(rehash_ht.Capacity(), 8U);

  LinearProbingHashTable fail_ht({roachpb::DataType::BIGINT}, {8}, 0, {false},
                                 false);
  ASSERT_EQ(fail_ht.Initialize(4), KStatus::SUCCESS);
  ASSERT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(chunk_one.get(), 0, {0},
                                                  agg_ptr, &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(chunk_two.get(), 0, {0},
                                                  agg_ptr, &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::SUCCESS);
  auto* saved_pool = g_pstBufferPoolInfo;
  g_pstBufferPoolInfo = nullptr;
  EXPECT_EQ(fail_ht.FindOrCreateGroupsAndAddTuple(chunk_three.get(), 0, {0},
                                                  agg_ptr, &hash_val, &is_used,
                                                  &is_abandoned),
            KStatus::FAIL);
  g_pstBufferPoolInfo = saved_pool;

  LinearProbingHashTable abandoned_ht(
      {roachpb::DataType::BIGINT, roachpb::DataType::CHAR},
      {8, kLargeFixedStringLen}, 0, {false, false}, true);
  ASSERT_EQ(abandoned_ht.Initialize(kOverflowInitCapacity), KStatus::SUCCESS);
  auto* abandoned_tuple_data =
      dynamic_cast<MemoryTupleData*>(abandoned_ht.tuple_data_.get());
  ASSERT_NE(abandoned_tuple_data, nullptr);
  abandoned_tuple_data->count_ = abandoned_tuple_data->capacity_;
  auto wide_chunk = MakeBigIntFixedCharChunk(99, 'z');
  ASSERT_EQ(abandoned_ht.FindOrCreateGroupsAndAddTuple(
                wide_chunk.get(), 0, {0, 1}, agg_ptr, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_TRUE(is_abandoned);

  LinearProbingHashTable nullable_chunk_ht(
      {roachpb::DataType::INT, roachpb::DataType::VARCHAR}, {4, 32}, 0,
      {false, true}, false);
  ASSERT_EQ(nullable_chunk_ht.Initialize(4), KStatus::SUCCESS);
  auto null_chunk = MakeIntVarcharChunk(7, "", true);
  ASSERT_EQ(nullable_chunk_ht.FindOrCreateGroupsAndAddTuple(
                null_chunk.get(), 0, {0, 1}, agg_ptr, &hash_val, &is_used,
                &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_abandoned);
}

TEST_F(HashTableTest, CombineRejectsMalformedPayloads) {
  std::vector<roachpb::DataType> group_types{
      roachpb::DataType::INT, roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{4, 32};
  std::vector<bool> group_allow_null{false, true};
  LinearProbingHashTable malformed_ht(group_types, group_lens, sizeof(k_int64),
                                      group_allow_null, false);
  ASSERT_EQ(malformed_ht.Initialize(4), KStatus::SUCCESS);
  char bad_payload = 0;
  ASSERT_EQ(malformed_ht.SaveToSpillPartition(0, &bad_payload, 1),
            KStatus::SUCCESS);
  malformed_ht.abandoned_count_ = 1;
  std::vector<AggregateFunc*> empty_funcs;
  EXPECT_EQ(malformed_ht.Combine(&empty_funcs, 0), KStatus::FAIL);
}

}  // namespace kwdbts
