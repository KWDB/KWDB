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
#include <vector>

#include "ee_aggregate_func.h"
#include "ee_hash_table.h"
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

}  // namespace

class HashTableTest : public ::testing::Test {
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
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc, &hash_val,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  EXPECT_FALSE(is_used);
  EXPECT_TRUE(is_abandoned);
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

}  // namespace kwdbts
