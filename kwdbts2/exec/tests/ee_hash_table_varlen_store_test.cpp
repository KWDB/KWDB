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
#include <vector>

#include "ee_aggregate_func.h"
#include "ee_hash_table.h"
#include "gtest/gtest.h"

namespace kwdbts {

TEST(HashTableVarlenStoreTest, StoreOnlyEffectiveLengthForVarString) {
      g_pstBufferPoolInfo = kwdbts::EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    EXPECT_EQ((g_pstBufferPoolInfo != nullptr), true);
  std::vector<roachpb::DataType> group_types{roachpb::DataType::VARCHAR};
  std::vector<k_uint32> group_lens{64};
  std::vector<bool> group_allow_null{false};
  LinearProbingHashTable ht(group_types, group_lens, 0, group_allow_null,
                            false);

  ASSERT_EQ(ht.Initialize(8), KStatus::SUCCESS);

  std::vector<char> tuple_data(ht.TupleStorageSize(), 0);
  DatumPtr col_ptr = tuple_data.data() + ht.group_offsets_[0];
  *reinterpret_cast<k_uint16*>(col_ptr) = 3;
  std::memcpy(col_ptr + STRING_WIDE, "abc", 3);
  std::memset(col_ptr + STRING_WIDE + 3, 'X', 8);
  AggregateFunc::SetNotNull(tuple_data.data() + ht.group_null_offset_, 0);

  k_uint64 loc = 0;
  size_t hash_val = 0;
  k_bool is_used = false;
  k_bool is_abandoned = false;
  ASSERT_EQ(ht.FindOrCreateGroupsAndAddTuple(tuple_data.data(), &loc, &hash_val,
                                             &is_used, &is_abandoned),
            KStatus::SUCCESS);
  ASSERT_FALSE(is_used);
  ASSERT_FALSE(is_abandoned);

  DatumPtr stored = ht.GetTuple(loc);
  ASSERT_NE(stored, nullptr);
  DatumPtr stored_col = stored + ht.group_offsets_[0];
  EXPECT_EQ(*reinterpret_cast<k_uint16*>(stored_col), 3);
  EXPECT_EQ(std::memcmp(stored_col + STRING_WIDE, "abc", 3), 0);
  EXPECT_EQ(*(stored_col + STRING_WIDE + 3), 0);
  EXPECT_LT(ht.TupleSize(), ht.TupleStorageSize());
  EXPECT_EQ(ht.TupleSize(), 6);
      kwdbts::KStatus status = kwdbts::EE_MemPoolCleanUp(g_pstBufferPoolInfo);
    EXPECT_EQ(status, kwdbts::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
}

}  // namespace kwdbts
