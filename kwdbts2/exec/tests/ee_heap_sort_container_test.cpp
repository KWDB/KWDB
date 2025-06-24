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
//

#include <ee_tag_row_batch.h>
#include "ee_kwthd_context.h"
#include "ee_heap_sort_container.h"
#include "ee_data_container.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_data_chunk_test_base.h"
using namespace kwdbts;  // NOLINT

class TestHeapSortContainer : public DataChunkTestBase {
 public:
  TestHeapSortContainer() = default;
};

TEST_F(TestHeapSortContainer, TestDiskDataContainer) {
  std::vector<ColumnOrderInfo> order_info;
  order_info.push_back(
      {0, TSOrdering_Column_Direction::TSOrdering_Column_Direction_ASC});
  order_info.push_back(
      {3, TSOrdering_Column_Direction::TSOrdering_Column_Direction_DESC});

  // test single chunk append
  {
    DataContainerPtr tempDataContainer =
        std::make_unique<kwdbts::HeapSortContainer>(order_info, col_info_,
                                                    col_num_, row_num_);
    tempDataContainer->Init();

    tempDataContainer->Append(data_chunk_);

    ASSERT_EQ(tempDataContainer->Count(), row_num_);

    tempDataContainer->Sort();
    
    for (int i = 0; i < row_num_; i++) {
      auto ptr1 = tempDataContainer->GetData(i, 0);
      ASSERT_TRUE(AssertEqualData(ptr1, i, 0));

      auto ptr2 = tempDataContainer->GetData(i, 1);
      ASSERT_TRUE(AssertEqualData(ptr2, i, 1));
      auto ptr3 = tempDataContainer->GetData(i, 2);
      ASSERT_TRUE(AssertEqualData(ptr3, i, 2));
      auto ptr4 = tempDataContainer->GetData(i, 3);
      ASSERT_TRUE(AssertEqualData(ptr4, i, 3));
    }
  }
}
