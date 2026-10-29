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

#include "ee_common.h"
#include "ee_kwthd_context.h"
#include "ee_data_chunk.h"
#include "ts_utils.h"
#include "cm_assert.h"
#include "ee_string_info.h"
#include "gtest/gtest.h"
#include "kwdb_type.h"
#include "me_metadata.pb.h"
#include "ee_linkedlist_container.h"
#include "ee_batch_data_container.h"

using namespace kwdbts;  // NOLINT

// TestLinkedListContainer for multiple model processing
class TestLinkedListContainer : public ::testing::Test {  // inherit testing::Test
 protected:
  static void SetUpTestCase() {
    g_pstBufferPoolInfo = kwdbts::EE_MemPoolInit(1024, ROW_BUFFER_SIZE);
    EXPECT_EQ((g_pstBufferPoolInfo != nullptr), true);
  }

  static void TearDownTestCase() {
    kwdbts::KStatus status = kwdbts::EE_MemPoolCleanUp(g_pstBufferPoolInfo);
    EXPECT_EQ(status, kwdbts::SUCCESS);
    g_pstBufferPoolInfo = nullptr;
  }
  void SetUp() override {}
  void TearDown() override {}

 public:
  TestLinkedListContainer() = default;
};

// LinkedListContainer test cases for multiple model processing
TEST_F(TestLinkedListContainer, TestAddAndGetData) {
  kwdbContext_t context;
  kwdbContext_p ctx = &context;
  InitServerKWDBContext(ctx);


  k_int64 row_count = 1000;
  
  // case 0: linkedlist kept in memory with default memory size 256M
  // case 1: linkedlist materialized in mmap file with 1024 data size threshold
  vector<int> data_size_threshold = {DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY, 1024*8};
  vector<bool> linked_list_is_materialized = {false, true};
  for (int case_num = 0; case_num < 2; ++case_num) {
    LinkedListContainer* linked_list_container = new LinkedListContainer(data_size_threshold[case_num]);
    for (u_short i = 0; i < 10; ++i) {
      LinkedListPtr linkedlist = std::make_unique<kwdbts::BatchDataLinkedList>();
      ASSERT_EQ(linkedlist->init(row_count), 0);
      for (u_short j = 0; j < row_count; ++j) {
        linkedlist->row_indice_list_[j].batch_no=i;
        linkedlist->row_indice_list_[j].offset_in_batch=j;
      }
      ASSERT_TRUE(linked_list_container->AddLinkedList(std::move(linkedlist)) == KStatus::SUCCESS);
    }

    ASSERT_EQ(linked_list_container->IsMaterialized(), linked_list_is_materialized[case_num]);

    delete linked_list_container;
  }
}

    // Check data integrity after retrieval for each linked list
    TEST_F(TestLinkedListContainer, TestDataIntegrity) {
      kwdbContext_t context;
      kwdbContext_p ctx = &context;
      InitServerKWDBContext(ctx);

      k_int64 row_count = 1000;
      std::vector<int> data_size_threshold = {DEFAULT_DATA_SIZE_THRESHOLD_IN_MEMORY, 8 * 1024};
      std::vector<bool> linked_list_is_materialized = {false, true};

      for (int case_num = 0; case_num < 2; ++case_num) {
        LinkedListContainer* linked_list_container = new LinkedListContainer(data_size_threshold[case_num]);
        for (u_short i = 0; i < 10; ++i) {
          LinkedListPtr linkedlist = std::make_unique<kwdbts::BatchDataLinkedList>();
          ASSERT_EQ(linkedlist->init(row_count), 0);
          for (u_short j = 0; j < row_count; ++j) {
            linkedlist->row_indice_list_[j].batch_no=i;
            linkedlist->row_indice_list_[j].offset_in_batch=j;
          }
          ASSERT_EQ(linked_list_container->AddLinkedList(std::move(linkedlist)), KStatus::SUCCESS);
        }

        ASSERT_EQ(linked_list_container->IsMaterialized(), linked_list_is_materialized[case_num]);

        for (int i = 0; i < 10; ++i) {
          LinkedListPtr& data = linked_list_container->GetLinkedList(i);
          ASSERT_NE(data, nullptr);
          for (int j = 0; j < row_count; ++j) {
            if (!linked_list_container->IsMaterialized()) {
              ASSERT_EQ(data->row_indice_list_[j].batch_no, i);
              ASSERT_EQ(data->row_indice_list_[j].offset_in_batch, j);
            } 
          }
        }
        delete linked_list_container;
      }
    }
