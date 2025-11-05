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

#include <gtest/gtest.h>
#include <cstdint>
#include "ts_del_item_manager.h"
#include "sys_utils.h"

using namespace kwdbts;  // NOLINT

const  std::string del_item_file_path = "./tests";

class TsDelItemMgrTest : public ::testing::Test {
 public:
  TsDelItemMgrTest() {
    auto ret = system(("rm -rf tests"));
    EXPECT_EQ(ret, 0);
    ret = system(("mkdir tests"));
    EXPECT_EQ(ret, 0);
  }
};

TEST_F(TsDelItemMgrTest, empty) {
  TsDelItemManager mgr(del_item_file_path);
  mgr.Open();
  mgr.DropAll();
}

TEST_F(TsDelItemMgrTest, simple) {
  TsDelItemManager mgr(del_item_file_path);
  mgr.Open();
  TsEntityDelItem del_item({2, 22}, {1, 11}, 1);
  KStatus s = mgr.AddDelItem(1, del_item);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  std::list<TsEntityDelItem*> del_items;
  s = mgr.GetDelItem(1, del_items);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(1, del_items.size());
  ASSERT_EQ(del_items.front()->entity_id, 1);
  ASSERT_EQ(del_items.front()->range.osn_span.begin, 1);
  ASSERT_EQ(del_items.front()->range.osn_span.end, 11);
  ASSERT_EQ(del_items.front()->range.ts_span.begin, 2);
  ASSERT_EQ(del_items.front()->range.ts_span.end, 22);
}

TEST_F(TsDelItemMgrTest, simpleInsert) {
  TsDelItemManager mgr(del_item_file_path);
  mgr.Open();
  mgr.Reset();
  for (size_t i = 0; i < 10; i++) {
    TsEntityDelItem del_item({(int64_t)(2 + i), (int64_t)(22 + i)}, {1 + i, 11 + i}, 1);
    KStatus s = mgr.AddDelItem(1, del_item);
    ASSERT_TRUE(s == KStatus::SUCCESS);
  }
  std::list<TsEntityDelItem*> del_items;
  KStatus s = mgr.GetDelItem(1, del_items);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(10, del_items.size());
  auto it = del_items.rbegin();
  int i = 0;
  while (it != del_items.rend()) {
    ASSERT_EQ((*it)->entity_id, 1);
    ASSERT_EQ((*it)->range.osn_span.begin, 1 + i);
    ASSERT_EQ((*it)->range.osn_span.end, 11 + i);
    ASSERT_EQ((*it)->range.ts_span.begin, 2 + i);
    ASSERT_EQ((*it)->range.ts_span.end, 22 + i);
    i++;
    it++;
  }
  bool has_valid;
  s = mgr.HasValidDelItem({1, 11}, has_valid);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  ASSERT_EQ(has_valid, true);
  s = mgr.DropEntity(1);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  s = mgr.HasValidDelItem({1, 11}, has_valid);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  // ASSERT_EQ(has_valid, false);
  del_items.clear();
  s = mgr.GetDelItem(1, del_items);
  ASSERT_TRUE(s == KStatus::SUCCESS);
  // ASSERT_EQ(0, del_items.size());
}

TEST_F(TsDelItemMgrTest, InsertAndRm) {
  int entity_num = 5;
  int del_item_pre_entity = 5;
  {
    TsDelItemManager mgr(del_item_file_path);
    mgr.Open();
    for (size_t i = 1; i <= entity_num; i++) {
      for (size_t j = 0; j < del_item_pre_entity; j++) {
        TsEntityDelItem del_item({(int64_t)(2 + i + j), (int64_t)(22 + i + j)}, {1 + i + j, 11 + i + j}, i);
        KStatus s = mgr.AddDelItem(i, del_item);
        ASSERT_TRUE(s == KStatus::SUCCESS);
      }
    }
    for (size_t i = 1; i <= entity_num; i++) {
      std::list<TsEntityDelItem*> del_items;
      KStatus s = mgr.GetDelItem(i, del_items);
      ASSERT_TRUE(s == KStatus::SUCCESS);
      ASSERT_EQ(del_item_pre_entity, del_items.size());
    }
    ASSERT_EQ(mgr.GetTotalNum(), entity_num * del_item_pre_entity);
    ASSERT_EQ(mgr.GetMaxEntityId(), entity_num);
    ASSERT_EQ(mgr.GetMinLsn(), 2);
    ASSERT_EQ(mgr.GetMaxLsn(), 11 + entity_num + del_item_pre_entity - 1);
    ASSERT_EQ(mgr.GetClearMaxLsn(), 0);
    ASSERT_EQ(mgr.GetDroppedNum(), 0);
    for (size_t i = 1; i <= entity_num; i++) {
      auto s = mgr.RmDeleteItems(i, {0, 15});
      ASSERT_TRUE(s == KStatus::SUCCESS);
    }
    ASSERT_EQ(mgr.GetClearMaxLsn(), 15);
    ASSERT_EQ(mgr.GetDroppedNum(), 10);
    for (size_t i = 1; i <= entity_num; i++) {
      std::list<STDelRange> del_items;
      KStatus s = mgr.GetDelRange(i, del_items);
      ASSERT_TRUE(s == KStatus::SUCCESS);
      int del_num = 15 - (11 + i) + 1;
      if (del_num > 0) {
        ASSERT_EQ(del_item_pre_entity - del_num, del_items.size());
      } else {
        ASSERT_EQ(del_item_pre_entity, del_items.size());
      }
    }
  }
}

TEST_F(TsDelItemMgrTest, simpleMultiInsert) {
  TsDelItemManager mgr(del_item_file_path);
  mgr.Open();
  mgr.Reset();
  int thread_num = 0;  //10;
  int entity_del_item_num = 1000000;
  std::vector<std::thread> threads;
  for (size_t i = 1; i <= thread_num; i++) {
    threads.push_back(thread([&](int index) {
      for (size_t i = 0; i < entity_del_item_num; i++) {
        TsEntityDelItem del_item({(int64_t)(2 + i), (int64_t)(22 + i)}, {1 + i, 11 + i}, index);
        KStatus s = mgr.AddDelItem(index, del_item);
        ASSERT_TRUE(s == KStatus::SUCCESS);
      }
    }, i));
  }
  for (size_t i = 0; i < threads.size(); i++) {
    threads[i].join();
  }
  for (size_t i = 1; i <= thread_num; i++) {
    std::list<TsEntityDelItem*> del_items;
    KStatus s = mgr.GetDelItem(i, del_items);
    ASSERT_TRUE(s == KStatus::SUCCESS);
    ASSERT_EQ(entity_del_item_num, del_items.size());
  }
}

TEST_F(TsDelItemMgrTest, reopen) {
  int entity_num = 5;
  int del_item_pre_entity = 5;
  {
    TsDelItemManager mgr(del_item_file_path);
    mgr.Open();
    for (size_t i = 1; i <= entity_num; i++) {
      for (size_t j = 0; j < del_item_pre_entity; j++) {
        TsEntityDelItem del_item({(int64_t)(2 + i + j), (int64_t)(22 + i + j)}, {1 + i + j, 11 + i + j}, i);
        KStatus s = mgr.AddDelItem(i, del_item);
        ASSERT_TRUE(s == KStatus::SUCCESS);
      }
    }
    for (size_t i = 1; i <= entity_num; i++) {
      std::list<TsEntityDelItem*> del_items;
      KStatus s = mgr.GetDelItem(i, del_items);
      ASSERT_TRUE(s == KStatus::SUCCESS);
      ASSERT_EQ(del_item_pre_entity, del_items.size());
    }
    ASSERT_EQ(mgr.GetTotalNum(), entity_num * del_item_pre_entity);
    ASSERT_EQ(mgr.GetMaxEntityId(), entity_num);
    ASSERT_EQ(mgr.GetMinLsn(), 2);
    ASSERT_EQ(mgr.GetMaxLsn(), 11 + entity_num + del_item_pre_entity - 1);
  }
  {
    TsDelItemManager mgr(del_item_file_path);
    mgr.Open();
    for (size_t i = 1; i <= entity_num; i++) {
      std::list<TsEntityDelItem*> del_items;
      KStatus s = mgr.GetDelItem(i, del_items);
      ASSERT_TRUE(s == KStatus::SUCCESS);
      ASSERT_EQ(del_item_pre_entity, del_items.size());
    }
    ASSERT_EQ(mgr.GetTotalNum(), entity_num * del_item_pre_entity);
    ASSERT_EQ(mgr.GetMaxEntityId(), entity_num);
    ASSERT_EQ(mgr.GetMinLsn(), 2);
    ASSERT_EQ(mgr.GetMaxLsn(), 11 + entity_num + del_item_pre_entity - 1);
  }
}

TEST_F(TsDelItemMgrTest, reopenTimes) {
  int entity_num = 5;
  int del_item_pre_entity = 5;
  for (size_t m = 1; m <= 5; m++) {
    {
      TsDelItemManager mgr(del_item_file_path);
      mgr.Open();
      for (size_t i = 1; i <= entity_num; i++) {
        for (size_t j = 0; j < del_item_pre_entity; j++) {
          TsEntityDelItem del_item({(int64_t)(2 + i + j), (int64_t)(22 + i + j)}, {1 + i + j, 11 + i + j}, i);
          KStatus s = mgr.AddDelItem(i, del_item);
          ASSERT_TRUE(s == KStatus::SUCCESS);
        }
      }
      for (size_t i = 1; i <= entity_num; i++) {
        std::list<TsEntityDelItem*> del_items;
        KStatus s = mgr.GetDelItem(i, del_items);
        ASSERT_TRUE(s == KStatus::SUCCESS);
        ASSERT_EQ(del_item_pre_entity * m, del_items.size());
      }
      ASSERT_EQ(mgr.GetTotalNum(), entity_num * del_item_pre_entity * m);
      ASSERT_EQ(mgr.GetMaxEntityId(), entity_num);
      ASSERT_EQ(mgr.GetMinLsn(), 2);
      ASSERT_EQ(mgr.GetMaxLsn(), 11 + entity_num + del_item_pre_entity - 1);
    }
    {
      TsDelItemManager mgr(del_item_file_path);
      mgr.Open();
      for (size_t i = 1; i <= entity_num; i++) {
        std::list<TsEntityDelItem*> del_items;
        KStatus s = mgr.GetDelItem(i, del_items);
        ASSERT_TRUE(s == KStatus::SUCCESS);
        ASSERT_EQ(del_item_pre_entity * m, del_items.size());
      }
      ASSERT_EQ(mgr.GetTotalNum(), entity_num * del_item_pre_entity * m);
      ASSERT_EQ(mgr.GetMaxEntityId(), entity_num);
      ASSERT_EQ(mgr.GetMinLsn(), 2);
      ASSERT_EQ(mgr.GetMaxLsn(), 11 + entity_num + del_item_pre_entity - 1);
    }
  }
}
