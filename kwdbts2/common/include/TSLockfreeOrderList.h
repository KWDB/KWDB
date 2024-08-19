// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#pragma once

#include <utility>
#include <functional>
#include <atomic>
#include <vector>

template<typename K, typename V>
class TSLockfreeOrderList {
  struct TSListItem {
    K key;
    V value;
    std::atomic<TSListItem*> prev_;
  };

 private:
  TSListItem tail_;

 public:
  TSLockfreeOrderList() {
    tail_.key = 0;
    tail_.prev_.store(nullptr);
  }

  ~TSLockfreeOrderList() {
    Clear();
  }

  void Clear() {
    TSListItem* cur = tail_.prev_.load();
    while (cur != nullptr) {
      TSListItem * prev = cur->prev_.load();
      delete cur;
      cur = prev;
    }
    tail_.prev_.store(nullptr);
  }

  bool Insert(K key, V value) {
    TSListItem *new_one = new TSListItem{key, value, nullptr};
    TSListItem *cur = &tail_;
    TSListItem *prev = cur->prev_.load();
    while (true) {
      if (prev == nullptr || prev->key < key) {
        new_one->prev_.store(prev);
        if (cur->prev_.compare_exchange_strong(prev, new_one)) {
          return true;
        } else {
          // restart from beging.
          cur = &tail_;
          prev = cur->prev_.load();
        }
      } else {
        cur = prev;
        prev = cur->prev_.load();
      }
    }
  }

  bool Seek(K s_key, K& key, V& value) const {
    const TSListItem *cur = &tail_;
    TSListItem *prev = cur->prev_.load();
    while (true) {
      if (prev == nullptr) {
        return false;
      }
      if (prev->key <= s_key) {
        key = prev->key;
        value = prev->value;
        return true;
      }
      cur = prev;
      prev = cur->prev_.load();
    }
  }

  bool Traversal(std::function<bool(K, V)> func) {
    TSListItem *cur = &tail_;
    TSListItem *prev = cur->prev_.load();
    while (true) {
      if (prev == nullptr) {
        return true;
      }
      if (!func(prev->key, prev->value)) {
        return false;
      }
      cur = prev;
      prev = cur->prev_.load();
    }
    return true;
  }

  void GetAllKey(std::vector<K>* keys) {
    TSListItem *cur = &tail_;
    TSListItem *prev = cur->prev_.load();
    while (true) {
      if (prev == nullptr) {
        return;
      }
      keys->push_back(prev->key);
      cur = prev;
      prev = cur->prev_.load();
    }
  }

  void GetAllValue(std::vector<V>* values) {
    TSListItem *cur = &tail_;
    TSListItem *prev = cur->prev_.load();
    while (true) {
      if (prev == nullptr) {
        return;
      }
      values->push_back(prev->value);
      cur = prev;
      prev = cur->prev_.load();
    }
  }
};