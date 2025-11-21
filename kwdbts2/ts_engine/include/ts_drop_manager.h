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

#pragma once
#include <unordered_set>
#include <mutex>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <optional>

#include "libkwdbts2.h"

namespace kwdbts {
class DropTableManager {
 public:
  static DropTableManager& getInstance();

  DropTableManager(const DropTableManager&) = delete;
  DropTableManager& operator=(const DropTableManager&) = delete;

  bool markTableDropped(int table_id);
  bool isTableDropped(int table_id) const;
  bool removeFromDropped(int table_id);
  void clearAllDroppedTables();

  size_t getDroppedTableCount() const;
  std::unordered_set<int> getAllDroppedTables() const;

  void markMultipleTablesDropped(const std::unordered_set<int>& table_ids);
  size_t removeMultipleTables(const std::unordered_set<int>& table_ids);

 private:
  DropTableManager() = default;
  ~DropTableManager() = default;

  mutable std::mutex mutex_;
  std::unordered_set<int> dropped_tables_;
};
}  // namespace kwdbts
