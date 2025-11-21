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

#include "ts_drop_manager.h"

#include <chrono>
#include <thread>

namespace kwdbts {
DropTableManager& DropTableManager::getInstance() {
    static DropTableManager instance;
    return instance;
}

bool DropTableManager::markTableDropped(int table_id) {
    if (table_id <= 0) {
        return false;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    if (dropped_tables_.find(table_id) != dropped_tables_.end()) {
        return false;
    }
    auto result = dropped_tables_.insert(table_id);
    return result.second;
}

bool DropTableManager::isTableDropped(int table_id) const {
    if (table_id <= 0) {
        return false;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    return dropped_tables_.find(table_id) != dropped_tables_.end();
}

bool DropTableManager::removeFromDropped(int table_id) {
    if (table_id <= 0) {
        return false;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    size_t removed_count = dropped_tables_.erase(table_id);
    if (removed_count > 0) {
        return true;
    }
    return false;
}

void DropTableManager::clearAllDroppedTables() {
    std::unique_lock<std::mutex> lock(mutex_);
    dropped_tables_.clear();
}

size_t DropTableManager::getDroppedTableCount() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return dropped_tables_.size();
}

std::unordered_set<int> DropTableManager::getAllDroppedTables() const {
    std::unique_lock<std::mutex> lock(mutex_);
    return dropped_tables_;
}

void DropTableManager::markMultipleTablesDropped(const std::unordered_set<int>& table_ids) {
    if (table_ids.empty()) {
        return;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    for (int table_id : table_ids) {
        if (table_id > 0) {
            dropped_tables_.insert(table_id);
        }
    }
}

size_t DropTableManager::removeMultipleTables(const std::unordered_set<int>& table_ids) {
    if (table_ids.empty()) {
        return 0;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    size_t total_removed = 0;
    for (int table_id : table_ids) {
        total_removed += dropped_tables_.erase(table_id);
    }
    return total_removed;
}
}   // namespace kwdbts
