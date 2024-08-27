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


#pragma once

#include <unordered_map>
#include "mmap/mmap_metrics_table.h"

using namespace kwdbts;

/**
 * @brief A class for managing multiple versions of root table.
 *
 * This class is responsible for creating, managing, and accessing root tables.
 * It uses read and write locks (rw_latch_) to control concurrent access to tables
 * and maintain current and historical versions of tables.
 * It also manages partition intervals and other metadata information for tables.
 */
class MMapRootTableManager {
private:
  // Read/write lock used to control concurrent access.
  KRWLatch rw_latch_;

protected:
  // Name of the table.
  string name_;
  // Storage path of the database.
  string db_path_;
  // Table of subpaths.
  string tbl_sub_path_;
  // ID of the table.
  uint32_t table_id_;
  // Version number of the current table.
  uint32_t cur_table_version_;
  // The currently active root table.
  MMapMetricsTable* cur_root_table_;
  // Partition interval.
  uint64_t partition_interval_;
  // Stores mappings of all versions of the root table.
  std::unordered_map<uint32_t, MMapMetricsTable*> root_tables_;

  /**
   * @brief Opens the root table for the specified version.
   *
   * @param table_version Table version number.
   * @param err_info Error message.
   * @return MMapMetricsTable* Pointer to root table
   */
  MMapMetricsTable* openRootTable(uint32_t table_version, ErrorInfo& err_info);

public:
  /**
   * @brief Constructor that initializes the root table manager.
   *
   * @param db_path The path to the database.
   * @param tbl_sub_path Table of subpaths.
   * @param table_id ID of the table.
   */
  MMapRootTableManager(const string& db_path, const string& tbl_sub_path, uint32_t table_id) :
      name_(to_string(table_id)), db_path_(db_path), tbl_sub_path_(tbl_sub_path), table_id_(table_id),
      cur_table_version_(0), rw_latch_(RWLATCH_ID_MMAP_ROOT_TABLE_RWLOCK) {}

  /**
   * @brief Constructor that initializes the root table manager and sets the partition interval.
   *
   * @param db_path The path to the database.
   * @param tbl_sub_path Table of subpaths.
   * @param table_id ID of the table.
   * @param partition_interval Partition interval.
   */
  MMapRootTableManager(const string& db_path, const string& tbl_sub_path, uint32_t table_id,
                       uint64_t partition_interval) :
      name_(to_string(table_id)), db_path_(db_path), tbl_sub_path_(tbl_sub_path), table_id_(table_id),
      cur_table_version_(0), partition_interval_(partition_interval), rw_latch_(RWLATCH_ID_MMAP_ROOT_TABLE_RWLOCK) {}

  /**
   * @brief Destructor, which frees resources.
   */
  ~MMapRootTableManager();

  /**
   * @brief Initializes the root table manager.
   *
   * @param err_info Error message
   * @return KStatus status
   */
  KStatus Init(ErrorInfo& err_info);

  /**
   * @brief Create a new version of the root table.
   *
   * @param schema Schema information for the table.
   * @param ts_version Version number of the table.
   * @param err_info Error message
   * @return KStatus status
   */
  KStatus CreateRootTable(vector<AttributeInfo>& schema, uint32_t ts_version,
                          ErrorInfo& err_info, uint32_t cur_version = 0);

  /**
   * @brief Add the table to the manager.
   *
   * @param table_version Version number of the table.
   * @param table Pointer to root table
   */
  KStatus PutTable(uint32_t table_version, MMapMetricsTable* table);

  /**
   * @brief Gets the root table of the specified version.
   *
   * @param table_version Version number of the table.
   * @param lock Whether to lock.
   * @return MMapMetricsTable* Pointer to root table
   */
  MMapMetricsTable* GetRootTable(uint32_t table_version, bool lock = true);

  /**
   * @brief Gets the version number of the current table.
   *
   * @return uint32_t Version number of the current table.
   */
  uint32_t GetCurrentTableVersion() const;

  /**
   * @brief Gets the name of the table.
   *
   * @return string Name of the table.
   */
  string GetTableName();

  /**
   * @brief Interval for obtaining partitions.
   *
   * @return uint64_t Partition interval.
   */
  uint64_t GetPartitionInterval();

  /**
   * @brief Set the partition interval.
   *
   * @param partition_interval New partition interval.
   * @return KStatus status
   */
  KStatus SetPartitionInterval(const uint64_t& partition_interval);

  /**
   * @brief Get the actual schema information (exclude dropped columns)
   *
   * @param table_version Version number of the table.
   * @return const vector<AttributeInfo>& The actual schema information
   */
  const vector<AttributeInfo>& GetSchemaInfoWithoutHidden(uint32_t table_version = 0);

  /**
   * @brief Gets schema information for a table.
   *
   * @param table_version Version number of the table. The default is 0.
   * @return Returns schema information for the table.
   */
  const vector<AttributeInfo>& GetSchemaInfoWithHidden(uint32_t table_version = 0);

  /**
   * @brief Gets index information for the actual column (exclude dropped columns)
   *
   * @param table_version Version number of the table. The default is 0.
   * @return Returns index information for the actual column.
   */
  const vector<uint32_t>& GetColsIdx(uint32_t table_version = 0);

  /**
   * @brief Gets the index of the column based on the attribute information.
   *
   * @param attr_info Attribute information.
   * @return Returns the index of the column, or a negative number if not found.
   */
  int GetColumnIndex(const AttributeInfo& attr_info);

  /**
   * @brief Refresh data to the database to ensure data persistence.
   *
   * @param check_lsn Logical sequence number of the check to ensure data consistency.
   * @param err_info Error message
   * @return Returns the status of the operation.
   */
  int Sync(const kwdbts::TS_LSN& check_lsn, ErrorInfo& err_info);

  /**
   * @brief Set the table to the deleted state.
   *
   * @return Returns the status of the operation.
   */
  KStatus SetDropped();

  /**
   * @brief Check whether the table has been deleted.
   *
   * @return Return true if the table has been deleted; Otherwise return false.
   */
  bool IsDropped();

  /**
   * @brief Clear all data in the table.
   *
   * @return Returns the status of the operation.
   */
  KStatus RemoveAll();

  /**
   * @brief Rolls back the table to the specified version.
   *
   * @param version Target version number of the rollback.
   * @return Returns the status of the operation.
   */
  KStatus RollBack(uint32_t old_version, uint32_t new_version);

  /**
   * @brief Update the version number of the table.
   *
   * @param version Version number of the table.
   * @return Returns the status of the operation.
   */
  KStatus UpdateVersion(uint32_t cur_version, uint32_t new_version);

  /**
   * @brief Update table version number of the last data.
   *
   * @param version Version number of the table.
   * @return Returns the status of the operation.
   */
  KStatus UpdateTableVersionOfLastData(uint32_t version);

  /**
   * @brief Get the table version of the last data.
   * @return table version
   */
  uint32_t GetTableVersionOfLatestData();

  /**
   * @brief Get the read lock.
   *
   * @return Returns the status of the operation.
   */
  int rdLock();

  /**
   * @brief Get the write lock.
   *
   * @return Returns the status of the operation.
   */
  int wrLock();

  /**
   * @brief Release the lock.
   *
   * @return Returns the status of the operation.
   */
  int unLock();

};