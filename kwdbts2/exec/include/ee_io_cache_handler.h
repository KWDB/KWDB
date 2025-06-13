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

#include <chrono>
#include <vector>
#include <list>
#include <unordered_map>

#include "kwdb_type.h"

namespace kwdbts {

constexpr const k_uint64 MAX_FILE_SIZE{21474836480};  // 256MB
constexpr const k_uint32 MAX_OPEN_FILE_NUM{10};

enum cache_type {
  CACHE_UNKNOW = -1,

  CACHE_READ = 1,
  CACHE_WRITE,
  CACHE_SEQ_READ_APPEND,

  CACHE_MAX = 0xFFFFFFFE
};
/**
 * @class IOCacheHandler
 * @brief A handler class for IO caching operations, managing file I/O with caching and LRU eviction.
 */
class IOCacheHandler {
 protected:
  /**
   * @struct IO_INFO
   * @brief Structure to hold information about an opened file.
   */
  struct IO_INFO {
    k_int32 fd_{-1};           ///< File descriptor, initialized to -1 indicating no open file.
    k_char *path_{nullptr};         ///< Pointer to the file path, initialized to nullptr.
    k_uint64 size_{0};        ///< Size of the file, initialized to 0.
    cache_type type_{CACHE_UNKNOW};      ///< Cache type of the file, initialized to CACHE_UNKNOW.
    std::chrono::steady_clock::time_point last_access_;  ///< Last access time of the file.
  };

  std::vector<IO_INFO> io_info_;  ///< Vector to store information of all managed files.
  k_uint32 current_file_id_{UINT32_MAX};    ///< ID of the currently accessed file, initialized to UINT32_MAX.

  using LRUList = std::list<k_uint32>;  ///< Type alias for a list used in LRU cache management.
  using LRUMap = std::unordered_map<k_uint32, LRUList::iterator>;  ///< Type alias for a map used in LRU cache management.
  LRUList lru_list_;  ///< List to implement LRU (Least Recently Used) caching strategy.
  LRUMap lru_map_;    ///< Map to keep track of the position of file IDs in the LRU list.
  k_uint32 max_file_size_{0};  ///< Maximum size of a file that can be managed.

  /**
   * @brief Open a file with the specified ID and cache type.
   * @param file_id The ID of the file to open.
   * @param type The cache type for the file.
   * @return KStatus indicating the result of the operation.
   */
  KStatus Open(const k_uint32 file_id, cache_type type);

  /**
   * @brief Update the LRU list when a file is accessed.
   * @param file_id The ID of the accessed file.
   */
  void update_lru(k_uint32 file_id);

  /**
   * @brief Remove the least recently used file from the LRU list.
   */
  void remove_lru();

  char* read_buffer_{nullptr};         ///< Pointer to the read buffer, initialized to nullptr.
  k_uint64 read_buffer_offset_{UINT64_MAX};  ///< Offset of the read buffer, initialized to UINT64_MAX.
  k_uint64 read_buffer_size_{64 * 1024};  ///< Size of the read buffer, initialized to 64KB.
  k_uint64 total_size_{0};       ///< Total size of all managed files.

  /**
   * @brief Read data from the buffer.
   * @param buf Pointer to the buffer where data will be stored.
   * @param offset Offset from which to start reading.
   * @param len Length of data to read.
   * @return KStatus indicating the result of the operation.
   */
  KStatus ReadFromBuffer(k_char *buf, k_uint64 offset, k_uint64 len);

  /**
   * @brief Read data directly from the file.
   * @param buf Pointer to the buffer where data will be stored.
   * @param offset Offset from which to start reading.
   * @param len Length of data to read.
   * @return KStatus indicating the result of the operation.
   */
  KStatus ReadFromFile(k_char *buf, k_uint64 offset, k_uint64 len);

 public:
  /**
   * @brief Constructor for IOCacheHandler.
   * @param max_file_size Maximum size of a file that can be managed.
   */
  explicit IOCacheHandler(k_uint64 max_file_size);

  /**
   * @brief Destructor for IOCacheHandler.
   */
  ~IOCacheHandler();

  /**
   * @brief Write data to the file.
   * @param buf Pointer to the buffer containing data to write.
   * @param len Length of data to write.
   * @return KStatus indicating the result of the operation.
   */
  KStatus Write(const k_char *buf, k_uint64 len);

  /**
   * @brief Read data from the file at the specified offset.
   * @param buf Pointer to the buffer where data will be stored.
   * @param offset Offset from which to start reading.
   * @param len Length of data to read.
   * @return KStatus indicating the result of the operation.
   */
  KStatus Read(k_char *buf, k_uint64 offset, k_uint64 len);

  /**
   * @brief Read data from the end of the file.
   * @param buf Pointer to the buffer where data will be stored.
   * @param len Length of data to read.
   * @return KStatus indicating the result of the operation.
   */
  KStatus ReadAppend(k_char *buf, k_uint64 len);

  /**
   * @brief Get the total size of all managed files.
   * @return Total size of all managed files.
   */
  k_uint64 Size() {return total_size_;}

  /**
   * @brief Reset the cache handler to its initial state.
   * @return KStatus indicating the result of the operation.
   */
  KStatus Reset();
};
}  // namespace kwdbts
