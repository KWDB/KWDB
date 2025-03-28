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

#include <queue>
#include <vector>
#include "st_wal_internal_logblock.h"
#include "st_wal_internal_logfile_mgr.h"
#include "st_wal_internal_log_structure.h"
#include "st_wal_types.h"

namespace kwdbts {

/**
* WAL Buffer Management class, each WALBufferMgr maintains a separate Buffer, its unit is LogBlock, and provides
* the ability to write WAL.
*/
class WALBufferMgr {
 public:
  WALBufferMgr(EngineOptions* opt, WALFileMgr* file_mgr);

  ~WALBufferMgr();

  /**
   * Init WAL Buffer
   * 1. Reads and caches the (HeaderBlock, Current LSN, etc) of the current WAL file.
   * 2. Initializes LogBlocks and allocates memory space based on init_buffer_size.
   * @return
   */
  KStatus init(TS_LSN start_lsn = 0);

  void ResetMeta();

  /**
   * Write WAL log entry into WAL Buffer
   * 1. Write WAL log entry into current EntryBlock.
   * 2. if the current EntryBlock does not have enough free space to store the WAL,a split operation is performed.
   * @param wal_log
   * @return
   */
  KStatus writeWAL(kwdbContext_p ctx, k_char* wal_log, size_t length,
                   TS_LSN& lsn_offset);

  /**
   * Flush WAL to log files
   * 1. The background thread periodically calls flush.
   * 2. If the current buffer reaches 60% of the maximum capacity, call flush to write log entry.
   * 3. After checkpoint logs wrote,the flush method is called forcibly.
   * 4. If the device is powered off normally, the checkpoint method indirectly calls flush.
   *
   * flush in progress:
   * 1. If there is log files switching, the HeaderBlock needs to be updated.
   *
   * @param ctx
   * @return
   */
  KStatus flush();

  KStatus flushInternal(bool flush_header);

  /**
   * Update the checkpoint info of HeaderBlock
   * @param lsn checkpoint LSN
   * @param no checkpoint NO
   * @return
   */
  KStatus setHeaderBlockCheckpointInfo(TS_LSN checkpoint_lsn, uint32_t checkpoint_no);

  /**
   * Read multiple log entries
   * @param[out] log_entries Result of read log entries
   * @param start_lsn LSN of the first log entry
   * @param end_lsn LSN of the last log entry
   * @param txn_id Only the log entries of this txn_id are read. The default value is 0, that is, all logs are read.
   * @return
   */
  KStatus readWALLogs(std::vector<LogEntry*>& log_entries, TS_LSN start_lsn, TS_LSN end_lsn, uint64_t txn_id = 0);

  /**
   * Cache specified length bytes from current EntryBlock.
   * @param[in,out] start_offset Start offset
   * @param[in] current_block Current Block pointer
   * @param[in] length The length to be read
   * @param[out] res Content
   * @return Current Block pointer
   */
  KStatus readBytes(TS_LSN& start_offset, std::queue<EntryBlock*>& read_queue, size_t length, char*& res);

  TS_LSN getCurrentLsn();

 private:
  // This mutex is used to protect current block,Header Block,EntryBlock Linked list.
  // Potential optimizations:
  // 1. WriteWAL only need to Lock the EntryBlock which block No. >= current block No.
  // 2. Flush need to Lock first the EntryBlock which block No. < current block No.
  // 3. After write, lock current block, then write current block,HeaderBlock.
  // std::mutex buf_mutex_;
  using WALBufferMgrBufLatch = KLatch;
  WALBufferMgrBufLatch* buf_mutex_;

  EngineOptions* opt_{nullptr};
  WALFileMgr* file_mgr_{nullptr};

  uint32_t init_buffer_size_;

  uint64_t begin_block_index_{0};
  uint64_t current_block_index_{0};
  uint64_t end_block_index_{0};

  // In-memory buffer for WAL writer/reader.
  EntryBlock* buffer_{nullptr};

  HeaderBlock headerBlock_ = HeaderBlock();
  EntryBlock* currentBlock_{nullptr};

  /**
   * If the number of LogBlocks in the current cache is greater than flush_buffer_size, WAL Buffer is forced to disk.
   * @return
   */
  [[nodiscard]] inline bool needFlush() const {
    uint32_t flush_buffer_size;
    if (init_buffer_size_ <= opt_->GetBlockNumPerFile()) {
      flush_buffer_size = uint32_t(init_buffer_size_ * 0.6);
    } else {
      flush_buffer_size = uint32_t(opt_->GetBlockNumPerFile() * 0.6);
    }
    return (buffer_[current_block_index_].getBlockNo() - buffer_[begin_block_index_].getBlockNo()) >= flush_buffer_size;
  }

  [[nodiscard]] inline uint32_t nextBlockIndex(uint32_t block_index) const {
    return (block_index + 1) % init_buffer_size_;
  }

  KStatus switchToNextBlock() {
    // flush the wal buffer when the usage is greater than 60%
    if (needFlush()) {
      KStatus status = flushInternal(true);
      if (status == FAIL) {
        return FAIL;
      }
    }

    // the current block is full now, switch to next one.
    current_block_index_ = nextBlockIndex(current_block_index_);
    currentBlock_ = &buffer_[current_block_index_];

    return SUCCESS;
  }

  KStatus readInsertLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                        TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readUpdateLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                        TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readDeleteLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                        TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readCreateIndexLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                            TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readDropIndexLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                            TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readCheckpointLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                            TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  KStatus readDDLCreateLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id, TS_LSN& current_offset,
                           queue<EntryBlock*>& read_queue);

  KStatus readDDLAlterLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id, TS_LSN& current_offset,
                          queue<EntryBlock*>& read_queue);

  KStatus readPartitionTierChangeLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn,
                              TS_LSN txn_id, TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue);

  void Lock() {
    MUTEX_LOCK(buf_mutex_);
  }

  void Unlock() {
    MUTEX_UNLOCK(buf_mutex_);
  }
};
}  // namespace kwdbts
