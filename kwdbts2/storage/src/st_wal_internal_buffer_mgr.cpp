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

#include "st_wal_internal_buffer_mgr.h"

#include "settings.h"

namespace kwdbts {
WALBufferMgr::WALBufferMgr(EngineOptions* opt, WALFileMgr* file_mgr)
    : opt_(opt), file_mgr_(file_mgr) {
  buf_mutex_ = new KLatch(LATCH_ID_WALBUFMGR_BUF_MUTEX);
  init_buffer_size_ = ((static_cast<uint32_t>(opt->wal_buffer_size)) << 20) / BLOCK_SIZE;
  buffer_ = new EntryBlock[init_buffer_size_];

  begin_block_index_ = 0;
  current_block_index_ = 0;
  end_block_index_ = init_buffer_size_ - 1;
}

WALBufferMgr::~WALBufferMgr() {
  delete[] buffer_;
  buffer_ = nullptr;
  currentBlock_ = nullptr;
  delete buf_mutex_;
}

KStatus WALBufferMgr::init(TS_LSN start_lsn) {
  // last_write_block_no: The last block number written
  uint32_t last_write_block_no = file_mgr_->GetBlockNoFromLsn(start_lsn);

  // init new buffer_
  vector<EntryBlock*> first_block;
  KStatus s = file_mgr_->readEntryBlocks(first_block, last_write_block_no, last_write_block_no);
  if (s == FAIL) {
    return s;
  }

  if (first_block.empty()) {
    buffer_[0].reset(last_write_block_no);
  } else {
    buffer_[0] = *(first_block[0]);
  }

  for (auto& block : first_block) {
    delete block;
  }

  for (int i = 1; i < init_buffer_size_; i++) {
    buffer_[i].reset(last_write_block_no + i);
  }

  // init header block
  headerBlock_ = file_mgr_->readHeaderBlock();
  currentBlock_ = &buffer_[current_block_index_];

  return SUCCESS;
}

void WALBufferMgr::ResetMeta() {
  begin_block_index_ = 0;
  current_block_index_ = 0;
  end_block_index_ = init_buffer_size_ - 1;
}

KStatus WALBufferMgr::readWALLogs(std::vector<LogEntry*>& log_entries,
                                  TS_LSN start_lsn, TS_LSN end_lsn, uint64_t txn_id) {
  if (end_lsn <= start_lsn || end_lsn > getCurrentLsn()) {
    return SUCCESS;
  }

  uint start_block = file_mgr_->GetBlockNoFromLsn(start_lsn);
  uint end_block = file_mgr_->GetBlockNoFromLsn(end_lsn);

  // Read WAL entry blocks form disk.
  std::vector<EntryBlock*> blocks;
  if (start_block < buffer_[begin_block_index_].getBlockNo()) {
    file_mgr_->readEntryBlocks(blocks, start_block, buffer_[begin_block_index_].getBlockNo() - 1);
  }

  std::queue<EntryBlock*> read_queue;

  // if the blocks is not empty, merge it when the writing buffer_.
  if (!blocks.empty()) {
    for (auto block : blocks) {
      read_queue.push(block);
    }
  }

  if (read_queue.empty() || buffer_[begin_block_index_].getBlockNo() == read_queue.back()->getBlockNo() + 1) {
    uint64_t index = begin_block_index_;
    do {
      if (buffer_[index].getBlockNo() >= start_block) {
        read_queue.push(&buffer_[index]);
      }
      index = nextBlockIndex(index);
    } while (buffer_[index].getBlockNo() <= end_block);
  }

  TS_LSN current_offset = start_lsn;
  TS_LSN current_lsn;

  if (read_queue.empty() || read_queue.front()->getBlockNo() != start_block) {
    LOG_ERROR("Failed to read the WAL log file from LSN %lu to %lu with transaction id %lu.",
              start_lsn, end_lsn, txn_id)
    return FAIL;
  }

  KStatus status;
  do {
    uint64_t x_id = 0;
    current_lsn = current_offset;
    char* read_buf;

    status = readBytes(current_offset, read_queue, 1, read_buf);
    if (status == FAIL) {
      delete[] read_buf;
      read_buf = nullptr;
      LOG_ERROR("Failed to parse the WAL log.")
      break;
    }

    WALLogType typ;
    memcpy(&typ, read_buf, LogEntry::LOG_TYPE_SIZE);
    delete[] read_buf;
    read_buf = nullptr;

    switch (typ) {
      case WALLogType::INSERT: {
        status = readInsertLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case WALLogType::DELETE: {
        status = readDeleteLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case WALLogType::CREATE_INDEX: {
        status = readCreateIndexLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case WALLogType::DROP_INDEX: {
        status = readDropIndexLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case WALLogType::CHECKPOINT: {
        status = readCheckpointLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case WALLogType::MTR_BEGIN: {
        x_id = current_lsn;
        uint64_t range_id, index;
        char tsx_id[LogEntry::TS_TRANS_ID_LEN];
        status = readBytes(current_offset, read_queue, MTRBeginEntry::header_length,
                           read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }

        if (x_id != current_lsn) {
          memcpy(&x_id, read_buf, sizeof(x_id));
        }
        uint64_t read_offset = sizeof(x_id);
        memcpy(tsx_id, read_buf + read_offset, LogEntry::TS_TRANS_ID_LEN);
        read_offset += LogEntry::TS_TRANS_ID_LEN;
        memcpy(&range_id, read_buf + read_offset, sizeof(range_id));
        read_offset += sizeof(range_id);
        memcpy(&index, read_buf + read_offset, sizeof(index));
        delete[] read_buf;
        read_buf = nullptr;

        if (txn_id == 0 || txn_id == x_id) {
          auto* mtr_entry = KNEW MTRBeginEntry(current_lsn, x_id, tsx_id, range_id, index);
          if (mtr_entry == nullptr) {
            LOG_ERROR("Failed to construct entry.")
            status = FAIL;
            break;
          }
          log_entries.push_back(mtr_entry);
        }
        break;
      }
      case WALLogType::MTR_COMMIT:
      case WALLogType::MTR_ROLLBACK: {
        char tsx_id[LogEntry::TS_TRANS_ID_LEN];
        status = readBytes(current_offset, read_queue, sizeof(x_id) + LogEntry::TS_TRANS_ID_LEN,
                           read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }

        if (x_id != current_lsn) {
          memcpy(&x_id, read_buf, sizeof(x_id));
        }
        memcpy(tsx_id, read_buf + sizeof(x_id), LogEntry::TS_TRANS_ID_LEN);
        delete[] read_buf;
        read_buf = nullptr;

        if (txn_id == 0 || txn_id == x_id) {
          auto* mtr_entry = KNEW MTREntry(current_lsn, typ, x_id, tsx_id);
          if (mtr_entry == nullptr) {
            LOG_ERROR("Failed to construct entry.")
            status = FAIL;
            break;
          }
          log_entries.push_back(mtr_entry);
        }
        break;
      }
      case UPDATE: {
        status = readUpdateLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        break;
      }
      case TS_BEGIN:
        x_id = current_lsn;
      case TS_COMMIT:
      case TS_ROLLBACK: {
        status = readBytes(current_offset, read_queue, sizeof(x_id), read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }

        if (x_id != current_lsn) {
          memcpy(&x_id, read_buf, sizeof(x_id));
        }

        delete[] read_buf;
        read_buf = nullptr;

        status = readBytes(current_offset, read_queue, TTREntry::TS_TRANS_ID_LEN, read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }
        auto* mtr_entry = KNEW TTREntry(current_lsn, typ, x_id, read_buf);
        delete[] read_buf;
        read_buf = nullptr;
        if (mtr_entry == nullptr) {
          LOG_ERROR("Failed to construct entry.")
          status = FAIL;
          break;
        }

        log_entries.push_back(mtr_entry);
        break;
      }
      case DDL_CREATE: {
        status = readDDLCreateLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
        }
        break;
      }
      case DDL_ALTER_COLUMN: {
        status = readDDLAlterLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
        }
        break;
      }
      case DDL_DROP: {
        uint64_t object_id;
        status = readBytes(current_offset, read_queue, sizeof(x_id) + sizeof(object_id),
                           read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }

        memcpy(&x_id, read_buf, sizeof(x_id));
        memcpy(&object_id, read_buf + sizeof(x_id), sizeof(object_id));
        auto* drop_entry = KNEW DDLDropEntry(current_lsn, typ, x_id, object_id);
        delete[] read_buf;
        read_buf = nullptr;
        if (drop_entry == nullptr) {
          LOG_ERROR("Failed to construct entry.")
          status = FAIL;
          break;
        }

        log_entries.push_back(drop_entry);
        break;
      }
      case RANGE_SNAPSHOT:
      {
        status = readBytes(current_offset, read_queue, SnapshotEntry::header_length, read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }
        TSTableID table_id;
        uint64_t b_hash, e_hash;
        KwTsSpan span;
        char* read_loc = read_buf;
        memcpy(&x_id, read_loc, sizeof(x_id));
        read_loc += sizeof(x_id);
        memcpy(&table_id, read_loc, sizeof(table_id));
        read_loc += sizeof(table_id);
        memcpy(&b_hash, read_loc, sizeof(b_hash));
        read_loc += sizeof(b_hash);
        memcpy(&e_hash, read_loc, sizeof(e_hash));
        read_loc += sizeof(e_hash);
        memcpy(&span.begin, read_loc, sizeof(span.begin));
        read_loc += sizeof(span.begin);
        memcpy(&span.end, read_loc, sizeof(span.end));
        read_loc += sizeof(span.end);
        auto* drop_entry = KNEW SnapshotEntry(current_lsn, x_id, table_id, b_hash, e_hash, span);
        delete[] read_buf;
        read_buf = nullptr;
        if (drop_entry == nullptr) {
          LOG_ERROR("Failed to construct entry.")
          status = FAIL;
          break;
        }

        log_entries.push_back(drop_entry);
        break;
      }
      case WALLogType::SNAPSHOT_TMP_DIRCTORY:
      {
        status = readBytes(current_offset, read_queue, TempDirectoryEntry::header_length, read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }
        size_t length;
        char* read_loc = read_buf;
        memcpy(&x_id, read_loc, sizeof(x_id));
        read_loc += sizeof(x_id);
        memcpy(&length, read_loc, sizeof(length));
        delete[] read_buf;
        read_buf = nullptr;
        status = readBytes(current_offset, read_queue, length, read_buf);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the WAL log.")
          break;
        }
        std::string file_path(read_buf, length);
        auto* drop_entry = KNEW TempDirectoryEntry(current_lsn, x_id, file_path);
        if (drop_entry == nullptr) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to construct entry.")
          status = FAIL;
          break;
        }
        delete[] read_buf;
        read_buf = nullptr;
        log_entries.push_back(drop_entry);
        break;
      }
      case WALLogType::PARTITION_TIER_CHANGE: {
        status = readPartitionTierChangeLog(log_entries, current_lsn, txn_id, current_offset, read_queue);
        if (status == FAIL) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to parse the PARTITION_TIER_CHANGE WAL log.")
        }
        break;
      }
      case DB_SETTING:
//        break;
      default:
        break;
    }
    if (status == FAIL) {
      LOG_ERROR("Failed to parse the WAL log.")
      break;
    }
  } while (current_offset < end_lsn);

  for (auto block : blocks) {
    delete block;
  }

  if (status == FAIL) {
    LOG_ERROR("Failed to parse the WAL log.")
    return status;
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readPartitionTierChangeLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn,
                              TS_LSN txn_id, TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf = nullptr;
  auto status = readBytes(current_offset, read_queue, PartitionTierChangeEntry::header_length, read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return KStatus::FAIL;
  }
  char* read_loc = read_buf;
  uint64_t x_id;
  memcpy(&x_id, read_loc, sizeof(x_id));
  read_loc += sizeof(x_id);
  size_t link_len;
  memcpy(&link_len, read_loc, sizeof(link_len));
  read_loc += sizeof(link_len);
  size_t tier_len;
  memcpy(&tier_len, read_loc, sizeof(tier_len));
  delete[] read_buf;
  read_buf = nullptr;
  status = readBytes(current_offset, read_queue, link_len + tier_len, read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return KStatus::FAIL;
  }
  std::string link_file_path(read_buf);
  std::string tier_file_path(read_buf + link_len);
  auto* drop_entry = KNEW PartitionTierChangeEntry(current_lsn, x_id, link_file_path, tier_file_path);
  if (drop_entry == nullptr) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to construct entry.")
    status = FAIL;
    return KStatus::FAIL;
  }
  delete[] read_buf;
  read_buf = nullptr;
  log_entries.push_back(drop_entry);
  return KStatus::SUCCESS;
}

KStatus WALBufferMgr::readBytes(TS_LSN& start_offset,
                                std::queue<EntryBlock*>& read_queue,
                                size_t length, char*& res) {
  size_t read_size = 0;
  auto current_block = read_queue.front();

  size_t offset_in_block = start_offset - file_mgr_->GetLSNFromBlockNo(current_block->getBlockNo());

  size_t n_read = 0;
  KStatus status = current_block->readBytes(offset_in_block, res, length, n_read);
  if (status == FAIL) {
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  read_size += n_read;

  // need to read more bytes from following blocks
  while (read_size < length) {
    char* temp;
    n_read = 0;
    offset_in_block = 0;

    read_queue.pop();
    if (!read_queue.empty()) {
      current_block = read_queue.front();
    } else {
      LOG_ERROR("Failed to parse the WAL log.")
      return FAIL;
    }

    status = current_block->readBytes(offset_in_block, temp, length - read_size, n_read);
    if (status == FAIL) {
      delete[] temp;
      temp = nullptr;
      LOG_ERROR("Failed to parse the WAL log.")
      return FAIL;
    }

    memcpy(res + read_size, temp, n_read);
    delete[] temp;
    temp = nullptr;
    read_size += n_read;
  }

  start_offset = file_mgr_->GetLSNFromBlockNo(current_block->getBlockNo()) + offset_in_block;;
  return SUCCESS;
}

KStatus WALBufferMgr::setHeaderBlockCheckpointInfo(TS_LSN checkpoint_lsn, uint32_t checkpoint_no) {
  headerBlock_.setCheckpointInfo(checkpoint_lsn, checkpoint_no);
  return SUCCESS;
}

KStatus WALBufferMgr::writeWAL(kwdbContext_p ctx, k_char* wal_log,
                               size_t length, TS_LSN& lsn_offset) {
  // In this method, buf_mutex is required to protect current block and the subsequent Block that may be used.
  this->Lock();

  size_t log_size = length;
  size_t offset = currentBlock_->getDataLen();

  if (offset + log_size <= LOG_BLOCK_MAX_LOG_SIZE) {
    size_t write_size = currentBlock_->writeBytes(wal_log, length, false);
    if (write_size != length) {
      LOG_ERROR("Failed to write the WAL log.")
      this->Unlock();
      return FAIL;
    }
    if (currentBlock_->getDataLen() == LOG_BLOCK_MAX_LOG_SIZE) {
      KStatus ret = switchToNextBlock();
      if (ret == FAIL) {
        LOG_ERROR("Failed to find an available WAL Buffer block.")
        this->Unlock();
        return FAIL;
      }
    }
    lsn_offset = getCurrentLsn();
    this->Unlock();
    return SUCCESS;
  }

  size_t max_str_nums = (log_size / (LOG_BLOCK_MAX_LOG_SIZE) + 2);
  auto lens = reinterpret_cast<size_t*>(malloc(sizeof(size_t) * max_str_nums));
  char** strs = reinterpret_cast<char**>(malloc(sizeof(char*) * max_str_nums));
  for (int i = 0; i < max_str_nums; i++) {
    lens[i] = 0;
  }

  size_t first_len = LOG_BLOCK_MAX_LOG_SIZE - offset;
  for (int i = 0; i < max_str_nums; i++) {
    if (i == 0) {
      strs[0] = wal_log;
      lens[0] = first_len;
    } else {
      strs[i] = wal_log + first_len;
      first_len += BLOCK_SIZE - LOG_BLOCK_HEADER_SIZE - LOG_BLOCK_CHECKSUM_SIZE;
      if (first_len >= log_size) {
        lens[i] =
            log_size - (first_len - (BLOCK_SIZE - (LOG_BLOCK_HEADER_SIZE +
                                                   LOG_BLOCK_CHECKSUM_SIZE)));
        break;
      }
      lens[i] = BLOCK_SIZE - (LOG_BLOCK_HEADER_SIZE + LOG_BLOCK_CHECKSUM_SIZE);
    }
  }
  for (int i = 0; i < max_str_nums; i++) {
    size_t write_length = currentBlock_->writeBytes(strs[i], lens[i], i > 0);
    if (lens[i] != write_length) {
      free(lens);
      free(strs);
      LOG_ERROR("Failed to write the WAL log.")
      this->Unlock();
      return FAIL;
    }
    if (currentBlock_->getDataLen() == LOG_BLOCK_MAX_LOG_SIZE) {
      KStatus ret = switchToNextBlock();
      if (ret == FAIL) {
        free(lens);
        free(strs);
        LOG_ERROR("Failed to find an available WAL Buffer block.")
        this->Unlock();
        return FAIL;
      }
    } else {
      if (i + 1 < max_str_nums && lens[i + 1] > 0) {
        free(lens);
        free(strs);
        LOG_ERROR("Failed to write the WAL log.")
        this->Unlock();
        return FAIL;
      }
    }
  }
  lsn_offset = getCurrentLsn();
  free(lens);
  free(strs);
  this->Unlock();
  return SUCCESS;
}

TS_LSN WALBufferMgr::getCurrentLsn() {
  TS_LSN lsn = file_mgr_->GetLSNFromBlockNo(currentBlock_->getBlockNo());
  return lsn + currentBlock_->getDataLen();
}

KStatus WALBufferMgr::flush() {
  Lock();
  KStatus s = flushInternal(true);
  Unlock();
  return s;
}

KStatus WALBufferMgr::flushInternal(bool flush_header) {
  file_mgr_->Lock();

  vector<EntryBlock*> blocks;

  if (buffer_[begin_block_index_].getDataLen() == 0) {
    file_mgr_->Unlock();
    return SUCCESS;
  }

  if (begin_block_index_ != current_block_index_) {
    size_t index = begin_block_index_;
    do {
      blocks.push_back(&buffer_[index]);
      index = nextBlockIndex(index);
    } while (index != current_block_index_);
  }
  // include the current block in flushing list.
  blocks.push_back(currentBlock_);

  // update the flushed lsn to ensure the recovery process loads the correct log records.
  headerBlock_.setFlushedLsn(getCurrentLsn());

  KStatus s = file_mgr_->writeBlocks(blocks, headerBlock_, flush_header);
  if (s == FAIL) {
    LOG_ERROR("Failed to flush the WAL logs to disk.")
    file_mgr_->Unlock();
    return FAIL;
  }

  file_mgr_->Unlock();

  begin_block_index_ = current_block_index_;
  uint64_t end_block_no = buffer_[end_block_index_].getBlockNo();
  for (int index = 0; index < blocks.size() - 1; index++) {
    blocks[index]->reset(++end_block_no);
  }

  end_block_index_ = (end_block_index_ + blocks.size() - 1) % init_buffer_size_;

  return s;
}

KStatus WALBufferMgr::readInsertLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                    TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf;
  WALTableType tbl_typ;
  uint64_t x_id = 0;
  KStatus status;

  status = readBytes(current_offset, read_queue, sizeof(x_id) + sizeof(WALTableType),
                     read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  memcpy(&x_id, read_buf, sizeof(x_id));
  memcpy(&tbl_typ, read_buf + sizeof(x_id), sizeof(tbl_typ));
  delete[] read_buf;
  read_buf = nullptr;

  switch (tbl_typ) {
    case WALTableType::TAG: {
      status = readBytes(current_offset, read_queue, InsertLogTagsEntry::header_length,
                         read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      int read_offset = 0;

      uint64_t time_partition = 0;
      uint64_t offset = 0;
      uint64_t length = 0;

      memcpy(&time_partition, read_buf,
             sizeof(InsertLogTagsEntry::time_partition_));
      read_offset += sizeof(InsertLogTagsEntry::time_partition_);
      memcpy(&offset, read_buf + read_offset,
             sizeof(InsertLogTagsEntry::offset_));
      read_offset += sizeof(InsertLogTagsEntry::offset_);
      memcpy(&length, read_buf + read_offset,
             sizeof(InsertLogTagsEntry::length_));
      // read_offset += sizeof(InsertLogTagsEntry::length);
      delete[] read_buf;
      read_buf = nullptr;

      status = readBytes(current_offset, read_queue, length, read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      if (txn_id == 0 || txn_id == x_id) {
        InsertLogTagsEntry* insert_tags = nullptr;
        try {
          insert_tags = KNEW InsertLogTagsEntry(current_lsn, WALLogType::INSERT, x_id, tbl_typ, time_partition,
                                                      offset, length, read_buf);
        } catch (exception &e) {
          LOG_ERROR("Failed to malloc memory for construct Entry.")
          return FAIL;
        }
        if (insert_tags == nullptr) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to construct entry.")
          return FAIL;
        }
        log_entries.push_back(insert_tags);
      }
      delete[] read_buf;
      read_buf = nullptr;
      break;
    }
    case WALTableType::DATA: {
      status = readBytes(current_offset, read_queue,
                         InsertLogMetricsEntry::header_length, read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      int read_offset = 0;

      uint64_t time_partition = 0;
      uint64_t offset = 0;
      uint64_t length = 0;
      size_t p_tag_len = 0;

      memcpy(&time_partition, read_buf, sizeof(InsertLogMetricsEntry::time_partition_));
      read_offset += sizeof(InsertLogMetricsEntry::time_partition_);
      memcpy(&offset, read_buf + read_offset, sizeof(InsertLogMetricsEntry::offset_));
      read_offset += sizeof(InsertLogMetricsEntry::offset_);
      memcpy(&length, read_buf + read_offset, sizeof(InsertLogMetricsEntry::length_));
      read_offset += sizeof(InsertLogMetricsEntry::length_);
      memcpy(&p_tag_len, read_buf + read_offset, sizeof(InsertLogMetricsEntry::p_tag_len_));
      delete[] read_buf;
      read_buf = nullptr;

      status = readBytes(current_offset, read_queue,
                         p_tag_len + length, read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      if (txn_id == 0 || txn_id == x_id) {
        auto* insert_metrics = KNEW InsertLogMetricsEntry(current_lsn, WALLogType::INSERT, x_id, tbl_typ,
                                                          time_partition, offset, length, p_tag_len, read_buf);
        if (insert_metrics == nullptr) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to construct entry.")
          return FAIL;
        }
        log_entries.push_back(insert_metrics);
      }

      delete[] read_buf;
      read_buf = nullptr;
      break;
    }
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readUpdateLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                    TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf;
  WALTableType tbl_typ;
  uint64_t x_id = 0;
  KStatus status;

  status = readBytes(current_offset, read_queue, sizeof(x_id) + sizeof(WALTableType),
                     read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  memcpy(&x_id, read_buf, sizeof(x_id));
  memcpy(&tbl_typ, read_buf + sizeof(x_id), sizeof(tbl_typ));
  delete[] read_buf;
  read_buf = nullptr;

  switch (tbl_typ) {
    case WALTableType::TAG: {
      status = readBytes(current_offset, read_queue, UpdateLogTagsEntry::header_length,
                         read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      int read_offset = 0;

      uint64_t time_partition = 0;
      uint64_t offset = 0;
      uint64_t length = 0;
      uint64_t old_len = 0;

      memcpy(&time_partition, read_buf,
             sizeof(UpdateLogTagsEntry::time_partition_));
      read_offset += sizeof(UpdateLogTagsEntry::time_partition_);
      memcpy(&offset, read_buf + read_offset,
             sizeof(UpdateLogTagsEntry::offset_));
      read_offset += sizeof(UpdateLogTagsEntry::offset_);
      memcpy(&length, read_buf + read_offset,
             sizeof(UpdateLogTagsEntry::length_));
      read_offset += sizeof(DeleteLogTagsEntry::p_tag_len_);
      memcpy(&old_len, read_buf + read_offset, sizeof(DeleteLogTagsEntry::tag_len_));
      delete[] read_buf;
      read_buf = nullptr;

      status = readBytes(current_offset, read_queue, length + old_len, read_buf);
      if (status == FAIL) {
        delete[] read_buf;
        read_buf = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      if (txn_id == 0 || txn_id == x_id) {
        UpdateLogTagsEntry* update_tags = nullptr;
        try {
          update_tags = KNEW UpdateLogTagsEntry(current_lsn, WALLogType::UPDATE, x_id, tbl_typ, time_partition,
                                                      offset, length, old_len, read_buf);
        } catch (exception &e) {
          LOG_ERROR("Failed to malloc memory for construct Entry.")
          return FAIL;
        }
        if (update_tags == nullptr) {
          delete[] read_buf;
          read_buf = nullptr;
          LOG_ERROR("Failed to construct entry.")
          return FAIL;
        }
        log_entries.push_back(update_tags);
      }
      delete[] read_buf;
      read_buf = nullptr;
      break;
    }
    case WALTableType::DATA: {
      break;
    }
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readDeleteLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                    TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  WALTableType table_type;
  char* res;
  uint64_t x_id = 0;
  KStatus status;

  status = readBytes(current_offset, read_queue, sizeof(x_id), res);
  if (status == FAIL) {
    delete[] res;
    res = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  memcpy(&x_id, res, sizeof(x_id));
  delete[] res;
  res = nullptr;

  status = readBytes(current_offset, read_queue, sizeof(table_type), res);
  if (status == FAIL) {
    delete[] res;
    res = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  memcpy(&table_type, res, sizeof(table_type));
  delete[] res;
  res = nullptr;

  switch (table_type) {
    case WALTableType::TAG: {
      status = readBytes(current_offset, read_queue, DeleteLogTagsEntry::header_length,
                         res);
      if (status == FAIL) {
        delete[] res;
        res = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      uint32_t construct_offset = 0;

      uint32_t group_id = 0;
      uint32_t entity_id = 0;
      size_t p_tag_len = 0;
      size_t tag_len = 0;

      memcpy(&group_id, res, sizeof(DeleteLogTagsEntry::group_id_));
      construct_offset += sizeof(DeleteLogTagsEntry::group_id_);
      memcpy(&entity_id, res + construct_offset, sizeof(DeleteLogTagsEntry::entity_id_));
      construct_offset += sizeof(DeleteLogTagsEntry::entity_id_);
      memcpy(&p_tag_len, res + construct_offset, sizeof(DeleteLogTagsEntry::p_tag_len_));
      construct_offset += sizeof(DeleteLogTagsEntry::p_tag_len_);
      memcpy(&tag_len, res + construct_offset, sizeof(DeleteLogTagsEntry::tag_len_));
      delete[] res;
      res = nullptr;

      status = readBytes(current_offset, read_queue, p_tag_len + tag_len, res);
      if (status == FAIL) {
        delete[] res;
        res = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      if (txn_id == 0 || txn_id == x_id) {
        DeleteLogTagsEntry* d_tags_entry = nullptr;
        try {
          d_tags_entry = KNEW DeleteLogTagsEntry(current_lsn, WALLogType::DELETE, x_id, table_type, group_id,
                                                       entity_id, p_tag_len, tag_len, res);
        } catch (exception &e) {
          LOG_ERROR("Failed to malloc memory for construct Entry.")
          return FAIL;
        }
        if (d_tags_entry == nullptr) {
          delete[] res;
          res = nullptr;
          LOG_ERROR("Failed to construct entry.")
          return FAIL;
        }
        log_entries.push_back(d_tags_entry);
      }
      delete[] res;
      res = nullptr;
      break;
    }
    case WALTableType::DATA: {
      status = readBytes(current_offset, read_queue,
                         DeleteLogMetricsEntry::header_length, res);
      if (status == FAIL) {
        delete[] res;
        res = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      int construct_offset = 0;

      size_t p_tag_len = 0;
      KTimestamp start_ts = 0;
      KTimestamp end_ts = 0;
      uint64_t range_size = 0;

      memcpy(&p_tag_len, res + construct_offset, sizeof(DeleteLogMetricsEntry::p_tag_len_));
      construct_offset += sizeof(DeleteLogMetricsEntry::p_tag_len_);
      memcpy(&start_ts, res + construct_offset, sizeof(DeleteLogMetricsEntry::start_ts_));
      construct_offset += sizeof(DeleteLogMetricsEntry::start_ts_);
      memcpy(&end_ts, res + construct_offset, sizeof(DeleteLogMetricsEntry::end_ts_));
      construct_offset += sizeof(DeleteLogMetricsEntry::end_ts_);
      memcpy(&range_size, res + construct_offset, sizeof(DeleteLogMetricsEntry::range_size_));
      delete[] res;
      res = nullptr;

      size_t partition_size = range_size * sizeof(DelRowSpan);
      status = readBytes(current_offset, read_queue, p_tag_len + partition_size, res);
      if (status == FAIL) {
        delete[] res;
        res = nullptr;
        LOG_ERROR("Failed to parse the WAL log.")
        return FAIL;
      }

      if (txn_id == 0 || txn_id == x_id) {
        auto* metrics_entry = KNEW DeleteLogMetricsEntry(current_lsn, WALLogType::DELETE, x_id, table_type, p_tag_len,
                                                         range_size, res);
        if (metrics_entry == nullptr) {
          delete[] res;
          res = nullptr;
          LOG_ERROR("Failed to construct entry.")
          return FAIL;
        }
        log_entries.push_back(metrics_entry);
      }
      delete[] res;
      res = nullptr;
      break;
    }
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readCreateIndexLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                           TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* res;
  KStatus  status;
  uint64_t x_id;
  uint64_t object_id;
  uint32_t index_id;
  uint32_t cur_ts_version;
  uint32_t new_ts_version;
  std::array<int32_t, 10> col_ids{};

  status = readBytes(current_offset, read_queue, CreateIndexEntry::fixed_length, res);
  if (status == FAIL) {
    delete[] res;
    res = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  int construct_offset = 0;

  // uint64_t = (x_id_)
  memcpy(&x_id, res + construct_offset, sizeof(uint64_t));
  construct_offset += sizeof(uint64_t);
  memcpy(&object_id, res + construct_offset, sizeof(CreateIndexEntry::object_id_));
  construct_offset += sizeof(CreateIndexEntry::object_id_);
  memcpy(&index_id, res + construct_offset, sizeof(CreateIndexEntry::index_id_));
  construct_offset += sizeof(CreateIndexEntry::index_id_);
  memcpy(&cur_ts_version, res + construct_offset, sizeof(CreateIndexEntry::cur_ts_version_));
  construct_offset += sizeof(CreateIndexEntry::cur_ts_version_);
  memcpy(&new_ts_version, res + construct_offset, sizeof(CreateIndexEntry::new_ts_version_));
  construct_offset += sizeof(CreateIndexEntry::new_ts_version_);
  memcpy(&col_ids, res + construct_offset, sizeof(CreateIndexEntry::col_ids_));
  delete[] res;
  res = nullptr;

  if (txn_id == 0 || txn_id == x_id) {
    auto* index_entry = KNEW CreateIndexEntry(current_lsn, WALLogType::CREATE_INDEX, x_id, object_id, index_id,
                                              cur_ts_version, new_ts_version, col_ids);
    if (index_entry == nullptr) {
      LOG_ERROR("Failed to construct entry.")
      return FAIL;
    }
    log_entries.push_back(index_entry);
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readDropIndexLog(std::vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                         TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* res;
  KStatus  status;
  uint64_t x_id;
  uint64_t object_id;
  uint32_t index_id;
  uint32_t cur_ts_version;
  uint32_t new_ts_version;
  std::array<int32_t, 10> col_ids{};

  status = readBytes(current_offset, read_queue, DropIndexEntry::fixed_length, res);
  if (status == FAIL) {
    delete[] res;
    res = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  int construct_offset = 0;

  // uint64_t = sizeof(x_id_)
  memcpy(&x_id, res + construct_offset, sizeof(uint64_t));
  construct_offset += sizeof(uint64_t);
  memcpy(&object_id, res + construct_offset, sizeof(DropIndexEntry::object_id_));
  construct_offset += sizeof(DropIndexEntry::object_id_);
  memcpy(&index_id, res + construct_offset, sizeof(DropIndexEntry::index_id_));
  construct_offset += sizeof(DropIndexEntry::index_id_);
  memcpy(&cur_ts_version, res + construct_offset, sizeof(DropIndexEntry::cur_ts_version_));
  construct_offset += sizeof(DropIndexEntry::cur_ts_version_);
  memcpy(&new_ts_version, res + construct_offset, sizeof(DropIndexEntry::new_ts_version_));
  construct_offset += sizeof(DropIndexEntry::new_ts_version_);
  memcpy(&col_ids, res + construct_offset, sizeof(DropIndexEntry::col_ids_));
  delete[] res;
  res = nullptr;

  if (txn_id == 0 || txn_id == x_id) {
    auto* index_entry = KNEW DropIndexEntry(current_lsn, WALLogType::DROP_INDEX, x_id, object_id, index_id,
                                            cur_ts_version, new_ts_version, col_ids);
    if (index_entry == nullptr) {
      LOG_ERROR("Failed to construct entry.")
      return FAIL;
    }
    log_entries.push_back(index_entry);
  }
  return SUCCESS;
}

KStatus WALBufferMgr::readCheckpointLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                        TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf;
  KStatus status;

  status = readBytes(current_offset, read_queue, CheckpointEntry::header_length,
                     read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  CheckpointEntry* checkpoint = nullptr;
  try {
    checkpoint = KNEW CheckpointEntry(current_lsn, WALLogType::CHECKPOINT, read_buf);
  } catch (exception &e) {
    LOG_ERROR("Failed to malloc memory for construct Entry.")
    return FAIL;
  }

  delete[] read_buf;
  read_buf = nullptr;
  if (checkpoint == nullptr) {
    LOG_ERROR("Failed to construct entry.")
    return FAIL;
  }

  size_t partition_size = checkpoint->getPartitionLen();
  if (partition_size == 0) {
    log_entries.push_back(checkpoint);
    return SUCCESS;
  }

  status = readBytes(current_offset, read_queue, partition_size, read_buf);
  if (status == FAIL) {
    delete checkpoint;
    checkpoint = nullptr;
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  checkpoint->setCheckpointPartitions(read_buf);
  delete[] read_buf;
  read_buf = nullptr;

  log_entries.push_back(checkpoint);

  return SUCCESS;
}

KStatus WALBufferMgr::readDDLCreateLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                       TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf;
  KStatus status;

  status = readBytes(current_offset, read_queue, DDLCreateEntry::head_length,
                     read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  DDLCreateEntry* entry = nullptr;
  try {
    entry = KNEW DDLCreateEntry(current_lsn, WALLogType::DDL_CREATE, read_buf);
  } catch (exception &e) {
    LOG_ERROR("Failed to malloc memory for construct Entry.")
    return FAIL;
  }

  delete[] read_buf;
  read_buf = nullptr;
  if (entry == nullptr) {
    LOG_ERROR("Failed to construct entry.")
    return FAIL;
  }

  status = readBytes(current_offset, read_queue, entry->getMetaLength(), read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    delete entry;
    entry = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  bool res = entry->getMeta()->ParseFromArray(read_buf, entry->getMetaLength());
  if (!res) {
    delete entry;
    entry = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  delete[] read_buf;
  read_buf = nullptr;

  status = readBytes(current_offset, read_queue, entry->getRangeGroupLen(), read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    delete entry;
    entry = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }
  entry->setRangeGroups(read_buf);
  delete[] read_buf;
  read_buf = nullptr;

  log_entries.push_back(entry);

  return SUCCESS;
}

KStatus WALBufferMgr::readDDLAlterLog(vector<LogEntry*>& log_entries, TS_LSN current_lsn, TS_LSN txn_id,
                                       TS_LSN& current_offset, std::queue<EntryBlock*>& read_queue) {
  char* read_buf;
  KStatus status;

  status = readBytes(current_offset, read_queue, DDLAlterEntry::head_length,
                     read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  DDLAlterEntry* entry = nullptr;
  try {
    entry = KNEW DDLAlterEntry(current_lsn, WALLogType::DDL_ALTER_COLUMN, read_buf);
  } catch (exception &e) {
    LOG_ERROR("Failed to malloc memory for construct Entry.")
    return FAIL;
  }

  delete[] read_buf;
  read_buf = nullptr;
  if (entry == nullptr) {
    LOG_ERROR("Failed to construct entry.")
    return FAIL;
  }

  status = readBytes(current_offset, read_queue, entry->getLength(), read_buf);
  if (status == FAIL) {
    delete[] read_buf;
    read_buf = nullptr;
    delete entry;
    entry = nullptr;
    LOG_ERROR("Failed to parse the WAL log.")
    return FAIL;
  }

  memcpy(entry->getData(), read_buf, entry->getLength());
  delete[] read_buf;
  read_buf = nullptr;

  log_entries.push_back(entry);

  return SUCCESS;
}

}  // namespace kwdbts
