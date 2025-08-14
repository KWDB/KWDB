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

#include "lg_api.h"
#include "st_wal_types.h"

namespace kwdbts {
class EntryBlock {
 public:
  EntryBlock() = default;

  EntryBlock(uint64_t block_no, uint16_t first_rec_grp);

  explicit EntryBlock(const char* value);

  ~EntryBlock() = default;

  [[nodiscard]] uint64_t getBlockNo() const;

  [[nodiscard]] uint16_t getDataLen() const;

  [[nodiscard]] uint32_t getCheckSum() const;

  [[nodiscard]] uint16_t getFirstRecOffset() const;

  [[nodiscard]] inline bool isFull() const {
    return data_len_ == LOG_BLOCK_MAX_LOG_SIZE;
  }

  EntryBlock& operator=(const EntryBlock& block) {
    if (this == &block) {
      return *this;
    }

    this->block_no_ = block.block_no_;
    this->data_len_ = block.data_len_;
    this->first_rec_offset_ = block.first_rec_offset_;
    this->checksum_ = block.checksum_;

    std::memcpy(body_, block.body_, LOG_BLOCK_MAX_LOG_SIZE);
    return *this;
  }

  void format();

  void reset(uint64_t block_no);

  void setDataLen(uint16_t data_len) { data_len_ = data_len; }

  char* encode();

  size_t writeBytes(char* log, size_t length, bool is_rest);

  KStatus readBytes(size_t& offset, char*& res, size_t length, size_t& read_size);

 private:
  void decode(const char* value);

  uint64_t block_no_{0};
  uint16_t data_len_{0};
  uint16_t first_rec_offset_{0};
  uint32_t checksum_{0};

  char body_[LOG_BLOCK_MAX_LOG_SIZE]{0};
};

class HeaderBlock {
 public:
  HeaderBlock();

  HeaderBlock(uint64_t object_id, uint64_t start_block_no, uint32_t block_num,
              TS_LSN start_lsn, TS_LSN first_lsn, TS_LSN checkpoint_lsn, uint32_t checkpoint_no);

  explicit HeaderBlock(const char* value);

  ~HeaderBlock() = default;

  /**
   * Serialize the current HeaderBlock class to a char array for the WAL file writing.
   * @return
   */
  char* encode();

  /**
   * Update the checkpoint info of the WAL log file.
   * @param checkpoint_lsn
   * @param checkpoint_no
   */
  void setCheckpointInfo(TS_LSN checkpoint_lsn, uint32_t checkpoint_no);

  void setFirstLSN(TS_LSN first_lsn);

  [[nodiscard]] TS_LSN getStartLSN() const;

  [[nodiscard]] TS_LSN getCheckpointLSN() const;

  [[nodiscard]] uint32_t getCheckpointNo() const;

  [[nodiscard]] TS_LSN getFlushedLsn() const;

  [[nodiscard]] uint64_t getStartBlockNo() const;

  [[nodiscard]] uint64_t getEndBlockNo() const;

  [[nodiscard]] uint32_t getBlockNum() const;

  [[nodiscard]] TS_LSN getFirstLSN() const;

  void setFlushedLsn(TS_LSN flushed_lsn);

 private:
  /**
   * The HeaderBlock if constructed from a char array for the WAL file reading.
   * @param value
   */
  void decode(const char* value);

  uint8_t format{1};
  uint64_t object_id_{0};
  uint64_t start_block_no_{0};
  uint32_t block_num_{0};
  TS_LSN start_lsn_{0};
  TS_LSN first_lsn_{0};
  TS_LSN checkpoint_lsn_{0};
  TS_LSN flushed_lsn_{0};
  uint32_t checkpoint_no_{0};
  uint32_t checksum_{0};
};
}  // namespace kwdbts
