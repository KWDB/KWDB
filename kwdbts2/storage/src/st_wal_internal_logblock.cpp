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

#include "st_wal_internal_logblock.h"

#include "st_wal_types.h"

namespace kwdbts {

EntryBlock::EntryBlock(uint64_t block_no, uint16_t first_rec_grp)
    : block_no_(block_no),
      first_rec_offset_(first_rec_grp) {
  reset(block_no);
}

EntryBlock::EntryBlock(const char* value) {
  decode(value);
}

uint64_t EntryBlock::getBlockNo() const { return block_no_; }

uint16_t EntryBlock::getDataLen() const { return data_len_; }

uint32_t EntryBlock::getCheckSum() const { return checksum_; }

uint32_t ComputeChecksum(const char* data, size_t size) {
  uint32_t sum = 0;
  for (size_t i = 0; i < size; i++) {
    sum += static_cast<uint8_t>(data[i]);
  }
  return sum;
}

void EntryBlock::format() {
  reset(0);
}

void EntryBlock::reset(uint64_t block_no) {
  block_no_ = block_no;
  data_len_ = 0;
  first_rec_offset_ = 0;
  checksum_ = 0;
  memset(body_, 0, LOG_BLOCK_MAX_LOG_SIZE);
}

char* EntryBlock::encode() {
  char* value = new char[BLOCK_SIZE];
  if (value == nullptr) {
    LOG_ERROR("Failed to malloc memory.")
    return nullptr;
  }
  size_t offset = 0;
  std::memcpy(value, &block_no_, sizeof(block_no_));
  offset += sizeof(block_no_);

  std::memcpy(value + offset, &data_len_, sizeof(data_len_));
  offset += sizeof(data_len_);

  std::memcpy(value + offset, &first_rec_offset_,
              sizeof(first_rec_offset_));
  offset += sizeof(first_rec_offset_);

  std::memcpy(value + offset, body_, data_len_);
  offset += data_len_;

  std::memset(value + offset, 0, BLOCK_SIZE - offset - sizeof(checksum_));
  checksum_ = 0;
  // checksum_ = ComputeChecksum(value, BLOCK_SIZE - sizeof(checksum_));
  std::memcpy(value + BLOCK_SIZE - sizeof(checksum_), &checksum_,
              sizeof(checksum_));

  return value;
}

void EntryBlock::decode(const char* value) {
  size_t offset = 0;
  std::memcpy(&block_no_, value, sizeof(block_no_));
  offset += sizeof(block_no_);

  std::memcpy(&data_len_, value + offset, sizeof(data_len_));
  offset += sizeof(data_len_);

  std::memcpy(&first_rec_offset_, value + offset,
              sizeof(first_rec_offset_));
  offset += sizeof(first_rec_offset_);

  std::memcpy(body_, value + offset, LOG_BLOCK_MAX_LOG_SIZE);

  std::memcpy(&checksum_, value + (BLOCK_SIZE - sizeof(checksum_)),
              sizeof(checksum_));
}

size_t EntryBlock::writeBytes(char* log, size_t length, bool is_rest) {
  if (data_len_ + length <= LOG_BLOCK_MAX_LOG_SIZE) {
    if (data_len_ == 0 && is_rest) {
      first_rec_offset_ = length;
    }
    memcpy(body_ + data_len_, log, length);
    data_len_ += length;
    return length;

  } else {
    uint32_t left_size = LOG_BLOCK_MAX_LOG_SIZE - data_len_;
    uint32_t write_length = left_size;
    memcpy(body_ + data_len_, log, write_length);
    if (data_len_ == 0 && is_rest) {
      first_rec_offset_ = write_length;
    }
    data_len_ += write_length;
    return write_length;
  }
}

KStatus EntryBlock::readBytes(size_t& offset, char*& res, size_t length,
                              size_t& read_size) {
  if (length == 0) {
    return FAIL;
  }
  if (length > MAX_PER_READ_LSN_RANGES) {
    LOG_ERROR("too large bytes for readBytes func, length:%ld", length)
    return FAIL;
  }
  res = new char[length];
  if (res == nullptr) {
    LOG_ERROR("Failed to malloc memory, length:%ld", length)
    return FAIL;
  }

  read_size = 0;

  if (offset + (length - read_size) <= LOG_BLOCK_MAX_LOG_SIZE) {
    memcpy(res + read_size, body_ + offset, length - read_size);
    offset += length - read_size;
    read_size += length;
    return SUCCESS;
  } else {
    size_t left_size = LOG_BLOCK_MAX_LOG_SIZE - offset;
    memcpy(res + read_size, body_ + offset, left_size);
    read_size += left_size;
    offset += left_size;
    return SUCCESS;
  }
}

uint16_t EntryBlock::getFirstRecOffset() const {
  return first_rec_offset_;
}

HeaderBlock::HeaderBlock() {}

HeaderBlock::HeaderBlock(uint64_t object_id, uint64_t start_block_no, uint32_t block_num,
                         TS_OSN start_lsn, TS_OSN first_lsn, TS_OSN checkpoint_lsn, uint32_t checkpoint_no)
    : object_id_(object_id), start_block_no_(start_block_no), block_num_(block_num),
    start_lsn_(start_lsn), first_lsn_(first_lsn), checkpoint_lsn_(checkpoint_lsn), checkpoint_no_(checkpoint_no) {
  flushed_lsn_ = start_lsn;
}

HeaderBlock::HeaderBlock(const char* value) {
  decode(value);
}

void HeaderBlock::setCheckpointInfo(TS_OSN checkpoint_lsn,
                                    uint32_t checkpoint_no) {
  checkpoint_lsn_ = checkpoint_lsn;
  checkpoint_no_ = checkpoint_no;
}

void HeaderBlock::setFirstLSN(TS_OSN first_lsn) {
  first_lsn_ = first_lsn;
}

char* HeaderBlock::encode() {
  char* value = new char[BLOCK_SIZE];
  if (value == nullptr) {
    LOG_ERROR("Failed to malloc memory.")
    return nullptr;
  }
  size_t offset = 0;
  std::memcpy(value, &format, sizeof(format));
  offset += sizeof(format);

  std::memcpy(value + offset, &object_id_, sizeof(object_id_));
  offset += sizeof(object_id_);

  std::memcpy(value + offset, &start_block_no_, sizeof(start_block_no_));
  offset += sizeof(start_block_no_);

  std::memcpy(value + offset, &block_num_, sizeof(block_num_));
  offset += sizeof(block_num_);

  std::memcpy(value + offset, &start_lsn_, sizeof(start_lsn_));
  offset += sizeof(start_lsn_);

  std::memcpy(value + offset, &first_lsn_, sizeof(first_lsn_));
  offset += sizeof(first_lsn_);

  std::memcpy(value + offset, &checkpoint_lsn_,
              sizeof(checkpoint_lsn_));
  offset += sizeof(checkpoint_lsn_);

  std::memcpy(value + offset, &flushed_lsn_,
              sizeof(flushed_lsn_));
  offset += sizeof(flushed_lsn_);

  std::memcpy(value + offset, &checkpoint_no_,
              sizeof(checkpoint_no_));
  offset += sizeof(checkpoint_no_);

  std::memset(value + offset, 0, BLOCK_SIZE - offset);
  uint32_t checksum = 0;
  // uint32_t checksum = ComputeChecksum(value, BLOCK_SIZE - sizeof(checksum_));
  std::memcpy(value + BLOCK_SIZE - sizeof(checksum_), &checksum, sizeof(checksum));
  return value;
}

void HeaderBlock::decode(const char* value) {
  size_t offset = 0;
  std::memcpy(&format, value, sizeof(format));
  offset += sizeof(format);

  std::memcpy(&object_id_, value + offset, sizeof(object_id_));
  offset += sizeof(object_id_);

  std::memcpy(&start_block_no_, value + offset, sizeof(start_block_no_));
  offset += sizeof(start_block_no_);

  std::memcpy(&block_num_, value + offset, sizeof(block_num_));
  offset += sizeof(block_num_);

  std::memcpy(&start_lsn_, value + offset, sizeof(start_lsn_));
  offset += sizeof(start_lsn_);

  std::memcpy(&first_lsn_, value + offset, sizeof(first_lsn_));
  offset += sizeof(first_lsn_);

  std::memcpy(&checkpoint_lsn_, value + offset,
              sizeof(checkpoint_lsn_));
  offset += sizeof(checkpoint_lsn_);

  std::memcpy(&flushed_lsn_, value + offset,
              sizeof(flushed_lsn_));
  offset += sizeof(flushed_lsn_);

  std::memcpy(&checkpoint_no_, value + offset,
              sizeof(checkpoint_no_));

  std::memcpy(&checksum_, value + BLOCK_SIZE - sizeof(checksum_), sizeof(checksum_));
}


TS_OSN HeaderBlock::getStartLSN() const {
  return start_lsn_;
}

TS_OSN HeaderBlock::getCheckpointLSN() const {
  return checkpoint_lsn_;
}

uint32_t HeaderBlock::getCheckpointNo() const {
  return checkpoint_no_;
}

TS_OSN HeaderBlock::getFlushedLsn() const {
  return flushed_lsn_;
}

void HeaderBlock::setFlushedLsn(TS_OSN flushed_lsn) {
  flushed_lsn_ = flushed_lsn;
}

uint64_t HeaderBlock::getStartBlockNo() const {
  return start_block_no_;
}

uint32_t HeaderBlock::getBlockNum() const {
  return block_num_;
}

TS_OSN  HeaderBlock::getFirstLSN() const {
  return first_lsn_;
}

uint64_t HeaderBlock::getEndBlockNo() const {
  return start_block_no_ + block_num_ - 1;
}

}  // namespace kwdbts
