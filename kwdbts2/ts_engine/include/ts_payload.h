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

#include <cassert>
#include <cstdint>
#include <vector>
#include <memory>
#include "data_type.h"
#include "libkwdbts2.h"
#include "mmap/mmap_metrics_table.h"
#include "rocksdb/slice.h"
#include "ts_coding.h"
#include "mmap/mmap_tag_column_table.h"
#include "ts_slice.h"
#include "utils/big_table_utils.h"

namespace kwdbts {


class TsRawPayloadRowParser {
 private:
  const std::vector<AttributeInfo>& schema_;
  std::vector<int> col_offset_;

 public:
  explicit TsRawPayloadRowParser(const std::vector<AttributeInfo>& data_schema);
  ~TsRawPayloadRowParser() {}

  bool IsColNull(const TSSlice& row_data, int col_id) {
    return isRowDeleted(row_data.data, col_id + 1);
  }

  bool GetColValueAddr(const TSSlice& row_data, int col_id, TSSlice* col_data);
  // ts used frequently,so optimize it.
  inline timestamp64 GetTimestamp(const TSSlice& row_data) const {
    return KTimestamp(row_data.data + col_offset_[0]);
  }
};

enum DataTagFlag {
  DATA_AND_TAG = 0,
  DATA_ONLY = 1,
  TAG_ONLY = 2,
};

// Very simple row major Payload format, Just for unit test;
// Ref: https://zzqonnd3sc.feishu.cn/wiki/Bw5JwdXh5ibT9qk5mEqcVHYzn1f
// +--------+----------+------+---------+-----+----------+------+
// | Header | PTag len | PTag | Tag len | Tag | data len | data |
// +--------+----------+------+---------+-----+----------+------+
class TsRawPayload {
 public:
  /*  header part
  ____________________________________________________________________________________________________
  |    16    |       2       |         4        |   4  |    8    |       4        |   4    |    1    |
  |----------|---------------|------------------|------|---------|----------------|--------|---------|
  |  txnID   | range groupID |  payloadVersion  | dbID |  tbID   |    TSVersion   | rowNum | rowType |
  */
  const static uint8_t txn_id_offset_ = 0;  // NOLINT
  const static uint8_t txn_id_size_ = 16;  // NOLINT

  const static uint8_t hash_point_id_offset_ = 16;  // NOLINT
  const static uint8_t hash_point_id_size_ = 2;  // NOLINT

  const static uint8_t payload_version_offset_ = 18;  // NOLINT
  const static uint8_t payload_version_size_ = 4;  // NOLINT

  const static uint8_t db_id_offset_ = 22;  // NOLINT
  const static uint8_t db_id_size_ = 4;  // NOLINT

  const static uint8_t table_id_offset_ = 26;  // NOLINT
  const static uint8_t table_id_size_ = 8;  // NOLINT

  const static uint8_t ts_version_offset_ = 34;  // NOLINT
  const static uint8_t ts_version_size_ = 4;  // NOLINT

  const static uint8_t row_num_offset_ = 38;  // NOLINT
  const static uint8_t row_num_size_ = 4;  // NOLINT

  const static uint8_t row_type_offset_ = 42;  // NOLINT
  const static uint8_t row_type_size_ = 1;  // NOLINT

  const static int header_size_ = row_type_offset_ + row_type_size_;  // NOLINT

 private:
  TSSlice payload_;
  const std::vector<AttributeInfo>& metric_schema_;
  TsRawPayloadRowParser row_parser_;
  TSSlice primary_key_;
  TSSlice tag_datas_;
  std::vector<TSSlice> row_data_;
  bool can_parse_ = false;

 public:
  explicit TsRawPayload(const TSSlice &raw, const std::vector<AttributeInfo>& data_schema = std::vector<AttributeInfo>());

  // rangeGroupID --> hashPoint
  uint32_t GetHashPoint() {
    return KUint16(payload_.data + hash_point_id_offset_);
  }

  inline uint32_t GetRowCount() const {
    return KUint32(payload_.data + row_num_offset_);
  }

  uint8_t GetRowType() {
    return  KUint8(payload_.data + row_type_offset_);
  }
  static TSTableID GetTableIDFromSlice(const TSSlice &raw) { return KUint64(raw.data + table_id_offset_); }

  static uint32_t GetTableVersionFromSlice(const TSSlice &raw) { return KUint32(raw.data + ts_version_offset_); }

  TSTableID GetTableID() const { return KUint64(payload_.data + table_id_offset_); }

  uint32_t GetTableVersion() const { return KUint32(payload_.data + ts_version_offset_); }

  inline TSSlice GetPrimaryTag() const {
    return primary_key_;
  }

  TSSlice GetTags() const {
    return tag_datas_;
  }

  bool GetColValue(int row, int col_id, TSSlice* col_data) {
    assert(can_parse_);
    assert(row < row_data_.size());
    return row_parser_.GetColValueAddr(row_data_[row], col_id, col_data);
  }

  timestamp64 GetTS(int row) {
    assert(can_parse_);
    assert(row < row_data_.size());
    return row_parser_.GetTimestamp(row_data_[row]);
  }

  TSSlice GetRowData(int row) {
    assert(row < row_data_.size());
    return row_data_[row];
  }

  TSSlice GetData() const {
    auto tmp = GetTags();
    tmp.data += tmp.len;
    assert(tmp.data - payload_.data + 4 < payload_.len);
    tmp.len = *reinterpret_cast<const uint32_t *>(tmp.data);
    tmp.data += sizeof(uint32_t);
    assert(tmp.len <= payload_.len);
    return tmp;
  }
};

// Parse row major payload as fast as possible
class TsRawPayloadV2 {
 private:
  TSSlice payload_;

  const static uint8_t txn_id_offset_ = 0;  // NOLINT
  const static uint8_t txn_id_size_ = 16;  // NOLINT

  const static uint8_t hash_point_id_offset_ = 16;  // NOLINT
  const static uint8_t hash_point_id_size_ = 2;  // NOLINT

  const static uint8_t payload_version_offset_ = 18;  // NOLINT
  const static uint8_t payload_version_size_ = 4;  // NOLINT

  const static uint8_t db_id_offset_ = 22;  // NOLINT
  const static uint8_t db_id_size_ = 4;  // NOLINT

  const static uint8_t table_id_offset_ = 26;  // NOLINT
  const static uint8_t table_id_size_ = 8;  // NOLINT

  const static uint8_t ts_version_offset_ = 34;  // NOLINT
  const static uint8_t ts_version_size_ = 4;  // NOLINT

  const static uint8_t row_num_offset_ = 38;  // NOLINT
  const static uint8_t row_num_size_ = 4;  // NOLINT

  const static uint8_t row_type_offset_ = 42;  // NOLINT
  const static uint8_t row_type_size_ = 1;  // NOLINT

  const static int header_size_ = row_type_offset_ + row_type_size_;  // NOLINT

  class RowIterator {
   private:
    char *ptr_, *end_;

   public:
    RowIterator(char* ptr, char* end) : ptr_(ptr), end_(end) {}
    bool Valid() const { return ptr_ < end_; }
    void Next() {
      uint32_t len = DecodeType<uint32_t>(ptr_);
      ptr_ += 4;
      ptr_ += len;
      // for DEBUG
      if (!Valid()) {
        assert(ptr_ == end_);
      }
    }
    TSSlice Value() const {
      assert(Valid());
      TSSlice res;
      res.len = DecodeType<uint32_t>(ptr_);
      res.data = ptr_ + sizeof(uint32_t);
      assert(res.data + res.len <= end_);
      return res;
    }
  };

 public:
  explicit TsRawPayloadV2(TSSlice payload) : payload_(payload) {
    assert(payload.len > header_size_);
  }
  uint32_t GetRowCount() const { return DecodeType<uint32_t>(payload_.data + row_num_offset_); }
  TSTableID GetTableID() const { return DecodeType<uint64_t>(payload_.data + table_id_offset_); }
  uint32_t GetTableVersion() const {
    return DecodeType<uint32_t>(payload_.data + ts_version_offset_);
  }
  TSSlice GetPrimaryTag() const {
    TSSlice pkey;
    pkey.len = DecodeType<uint16_t>(payload_.data + header_size_);
    pkey.data = payload_.data + header_size_ + sizeof(uint16_t);
    assert(pkey.data - payload_.data + 4 < payload_.len);
    return pkey;
  }
  TSSlice GetTags() const {
    TSSlice ptag = GetPrimaryTag();
    TSSlice tag;
    tag.data = ptag.data + ptag.len + sizeof(uint32_t);
    tag.len = DecodeType<uint32_t>(tag.data - sizeof(uint32_t));
    assert(tag.len <= payload_.len);
    return tag;
  }

  RowIterator GetRowIterator() const {
    TSSlice tag = GetTags();
    char* mem = tag.data + tag.len + 4;
    RowIterator it{mem, payload_.data + payload_.len};
    return it;
  }
};

typedef struct {
  int offset_;
  int len_;
} TsPrimaryTagInfo;


class TsRawPayloadRowBuilder {
 private:
  const std::vector<AttributeInfo>& schema_;
  std::vector<TSSlice> col_value_;

 public:
  explicit TsRawPayloadRowBuilder(const std::vector<AttributeInfo>& data_schema) : schema_(data_schema) {
    col_value_.clear();
    for (size_t i = 0; i < schema_.size(); i++) {
      col_value_.push_back({nullptr, 0});
    }
  }
  ~TsRawPayloadRowBuilder() {
    for (size_t i = 0; i < col_value_.size(); i++) {
      if (col_value_[i].data != nullptr) {
        free(col_value_[i].data);
      }
    }
    col_value_.clear();
  }

  void SetColValue(int col_id, TSSlice mem) {
    if (mem.len == 0) {
      return;
    }
    if (col_value_[col_id].data == nullptr) {
      col_value_[col_id].data = reinterpret_cast<char*>(malloc(mem.len));
      col_value_[col_id].len = mem.len;
    }
    std::memcpy(const_cast<char*>(col_value_[col_id].data), mem.data, mem.len);
  }

  void SetColNull(int col_id) {
    if (col_value_[col_id].data == nullptr) {
      return;
    }
    free(col_value_[col_id].data);
    col_value_[col_id].data = nullptr;
    col_value_[col_id].len = 0;
  }

  bool Build(TSSlice* row_data);
};


/**
 * @brief build payload struct data.
 *        Builder function return the memroy ,which need free above.
*/
class TSRowPayloadBuilder {
 private:
  std::vector<TagInfo> tag_schema_;
  std::vector<AttributeInfo> data_schema_;
  // TSSlice primary_tag_{nullptr, 0};
  std::vector<int32_t> data_schema_offset_;
  std::vector<TsPrimaryTagInfo> primary_tags_;
  int count_{0};
  int primary_offset_;
  int tag_offset_;
  int data_offset_;

  char* tag_value_mem_{nullptr};
  int tag_value_mem_len_{0};
  int tag_value_mem_bitmap_len_{0};

  std::vector<std::unique_ptr<TsRawPayloadRowBuilder>> rows_;

 public:
  TSRowPayloadBuilder(const std::vector<TagInfo>& tag_schema,
                   const std::vector<AttributeInfo>& data_schema, int row_num);

  ~TSRowPayloadBuilder() {
    if (tag_value_mem_) {
      free(tag_value_mem_);
    }
  }

  const char* GetTagAddr();

  bool SetTagValue(int tag_id, char* mem, int length);

  bool SetColumnValue(int row_num, int col_idx, char* mem, size_t length);

  bool SetColumnNull(int row_num, int col_idx);

  bool Build(TSTableID table_id, uint32_t table_version, TSSlice *payload);
};



}  // namespace kwdbts
