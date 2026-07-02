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
#include <set>
#include <memory>
#include "cm_func.h"
#include "data_type.h"
#include "libkwdbts2.h"
#include "mmap/mmap_metrics_table.h"
#include "ts_coding.h"
#include "mmap/mmap_tag_column_table.h"
#include "utils/big_table_utils.h"
#include "ts_bitmap.h"

namespace kwdbts {

const uint32_t MAX_PAYLOAD_VERSION = 2;

enum DataTagFlag {
  DATA_AND_TAG = 0,
  DATA_ONLY = 1,
  TAG_ONLY = 2,
};

// used for payload version 2. payload version 1 not has this paramter.
enum TSPayloadRowStructType : uint16_t {
  TS_PAYLOAD_ROW_TYPE_TUPLE = 1,
  TS_PAYLOAD_ROW_TYPE_VECTOR = 2,
  TS_PAYLOAD_ROW_TYPE_BITMAP = 3,
};
static inline bool __isVarType(int type) { return ((type == VARSTRING) || (type == VARBINARY)); }

class TsRawPayloadRowParser {
 protected:
  const std::vector<AttributeInfo>* schema_ = nullptr;
  std::vector<int> col_offset_;
  const std::vector<uint32_t>& actual_col_idx_in_schema_;
  std::vector<uint32_t> col_id_map_valid_list_;

  void FailReport(TSSlice row_data) const;

 public:
  explicit TsRawPayloadRowParser(const std::vector<AttributeInfo>* data_schema, const std::vector<uint32_t>& valid_col = {});
  ~TsRawPayloadRowParser() {}

  bool GetColValueAddr(const TSSlice row_data, int col_id, TSSlice* col_data) const {
    assert(col_id < schema_->size());
    auto actual_idx = col_id;
    if (col_id_map_valid_list_.size() > 0) {
      actual_idx = col_id_map_valid_list_[col_id];
      if (actual_idx == UINT32_MAX) {
        LOG_INFO("column %d is none. we shold not get it.", col_id);
        return false;
      }
    }
    if UNLIKELY (col_id >= schema_->size() || row_data.len <= col_offset_[actual_idx]) {
      FailReport(row_data);
      return false;
    }
    const auto& col_schema = (*schema_)[col_id];
    const auto offset = col_offset_[actual_idx];
    if (!isVarLenType(col_schema.type)) {
      col_data->data = row_data.data + offset;
      col_data->len = col_schema.size;
      auto end_offset = offset + col_schema.size;
      if LIKELY (end_offset <= row_data.len) {
        return true;
      }
    } else {
      size_t actual_offset = KUint64(row_data.data + offset);
      col_data->len = KUint16(row_data.data + actual_offset);
      auto end_offset = actual_offset + col_data->len + kStringLenLen;
      if LIKELY (end_offset <= row_data.len) {
        col_data->data = row_data.data + actual_offset + kStringLenLen;
        return true;
      }
    }

    FailReport(row_data);
    return false;
  }

  // ts used frequently,so optimize it.
  inline timestamp64 GetTimestamp(const TSSlice& row_data) const {
    return KTimestamp(row_data.data + col_offset_[0]);
  }
  DataFlags GetColFlags(TSSlice row, int col_id) const {
    assert(col_id < schema_->size());
    auto actual_idx = col_id;
    if (col_id_map_valid_list_.size() > 0) {
      actual_idx = col_id_map_valid_list_[col_id];
      if (actual_idx == UINT32_MAX) {
        return DataFlags::kNone;
      }
    }
    if (isRowDeleted(row.data, actual_idx + 1)) {
      return DataFlags::kNull;
    }
    return  DataFlags::kValid;
  }
};

class TsRawPayloadSparseRowWithTypeParser {
 private:
  const std::vector<AttributeInfo>* schema_;
  TSSlice tuple_{};
  TSPayloadRowStructType row_type_{TS_PAYLOAD_ROW_TYPE_TUPLE};
  TsRawPayloadRowParser* parser_{nullptr};
  std::vector<uint32_t> actual_col_idx_in_schema_;

 public:
  explicit TsRawPayloadSparseRowWithTypeParser(const std::vector<AttributeInfo>* data_schema) :
    schema_(data_schema) {}

  ~TsRawPayloadSparseRowWithTypeParser() {
    if (parser_) {
      delete parser_;
    }
  }

  bool Init(TSSlice row);

  DataFlags GetColFlags(int col_id) const {
    return parser_->GetColFlags(tuple_, col_id);
  }

  bool GetColValueAddr(int col_id, TSSlice* col_data) const {
    return parser_->GetColValueAddr(tuple_, col_id, col_data);
  }

  // ts used frequently,so optimize it.
  inline timestamp64 GetTimestamp() const {
    return parser_->GetTimestamp(tuple_);
  }
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
  |  txnID   |   hash point  |  payloadVersion  | dbID |  tbID   |    TSVersion   | rowNum | rowType |
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
  const std::shared_ptr<MMapMetricsTable> metric_table_;
  const std::vector<AttributeInfo>* data_schema_;
  bool parse_metric_;
  TsRawPayloadRowParser* row_parser_{nullptr};
  TSSlice payload_{nullptr, 0};
  TSSlice primary_key_;
  TSSlice tag_datas_;
  std::vector<TSSlice> row_data_;
  TSPayloadRowStructType row_type_;
  TSSlice valid_col_info_{nullptr, 0};
  std::vector<uint32_t> valid_col_idx_list_;

 public:
  explicit TsRawPayload(const std::vector<AttributeInfo>* data_schema, bool parse_metric = true);
  explicit TsRawPayload(const std::shared_ptr<MMapMetricsTable>& metric_table);

  ~TsRawPayload() {
    if (row_parser_ != nullptr) {
      delete row_parser_;
    }
  }

  KStatus ParsePayLoadStruct(const TSSlice &raw);

  KStatus ParseBatchDataStruct(const TSSlice &raw);

  const std::shared_ptr<MMapMetricsTable> GetMetricTable() const { return metric_table_; }

  // rangeGroupID --> hashPoint
  uint64_t GetOSN() {
    return KUint64(payload_.data + txn_id_offset_);
  }

  uint32_t GetHashPoint() {
    return KUint16(payload_.data + hash_point_id_offset_);
  }

  inline uint32_t GetRowCount() const {
    return KUint32(payload_.data + row_num_offset_);
  }

  uint8_t GetRowType() {
    return  KUint8(payload_.data + row_type_offset_);
  }

  uint32_t GetPayloadVersion() {
    return KUint32(payload_.data + payload_version_offset_);
  }

  static uint8_t GetRowTypeFromSlice(const TSSlice &raw) { return KUint8(raw.data + row_type_offset_); }

  static uint32_t GetRowCountFromSlice(const TSSlice &raw) { return KUint32(raw.data + row_num_offset_); }

  static TSTableID GetTableIDFromSlice(const TSSlice &raw) { return KUint64(raw.data + table_id_offset_); }

  static TSSlice GetPrimaryKeyFromSlice(const TSSlice &raw) {
    uint16_t ptag_len = KUint16(raw.data + header_size_);
    return  {raw.data + header_size_ + sizeof(ptag_len), ptag_len};
  }

  static uint32_t GetDatabaseIdFromSlice(const TSSlice &raw) { return KUint32(raw.data + db_id_offset_); }

  static uint32_t GetTableVersionFromSlice(const TSSlice &raw) { return KUint32(raw.data + ts_version_offset_); }

  static void SetOSN(const TSSlice &raw, uint64_t osn) { KUint64(raw.data + txn_id_offset_) = osn; }
  static uint64_t GetOSN(const TSSlice &raw) { return KUint64(raw.data + txn_id_offset_); }

  static void SetHashPoint(const TSSlice &raw, uint32_t hash_point) {
    KUint16(raw.data + hash_point_id_offset_) = hash_point;
  }
  static uint32_t GetHashPoint(const TSSlice &raw) {
    return KUint16(raw.data + hash_point_id_offset_);
  }

  static void SetRowType(const TSSlice &raw, DataTagFlag row_type) {
    KUint8(raw.data + row_type_offset_) = row_type;
  }

  TSTableID GetTableID() const { return KUint64(payload_.data + table_id_offset_); }

  uint32_t GetTableVersion() const { return KUint32(payload_.data + ts_version_offset_); }

  inline TSSlice GetPrimaryTag() const {
    return primary_key_;
  }

  TSSlice GetNormalTag(int32_t offset, int32_t len) {
      return TSSlice{tag_datas_.data + offset, static_cast<size_t>(len)};
  }

  TSSlice GetTags() const {
    return tag_datas_;
  }

  static bool ParseValidInfo(TSPayloadRowStructType valid_type, TSSlice valid_info, std::vector<uint32_t>& valid_cols) {
    valid_cols.clear();
    if (valid_type == TS_PAYLOAD_ROW_TYPE_TUPLE) {
      assert(valid_info.len == 0);
      // no need do any thing. using *data_schema_
    } else if (valid_type == TS_PAYLOAD_ROW_TYPE_VECTOR) {
      auto valid_col_num = KUint32(valid_info.data);
      assert (valid_info.len == sizeof(uint32_t) * (valid_col_num + 1));
      for (size_t i = 0; i < valid_col_num; i++) {
        auto col_idx = KUint32(valid_info.data + sizeof(uint32_t) * (i + 1));
        valid_cols.push_back(col_idx);
      }
    } else if (valid_type == TS_PAYLOAD_ROW_TYPE_BITMAP) {
      auto bitmap_len = KUint32(valid_info.data);
      assert((bitmap_len + 7) / 8 + 4 == valid_info.len);
      for (size_t i = 0; i < bitmap_len; i++) {
        if (!isRowDeleted(valid_info.data + 4, i + 1)) {
          valid_cols.push_back(i);
        }
      }
    } else {
      LOG_ERROR("invalid row type %d.", valid_type);
      return false;
    }
    return true;
  }
  const TsRawPayloadRowParser* GetRowParser() {
    return row_parser_;
  }
  std::vector<uint32_t> GetValidColumns() {
    if (GetPayloadVersion() < 2) {
      LOG_WARN("payload version is less than 2. no valid column info.");
      return {};
    }
    if (GetRowType() == DataTagFlag::TAG_ONLY) {
      return {};
    }
    if (row_type_ == TSPayloadRowStructType::TS_PAYLOAD_ROW_TYPE_TUPLE) {
      std::vector<uint32_t> valid_col_idx_in_all;
      if (data_schema_ != nullptr) {
        for (size_t i = 0; i < data_schema_->size(); i++) {
          valid_col_idx_in_all.push_back(i);
        }
      } else {
        LOG_WARN("data schema is null. can not get all valid column list.");
      }
      return valid_col_idx_in_all;
    }
    assert(parse_metric_);
    if (!std::is_sorted(valid_col_idx_list_.begin(), valid_col_idx_list_.end())) {
      LOG_WARN("valid_col_idx_list_ is not sorted, return empty");
      return {};
    }
    return valid_col_idx_list_;
  }

  bool GetColValue(int row, int col_id, TSSlice* col_data) {
    assert(parse_metric_);
    assert(row < row_data_.size());
    return row_parser_->GetColValueAddr(row_data_[row], col_id, col_data);
  }

  DataFlags GetColFlags(int row, int col_id) {
    assert(parse_metric_);
    assert(row < row_data_.size());
    return row_parser_->GetColFlags(row_data_[row], col_id);
  }

  timestamp64 GetTS(int row) {
    assert(parse_metric_);
    assert(row < row_data_.size());
    return row_parser_->GetTimestamp(row_data_[row]);
  }

  TSSlice GetRowData(int row) {
    assert(row < row_data_.size());
    return row_data_[row];
  }
  TSSlice GenRowDataWithValidInfo(int row);

  static void ParseRowType(TSSlice raw, uint32_t pd_version, TSPayloadRowStructType* row_type, TSSlice* row_data) {
    if (pd_version <= 1) {
      *row_type = TS_PAYLOAD_ROW_TYPE_TUPLE;
      *row_data = raw;
      return;
    }
    *row_type = (TSPayloadRowStructType)(KUint16(raw.data));
    *row_data = {raw.data + sizeof(uint16_t), raw.len - sizeof(uint16_t)};
  }

  bool GetData(TSSlice* data, uint32_t* pd_version) {
    *pd_version = GetPayloadVersion();
    if (GetRowType() != TAG_ONLY) {
      auto tmp = GetTags();
      tmp.data += tmp.len;
      assert(tmp.data - payload_.data + 4 < payload_.len);
      tmp.len = *reinterpret_cast<const uint32_t *>(tmp.data);
      tmp.data += sizeof(uint32_t);
      assert(tmp.len <= payload_.len);
      *data = tmp;
      return true;
    } else {
      *data = {nullptr, 0};
      return false;
    }
  }
  TSSlice GetPayload() {
    return payload_;
  }

 private:
  bool parservalidColInfo(TSPayloadRowStructType type, char*& mem);
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
      uint32_t len = DecodeFixed32(ptr_);
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
      res.len = DecodeFixed32(ptr_);
      res.data = ptr_ + sizeof(uint32_t);
      assert(res.data + res.len <= end_);
      return res;
    }
  };

 public:
  explicit TsRawPayloadV2(TSSlice payload) : payload_(payload) {
    assert(payload.len > header_size_);
  }
  uint32_t GetRowCount() const { return DecodeFixed32(payload_.data + row_num_offset_); }
  TSTableID GetTableID() const { return DecodeFixed64(payload_.data + table_id_offset_); }
  uint32_t GetTableVersion() const {
    return DecodeFixed32(payload_.data + ts_version_offset_);
  }
  TSSlice GetPrimaryTag() const {
    TSSlice pkey;
    pkey.len = DecodeFixed16(payload_.data + header_size_);
    pkey.data = payload_.data + header_size_ + sizeof(uint16_t);
    assert(pkey.data - payload_.data + 4 < payload_.len);
    return pkey;
  }
  TSSlice GetTags() const {
    TSSlice ptag = GetPrimaryTag();
    TSSlice tag;
    tag.data = ptag.data + ptag.len + sizeof(uint32_t);
    tag.len = DecodeFixed32(tag.data - sizeof(uint32_t));
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
  std::vector<DataFlags> col_nullbitmap_;

 public:
  explicit TsRawPayloadRowBuilder(const std::vector<AttributeInfo>& data_schema) : schema_(data_schema) {
    col_value_.clear();
    col_value_.resize(schema_.size(), {nullptr, 0});
    col_nullbitmap_.clear();
    col_nullbitmap_.resize(schema_.size(), DataFlags::kNone);
  }
  ~TsRawPayloadRowBuilder() {
    for (size_t i = 0; i < col_value_.size(); i++) {
      if (col_value_[i].data != nullptr) {
        free(col_value_[i].data);
        col_value_[i].data = nullptr;
      }
    }
    col_value_.clear();
  }

  void Reset() {
    for (size_t i = 0; i < col_value_.size(); i++) {
      if (col_value_[i].data != nullptr) {
        free(col_value_[i].data);
        col_value_[i].data = nullptr;
      }
    }
    col_value_.clear();
    col_value_.resize(schema_.size(), {nullptr, 0});
    col_nullbitmap_.clear();
    col_nullbitmap_.resize(schema_.size(), DataFlags::kNone);
  }

  void SetColValue(int col_id, TSSlice mem) {
    if (mem.data == nullptr && mem.len == 0) {
      col_nullbitmap_[col_id] = DataFlags::kNull;
      return;
    }
    if (col_value_[col_id].data == nullptr) {
      col_value_[col_id].data = reinterpret_cast<char*>(malloc(mem.len));
      col_value_[col_id].len = mem.len;
    }
    std::memcpy(const_cast<char*>(col_value_[col_id].data), mem.data, mem.len);
    col_nullbitmap_[col_id] = DataFlags::kValid;
  }

  void SetColNull(int col_id) {
    if (col_value_[col_id].data != nullptr) {
      free(col_value_[col_id].data);
      col_value_[col_id].data = nullptr;
      col_value_[col_id].len = 0;
    }
    col_nullbitmap_[col_id] = DataFlags::kNull;
  }
  void SetColNone(int col_id) {
    if (col_value_[col_id].data != nullptr) {
      free(col_value_[col_id].data);
      col_value_[col_id].data = nullptr;
      col_value_[col_id].len = 0;
    }
    col_nullbitmap_[col_id] = DataFlags::kNone;
  }

  std::set<uint32_t> GetNononeCols() {
    std::set<uint32_t> cols;
    for (size_t i = 0; i < col_nullbitmap_.size(); i++) {
      if (col_nullbitmap_[i] != DataFlags::kNone) {
        cols.insert(i);
      }
    }
    return cols;
  }

  bool Build(TSSlice* row_data, bool need_malloc = true, const std::vector<uint32_t>& nonone_col_idx = {});

  void GetRowInfo(size_t& bitmap_len, size_t& fixed_tuple_len, size_t& var_part_len) {
    getNoNoneRowInfo(schema_, col_value_, bitmap_len, fixed_tuple_len, var_part_len);
  }
  static TSSlice DataAddValidColInfo(TSSlice row_data, TSSlice info, uint32_t pd_version, TSPayloadRowStructType type);
  static void ParseWithValidColInfo(TSSlice row_data, TSSlice& valid_info,
        uint32_t& pd_version, TSPayloadRowStructType& type, TSSlice& actual_data);

 private:
  void getNoNoneRowInfo(const std::vector<AttributeInfo>& schema, std::vector<TSSlice>& values,
    size_t& bitmap_len, size_t& fixed_tuple_len, size_t& var_part_len) {
    bitmap_len = (schema.size() + 7) / 8;
    fixed_tuple_len = 0;
    var_part_len = 0;
    for (size_t i = 0; i < schema.size(); i++) {
      if (isVarLenType(schema[i].type)) {
        fixed_tuple_len += 8;
        size_t cur_col_var_len = 2;
        if (values[i].data != nullptr) {
          cur_col_var_len += values[i].len;
        }
        var_part_len += cur_col_var_len;
      } else {
        fixed_tuple_len += schema[i].size;
      }
    }
  }
};

/**
 * @brief build payload struct data.
 *        Builder function return the memroy ,which need free above.
*/
class TSRowPayloadSparseBuilder {
 private:
  std::vector<TagInfo> tag_schema_;
  std::vector<AttributeInfo> data_schema_;
  // TSSlice primary_tag_{nullptr, 0};
  std::vector<int32_t> data_schema_offset_;
  std::vector<TsPrimaryTagInfo> primary_tags_;
  std::vector<TagInfo> primary_key_info_;
  std::vector<uint32_t> valid_col_idx_;
  int count_{0};
  int primary_offset_;
  int tag_offset_;
  int data_offset_;

  char* tag_value_mem_{nullptr};
  int tag_value_mem_len_{0};
  int tag_value_mem_bitmap_len_{0};

  std::vector<std::unique_ptr<TsRawPayloadRowBuilder>> rows_;
  TSPayloadRowStructType type_;

 public:
  TSRowPayloadSparseBuilder() {}
  bool Init(const std::vector<TagInfo>& tag_schema,
            const std::vector<AttributeInfo>& data_schema,
            int row_num,
            TSPayloadRowStructType type);
  ~TSRowPayloadSparseBuilder() {
    if (tag_value_mem_) {
      free(tag_value_mem_);
    }
  }
  void SetTagMem();
  void Reset();
  std::vector<TagInfo>& GetTagSchema() {
    return tag_schema_;
  }
  std::vector<AttributeInfo>& GetMetricSchema() {
    return data_schema_;
  }

  const char* GetTagAddr();

  bool SetTagValue(int tag_id, char* mem, int length);

  bool SetColumnValue(int row_num, int col_idx, char* mem, size_t length);

  bool SetColumnNull(int row_num, int col_idx);

  bool SetValidCols(const std::vector<uint32_t>& cols);

  bool Build(TSTableID table_id, uint32_t table_version, TSSlice *payload);

 private:
  bool genValidColInfo(TSSlice& all_col_none_bp_mem, std::vector<uint32_t>& nonone_col_idx);
};


}  // namespace kwdbts
