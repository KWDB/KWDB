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

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "ts_block_span_sorted_iterator.h"
#include "ts_bufferbuilder.h"
#include "ts_common.h"
#include "ts_compatibility.h"
#include "ts_table.h"
#include "ts_version.h"

namespace kwdbts {

// +--------+----------+------+---------+-----+-----------------------------+
// | Header | PTag len | PTag | Tag len | Tag | TsBlockSpan Compressed Data |
// +--------+----------+------+---------+-----+-----------------------------+
// |   43   |     2    |  xx  |    4    | xx  |            xxx              |
// +--------+----------+------+---------+-----+-----------------------------+

class TsBatchData {
 private:
  struct __attribute__((packed)) BatchHeader {
    char checksum[16];
    uint16_t hash_point_id;
    uint32_t data_length;
    char reserve[8];
    uint32_t batch_version;
    uint32_t ts_version;
    uint32_t row_num;
    uint8_t row_type;
  };

 public:
  /*  header part
  ____________________________________________________________________________________________________________________
  |    16    |       2       |         4        |        8       |       4       |       4        |   4    |    1    |
  |----------|---------------|------------------|----------------|---------------|----------------|--------|---------|
  | checksum |   hash point  |    data length   |     reserve    |  BatchVersion |    TSVersion   | rowNum | rowType |
  */
  constexpr static uint8_t checksum_offset_ = offsetof(BatchHeader, checksum);  // NOLINT
  constexpr static uint8_t checksum_size_ = sizeof(BatchHeader::checksum);  // NOLINT

  constexpr static uint8_t hash_point_id_offset_ = offsetof(BatchHeader, hash_point_id);  // NOLINT
  constexpr static uint8_t hash_point_id_size_ = sizeof(BatchHeader::hash_point_id);  // NOLINT

  constexpr static uint8_t data_length_offset_ = offsetof(BatchHeader, data_length);  // NOLINT
  constexpr static uint8_t data_length_size_ = sizeof(BatchHeader::data_length);  // NOLINT

  constexpr static uint8_t reserve_offset_ = offsetof(BatchHeader, reserve);  // NOLINT
  constexpr static uint8_t reserve_size_ = sizeof(BatchHeader::reserve);  // NOLINT

  constexpr static uint8_t batch_version_offset_ = offsetof(BatchHeader, batch_version);  // NOLINT
  constexpr static uint8_t batch_version_size_ = sizeof(BatchHeader::batch_version);  // NOLINT

  constexpr static uint8_t ts_version_offset_ = offsetof(BatchHeader, ts_version);  // NOLINT
  constexpr static uint8_t ts_version_size_ = sizeof(BatchHeader::ts_version);  // NOLINT

  constexpr static uint8_t row_num_offset_ = offsetof(BatchHeader, row_num);  // NOLINT
  constexpr static uint8_t row_num_size_ = sizeof(BatchHeader::row_num);  // NOLINT

  constexpr static uint8_t row_type_offset_ = offsetof(BatchHeader, row_type);  // NOLINT
  constexpr static uint8_t row_type_size_ = sizeof(BatchHeader::row_type);  // NOLINT

  constexpr static int header_size_ = row_type_offset_ + row_type_size_;  // NOLINT
  static_assert(header_size_ == 43, "header size is not 43");

  // tag part
  uint32_t p_tag_offset_ = 0;
  uint16_t p_tag_size_ = 0;
  uint32_t tags_data_offset_ = 0;
  uint32_t tags_data_size_ = 0;

  /*  block span part
  ________________________________________________________________________________________________________________________________________________________________________________________
  |    4        |       8       |         8        |       8       |         8        |       8       |         8        |       4       |       4      |               xxx              |
  |-------------|---------------|------------------|---------------|------------------|---------------|------------------|---------------|--------------|--------------------------------|
  | data length |     min ts    |      max ts      |     min osn   |      max osn     |   first osn   |    last osn      |     n_cols    |    n_rows    |  entity segment compressed data|
  */
  uint32_t block_span_data_offset_ = 0;
  uint32_t block_span_data_size_ = 0;

 private:
  struct __attribute__((packed)) BlockSpanHeader {
    uint32_t length;
    timestamp64 min_ts, max_ts;
    uint64_t min_osn, max_osn, first_osn, last_osn;
    uint32_t n_cols, n_rows;
    uint32_t block_version;
  };

 public:
  // block span length + min ts + max ts + n_cols + n_rows + min osn + max osn + first osn + last osn
  const static int block_span_data_header_size_ = sizeof(BlockSpanHeader);  // NOLINT

  constexpr static uint8_t length_offset_in_span_data_ = offsetof(BlockSpanHeader, length);  // NOLINT
  constexpr static uint8_t length_size_in_span_data_ = sizeof(BlockSpanHeader::length);      // NOLINT

  constexpr static uint8_t min_ts_offset_in_span_data_ = offsetof(BlockSpanHeader, min_ts);  // NOLINT
  constexpr static uint8_t min_ts_size_in_span_data_ = sizeof(BlockSpanHeader::min_ts);      // NOLINT

  constexpr static uint8_t max_ts_offset_in_span_data_ = offsetof(BlockSpanHeader, max_ts);  // NOLINT
  constexpr static uint8_t max_ts_size_in_span_data_ = sizeof(BlockSpanHeader::max_ts);      // NOLINT

  constexpr static uint8_t min_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, min_osn);  // NOLINT
  constexpr static uint8_t min_osn_size_in_span_data_ = sizeof(BlockSpanHeader::min_osn);      // NOLINT

  constexpr static uint8_t max_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, max_osn);  // NOLINT
  constexpr static uint8_t max_osn_size_in_span_data_ = sizeof(BlockSpanHeader::max_osn);      // NOLINT

  constexpr static uint8_t first_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, first_osn);  // NOLINT
  constexpr static uint8_t first_osn_size_in_span_data_ = sizeof(BlockSpanHeader::first_osn);      // NOLINT

  constexpr static uint8_t last_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, last_osn);  // NOLINT
  constexpr static uint8_t last_osn_size_in_span_data_ = sizeof(BlockSpanHeader::last_osn);      // NOLINT

  constexpr static uint8_t n_cols_offset_in_span_data_ = offsetof(BlockSpanHeader, n_cols);  // NOLINT
  constexpr static uint8_t n_cols_size_in_span_data_ = sizeof(BlockSpanHeader::n_cols);      // NOLINT

  constexpr static uint8_t n_rows_offset_in_span_data_ = offsetof(BlockSpanHeader, n_rows);  // NOLINT
  constexpr static uint8_t n_rows_size_in_span_data_ = sizeof(BlockSpanHeader::n_rows);      // NOLINT

  constexpr static uint8_t block_version_offset_in_span_data_ = offsetof(BlockSpanHeader, block_version);  // NOLINT
  constexpr static uint8_t block_version_size_in_span_data_ = sizeof(BlockSpanHeader::block_version);      // NOLINT

  TsBufferBuilder data_;

  void SetBatchVersion(uint32_t batch_version) {
    memcpy(data_.data() + batch_version_offset_, &batch_version, batch_version_size_);
  }

 public:
  // explicit TsBatchData(std::string batch_data) : data_(batch_data) {
  //   p_tag_size_ = KUint16(const_cast<char *>(data_.data()) + header_size_);
  //   p_tag_offset_ = header_size_ + sizeof(p_tag_size_);
  //   tags_data_offset_ = p_tag_offset_ + p_tag_size_ + sizeof(tags_data_size_);
  //   tags_data_size_ = KUint32(const_cast<char *>(data_.data()) + tags_data_offset_ - sizeof(tags_data_size_));
  //   assert(tags_data_offset_ + tags_data_size_ <= data_.size());
  //   if (GetRowType() == DataTagFlag::DATA_AND_TAG) {
  //     block_span_data_offset_ = tags_data_offset_ + tags_data_size_;
  //     block_span_data_size_ = data_.size() - block_span_data_offset_;
  //   }
  // }
  TsBatchData() {
    data_.resize(header_size_);
    SetBatchVersion(CURRENT_BATCH_VERSION);
  }
  ~TsBatchData() = default;

  uint32_t GetBatchVersion() const { return *reinterpret_cast<const uint32_t *>(data_.data() + batch_version_offset_); }

  TSSlice GetCheckSum() const {
    return TSSlice{const_cast<char *>(const_cast<char *>(data_.data()) + checksum_offset_), checksum_size_};
  }

  void SetCheckSum(std::string checksum) {
    memcpy(data_.data() + checksum_offset_, checksum.data(), checksum_size_);
  }

  uint16_t GetHashPoint() {
    return KUint16(const_cast<char *>(data_.data()) + hash_point_id_offset_);
  }

  void SetHashPoint(uint16_t hash_point) {
    memcpy(data_.data() + hash_point_id_offset_, &hash_point, hash_point_id_size_);
  }

  uint32_t GetDataLength() const {
    return KUint32(const_cast<char *>(data_.data()) + data_length_offset_);
  }

  void SetDataLength(uint32_t data_length) {
    memcpy(data_.data() + data_length_offset_, &data_length, data_length_size_);
  }

  uint32_t GetRowCount() const {
    return KUint32(const_cast<char *>(data_.data()) + row_num_offset_);
  }

  void SetRowCount(uint32_t row_count) {
    memcpy(data_.data() + row_num_offset_, &row_count, row_num_size_);
  }

  uint8_t GetRowType() {
    return KUint8(const_cast<char *>(data_.data()) + row_type_offset_);
  }

  void SetRowType(uint8_t row_type) {
    memcpy(data_.data() + row_type_offset_, &row_type, row_type_size_);
  }

  uint32_t GetTableVersion() const { return KUint32(const_cast<char*>(data_.data()) + ts_version_offset_); }

  void SetTableVersion(uint32_t table_version) {
    memcpy(data_.data() + ts_version_offset_, &table_version, ts_version_size_);
  }

  TSSlice GetPrimaryTag() const {
    return {const_cast<char *>(data_.data()) + p_tag_offset_, p_tag_size_};
  }

  void AddPrimaryTag(TSSlice ptag) {
    assert(header_size_ != 0);
    p_tag_size_ = ptag.len;
    data_.append(reinterpret_cast<const char *>(&p_tag_size_), sizeof(p_tag_size_));
    p_tag_offset_ = data_.size();
    data_.append(ptag.data, ptag.len);
  }

  TSSlice GetNormalTag(int32_t offset, int32_t len) {
    return TSSlice{const_cast<char *>(data_.data()) + tags_data_offset_ + offset, static_cast<size_t>(len)};
  }

  TSSlice GetTags() const {
    return TSSlice{const_cast<char *>(data_.data()) + tags_data_offset_, tags_data_size_};
  }

  void AddTags(TSSlice tags) {
    assert(p_tag_size_ != 0 && p_tag_offset_ != 0);
    tags_data_size_ = tags.len;
    data_.append(reinterpret_cast<const char*>(&tags_data_size_), sizeof(tags_data_size_));
    tags_data_offset_ = data_.size();
    data_.append(tags.data, tags.len);
  }

  TSSlice GetBlockSpanData() const {
    return TSSlice{const_cast<char*>(data_.data()) + block_span_data_offset_, block_span_data_size_};
  }

  void AddBlockSpanDataHeader(uint32_t block_span_length, timestamp64 min_ts, timestamp64 max_ts,
                              uint64_t min_osn, uint64_t max_osn, uint64_t first_osn, uint64_t last_osn,
                              uint32_t n_cols, uint32_t n_rows, uint32_t block_version) {
    assert(tags_data_size_ != 0 && tags_data_offset_ != 0);
    block_span_data_offset_ = data_.size();
    data_.append(reinterpret_cast<const char *>(&block_span_length), sizeof(uint32_t));
    data_.append(reinterpret_cast<const char *>(&min_ts), sizeof(timestamp64));
    data_.append(reinterpret_cast<const char *>(&max_ts), sizeof(timestamp64));
    data_.append(reinterpret_cast<const char *>(&min_osn), sizeof(uint64_t));
    data_.append(reinterpret_cast<const char *>(&max_osn), sizeof(uint64_t));
    data_.append(reinterpret_cast<const char *>(&first_osn), sizeof(uint64_t));
    data_.append(reinterpret_cast<const char *>(&last_osn), sizeof(uint64_t));
    data_.append(reinterpret_cast<const char *>(&n_cols), sizeof(uint32_t));
    data_.append(reinterpret_cast<const char *>(&n_rows), sizeof(uint32_t));
    data_.append(reinterpret_cast<const char *>(&block_version), sizeof(uint32_t));
    assert(data_.size() - block_span_data_offset_ == block_span_data_header_size_);
  }

  void UpdateBatchDataInfo() {
    // data length
    SetDataLength(data_.size());
    // block span length
    if (block_span_data_offset_ != 0) {
      block_span_data_size_ = data_.size() - block_span_data_offset_;
      memcpy(data_.data() + block_span_data_offset_, &block_span_data_size_, sizeof(block_span_data_size_));
      SetRowType(DataTagFlag::DATA_AND_TAG);
      uint32_t block_span_row_num_offset = block_span_data_offset_ + n_rows_offset_in_span_data_;
      uint32_t row_num = *reinterpret_cast<uint64_t*>(data_.data() + block_span_row_num_offset);
      SetRowCount(row_num);
    } else {
      SetRowType(DataTagFlag::TAG_ONLY);
      SetRowCount(1);
    }
  }

  void Clear() {
    data_.clear();
    data_.resize(header_size_);
    SetBatchVersion(CURRENT_BATCH_VERSION);
  }
};

class TsBatchDataWorker {
 protected:
  uint64_t job_id_;
  bool is_finished_ = false;
  bool is_canceled_ = false;

 public:
  explicit TsBatchDataWorker(uint64_t job_id) : job_id_(job_id) {}
  virtual ~TsBatchDataWorker() {}

  uint64_t GetJobId() const { return job_id_; }

  virtual KStatus Init(kwdbContext_p ctx) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Write(kwdbContext_p ctx, TSTableID table_id, uint32_t table_version, TSSlice* data, uint32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Finish(kwdbContext_p ctx) {
    is_finished_ = true;
    return KStatus::SUCCESS;
  }

  virtual void Cancel(kwdbContext_p ctx) {
    is_canceled_ = true;
  }
};

class TSEngineImpl;
class TsReadBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineImpl* ts_engine_;
  TSTableID table_id_;
  uint64_t table_version_;
  KwTsSpan ts_span_;
  KwTsSpan actual_ts_span_;

  DATATYPE ts_col_type_;
  vector<EntityResultIndex> entity_indexes_;
  std::shared_ptr<TsTable> ts_table_;
  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;
  std::shared_ptr<TsBlockSpanSortedIterator> block_spans_iterator_ = nullptr;
  std::shared_ptr<const TsVGroupVersion> current_ = nullptr;
  EntityResultIndex cur_entity_index_;
  uint32_t n_cols_ = 0;
  TsBatchData cur_batch_data_;

  uint64_t total_read_ = 0;

  KStatus GetTagValue(kwdbContext_p ctx, bool& not_found_tag);

  KStatus AddTsBlockSpanInfo(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span);

  KStatus NextBlockSpansIterator();

  KStatus GenerateBatchData(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan> block_span, bool& not_found_tag);

 public:
  TsReadBatchDataWorker(TSEngineImpl* ts_engine, TSTableID table_id, uint64_t table_version, KwTsSpan ts_span,
                        uint64_t job_id, vector<EntityResultIndex> entity_indexes_);

  KStatus Init(kwdbContext_p ctx) override;

  static std::string GenKey(TSTableID table_id, uint32_t table_version, uint64_t begin_hash,
                            uint64_t end_hash, KwTsSpan ts_span);

  KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) override;

  KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num, bool& not_found_tag);

  KStatus Finish(kwdbContext_p ctx) override;
};

class TsWriteBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineImpl* ts_engine_;

  struct BatchDataHeader {
    TSTableID table_id;
    uint32_t table_version;
    uint32_t vgroup_id;
    TSEntityID entity_id;
    timestamp64 p_time;
    uint64_t data_length;
    uint32_t batch_version = INVALID_BATCH_VERSION;
    uint32_t padding = 0;
  };
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  KLatch w_file_latch_;

  KStatus GetTagPayload(uint32_t table_version, TSSlice* data, std::string& tag_payload_str);

 public:
  TsWriteBatchDataWorker(TSEngineImpl* ts_engine, uint64_t job_id);
  ~TsWriteBatchDataWorker();

  KStatus Init(kwdbContext_p ctx) override;

  KStatus Write(kwdbContext_p ctx, TSTableID table_id, uint32_t table_version, TSSlice* data, uint32_t* row_num) override;
};

}  // namespace kwdbts
