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

#include <map>
#include <memory>
#include <string>
#include <vector>
#include <cstdio>
#include <list>
#include <unordered_map>
#include <utility>
#include "ts_block_span_sorted_iterator.h"
#include "ts_common.h"
#include "ts_const.h"
#include "ts_entity_segment_builder.h"
#include "ts_version.h"

namespace kwdbts {

// +--------+----------+------+---------+-----+-----------------------------+
// | Header | PTag len | PTag | Tag len | Tag | TsBlockSpan Compressed Data |
// +--------+----------+------+---------+-----+-----------------------------+
// |   43   |     2    |  xx  |    4    | xx  |            xxx              |
// +--------+----------+------+---------+-----+-----------------------------+

class TsBatchData {
 public:
  /*  header part
  ____________________________________________________________________________________________________
  |    16    |       2       |         4        |       12       |       4        |   4    |    1    |
  |----------|---------------|------------------|----------------|----------------|--------|---------|
  | checksum |   hash point  |    data length   |     reserve    |    TSVersion   | rowNum | rowType |
  */
  const static uint8_t checksum_offset_ = 0;  // NOLINT
  const static uint8_t checksum_size_ = 16;  // NOLINT

  const static uint8_t hash_point_id_offset_ = 16;  // NOLINT
  const static uint8_t hash_point_id_size_ = 2;  // NOLINT

  const static uint8_t data_length_offset_ = 18;  // NOLINT
  const static uint8_t data_length_size_ = 4;  // NOLINT

  const static uint8_t reserve_offset_ = 22;  // NOLINT
  const static uint8_t reserve_size_ = 12;  // NOLINT

  const static uint8_t ts_version_offset_ = 34;  // NOLINT
  const static uint8_t ts_version_size_ = 4;  // NOLINT

  const static uint8_t row_num_offset_ = 38;  // NOLINT
  const static uint8_t row_num_size_ = 4;  // NOLINT

  const static uint8_t row_type_offset_ = 42;  // NOLINT
  const static uint8_t row_type_size_ = 1;  // NOLINT

  const static int header_size_ = row_type_offset_ + row_type_size_;  // NOLINT

  // tag part
  uint32_t p_tag_offset_ = 0;
  uint16_t p_tag_size_ = 0;
  uint32_t tags_data_offset_ = 0;
  uint32_t tags_data_size_ = 0;

  /*  block span part
  __________________________________________________________________________________________________________________
  |    4        |       8       |         8        |       4       |       4      |               xxx              |
  |-------------|---------------|------------------|---------------|--------------|--------------------------------|
  | data length |     min ts    |      max ts      |     n_cols    |    n_rows    |  entity segment compressed data|
  */
  uint32_t block_span_data_offset_ = 0;
  uint32_t block_span_data_size_ = 0;

  // block span length + min ts + max ts + n_cols + n_rows
  const static int block_span_data_header_size_ = 3 * sizeof(uint32_t) + 2 * sizeof(timestamp64);  // NOLINT

  const static uint8_t length_offset_in_span_data_ = 0;  // NOLINT
  const static uint8_t length_size_in_span_data_ = sizeof(uint32_t);  // NOLINT

  const static uint8_t min_ts_offset_in_span_data_ = length_offset_in_span_data_ + length_size_in_span_data_;  // NOLINT
  const static uint8_t min_ts_size_in_span_data_ = sizeof(timestamp64);  // NOLINT

  const static uint8_t max_ts_offset_in_span_data_ = min_ts_offset_in_span_data_ + min_ts_size_in_span_data_;  // NOLINT
  const static uint8_t max_ts_size_in_span_data_ = sizeof(timestamp64);  // NOLINT

  const static uint8_t n_cols_offset_in_span_data_ = max_ts_offset_in_span_data_ + max_ts_size_in_span_data_;  // NOLINT
  const static uint8_t n_cols_size_in_span_data_ = sizeof(uint32_t);  // NOLINT

  const static uint8_t n_rows_offset_in_span_data_ = n_cols_offset_in_span_data_ + n_cols_size_in_span_data_;  // NOLINT
  const static uint8_t n_rows_size_in_span_data_ = sizeof(uint32_t);  // NOLINT

  std::string data_;

 public:
  explicit TsBatchData(std::string batch_data) : data_(batch_data) {
    p_tag_size_ = KUint16(const_cast<char *>(data_.data()) + header_size_);
    p_tag_offset_ = header_size_ + sizeof(p_tag_size_);
    tags_data_offset_ = p_tag_offset_ + p_tag_size_ + sizeof(tags_data_size_);
    tags_data_size_ = KUint32(const_cast<char *>(data_.data()) + tags_data_offset_ - sizeof(tags_data_size_));
    assert(tags_data_offset_ + tags_data_size_ <= data_.size());
    if (GetRowType() == DataTagFlag::DATA_AND_TAG) {
      block_span_data_offset_ = tags_data_offset_ + tags_data_size_;
      block_span_data_size_ = data_.size() - block_span_data_offset_;
    }
  }
  TsBatchData() {
    data_.resize(header_size_);
  }
  ~TsBatchData() = default;

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
                              uint32_t n_cols, uint32_t n_rows) {
    assert(tags_data_size_ != 0 && tags_data_offset_ != 0);
    block_span_data_offset_ = data_.size();
    data_.append(reinterpret_cast<const char *>(&block_span_length), sizeof(uint32_t));
    data_.append(reinterpret_cast<const char *>(&min_ts), sizeof(timestamp64));
    data_.append(reinterpret_cast<const char *>(&max_ts), sizeof(timestamp64));
    data_.append(reinterpret_cast<const char *>(&n_cols), sizeof(uint32_t));
    data_.append(reinterpret_cast<const char *>(&n_rows), sizeof(uint32_t));
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
      uint32_t block_span_row_num_offset = block_span_data_offset_ + 2 * sizeof(uint32_t) + 2 * sizeof(timestamp64);
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
  }
};

class TsBatchDataWorker {
 private:
  uint64_t job_id_;

 protected:
  bool is_finished_ = false;

 public:
  explicit TsBatchDataWorker(uint64_t job_id) : job_id_(job_id) {}
  virtual ~TsBatchDataWorker() {};

  uint64_t GetJobId() const { return job_id_; }

  virtual KStatus Init(kwdbContext_p ctx) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Write(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) {
    return KStatus::SUCCESS;
  }

  virtual KStatus Finish(kwdbContext_p ctx) {
    is_finished_ = true;
    return KStatus::SUCCESS;
  }

  virtual void Cancel(kwdbContext_p ctx) {
    is_finished_ = true;
  }
};

class TSEngineV2Impl;
class TsReadBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineV2Impl* ts_engine_;
  TSTableID table_id_;
  uint64_t table_version_;
  KwTsSpan ts_span_;
  KwTsSpan actual_ts_span_;

  DATATYPE ts_col_type_;
  vector<EntityResultIndex> entity_indexes_;
  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;
  std::shared_ptr<TsBlockSpanSortedIterator> block_spans_iterator_ = nullptr;

  EntityResultIndex cur_entity_index_;
  uint32_t n_cols_ = 0;
  TsBatchData cur_batch_data_;

  KStatus GetTagValue(kwdbContext_p ctx);

  KStatus AddTsBlockSpanInfo(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span);

  KStatus NextBlockSpansIterator();

 public:
  TsReadBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id, uint64_t table_version, KwTsSpan ts_span,
                        uint64_t job_id, vector<EntityResultIndex> entity_indexes_);

  KStatus Init(kwdbContext_p ctx) override;

  static std::string GenKey(TSTableID table_id, uint32_t table_version, uint64_t begin_hash,
                            uint64_t end_hash, KwTsSpan ts_span);

  KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) override;
};

class TsWriteBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineV2Impl* ts_engine_;
  TSTableID table_id_;
  uint32_t table_version_;

  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;

  std::unordered_map<uint64_t, TS_LSN> vgroups_lsn_;
  std::string tag_payload_;

  std::map<PartitionIdentifier, std::shared_ptr<TsEntitySegmentBuilder>> entity_segment_builders_;

  KStatus GetTagPayload(TSSlice* data, std::shared_ptr<TsRawPayload>& payload_only_tag);

  KStatus UpdateLSN(uint32_t vgroup_id, TSSlice* input, std::string& result);

 public:
  TsWriteBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id, uint32_t table_version, uint64_t job_id);

  KStatus Init(kwdbContext_p ctx) override;

  KStatus Write(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) override;

  KStatus Finish(kwdbContext_p ctx) override;

  void Cancel(kwdbContext_p ctx) override;
};

}  // namespace kwdbts
