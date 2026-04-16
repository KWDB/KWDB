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
#include <atomic>
#include <cstring>
#include <memory>
#include <string>
#include <type_traits>
#include <vector>

#include "ts_block_span_sorted_iterator.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compatibility.h"
#include "ts_table.h"
#include "ts_version.h"

namespace kwdbts {

// TsBatchData binary layout.
//
// Protocol notes:
// - All 16/32/64-bit integer fields in BatchHeader, p_tag_size, tags_data_size and
//   BlockSpanHeader are encoded in little-endian via EncodeFixed**/DecodeFixed**.
// - row_type is stored as a raw single byte.
// - checksum, primary-tag bytes, tag payload bytes and compressed block-span payload
//   are copied as-is.
//
// +--------+----------+------+---------+-----+-----------------------------+
// | Header | PTag len | PTag | Tag len | Tag | TsBlockSpan Compressed Data |
// +--------+----------+------+---------+-----+-----------------------------+
// |   43   |     2    |  xx  |    4    | xx  |            xxx              |
// +--------+----------+------+---------+-----+-----------------------------+

class TsBatchData {
 private:
  template <typename T>
  using FixedUnsignedType = std::conditional_t<sizeof(T) == sizeof(uint8_t), uint8_t,
                            std::conditional_t<sizeof(T) == sizeof(uint16_t), uint16_t,
                            std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>>>;

  template <typename T>
  [[nodiscard]]
  static T DecodeLittleEndian(const char* ptr) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    static_assert(sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint16_t) || sizeof(T) == sizeof(uint32_t) ||
                  sizeof(T) == sizeof(uint64_t), "unsupported fixed-width field size");
    if constexpr (sizeof(T) == sizeof(uint8_t)) {
      return static_cast<T>(static_cast<uint8_t>(*ptr));
    } else if constexpr (std::is_same_v<T, timestamp64>) {
      return DecodeFixedTimestamp64(ptr);
    } else {
      using RawType = FixedUnsignedType<T>;
      RawType raw{};
      if constexpr (sizeof(T) == sizeof(uint16_t)) {
        raw = DecodeFixed16(ptr);
      } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
        raw = DecodeFixed32(ptr);
      } else {
        raw = DecodeFixed64(ptr);
      }
      T value{};
      memcpy(&value, &raw, sizeof(T));
      return value;
    }
  }

  template <typename T>
  static void EncodeLittleEndian(char* ptr, const T& value) {
    static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
    static_assert(sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint16_t) || sizeof(T) == sizeof(uint32_t) ||
                  sizeof(T) == sizeof(uint64_t), "unsupported fixed-width field size");
    if constexpr (sizeof(T) == sizeof(uint8_t)) {
      *ptr = static_cast<char>(value);
    } else if constexpr (std::is_same_v<T, timestamp64>) {
      EncodeFixedTimestamp64(ptr, value);
    } else {
      using RawType = FixedUnsignedType<T>;
      RawType raw{};
      memcpy(&raw, &value, sizeof(T));
      if constexpr (sizeof(T) == sizeof(uint16_t)) {
        EncodeFixed16(ptr, raw);
      } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
        EncodeFixed32(ptr, raw);
      } else {
        EncodeFixed64(ptr, raw);
      }
    }
  }

  template <typename T>
  [[nodiscard]]
  T LoadAt(size_t offset) const {
    return DecodeLittleEndian<T>(data_.data() + offset);
  }

  template <typename T>
  void StoreAt(size_t offset, const T& value) {
    EncodeLittleEndian(data_.data() + offset, value);
  }

  template <typename T>
  void AppendFixedValue(const T& value) {
    char buf[sizeof(T)];
    EncodeLittleEndian(buf, value);
    data_.append(buf, sizeof(T));
  }

  struct __attribute__((packed)) BatchHeader {
    char checksum[16];
    uint16_t hash_point_id;
    uint32_t data_length;
    TS_OSN tag_osn;
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
  | checksum |   hash point  |    data length   |     tag_osn    |  BatchVersion |    TSVersion   | rowNum | rowType |
  */
  constexpr static uint8_t checksum_offset_ = offsetof(BatchHeader, checksum);  // NOLINT
  constexpr static uint8_t checksum_size_ = sizeof(BatchHeader::checksum);  // NOLINT

  constexpr static uint8_t hash_point_id_offset_ = offsetof(BatchHeader, hash_point_id);  // NOLINT
  constexpr static uint8_t data_length_offset_ = offsetof(BatchHeader, data_length);  // NOLINT
  constexpr static uint8_t tag_osn_offset_ = offsetof(BatchHeader, tag_osn);  // NOLINT
  constexpr static uint8_t batch_version_offset_ = offsetof(BatchHeader, batch_version);  // NOLINT
  constexpr static uint8_t ts_version_offset_ = offsetof(BatchHeader, ts_version);  // NOLINT
  constexpr static uint8_t row_num_offset_ = offsetof(BatchHeader, row_num);  // NOLINT
  constexpr static uint8_t row_type_offset_ = offsetof(BatchHeader, row_type);  // NOLINT

  constexpr static int header_size_ = row_type_offset_ + sizeof(BatchHeader::row_type);  // NOLINT
  static_assert(header_size_ == 43, "header size is not 43");

  // tag part
  uint32_t p_tag_offset_ = 0;
  uint16_t p_tag_size_ = 0;
  uint32_t tags_data_offset_ = 0;
  uint32_t tags_data_size_ = 0;

  /*  block span part
  _______________________________________________________________________________________________________________________________________________________________________________________________________________
  |    4        |       8       |         8        |       8       |         8        |       8       |         8        |       4       |       4      |       4       |               xxx              |
  |-------------|---------------|------------------|---------------|------------------|---------------|------------------|---------------|--------------|---------------|--------------------------------|
  | data length |     min ts    |      max ts      |     min osn   |      max osn     |   first osn   |    last osn      |     n_cols    |    n_rows    | block_version |  entity segment compressed data|
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
  constexpr static int block_span_data_header_size_ = sizeof(BlockSpanHeader);  // NOLINT
  constexpr static uint8_t length_offset_in_span_data_ = offsetof(BlockSpanHeader, length);  // NOLINT
  constexpr static uint8_t min_ts_offset_in_span_data_ = offsetof(BlockSpanHeader, min_ts);  // NOLINT
  constexpr static uint8_t max_ts_offset_in_span_data_ = offsetof(BlockSpanHeader, max_ts);  // NOLINT
  constexpr static uint8_t min_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, min_osn);  // NOLINT
  constexpr static uint8_t max_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, max_osn);  // NOLINT
  constexpr static uint8_t first_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, first_osn);  // NOLINT
  constexpr static uint8_t last_osn_offset_in_span_data_ = offsetof(BlockSpanHeader, last_osn);  // NOLINT
  constexpr static uint8_t n_cols_offset_in_span_data_ = offsetof(BlockSpanHeader, n_cols);  // NOLINT
  constexpr static uint8_t n_rows_offset_in_span_data_ = offsetof(BlockSpanHeader, n_rows);  // NOLINT
  constexpr static uint8_t block_version_offset_in_span_data_ = offsetof(BlockSpanHeader, block_version);  // NOLINT

  TsBufferBuilder data_;

  void SetBatchVersion(uint32_t batch_version) {
    StoreAt(batch_version_offset_, batch_version);
  }

  void SetTagOSN(TS_OSN osn) {
    StoreAt(tag_osn_offset_, osn);
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

  [[nodiscard]] uint32_t GetBatchVersion() const { return LoadAt<uint32_t>(batch_version_offset_); }

  [[nodiscard]] TS_OSN GetTagOSN() const { return LoadAt<TS_OSN>(tag_osn_offset_); }

  [[nodiscard]]
  TSSlice GetCheckSum() const {
    return TSSlice{const_cast<char *>(data_.data()) + checksum_offset_, checksum_size_};
  }

  void SetCheckSum(const std::string& checksum) {
    memcpy(data_.data() + checksum_offset_, checksum.data(), checksum_size_);
  }

  [[nodiscard]] uint16_t GetHashPoint() const {
    return LoadAt<uint16_t>(hash_point_id_offset_);
  }

  void SetHashPoint(uint16_t hash_point) {
    StoreAt(hash_point_id_offset_, hash_point);
  }

  [[nodiscard]] uint32_t GetDataLength() const {
    return LoadAt<uint32_t>(data_length_offset_);
  }

  void SetDataLength(uint32_t data_length) {
    StoreAt(data_length_offset_, data_length);
  }

  [[nodiscard]] uint32_t GetRowCount() const {
    return LoadAt<uint32_t>(row_num_offset_);
  }

  void SetRowCount(uint32_t row_count) {
    StoreAt(row_num_offset_, row_count);
  }

  [[nodiscard]] uint8_t GetRowType() const {
    return LoadAt<uint8_t>(row_type_offset_);
  }

  void SetRowType(uint8_t row_type) {
    StoreAt(row_type_offset_, row_type);
  }

  [[nodiscard]] uint32_t GetTableVersion() const { return LoadAt<uint32_t>(ts_version_offset_); }

  void SetTableVersion(uint32_t table_version) {
    StoreAt(ts_version_offset_, table_version);
  }

  [[nodiscard]]
  TSSlice GetPrimaryTag() const {
    return {const_cast<char *>(data_.data()) + p_tag_offset_, p_tag_size_};
  }

  void AddPrimaryTag(TSSlice ptag) {
    assert(header_size_ != 0);
    assert(ptag.len <= UINT16_MAX);
    p_tag_size_ = ptag.len;
    AppendFixedValue(p_tag_size_);
    p_tag_offset_ = data_.size();
    data_.append(ptag.data, ptag.len);
  }

  TSSlice GetNormalTag(int32_t offset, int32_t len) {
    return TSSlice{const_cast<char *>(data_.data()) + tags_data_offset_ + offset, static_cast<size_t>(len)};
  }

  [[nodiscard]]
  TSSlice GetTags() const {
    return TSSlice{const_cast<char *>(data_.data()) + tags_data_offset_, tags_data_size_};
  }

  void AddTags(TSSlice tags) {
    assert(p_tag_size_ != 0 && p_tag_offset_ != 0);
    assert(tags.len <= UINT32_MAX);
    tags_data_size_ = tags.len;
    AppendFixedValue(tags_data_size_);
    tags_data_offset_ = data_.size();
    data_.append(tags.data, tags.len);
  }

  [[nodiscard]]
  TSSlice GetBlockSpanData() const {
    return TSSlice{const_cast<char*>(data_.data()) + block_span_data_offset_, block_span_data_size_};
  }

  [[nodiscard]] bool HasBlockSpanData() const {
    return block_span_data_offset_ != 0;
  }

  void AddBlockSpanDataHeader(uint32_t block_span_length, timestamp64 min_ts, timestamp64 max_ts,
                              uint64_t min_osn, uint64_t max_osn, uint64_t first_osn, uint64_t last_osn,
                              uint32_t n_cols, uint32_t n_rows, uint32_t block_version) {
    assert(tags_data_size_ != 0 && tags_data_offset_ != 0);
    block_span_data_offset_ = data_.size();
    AppendFixedValue(block_span_length);
    AppendFixedValue(min_ts);
    AppendFixedValue(max_ts);
    AppendFixedValue(min_osn);
    AppendFixedValue(max_osn);
    AppendFixedValue(first_osn);
    AppendFixedValue(last_osn);
    AppendFixedValue(n_cols);
    AppendFixedValue(n_rows);
    AppendFixedValue(block_version);
    assert(data_.size() - block_span_data_offset_ == block_span_data_header_size_);
  }

  void UpdateBatchDataInfo() {
    // data length
    SetDataLength(data_.size());
    // block span length
    if (HasBlockSpanData()) {
      assert(data_.size() > tags_data_offset_ + tags_data_size_);
      block_span_data_size_ = data_.size() - block_span_data_offset_;
      StoreAt(block_span_data_offset_, block_span_data_size_);
      SetRowType(DataTagFlag::DATA_AND_TAG);
      size_t block_span_row_num_offset = block_span_data_offset_ + n_rows_offset_in_span_data_;
      auto row_num = LoadAt<uint32_t>(block_span_row_num_offset);
      SetRowCount(row_num);
    } else {
      SetRowType(DataTagFlag::TAG_ONLY);
      SetRowCount(1);
    }
  }

  void Clear() {
    p_tag_offset_ = 0;
    p_tag_size_ = 0;
    tags_data_offset_ = 0;
    tags_data_size_ = 0;
    block_span_data_offset_ = 0;
    block_span_data_size_ = 0;
    data_.clear();
    data_.resize(header_size_);
    SetBatchVersion(CURRENT_BATCH_VERSION);
  }
};

class TsBatchDataWorker {
 protected:
  uint64_t job_id_;
  std::atomic<bool> is_finished_{false};
  std::atomic<bool> is_canceled_{false};

 public:
  explicit TsBatchDataWorker(uint64_t job_id) : job_id_(job_id) {}
  virtual ~TsBatchDataWorker() = default;

  [[nodiscard]] uint64_t GetJobId() const { return job_id_; }

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
    is_finished_.store(true, std::memory_order_release);
    return KStatus::SUCCESS;
  }

  virtual void Cancel(kwdbContext_p ctx) {
    is_canceled_.store(true, std::memory_order_release);
  }
};

class TSEngineImpl;
class TsReadBatchDataWorker : public TsBatchDataWorker {
 public:
#ifdef WITH_TESTS
  struct TagCacheStats {
    uint32_t lookup_count = 0;
    uint32_t fill_count = 0;
    uint32_t hit_count = 0;
  };
#endif

 private:
#ifdef WITH_TESTS
  class TagCacheStatsHelper {
   public:
    void OnLookup() { ++stats_.lookup_count; }
    void OnFill() { ++stats_.fill_count; }
    void OnHit() { ++stats_.hit_count; }
    [[nodiscard]] TagCacheStats GetStatsForTest() const { return stats_; }

   private:
    TagCacheStats stats_{};
  };
#else
  class TagCacheStatsHelper {
   public:
    void OnLookup() {}
    void OnFill() {}
    void OnHit() {}
  };
#endif

  TSEngineImpl* ts_engine_;
  TSTableID table_id_;
  uint64_t table_version_;
  KwTsSpan ts_span_;
  KwTsSpan actual_ts_span_;

  DATATYPE ts_col_type_ = DATATYPE::INVALID;
  vector<EntityResultIndex> entity_indexes_;
  std::vector<TagInfo> tags_info_;
  std::vector<uint32_t> scan_tags_;
  std::shared_ptr<TsTable> ts_table_;
  std::shared_ptr<TsTableSchemaManager> schema_ = nullptr;
  std::shared_ptr<TsBlockSpanSortedIterator> block_spans_iterator_ = nullptr;
  std::shared_ptr<const TsVGroupVersion> current_ = nullptr;
  EntityResultIndex cur_entity_index_;
  uint32_t n_cols_ = 0;
  TsBatchData cur_batch_data_;
  std::string cached_tags_data_;
  bool is_tags_data_cached_ = false;
  TagCacheStatsHelper tag_cache_stats_helper_;

  uint64_t total_read_ = 0;

  KStatus InitTagSchema();

  void ResetTagValueCache();

  KStatus GetTagValue(kwdbContext_p ctx);

  void AddTsBlockSpanInfo(const std::shared_ptr<TsBlockSpan>& block_span);

  KStatus NextBlockSpansIterator();

  KStatus GenerateBatchData(kwdbContext_p ctx, const std::shared_ptr<TsBlockSpan>& block_span);

 public:
  TsReadBatchDataWorker(TSEngineImpl* ts_engine, TSTableID table_id, uint64_t table_version, KwTsSpan ts_span,
                        uint64_t job_id, vector<EntityResultIndex> entity_indexes_);

  KStatus Init(kwdbContext_p ctx) override;

  static std::string GenKey(TSTableID table_id, uint32_t table_version, uint64_t begin_hash,
                            uint64_t end_hash, KwTsSpan ts_span);

  KStatus Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) override;

  KStatus ReadOnce(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num);

#ifdef WITH_TESTS
  [[nodiscard]] TagCacheStats GetTagCacheStatsForTest() const { return tag_cache_stats_helper_.GetStatsForTest(); }
#endif

  KStatus Finish(kwdbContext_p ctx) override;
};

class TsWriteBatchDataWorker : public TsBatchDataWorker {
 private:
  TSEngineImpl* ts_engine_;
  TsDataSource source_;

  struct BatchDataHeader {
    TSTableID table_id = 0;
    uint32_t table_version = 0;
    uint32_t vgroup_id = 0;
    TSEntityID entity_id = 0;
    timestamp64 p_time = 0;
    uint64_t data_length = 0;
    uint32_t batch_version = INVALID_BATCH_VERSION;
    uint32_t padding = 0;
  };
  std::unique_ptr<TsAppendOnlyFile> w_file_;
  KLatch w_file_latch_;

  static void GetTagPayload(uint32_t table_version, const TSSlice& data, std::string& tag_payload_str);

 public:
  TsWriteBatchDataWorker(TSEngineImpl* ts_engine, uint64_t job_id, TsDataSource source);
  ~TsWriteBatchDataWorker() override;

  KStatus Init(kwdbContext_p ctx) override;

  KStatus Write(kwdbContext_p ctx, TSTableID table_id, uint32_t table_version, TSSlice* data, uint32_t* row_num) override;
};

}  // namespace kwdbts
