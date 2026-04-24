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

#include <atomic>
#include <chrono>
#include <cstring>
#include <string>
#include <thread>

#include "ts_batch_data_worker.h"
#include "ee_tag_row_batch.h"
#include "libkwdbts2.h"
#include "ts_engine.h"

namespace kwdbts {

namespace {

template <typename T>
using FixedUnsignedType = std::conditional_t<sizeof(T) == sizeof(uint8_t), uint8_t,
                          std::conditional_t<sizeof(T) == sizeof(uint16_t), uint16_t,
                          std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>>>;

template <typename T>
T DecodeBatchField(const char* data) {
  static_assert(std::is_trivially_copyable<T>::value, "T must be trivially copyable");
  static_assert(sizeof(T) == sizeof(uint8_t) || sizeof(T) == sizeof(uint16_t) || sizeof(T) == sizeof(uint32_t) ||
                sizeof(T) == sizeof(uint64_t), "unsupported fixed-width field size");
  if constexpr (sizeof(T) == sizeof(uint8_t)) {
    return static_cast<T>(static_cast<uint8_t>(*data));
  } else if constexpr (std::is_same_v<T, timestamp64>) {
    return DecodeFixedTimestamp64(data);
  } else {
    using RawType = FixedUnsignedType<T>;
    RawType raw{};
    if constexpr (sizeof(T) == sizeof(uint16_t)) {
      raw = DecodeFixed16(data);
    } else if constexpr (sizeof(T) == sizeof(uint32_t)) {
      raw = DecodeFixed32(data);
    } else {
      raw = DecodeFixed64(data);
    }
    T value{};
    memcpy(&value, &raw, sizeof(T));
    return value;
  }
}

bool HasEnoughBytes(size_t total_len, size_t offset, size_t need) {
  return offset <= total_len && need <= total_len - offset;
}

struct ParsedBatchDataLayout {
  uint32_t batch_version = INVALID_BATCH_VERSION;
  uint16_t p_tag_size = 0;
  size_t p_tag_offset = 0;
  size_t tags_data_offset = 0;
  uint32_t tags_data_size = 0;
  uint8_t row_type = DataTagFlag::TAG_ONLY;
  size_t block_span_data_offset = 0;
  uint32_t block_span_data_size = 0;
};

KStatus ParseBatchDataLayout(const TSSlice& data, TSTableID table_id, ParsedBatchDataLayout& layout) {
  auto log_layout_fail = [&](const char* reason) {
    LOG_ERROR("ParseBatchDataLayout failed, table_id[%lu], reason[%s], len=%lu, batch_version=%u, "
              "p_tag_size=%u, p_tag_offset=%lu, tags_data_offset=%lu, tags_data_size=%u, row_type=%u, "
              "block_span_data_offset=%lu, "
              "block_span_data_size=%u",
              table_id, reason, data.len, layout.batch_version,
              layout.p_tag_size, layout.p_tag_offset,
              layout.tags_data_offset, layout.tags_data_size, layout.row_type,
              layout.block_span_data_offset, layout.block_span_data_size);
  };

  if (data.data == nullptr) {
    log_layout_fail("data pointer is null");
    return KStatus::FAIL;
  }
  if (data.len < TsBatchData::header_size_) {
    LOG_ERROR("ParseBatchDataLayout failed, table_id[%lu], reason[data len too small], len=%lu, "
              "header_size=%d",
              table_id, data.len, TsBatchData::header_size_);
    return KStatus::FAIL;
  }

  const auto data_length = DecodeBatchField<uint32_t>(data.data + TsBatchData::data_length_offset_);
  if (data_length != data.len) {
    LOG_ERROR("ParseBatchDataLayout failed, table_id[%lu], reason[data length mismatch], "
              "declared_len=%u, actual_len=%lu",
              table_id, data_length, data.len);
    return KStatus::FAIL;
  }

  layout.batch_version = DecodeBatchField<uint32_t>(data.data + TsBatchData::batch_version_offset_);
  if (!HasEnoughBytes(data.len, TsBatchData::header_size_, sizeof(uint16_t))) {
    log_layout_fail("missing primary tag length field");
    return KStatus::FAIL;
  }
  layout.p_tag_size = DecodeBatchField<uint16_t>(data.data + TsBatchData::header_size_);
  layout.p_tag_offset = TsBatchData::header_size_ + sizeof(layout.p_tag_size);
  if (!HasEnoughBytes(data.len, layout.p_tag_offset, layout.p_tag_size + sizeof(uint32_t))) {
    log_layout_fail("primary tag section exceeds batch boundary");
    return KStatus::FAIL;
  }

  layout.tags_data_offset = layout.p_tag_offset + layout.p_tag_size + sizeof(uint32_t);
  layout.tags_data_size = DecodeBatchField<uint32_t>(data.data + layout.tags_data_offset - sizeof(uint32_t));
  layout.row_type = DecodeBatchField<uint8_t>(data.data + TsBatchData::row_type_offset_);
  layout.block_span_data_offset = layout.tags_data_offset + layout.tags_data_size;
  if (layout.block_span_data_offset > data.len) {
    log_layout_fail("tags payload exceeds batch boundary");
    return KStatus::FAIL;
  }

  if (layout.block_span_data_offset == data.len) {
    if (layout.row_type != DataTagFlag::TAG_ONLY) {
      log_layout_fail("row_type expects block span payload but batch ends after tags");
      return KStatus::FAIL;
    }
    return KStatus::SUCCESS;
  }
  if (layout.row_type != DataTagFlag::DATA_AND_TAG) {
    log_layout_fail("block span payload exists but row_type is not DATA_AND_TAG");
    return KStatus::FAIL;
  }
  if (!HasEnoughBytes(data.len, layout.block_span_data_offset, sizeof(uint32_t))) {
    log_layout_fail("missing block span length field");
    return KStatus::FAIL;
  }

  layout.block_span_data_size = DecodeBatchField<uint32_t>(data.data + layout.block_span_data_offset);
  if (layout.block_span_data_size < TsBatchData::block_span_data_header_size_) {
    LOG_ERROR("ParseBatchDataLayout failed, table_id[%lu], reason[block span payload too short], "
              "payload_len=%u, min_header_len=%d, "
              "offset=%lu, total_len=%lu",
              table_id,
              layout.block_span_data_size, TsBatchData::block_span_data_header_size_,
              layout.block_span_data_offset, data.len);
    return KStatus::FAIL;
  }
  if (!HasEnoughBytes(data.len, layout.block_span_data_offset, layout.block_span_data_size)) {
    log_layout_fail("block span payload exceeds batch boundary");
    return KStatus::FAIL;
  }
  if (layout.block_span_data_offset + layout.block_span_data_size != data.len) {
    log_layout_fail("batch has trailing bytes after block span payload");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

}  // namespace

enum class WriteBatchStatus : uint8_t {
  None = 0,
  Writing,
};
std::atomic<WriteBatchStatus> write_batch_status{WriteBatchStatus::None};

bool TrySetWriteBusy() {
  WriteBatchStatus expected = WriteBatchStatus::None;
  return write_batch_status.compare_exchange_strong(expected, WriteBatchStatus::Writing);
}

void ResetWriteStatus() {
  write_batch_status.store(WriteBatchStatus::None);
}

TsReadBatchDataWorker::TsReadBatchDataWorker(TSEngineImpl* ts_engine, TSTableID table_id,
                                             uint64_t table_version, KwTsSpan ts_span, uint64_t job_id,
                                             vector<EntityResultIndex> entity_indexes)
                                             : TsBatchDataWorker(job_id), ts_engine_(ts_engine), table_id_(table_id),
                                               table_version_(table_version), ts_span_(ts_span), actual_ts_span_(ts_span),
                                               entity_indexes_(std::move(entity_indexes)) {}

KStatus TsReadBatchDataWorker::InitTagSchema() {
  tags_info_.clear();
  scan_tags_.clear();
  KStatus s = schema_->GetTagMeta(table_version_, tags_info_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagMeta failed, table_id[%lu], table_version[%lu]", table_id_, table_version_);
    return KStatus::FAIL;
  }
  scan_tags_.reserve(tags_info_.size());
  for (size_t i = 0; i < tags_info_.size(); ++i) {
    scan_tags_.push_back(static_cast<uint32_t>(i));
  }
  return KStatus::SUCCESS;
}

void TsReadBatchDataWorker::ResetTagValueCache() {
  cached_tags_data_.clear();
  is_tags_data_cached_ = false;
}

KStatus TsReadBatchDataWorker::GetTagValue(kwdbContext_p ctx) {
  if (is_tags_data_cached_) {
    tag_cache_stats_helper_.OnHit();
    cur_batch_data_.AddTags({cached_tags_data_.data(), cached_tags_data_.size()});
    return KStatus::SUCCESS;
  }

  auto* ts_table_v2 = dynamic_cast<TsTableImpl*>(ts_table_.get());
  if (ts_table_v2 == nullptr) {
    LOG_ERROR("unexpected ts table implementation, table_id[%lu]", table_id_);
    return KStatus::FAIL;
  }

  // init tag iterator
  ResultSet res(scan_tags_.size());
  uint32_t count = 0;
  tag_cache_stats_helper_.OnLookup();
  KStatus s = ts_table_v2->GetTagListByRowNum(ctx, {cur_entity_index_}, scan_tags_, UINT64_MAX, &res, &count,
                                              table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagIterator failed");
    return KStatus::FAIL;
  }
  if (count != 1) {
    LOG_ERROR("cannot find this tag. count=%d", count);
    return KStatus::FAIL;
  }

  // tag data
  uint32_t tag_value_bitmap_len_ = (tags_info_.size() + 7) / 8;  // bitmap
  uint32_t tag_value_len_ = tag_value_bitmap_len_;
  for (const auto& tag : tags_info_) {
    if (isVarLenType(tag.m_data_type)) {
      // not allocate space now. Then insert tag value, resize this tmp space.
      if (tag.m_tag_type == PRIMARY_TAG) {
        // primary tag all store in tuple.
        tag_value_len_ += tag.m_length;
      } else {
        tag_value_len_ += sizeof(uint64_t);
      }
    } else {
      tag_value_len_ += tag.m_size;
    }
  }
  cached_tags_data_.clear();
  cached_tags_data_.resize(tag_value_len_);
  assert(res.col_num_ == tags_info_.size());
  for (int tag_idx = 0; tag_idx < res.col_num_; ++tag_idx) {
    auto& tag_info = tags_info_[tag_idx];
    const Batch* col_batch = res.data[tag_idx][0];
    const bool is_primary_tag = tag_info.m_tag_type == PRIMARY_TAG;
    if (is_primary_tag) {
      unset_null_bitmap(reinterpret_cast<unsigned char *>(cached_tags_data_.data()), tag_idx);
      memcpy(cached_tags_data_.data() + tag_value_bitmap_len_ + tag_info.m_offset,
             reinterpret_cast<char*>(col_batch->mem), tag_info.m_size);
      continue;
    }

    bool is_null = false;
    s = col_batch->isNull(0, &is_null);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("tag col value isNull failed");
      return s;
    }
    if (is_null) {
      set_null_bitmap(reinterpret_cast<unsigned char *>(cached_tags_data_.data()), tag_idx);
      continue;
    }

    unset_null_bitmap(reinterpret_cast<unsigned char *>(cached_tags_data_.data()), tag_idx);
    const bool is_var_len_tag = tag_info.m_data_type == VARSTRING || tag_info.m_data_type == VARBINARY;
    if (is_var_len_tag) {
      const uint64_t offset = cached_tags_data_.size();
      char* tag_value_addr = cached_tags_data_.data() + tag_value_bitmap_len_ + tag_info.m_offset;
      memcpy(tag_value_addr, &offset, sizeof(uint64_t));
      uint16_t var_data_len = col_batch->getDataLen(0);
      cached_tags_data_.append(reinterpret_cast<const char *>(&var_data_len), sizeof(uint16_t));
      cached_tags_data_.append(col_batch->getData(0) + sizeof(uint16_t), var_data_len);
    } else {
      memcpy(cached_tags_data_.data() + tag_value_bitmap_len_ + tag_info.m_offset,
             reinterpret_cast<char*>(col_batch->mem) + 1, tag_info.m_size);
    }
  }

  is_tags_data_cached_ = true;
  tag_cache_stats_helper_.OnFill();
  cur_batch_data_.AddTags({cached_tags_data_.data(), cached_tags_data_.size()});
  return KStatus::SUCCESS;
}

void TsReadBatchDataWorker::AddTsBlockSpanInfo(const std::shared_ptr<TsBlockSpan>& block_span) {
  timestamp64 first_row_ts = block_span->GetTS(0);
  uint64_t min_osn = UINT64_MAX;
  uint64_t max_osn = 0;
  block_span->GetMinAndMaxOSN(min_osn, max_osn);
  uint64_t first_osn = block_span->GetFirstOSN();
  uint64_t last_osn = block_span->GetLastOSN();
  timestamp64 end_row_ts = block_span->GetTS(block_span->GetRowNum() - 1);
  uint32_t n_rows = block_span->GetRowNum();
  cur_batch_data_.AddBlockSpanDataHeader(0, first_row_ts, end_row_ts, min_osn, max_osn,
                                         first_osn, last_osn, n_cols_, n_rows, block_span->GetBlockVersion());
}

KStatus TsReadBatchDataWorker::NextBlockSpansIterator() {
  if (entity_indexes_.empty()) {
    ResetTagValueCache();
    is_finished_.store(true, std::memory_order_release);
    return KStatus::SUCCESS;
  }
  // init iterator
  ResetTagValueCache();
  cur_entity_index_ = entity_indexes_[entity_indexes_.size() - 1];
  entity_indexes_.pop_back();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  KStatus s = ts_engine_->GetTsVGroup(cur_entity_index_.subGroupId)->GetBlockSpans(table_id_, cur_entity_index_.entityId,
                                                                                   actual_ts_span_, ts_col_type_, schema_,
                                                                                   table_version_, current_, &block_spans);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsReadBatchDataWorker::Init failed, failed to get block span, "
              "table_id[%lu], table_version[%lu], entity_id[%u]",
              table_id_, table_version_, cur_entity_index_.entityId);
    return s;
  }
  // filter block span
  auto it = block_spans.begin();
  while (it != block_spans.end()) {
    if (it->get()->GetTableVersion() > table_version_) {
      it = block_spans.erase(it);
    } else {
      ++it;
    }
  }
  block_spans_iterator_ = nullptr;
  if (!block_spans.empty()) {
    // init block span iterator
    block_spans_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(block_spans,
                                                                        ts_engine_->GetEngineSchemaManager().get(),
                                                                        EngineOptions::g_dedup_rule);
    s = block_spans_iterator_->Init();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsReadBatchDataWorker::Init failed, failed to init block span iterator, "
                "table_id[%lu], table_version[%lu], entity_id[%u]",
                table_id_, table_version_, cur_entity_index_.entityId);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::Init(kwdbContext_p ctx) {
  bool is_dropped = false;
  KStatus s = ts_engine_->GetTsTable(ctx, table_id_, ts_table_, is_dropped, true, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTsTable[%lu] failed", table_id_);
    return KStatus::FAIL;
  }
  schema_ = ts_table_->GetSchemaManager();
  ResetTagValueCache();
  s = InitTagSchema();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  // update ts span with lifetime
  auto life_time = schema_->GetLifeTime();
  if (life_time.ts != 0) {
    int64_t acceptable_ts = INT64_MIN;
    auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    acceptable_ts = now.time_since_epoch().count() - life_time.ts;
    if (acceptable_ts > actual_ts_span_.begin) {
      actual_ts_span_.begin = acceptable_ts;
    }
  }
  // convert second to actual timestamp
  std::shared_ptr<MMapMetricsTable> metric_schema;
  s = schema_->GetMetricSchema(table_version_, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get metric schema, table id[%lu], version[%lu]", table_id_, table_version_);
    return KStatus::FAIL;
  }
  const vector<AttributeInfo>& attrs = *metric_schema->getSchemaInfoExcludeDroppedPtr();
  assert(!attrs.empty());
  n_cols_ = attrs.size() + 1;  // add osn
  ts_col_type_ = static_cast<DATATYPE>(attrs[0].type);
  actual_ts_span_.begin = convertSecondToPrecisionTS(actual_ts_span_.begin, ts_col_type_);
  actual_ts_span_.end = convertSecondToPrecisionTS(actual_ts_span_.end, ts_col_type_);
  if (actual_ts_span_.begin > actual_ts_span_.end) {
    is_finished_.store(true, std::memory_order_release);
    return KStatus::SUCCESS;
  }
  s = NextBlockSpansIterator();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("get next TsBlockSpansIterator failed");
  }
  return s;
}

std::string TsReadBatchDataWorker::GenKey(TSTableID table_id, uint32_t table_version, uint64_t begin_hash,
                                          uint64_t end_hash, KwTsSpan ts_span) {
  char buffer[128];
  memset(buffer, 0, sizeof(buffer));
  std::snprintf(buffer, sizeof(buffer), "%lu-%d-%lu-%lu-%ld-%ld", table_id, table_version,
                begin_hash, end_hash, ts_span.begin, ts_span.end);
  return buffer;
}

KStatus TsReadBatchDataWorker::GenerateBatchData(kwdbContext_p ctx,
  const std::shared_ptr<TsBlockSpan>& block_span) {
  cur_batch_data_.Clear();
  auto op_osn = reinterpret_cast<OperatorInfoOfRecord*>(cur_entity_index_.op_with_osn.get());
  assert(op_osn != nullptr);
  cur_batch_data_.SetTagOSN(op_osn->osn);
  // hash point
  cur_batch_data_.SetHashPoint(cur_entity_index_.hash_point);
  // ts version
  cur_batch_data_.SetTableVersion(table_version_);
  // ptag
  uint32_t ptags_size = cur_entity_index_.p_tags_size;
  cur_batch_data_.AddPrimaryTag({reinterpret_cast<char*>(cur_entity_index_.mem.get()), ptags_size});
  // tag value
  KStatus s = GetTagValue(ctx);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("add tag failed");
    return s;
  }
  // add TsBlockSpan info
  if (block_span) {
    AddTsBlockSpanInfo(block_span);
    // get compress data
    s = block_span->GetCompressData(&cur_batch_data_.data_);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetCompressData failed");
      return s;
    }
  }
  // set ts block span data length
  cur_batch_data_.UpdateBatchDataInfo();
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::Read(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) {
  do {
    KStatus s = ReadOnce(ctx, data, row_num);
    if (s != KStatus::SUCCESS) {
      return s;
    }
  } while (*row_num == 0 && !is_finished_.load(std::memory_order_acquire));
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::ReadOnce(kwdbContext_p ctx, TSSlice* data, uint32_t* row_num) {
  *row_num = 0;
  data->data = nullptr;
  data->len = 0;
  if (is_finished_.load(std::memory_order_acquire) || is_canceled_.load(std::memory_order_acquire)) {
    return KStatus::SUCCESS;
  }
  // get next ts block span
  std::shared_ptr<TsBlockSpan> cur_block_span;
  bool block_span_read_finished = false;
  bool cur_entity_tag_only = false;
  while (!is_finished_.load(std::memory_order_acquire)) {
    if (block_spans_iterator_ == nullptr) {
      cur_entity_tag_only = true;
      // generate batch data
      KStatus s = GenerateBatchData(ctx, nullptr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GenerateBatchData failed");
        return s;
      }
      *row_num = 1;
      total_read_ += *row_num;
      *data = cur_batch_data_.data_.AsSlice();
      block_span_read_finished = true;
    } else {
      KStatus s = block_spans_iterator_->Next(cur_block_span, &block_span_read_finished);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("get next block span failed");
        return s;
      }
    }
    if (block_span_read_finished) {
      KStatus s = NextBlockSpansIterator();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NextBlockSpansIterator failed");
        return s;
      }
      if (cur_entity_tag_only) {
        return s;
      }
    } else {
      break;
    }
  }
  if (is_finished_.load(std::memory_order_acquire)) {
    return KStatus::SUCCESS;
  }
  // generate batch data
  KStatus s = GenerateBatchData(ctx, cur_block_span);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GenerateBatchData failed");
    return s;
  }
  // set data
  *row_num = cur_block_span->GetRowNum();
  total_read_ += *row_num;
  *data = cur_batch_data_.data_.AsSlice();
  LOG_DEBUG("current batch data read success, job_id[%lu], table_id[%lu], table_version[%lu], block_version[%u] row_num[%u]",
           job_id_, table_id_, table_version_, cur_block_span->GetTableVersion(), *row_num);
  return s;
}

KStatus TsReadBatchDataWorker::Finish(kwdbContext_p ctx) {
  is_finished_.store(true, std::memory_order_release);
  LOG_INFO("Read batch data finished, job_id[%lu], table_id[%lu], table_version[%lu], ts_span[%lu, %lu], total read: %lu",
           job_id_, table_id_, table_version_, ts_span_.begin, ts_span_.end, total_read_);
  return KStatus::SUCCESS;
}

TsWriteBatchDataWorker::TsWriteBatchDataWorker(TSEngineImpl* ts_engine, uint64_t job_id, TsDataSource source)
                                               : TsBatchDataWorker(job_id), ts_engine_(ts_engine), source_(source),
                                                 w_file_latch_(LATCH_ID_TAG_TABLE_VERSION_MUTEX) {}

TsWriteBatchDataWorker::~TsWriteBatchDataWorker() {
  if (!is_finished_.load(std::memory_order_acquire) || is_canceled_.load(std::memory_order_acquire) || w_file_ == nullptr) {
    return;
  }

  KStatus s = KStatus::SUCCESS;
  while (!TrySetWriteBusy()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }

  Defer defer([&]() {
    if (s != KStatus::SUCCESS) {
      auto vgroups = ts_engine_->GetTsVGroups();
      for (const auto& vgroup : *vgroups) {
        vgroup->CancelWriteBatchData();
      }
    }
    ResetWriteStatus();
  });

  // write batch data to entity segment
  {
    BatchDataHeader header{};
    const size_t batch_header_size = sizeof(BatchDataHeader);
    std::unique_ptr<TsSequentialReadFile> r_file;

    w_file_->Sync();
    TsIOEnv *env = &TsIOEnv::GetInstance();
    s = env->NewSequentialReadFile(w_file_->GetFilePath(), &r_file, w_file_->GetFileSize());
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("NewSequentialReadFile failed, job_id[%lu]", job_id_);
      return;
    }

    uint64_t left = 0;
    const uint64_t file_size = w_file_->GetFileSize();
    while (left < file_size) {
      if (file_size - left < batch_header_size) {
        s = KStatus::FAIL;
        LOG_ERROR("Invalid batch header length, job_id[%lu], left[%lu], file_size[%lu]", job_id_, left, file_size);
        return;
      }
      TsSliceGuard batch_header;
      s = r_file->Read(batch_header_size, &batch_header);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Read batch header failed, job_id[%lu]", job_id_);
        return;
      }
      memcpy(&header, batch_header.data(), sizeof(header));
      if (header.data_length > file_size - left - batch_header_size) {
        s = KStatus::FAIL;
        LOG_ERROR("Invalid batch data length, job_id[%lu], data_length[%lu], left[%lu], file_size[%lu]",
                  job_id_, header.data_length, left, file_size);
        return;
      }
      TsSliceGuard block_data;
      s = r_file->Read(header.data_length, &block_data);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("Read batch data failed, job_id[%lu]", job_id_);
        return;
      }
      TSSlice data = block_data.AsSlice();
      s = ts_engine_->GetTsVGroup(header.vgroup_id)
              ->WriteBatchData(header.table_id, header.table_version, header.entity_id, header.p_time,
                               header.batch_version, data, source_);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("WriteBatchData failed, table_id[%lu], entity_id[%lu]", header.table_id, header.entity_id);
        return;
      }
      left += batch_header_size + header.data_length;
    }
  }
  // write batch finish
  {
    auto vgroups = ts_engine_->GetTsVGroups();
    for (const auto& vgroup : *vgroups) {
      s = vgroup->FinishWriteBatchData();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("FinishWriteBatchData failed, job_id[%lu]", job_id_);
        return;
      }
    }
  }
}

std::atomic<int64_t> w_file_no{0};

KStatus TsWriteBatchDataWorker::Init(kwdbContext_p ctx) {
  TsIOEnv* env = &TsIOEnv::GetInstance();
  std::string tmp_path = ts_engine_->GetDbDir() + "/temp_db_/";
  if (access(tmp_path.c_str(), 0)) {
    fs::create_directories(tmp_path);
  }
  std::string file_path = tmp_path + std::to_string(job_id_)
                          + "." + std::to_string(w_file_no.fetch_add(1, std::memory_order_relaxed)) + ".data";
  if (env->NewAppendOnlyFile(file_path, &w_file_, true, -1) != KStatus::SUCCESS) {
    LOG_ERROR("TsWriteBatchDataWorker::Init NewAppendOnlyFile failed, file_path=%s", file_path.c_str())
    return KStatus::FAIL;
  }
  w_file_->MarkDelete();
  return KStatus::SUCCESS;
}

void TsWriteBatchDataWorker::GetTagPayload(uint32_t table_version, const TSSlice& data, std::string& tag_payload_str) {
  tag_payload_str.clear();
  tag_payload_str.append(data.data, data.len);
  auto tag_osn = DecodeBatchField<TS_OSN>(data.data + TsBatchData::tag_osn_offset_);
  memcpy(tag_payload_str.data() + TsRawPayload::txn_id_offset_, &tag_osn, sizeof(tag_osn));
  // update table version
  memcpy(tag_payload_str.data() + TsRawPayload::ts_version_offset_, &table_version, sizeof(table_version));
  // update tag row num
  uint32_t row_num = 1;
  memcpy(tag_payload_str.data() + TsRawPayload::row_num_offset_, &row_num, sizeof(row_num));
  // update tag type
  uint8_t tag_type = DataTagFlag::TAG_ONLY;
  memcpy(tag_payload_str.data() + TsRawPayload::row_type_offset_, &tag_type, sizeof(tag_type));
}

KStatus TsWriteBatchDataWorker::Write(kwdbContext_p ctx, TSTableID table_id, uint32_t table_version,
                                      TSSlice* data, uint32_t* row_num) {
  if (data == nullptr || row_num == nullptr || data->data == nullptr) {
    LOG_ERROR("invalid input for write batch data, job_id[%lu], table_id[%lu]", job_id_, table_id);
    return KStatus::FAIL;
  }

  ParsedBatchDataLayout layout;
  KStatus s = ParseBatchDataLayout(*data, table_id, layout);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("invalid batch data layout, job_id[%lu], table_id[%lu], len=%lu", job_id_, table_id, data->len);
    return KStatus::FAIL;
  }

  // parse batch version first;
  uint32_t batch_version = layout.batch_version;
  if (batch_version > CURRENT_BATCH_VERSION) {
    LOG_ERROR("batch version is too large, version=%u", batch_version);
    return KStatus::FAIL;
  }

  // get or create ts table
  std::shared_ptr<TsTable> ts_table;
  bool is_dropped = false;
  s = ts_engine_->GetTsTable(ctx, table_id, ts_table, is_dropped, true, table_version);
  if (s != KStatus::SUCCESS) {
    if (is_dropped) {
      LOG_WARN("TsWriteBatchDataWorker::Write abort, table %lu has been dropped", table_id);
      return KStatus::SUCCESS;
    }
    LOG_ERROR("GetTsTable[%lu] failed", table_id);
    return KStatus::FAIL;
  }
  // get table schema && create ts table
  std::shared_ptr<TsTableSchemaManager> schema = ts_table->GetSchemaManager();

  TSSlice tag_slice = {data->data, layout.tags_data_offset + layout.tags_data_size};
  std::string tag_payload_str;
  GetTagPayload(table_version, tag_slice, tag_payload_str);
  TSSlice pay_load_struct{tag_payload_str.data(), tag_payload_str.size()};
  // insert tag record
  uint32_t vgroup_id;
  TSEntityID entity_id;
  bool new_tag;
  s = ts_engine_->InsertTagData(ctx, schema, 0, pay_load_struct, false,
                                vgroup_id, entity_id, new_tag);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("InsertTagData[%lu] failed", table_id);
    return KStatus::FAIL;
  }

  if (layout.row_type == TAG_ONLY) {
    *row_num = DecodeBatchField<uint32_t>(data->data + TsBatchData::row_num_offset_);
    LOG_DEBUG("current batch data write success, job_id[%lu], table_id[%lu], vgroup_id[%u], entity_id[%lu], row_num[%u]",
             job_id_, table_id, vgroup_id, entity_id, *row_num);
    return KStatus::SUCCESS;
  }

  TSSlice block_span_slice = {data->data + layout.block_span_data_offset, layout.block_span_data_size};
//  std::string block_span_data;
//  UpdateLSN(vgroup_id, &block_span_slice, block_span_data);
//  TSSlice new_block_data = {block_span_data.data(), block_span_data.size()};
  // get ptime
  auto ts = DecodeBatchField<timestamp64>(block_span_slice.data + TsBatchData::min_ts_offset_in_span_data_);
  std::shared_ptr<MMapMetricsTable> metric_schema;
  s = schema->GetMetricSchema(table_version, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get metric schema, table id[%lu], version[%u]", table_id, table_version);
    return KStatus::FAIL;
  }
  const vector<AttributeInfo>& attrs = *metric_schema->getSchemaInfoExcludeDroppedPtr();
  assert(!attrs.empty());
  auto ts_col_type = static_cast<DATATYPE>(attrs[0].type);
  timestamp64 p_time = convertTsToPTime(ts, ts_col_type);

  // write batch data to tmp file
  {
    BatchDataHeader header{table_id, table_version, vgroup_id, entity_id, p_time, block_span_slice.len, batch_version};
    TSSlice header_data{reinterpret_cast<char *>(&header), sizeof(BatchDataHeader)};
    MUTEX_LOCK(&w_file_latch_);
    Defer defer([&]() {
      MUTEX_UNLOCK(&w_file_latch_);
    });
    s = w_file_->Append(header_data);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsWriteBatchDataWorker::Write append header failed");
      return KStatus::FAIL;
    }
    s = w_file_->Append(block_span_slice);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsWriteBatchDataWorker::Write append content failed");
      return KStatus::FAIL;
    }
  }

  *row_num = DecodeBatchField<uint32_t>(data->data + TsBatchData::row_num_offset_);
  LOG_DEBUG("current batch data write success, job_id[%lu], table_id[%lu], vgroup_id[%u], entity_id[%lu], row_num[%u]",
           job_id_, table_id, vgroup_id, entity_id, *row_num);
  return s;
}
}  // namespace kwdbts
