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

#include "ts_lastsegment_builder.h"
#include <algorithm>
#include <cstdint>

#include "data_type.h"
#include "kwdb_type.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compressor.h"
#include "ts_compressor_impl.h"
#include "ts_io.h"
#include "ts_lastsegment.h"

namespace kwdbts {

KStatus TsLastSegmentBuilder::FlushBuffer() {
  KStatus s = FlushPayloadBuffer();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = FlushColDataBuffer();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return s;
}

KStatus TsLastSegmentBuilder::FlushPayloadBuffer() {
  if (payload_buffer_.buffer.empty()) {
    return SUCCESS;
  }
  payload_buffer_.sort();
  int kNRowPerBlock = TsLastSegment::kNRowPerBlock;

  int left = payload_buffer_.buffer.size();
  data_block_builder_->Reset(table_id_, version_);
  auto reserved_size = std::min<int>(payload_buffer_.buffer.size(), kNRowPerBlock);
  data_block_builder_->Reserve(reserved_size);

  if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
    EntityPayload *last_entity_payload = nullptr;
    for (int idx = 0; idx < payload_buffer_.buffer.size(); ++idx) {
      --left;
      if (!last_entity_payload ||
          last_entity_payload->IsSameEntityAndTs(payload_buffer_.parser, payload_buffer_.buffer[idx])) {
        last_entity_payload = &payload_buffer_.buffer[idx];
        continue;
      }

      data_block_builder_->Add(last_entity_payload->entity_id, last_entity_payload->lsn, last_entity_payload->metric);
      if (data_block_builder_->GetNRows() % kNRowPerBlock == 0) {
        data_block_builder_->Finish();
        auto s = WriteMetricBlock(data_block_builder_.get());
        if (s != SUCCESS) return FAIL;
        data_block_builder_->Reset(table_id_, version_);

        auto reserved_size = std::min<int>(left + 1, kNRowPerBlock);
        data_block_builder_->Reserve(reserved_size);
      }
      last_entity_payload = &payload_buffer_.buffer[idx];
    }
    data_block_builder_->Add(last_entity_payload->entity_id, last_entity_payload->lsn, last_entity_payload->metric);
    data_block_builder_->Finish();
    auto s = WriteMetricBlock(data_block_builder_.get());
    if (s != SUCCESS) return FAIL;
    data_block_builder_->Reset(table_id_, version_);
  } else if (EngineOptions::g_dedup_rule == DedupRule::DISCARD) {
    EntityPayload *last_entity_payload = nullptr;
    for (int idx = 0; idx < payload_buffer_.buffer.size(); ++idx) {
      const EntityPayload& p = payload_buffer_.buffer[idx];
      --left;
      if (!last_entity_payload ||
          !last_entity_payload->IsSameEntityAndTs(payload_buffer_.parser, p)) {
        data_block_builder_->Add(p.entity_id, p.lsn, p.metric);
        if (data_block_builder_->GetNRows() % kNRowPerBlock == 0) {
          data_block_builder_->Finish();
          auto s = WriteMetricBlock(data_block_builder_.get());
          if (s != SUCCESS) return FAIL;
          data_block_builder_->Reset(table_id_, version_);

          auto reserved_size = std::min<int>(left, kNRowPerBlock);
          data_block_builder_->Reserve(reserved_size);
        }
        last_entity_payload = &payload_buffer_.buffer[idx];
      }
    }
    data_block_builder_->Finish();
    auto s = WriteMetricBlock(data_block_builder_.get());
    if (s != SUCCESS) return FAIL;
    data_block_builder_->Reset(table_id_, version_);
  } else {
    for (int idx = 0; idx < payload_buffer_.buffer.size(); ++idx) {
      const EntityPayload& p = payload_buffer_.buffer[idx];
      data_block_builder_->Add(p.entity_id, p.lsn, p.metric);
      --left;

      if ((idx + 1) % kNRowPerBlock == 0 || (idx + 1) == payload_buffer_.buffer.size()) {
        data_block_builder_->Finish();
        auto s = WriteMetricBlock(data_block_builder_.get());
        if (s != SUCCESS) return FAIL;
        data_block_builder_->Reset(table_id_, version_);

        auto reserved_size = std::min<int>(left, kNRowPerBlock);
        data_block_builder_->Reserve(reserved_size);
      }
    }
  }
  payload_buffer_.clear();
  return SUCCESS;
}

KStatus TsLastSegmentBuilder::FlushColDataBuffer() {
  if (cols_data_buffer_.buffer.empty()) {
    return SUCCESS;
  }
  cols_data_buffer_.sort();
  int kNRowPerBlock = TsLastSegment::kNRowPerBlock;
  std::shared_ptr<TsTableSchemaManager> table_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(table_id_, table_mgr);
  schema_mgr_->GetTableSchemaMgr(table_id_, table_mgr);
  std::vector<AttributeInfo> data_schema;
  table_mgr->GetColumnsExcludeDropped(data_schema);

  int left = cols_data_buffer_.buffer.size();
  data_block_builder_->Reset(table_id_, version_);
  auto reserved_size = std::min<int>(cols_data_buffer_.buffer.size(), kNRowPerBlock);
  data_block_builder_->Reserve(reserved_size);

  if (EngineOptions::g_dedup_rule == DedupRule::OVERRIDE) {
    EntityColData *last_entity_col_data = nullptr;
    for (int idx = 0; idx < cols_data_buffer_.buffer.size(); ++idx) {
      --left;
      if (!last_entity_col_data ||
          last_entity_col_data->IsSameEntityAndTs(cols_data_buffer_.buffer[idx])) {
        last_entity_col_data = &cols_data_buffer_.buffer[idx];
        continue;
      }

      data_block_builder_->Add(last_entity_col_data->entity_id, last_entity_col_data->lsn,
                               last_entity_col_data->col_data, last_entity_col_data->data_flags);
      if (data_block_builder_->GetNRows() % kNRowPerBlock == 0) {
        data_block_builder_->Finish();
        auto s = WriteMetricBlock(data_block_builder_.get());
        if (s != SUCCESS) return FAIL;
        data_block_builder_->Reset(table_id_, version_);

        auto reserved_size = std::min<int>(left + 1, kNRowPerBlock);
        data_block_builder_->Reserve(reserved_size);
      }
      last_entity_col_data = &cols_data_buffer_.buffer[idx];
    }
    data_block_builder_->Add(last_entity_col_data->entity_id, last_entity_col_data->lsn,
                             last_entity_col_data->col_data, last_entity_col_data->data_flags);
    data_block_builder_->Finish();
    auto s = WriteMetricBlock(data_block_builder_.get());
    if (s != SUCCESS) return FAIL;
    data_block_builder_->Reset(table_id_, version_);
  } else if (EngineOptions::g_dedup_rule == DedupRule::KEEP) {
    for (int idx = 0; idx < cols_data_buffer_.buffer.size(); ++idx) {
      const EntityColData& p = cols_data_buffer_.buffer[idx];
      data_block_builder_->Add(p.entity_id, p.lsn, p.col_data, p.data_flags);
      --left;

      if ((idx + 1) % kNRowPerBlock == 0 || (idx + 1) == cols_data_buffer_.buffer.size()) {
        data_block_builder_->Finish();
        auto s = WriteMetricBlock(data_block_builder_.get());
        if (s != SUCCESS) return FAIL;
        data_block_builder_->Reset(table_id_, version_);

        auto reserved_size = std::min<int>(left, kNRowPerBlock);
        data_block_builder_->Reserve(reserved_size);
      }
    }
  }
  cols_data_buffer_.clear();
  return SUCCESS;
}

KStatus TsLastSegmentBuilder::PutRowData(TSTableID table_id, uint32_t version, TSEntityID entity_id,
                                         TS_LSN seq_no, TSSlice row_data) {
  assert(last_segment_ != nullptr);
  if (table_id != table_id_ || version != version_) {
    auto s = FlushPayloadBuffer();

    std::shared_ptr<TsTableSchemaManager> table_mgr;
    s = schema_mgr_->GetTableSchemaMgr(table_id, table_mgr);
    if (s == FAIL) {
      LOG_ERROR("can not get table schema manager, table id: %lu", table_id);
      return FAIL;
    }
    std::vector<AttributeInfo> data_schema;
    s = table_mgr->GetColumnsExcludeDropped(data_schema, version);
    if (s == FAIL) {
      LOG_ERROR("can not get schema, table id: %lu, version: %u", table_id, version);
      return FAIL;
    }
    payload_buffer_.Reset(data_schema);
  }
  table_id_ = table_id;
  version_ = version;
  bloom_filter_->Add(entity_id);
  payload_buffer_.push_back({seq_no, entity_id, row_data});
  return KStatus::SUCCESS;
}

KStatus TsLastSegmentBuilder::PutColData(TSTableID table_id, uint32_t version, TSEntityID entity_id,
                                         kwdbts::TS_LSN seq_no, std::vector<TSSlice> col_data,
                                         std::vector<DataFlags> data_flags) {
  if (table_id != table_id_ || version != version_) {
    auto s = FlushColDataBuffer();
    if (s != SUCCESS) return FAIL;
  }
  table_id_ = table_id;
  version_ = version;
  bloom_filter_->Add(entity_id);
  cols_data_buffer_.push_back({seq_no, entity_id, std::move(col_data), std::move(data_flags)});
  return KStatus::SUCCESS;
}

KStatus TsLastSegmentBuilder::WriteMetricBlock(MetricBlockBuilder* builder) {
  if (builder->Empty()) {
    return SUCCESS;
  }

  ++nblock_;
  assert(builder->IsFinished());
  size_t len = 0;
  for (int i = 0; i < builder->GetNColumns(); ++i) {
    auto bitmap = builder->GetColumnBitmap(i);
    KStatus s = last_segment_->Append(bitmap);
    len += bitmap.len;
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("last_segment Append failed.");
      return FAIL;
    }

    auto data = builder->GetColumnData(i);
    s = last_segment_->Append(data);
    len += data.len;
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("last_segment Append failed.");
      return FAIL;
    }
  }
  auto var_offset = len;
  TSSlice varchar_buf = builder->GetVarcharBuffer();
  auto s = last_segment_->Append(varchar_buf);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("last_segment Append failed.");
    return FAIL;
  }
  len += varchar_buf.len;
  auto info = builder->GetBlockInfo();
  info.var_offset = var_offset;
  info.var_len = varchar_buf.len;
  auto info_len = info_handle_->RecordBlock(len, info);
  index_handle_->RecordBlockInfo(info_len, info);
  return SUCCESS;
}

KStatus TsLastSegmentBuilder::Finalize() {
  // Write the last block
  auto s = FlushBuffer();
  assert(data_block_builder_->Empty());
  data_block_builder_->Finish();
  if (s != SUCCESS) return FAIL;

  size_t infoblock_offset = last_segment_->GetFileSize();
  index_handle_->ApplyInfoBlockOffset(infoblock_offset);
  s = info_handle_->WriteInfo(last_segment_.get());
  if (s != SUCCESS) return FAIL;

  size_t index_block_offset = last_segment_->GetFileSize();
  s = index_handle_->WriteIndex(last_segment_.get());
  if (s != SUCCESS) return FAIL;

  assert(last_segment_->GetFileSize() - index_block_offset ==
         nblock_ * sizeof(TsLastSegmentBlockIndex));

  // TODO(zzr) meta block API
  int nmeta = meta_blocks_.size();
  std::vector<uint64_t> meta_offset(nmeta);
  std::vector<uint64_t> meta_len(nmeta);

  for (int i = 0; i < nmeta; ++i) {
    meta_offset[i] = last_segment_->GetFileSize();
    std::string serialized;
    meta_blocks_[i]->Serialize(&serialized);
    last_segment_->Append(serialized);
    meta_len[i] = serialized.size();
  }
  size_t meta_index_offset = last_segment_->GetFileSize();
  std::string meta_idx_data;
  meta_idx_data.reserve(nmeta * 16);
  for (int i = 0; i < nmeta; ++i) {
    PutFixed64(&meta_idx_data, meta_offset[i]);
    PutFixed64(&meta_idx_data, meta_len[i]);
  }
  s = last_segment_->Append(meta_idx_data);
  if (s == FAIL) {
    return s;
  }

  TsLastSegmentFooter footer;
  footer.block_info_idx_offset = index_block_offset;
  footer.n_data_block = nblock_;
  footer.meta_block_idx_offset = meta_index_offset;
  footer.n_meta_block = nmeta;
  footer.file_version = 1;
  footer.magic_number = FOOTER_MAGIC;

  // TODO(zzr): take care of endian
  auto ss =
      last_segment_->Append(TSSlice{reinterpret_cast<char*>(&footer), sizeof(TsLastSegmentFooter)});
  if (ss != KStatus::SUCCESS) {
    LOG_ERROR("IO error when write lastsegment.");
    return FAIL;
  }
  s = last_segment_->Flush();
  if (s == FAIL) {
    LOG_ERROR("IO error when flush lastsegment.");
    return FAIL;
  }
  last_segment_.reset();
  return SUCCESS;
}

void TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder::Add(
    const TSSlice& col_data, DataFlags data_flag) noexcept {
  if (has_bitmap_) {
    bitmap_[row_cnt_] = data_flag;
  }
  row_cnt_++;
  data_buffer_.append(col_data.data, dsize_);
}

void TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder::Compress() {
  bitmap_buffer_.clear();
  if (has_bitmap_) {
    // TODO(zzr) Compress bitmap..
    bitmap_buffer_.push_back(0);  // TODO(zzr) which means plain, nocompression
    auto slice = bitmap_.GetData();
    bitmap_buffer_.append(slice.data, slice.len);
  }
  TsBitmap* bm = has_bitmap_ ? &bitmap_ : nullptr;

  std::string compressed;
  const auto& mgr = CompressorManager::GetInstance();
  auto [first, second] = mgr.GetDefaultAlgorithm(dtype_);
  if (isVarLenType(dtype_)) {
    first = TsCompAlg::kGorilla_32;
  }
  TSSlice plain{data_buffer_.data(), data_buffer_.size()};
  mgr.CompressData(plain, bm, row_cnt_, &compressed, first, second);
  data_buffer_.swap(compressed);
}

auto TsLastSegmentBuilder::MetricBlockBuilder::GetBlockInfo() const -> BlockInfo {
  assert(finished_);
  return info_;
}

void TsLastSegmentBuilder::MetricBlockBuilder::Reserve(size_t nrow) {
  assert(!finished_);
  for (auto& p : colblocks_) {
    p->Reserve(nrow);
  }
}

TsLastSegmentBuilder::MetricBlockBuilder::MetricBlockBuilder(TsEngineSchemaManager* schema_mgr)
    : schema_mgr_(schema_mgr) {
  varchar_buffer_.reserve(4 << 10);  // reserve 4K byte
}
KStatus TsLastSegmentBuilder::MetricBlockBuilder::Reset(TSTableID table_id, uint32_t version) {
  info_.Reset(table_id, version);
  finished_ = false;
  last_entity_id_ = -1;

  varchar_buffer_.clear();
  colblocks_.clear();
  auto s =
      schema_mgr_->GetTableMetricSchema(nullptr, info_.table_id, info_.version, &table_schema_);
  if (s == KStatus::FAIL) {
    return s;
  }
  metric_schema_ = table_schema_->getSchemaInfoExcludeDropped();
  parser_ = std::make_unique<TsRawPayloadRowParser>(metric_schema_);
  int ncol = metric_schema_.size() + 2;  // one for entity_id, one for SeqNo
  colblocks_.reserve(ncol);
  colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(INT64, 8, false));  // for entity_id;
  colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(INT64, 8, false));  // for SeqNo;
  for (int i = 0; i < metric_schema_.size(); ++i) {
    bool nullable = true;  // TODO(zzr): read from schema;
    if (isVarLenType(metric_schema_[i].type)) {
      colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(
          static_cast<DATATYPE>(metric_schema_[i].type), sizeof(uint32_t), nullable));
    } else {
      colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(
          static_cast<DATATYPE>(metric_schema_[i].type), metric_schema_[i].size, nullable));
    }
  }
  return KStatus::SUCCESS;
}

void TsLastSegmentBuilder::MetricBlockBuilder::Add(TSEntityID entity_id, TS_LSN seq_no,
                                                   TSSlice metric_data) {
  assert(!finished_);
  assert(parser_ != nullptr);
  info_.nrow++;
  if (entity_id != last_entity_id_) {
    ++info_.ndevice;
    last_entity_id_ = entity_id;
  }

  assert(metric_data.len >= 8);

  TSSlice data;
  parser_->GetColValueAddr(metric_data, 0, &data);

  int64_t ts = DecodeFixed64(data.data);
  info_.max_ts = std::max(info_.max_ts, ts);
  info_.min_ts = std::min(info_.min_ts, ts);

  info_.max_entity_id = std::max(info_.max_entity_id, entity_id);
  info_.min_entity_id = std::min(info_.min_entity_id, entity_id);

  colblocks_[0]->Add({reinterpret_cast<char*>(&entity_id), sizeof(entity_id)});
  colblocks_[1]->Add({reinterpret_cast<char*>(&seq_no), sizeof(seq_no)});
  for (int i = 2; i < colblocks_.size(); ++i) {
    int col_id = i - 2;
    TSSlice data;
    parser_->GetColValueAddr(metric_data, col_id, &data);
    bool is_null = parser_->IsColNull(metric_data, col_id);
    if (!isVarLenType(metric_schema_[col_id].type)) {
      colblocks_[i]->Add(data, is_null ? kNull : kValid);
    } else {
      uint32_t var_off = varchar_buffer_.size();
      colblocks_[i]->Add({reinterpret_cast<char*>(&var_off), sizeof(var_off)},
                         is_null ? kNull : kValid);
      uint16_t len = data.len;
      if (!is_null) {
        varchar_buffer_.append(reinterpret_cast<char*>(&len), sizeof(len));
        varchar_buffer_.append(data.data, data.len);
      }
    }
  }
}

void TsLastSegmentBuilder::MetricBlockBuilder::Add(TSEntityID entity_id, TS_LSN seq_no,
                                                   const std::vector<TSSlice>& col_data,
                                                   const std::vector<DataFlags>& data_flags) {
  assert(!finished_);
  assert(parser_ != nullptr);
  info_.nrow++;
  if (entity_id != last_entity_id_) {
    ++info_.ndevice;
    last_entity_id_ = entity_id;
  }

  assert(col_data.size() > 0);
  assert(col_data[0].len >= 8);

  int64_t ts = DecodeFixed64(col_data[0].data);
  info_.max_ts = std::max(info_.max_ts, ts);
  info_.min_ts = std::min(info_.min_ts, ts);

  info_.max_entity_id = std::max(info_.max_entity_id, entity_id);
  info_.min_entity_id = std::min(info_.min_entity_id, entity_id);

  colblocks_[0]->Add({reinterpret_cast<char*>(&entity_id), sizeof(entity_id)});
  colblocks_[1]->Add({reinterpret_cast<char*>(&seq_no), sizeof(seq_no)});
  for (int i = 2; i < colblocks_.size(); ++i) {
    int col_id = i - 2;
    TSSlice data = col_data[col_id];
    if (!isVarLenType(metric_schema_[col_id].type)) {
      colblocks_[i]->Add(data, data_flags[col_id]);
    } else {
      size_t var_off = varchar_buffer_.size();
      colblocks_[i]->Add({reinterpret_cast<char*>(&var_off), 8}, data_flags[col_id]);
      uint16_t len = data.len;
      varchar_buffer_.append(reinterpret_cast<char*>(&len), sizeof(len));
      varchar_buffer_.append(data.data, data.len);
    }
  }
}

void TsLastSegmentBuilder::MetricBlockBuilder::Finish() {
  if (Empty()) {
    finished_ = true;
    return;
  }
  uint32_t offset = 0;
  for (int i = 0; i < colblocks_.size(); ++i) {
    colblocks_[i]->Truncate();
    // TODO(zzr)
    // A. Calculate aggregate information
    //    Add some API in ColumnBlock to do this.
    //
    // B. Compress the column blocks
    //    Steps:
    //    1. Get compression type from schema_mgr
    //    2. compress

    colblocks_[i]->Compress();
    int bitmap_len = colblocks_[i]->GetBitmap().len;
    int data_len = colblocks_[i]->GetData().len;
    BlockInfo::ColInfo col_info;
    col_info.col_offset = offset;
    col_info.bitmap_len = bitmap_len;
    col_info.data_len = data_len;
    info_.col_infos.push_back(std::move(col_info));
    // update blockinfo
    offset += data_len + bitmap_len;
  }

  // compress varchar;
  const auto& snappy = SnappyString::GetInstance();
  std::string tmp;
  std::string out;
  if (snappy.Compress({varchar_buffer_.data(), varchar_buffer_.size()}, 0, &out)) {
    tmp.push_back(static_cast<char>(GenCompAlg::kSnappy));
    tmp.append(out);
  } else {
    tmp.push_back(static_cast<char>(GenCompAlg::kPlain));
    tmp.append(varchar_buffer_);
  }
  varchar_buffer_.swap(tmp);
  finished_ = true;
}

TSSlice TsLastSegmentBuilder::MetricBlockBuilder::GetColumnData(size_t i) {
  assert(i < GetNColumns());
  return colblocks_[i]->GetData();
}

std::vector<TSSlice> TsLastSegmentBuilder::MetricBlockBuilder::GetColumnDatas() {
  assert(finished_);
  std::vector<TSSlice> result;
  for (const auto& p : colblocks_) {
    result.push_back(p->GetData());
  }
  return result;
}

TSSlice TsLastSegmentBuilder::MetricBlockBuilder::GetColumnBitmap(size_t i) {
  return colblocks_[i]->GetBitmap();
}

size_t TsLastSegmentBuilder::InfoHandle::RecordBlock(size_t block_length, const BlockInfo& info) {
  offset_.push_back(cursor_);
  cursor_ += block_length;

  infos_.push_back(info);
  size_t length = LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE +
                  info.col_infos.size() * LAST_SEGMENT_BLOCK_INFO_COL_INFO_SIZE;

  length_ += length;

  return length;
}

KStatus TsLastSegmentBuilder::InfoHandle::WriteInfo(TsAppendOnlyFile* file) {
  std::string buf;
  assert(infos_.size() == offset_.size());
  for (int i = 0; i < infos_.size(); ++i) {
    PutFixed64(&buf, offset_[i]);
    PutFixed32(&buf, infos_[i].nrow);
    PutFixed32(&buf, infos_[i].col_infos.size());
    PutFixed32(&buf, infos_[i].var_offset);
    PutFixed32(&buf, infos_[i].var_len);
    for (int j = 0; j < infos_[i].col_infos.size(); ++j) {
      PutFixed32(&buf, infos_[i].col_infos[j].col_offset);
      PutFixed16(&buf, infos_[i].col_infos[j].bitmap_len);
      PutFixed32(&buf, infos_[i].col_infos[j].data_len);
    }
  }
  assert(buf.size() == length_);
  auto s = file->Append(buf);
  return s;
}

void TsLastSegmentBuilder::IndexHandle::RecordBlockInfo(size_t info_length, const BlockInfo& info) {
  assert(!finished_);
  indices_.push_back({cursor_, info_length, info.table_id, info.version, info.ndevice, info.min_ts,
                      info.max_ts, info.min_entity_id, info.max_entity_id});
  cursor_ += info_length;
}

void TsLastSegmentBuilder::IndexHandle::ApplyInfoBlockOffset(size_t offset) {
  finished_ = true;
  for (auto& i : indices_) {
    i.offset += offset;
  }
}

KStatus TsLastSegmentBuilder::IndexHandle::WriteIndex(TsAppendOnlyFile* file) {
  assert(finished_);
  std::string buf;
  for (const auto idx : indices_) {
    PutFixed64(&buf, idx.offset);
    PutFixed64(&buf, idx.length);
    PutFixed64(&buf, idx.table_id);
    PutFixed32(&buf, idx.table_version);
    PutFixed32(&buf, idx.n_entity);
    PutFixed64(&buf, idx.min_ts);
    PutFixed64(&buf, idx.max_ts);
    PutFixed64(&buf, idx.min_entity_id);
    PutFixed64(&buf, idx.max_entity_id);
  }
  return file->Append(buf);
}

}  // namespace kwdbts
