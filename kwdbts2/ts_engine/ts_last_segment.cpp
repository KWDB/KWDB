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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iostream>
#include <memory>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "me_metadata.pb.h"
#include "rocksdb/slice.h"
#include "rocksdb/types.h"
#include "ts_bitmap.h"
#include "ts_coding.h"
#include "ts_env.h"
#include "ts_io.h"
#include "ts_last_segment_manager.h"
#include "ts_payload.h"
#include "ts_slice.h"
#include "ts_status.h"
#include "ts_table_schema_manager.h"
#include "utils/big_table_utils.h"
namespace kwdbts {

TsStatus TsLastSegment::Append(const TSSlice& data) {
  return file_->Append(data);
}

TsStatus TsLastSegment::Flush() {
  return file_->Flush();
}

size_t TsLastSegment::GetFileSize() const {
  return file_->GetFileSize();
}

TsFile* TsLastSegment::GetFilePtr() {
  return file_.get();
}

uint32_t TsLastSegment::GetVersion() const {
  return ver_;
}

KStatus TsLastSegment::GetFooter(TsLastSegmentFooter* footer) {
  TSSlice result;
  size_t offset = file_->GetFileSize() - sizeof(TsLastSegmentFooter);
  file_->Read(offset, sizeof(TsLastSegmentFooter), &result, reinterpret_cast<char *>(footer));
  if (result.len != sizeof(TsLastSegmentFooter)) {
    LOG_ERROR("last segment[%s] GetFooter failed.", file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(TsLastSegmentFooter& footer, std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TSSlice result;
  uint64_t nblock = footer.n_data_block;
  block_indexes->resize(nblock);
  for (uint64_t i = 0; i < nblock; ++i) {
    file_->Read(footer.block_info_idx_offset + i * sizeof(TsLastSegmentBlockIndex), sizeof(TsLastSegmentBlockIndex), &result,
                reinterpret_cast<char *>(&(*block_indexes)[i]));
    if (result.len != sizeof(TsLastSegmentBlockIndex)) {
      LOG_ERROR("last segment[%s] GetAllBlockIndex failed.", file_->GetFilePath().c_str());
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TsLastSegmentFooter last_segment_footer;
  KStatus s = GetFooter(&last_segment_footer);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = GetAllBlockIndex(last_segment_footer, block_indexes);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlockInfo(TsLastSegmentBlockIndex& block_index, size_t col_num,
                                    TsLastSegmentBlockInfo* block_info) {
  TSSlice result;
  size_t block_info_size = LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE + col_num * sizeof(uint32_t);
  char* block_info_data = new char[block_info_size];
  file_->Read(block_index.offset, block_info_size, &result, block_info_data);
  if (result.len != block_info_size) {
    delete[] block_info_data;
    LOG_ERROR("last segment[%s] GetBlockInfo failed, read block info failed. "
              "table id: %lu, table version: %u, block info offset: %lu.",
              file_->GetFilePath().c_str(), block_index.table_id,
              block_index.table_version, block_index.offset);
    return KStatus::FAIL;
  }
  // block info header
  memcpy(reinterpret_cast<char *>(block_info), block_info_data, LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE);
  // block info column offset
  block_info->col_offset.resize(col_num);
  for (size_t col_idx = 0; col_idx < col_num; ++col_idx) {
    char* col_offset_addr = block_info_data + LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE + col_idx * sizeof(uint32_t);
    block_info->col_offset[col_idx] = *reinterpret_cast<uint32_t*>(col_offset_addr);
  }
  delete[] block_info_data;
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block) {
  block->column_blocks.resize(block_info.ncol);
  // column block
  TSSlice result;
  size_t offset = block_info.block_offset;
  for (uint32_t i = 0; i < block_info.ncol; ++i) {
    size_t next_col_block_offset = i == block_info.ncol - 1 ? block_info.var_offset : block_info.col_offset[i + 1];
    size_t col_block_len = next_col_block_offset - block_info.col_offset[i];
    // read col block data
    char *col_block_buf = new char[col_block_len];
    file_->Read(offset, col_block_len, &result, col_block_buf);
    if (result.len != col_block_len) {
      delete[] col_block_buf;
      LOG_ERROR("last segment[%s] GetBlock failed, read column block[%u] failed.",
                file_->GetFilePath().c_str(), i);
      return KStatus::FAIL;
    }
    // decompress
    auto compressor = TsEnvInstance::GetInstance().Compressor();
    TSSlice col_block, bitmap;
    bool ret = compressor->Decode({col_block_buf, col_block_len}, block_info.nrow, &col_block, &bitmap);
    delete[] col_block_buf;
    if (!ret) {
      LOG_ERROR("last segment[%s] GetBlock failed, decode column block[%u] failed.",
                file_->GetFilePath().c_str(), i);
      return KStatus::FAIL;
    }
    // save decompressed col block data
    block->column_blocks[i].buffer.assign(col_block.data, col_block.len);
    block->column_blocks[i].bitmap.Reset(block_info.nrow);
    block->column_blocks[i].bitmap.SetData(bitmap);
    std::free(col_block.data);
    std::free(bitmap.data);
    offset += col_block_len;
  }
  // read var data
  char* var_buf = new char[block_info.var_len];
  file_->Read(offset, block_info.var_len, &result, var_buf);
  if (result.len != block_info.var_len) {
    delete[] var_buf;
    LOG_ERROR("last segment[%s] GetBlock failed, read var data failed.", file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  // save var data
  block->var_buffer.assign(var_buf, block_info.var_len);
  delete[] var_buf;

  return KStatus::SUCCESS;
}

KStatus TsLastSegmentBuilder::FlushPayloadBuffer() {
  if (payload_buffer_.empty()) {
    return SUCCESS;
  }
  std::shared_ptr<TsTableSchemaManager> table_mgr;
  auto s = schema_mgr_->GetTableSchemaMgr(table_id_, table_mgr);
  schema_mgr_->GetTableSchemaMgr(table_id_, table_mgr);
  std::vector<AttributeInfo> data_schema;
  table_mgr->GetColumnsExcludeDropped(data_schema);
  TsRawPayloadRowParser parser(data_schema);

  auto comp = [&parser](const EntityPayload& l, const EntityPayload& r) {
    auto ts_lhs = parser.GetTimestamp(l.metric);
    auto ts_rhs = parser.GetTimestamp(r.metric);
    return l.entity_id < r.entity_id && ts_lhs < ts_rhs;
  };
  // std::sort(payload_buffer_.begin(), payload_buffer_.end(), comp);

  int left = payload_buffer_.size();
  data_block_builder_->Reset(table_id_, version_);
  auto reserved_size = std::min<int>(payload_buffer_.size(), kNRowPerBlock);
  data_block_builder_->Reserve(reserved_size);

  for (int idx = 0; idx < payload_buffer_.size(); ++idx) {
    const EntityPayload& p = payload_buffer_[idx];
    data_block_builder_->Add(p.entity_id, p.seq_no, p.metric);
    --left;

    if ((idx + 1) % kNRowPerBlock == 0 || (idx + 1) == payload_buffer_.size()) {
      data_block_builder_->Finish();
      auto s = WriteMetricBlock(data_block_builder_.get());
      if (s != SUCCESS) return FAIL;
      data_block_builder_->Reset(table_id_, version_);

      auto reserved_size = std::min<int>(left, kNRowPerBlock);
      data_block_builder_->Reserve(reserved_size);
    }
  }
  payload_buffer_.clear();
  return SUCCESS;
}

KStatus TsLastSegmentBuilder::PutRowData(TSTableID table_id, uint32_t version, TSEntityID entity_id,
                                         rocksdb::SequenceNumber seq_no, TSSlice row_data) {
  if (table_id != table_id_ || version != version_) {
    auto s = FlushPayloadBuffer();
    if (s != SUCCESS) return FAIL;
  }
  table_id_ = table_id;
  version_ = version;
  payload_buffer_.push_back({seq_no, entity_id, row_data});
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
    auto data = builder->GetColumnData(i);
    len += data.len;
    TsStatus s = last_segment_->Append(data);
    if (!s.ok()) {
      LOG_ERROR("IO Fail: %s", s.ToString().c_str());
      return FAIL;
    }
  }
  auto var_offset = len;
  TSSlice varchar_buf = builder->GetVarcharBuffer();
  auto s = last_segment_->Append(varchar_buf);
  if (!s.ok()) {
    LOG_ERROR("IO Fail: %s", s.ToString().c_str());
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
  auto s = FlushPayloadBuffer();
  if (s != SUCCESS) return FAIL;

  size_t infoblock_offset = last_segment_->GetFileSize();
  index_handle_->ApplyInfoBlockOffset(infoblock_offset);
  s = info_handle_->WriteInfo(last_segment_->GetFilePtr());
  if (s != SUCCESS) return FAIL;

  size_t index_block_offset = last_segment_->GetFileSize();
  s = index_handle_->WriteIndex(last_segment_->GetFilePtr());
  if (s != SUCCESS) return FAIL;

  assert(last_segment_->GetFileSize() - index_block_offset == nblock_ * 56);

  // TODO(zzr) meta block API
  size_t meta_block_offset = last_segment_->GetFileSize();
  size_t meta_index_offset = meta_block_offset;

  TsLastSegmentFooter footer;
  footer.block_info_idx_offset = index_block_offset;
  footer.n_data_block = nblock_;
  footer.meta_block_idx_offset = meta_index_offset;
  footer.n_meta_block = 0;
  footer.file_version = 1;
  auto ss = last_segment_->Append(TSSlice{reinterpret_cast<char*>(&footer), sizeof(TsLastSegmentFooter)});
  if (!ss.ok()) {
    LOG_ERROR("IO error when write lastsegment %s", ss.ToString().c_str());
    return FAIL;
  }
  return SUCCESS;
}

void TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder::Add(
    const TSSlice& col_data) noexcept {
#ifndef NDEBUG
  assert(getDataTypeSize(dtype_) == col_data.len);
#endif
  // TODO(zzr): parse bitmap from payload;
  // bitmap_[row_cnt_] = kValid;
  row_cnt_++;
  buffer_.append(col_data.data, dsize_);
}

void TsLastSegmentBuilder::MetricBlockBuilder::ColumnBlockBuilder::Compress() {
  auto compressor = TsEnvInstance::GetInstance().Compressor();
  TSSlice plain{buffer_.data(), buffer_.size()};
  // TODO(zzr)
  char bitmap[1024];
  memset(bitmap, 0xFFFFFFFF, sizeof(bitmap));
  auto out = compressor->Encode(plain, {bitmap, (row_cnt_ + 7) / 8}, row_cnt_, dtype_);
  buffer_.assign(out.data, out.len);
  free(out.data);
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
  colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(INT64));  // for entity_id;
  colblocks_.push_back(std::make_unique<ColumnBlockBuilder>(INT64));  // for SeqNo;
  for (int i = 0; i < metric_schema_.size(); ++i) {
    colblocks_.push_back(
        std::make_unique<ColumnBlockBuilder>(static_cast<DATATYPE>(metric_schema_[i].type)));
  }
  return KStatus::SUCCESS;
}

void TsLastSegmentBuilder::MetricBlockBuilder::Add(TSEntityID entity_id,
                                                   rocksdb::SequenceNumber seq_no,
                                                   TSSlice metric_data) {
  assert(!finished_);
  assert(parser_ != nullptr);
  info_.nrow++;
  info_.ndevice += (entity_id != last_entity_id_);
  last_entity_id_ = entity_id;

  assert(metric_data.len >= 8);

  TSSlice data;
  parser_->GetColValueAddr(metric_data, 0, &data);

  int64_t ts = DecodeFixed<64>(data.data);
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
    if (!isVarLenType(metric_schema_[col_id].type)) {
      colblocks_[i]->Add(data);
    } else {
      size_t var_off = varchar_buffer_.size();
      colblocks_[i]->Add({reinterpret_cast<char*>(&var_off), 8});
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
    // TODO(zzr)
    // A. Calculate aggregate information
    //    Add some API in ColumnBlock to do this.
    //
    // B. Compress the column blocks
    //    Steps:
    //    1. Get compression type from schema_mgr
    //    2. compress

    colblocks_[i]->Compress();
    info_.col_offset.push_back(offset);

    // update blockinfo
    offset += colblocks_[i]->GetData().len;
  }
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
  size_t length = LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE + info.col_offset.size() * 4;

  length_ += length;

  return length;
}

KStatus TsLastSegmentBuilder::InfoHandle::WriteInfo(TsFile* file) {
  std::string buf;
  assert(infos_.size() == offset_.size());
  for (int i = 0; i < infos_.size(); ++i) {
    PutFixed<64>(&buf, offset_[i]);
    // PutFixed<64>(&buf, infos_[i].table_id);
    // PutFixed<32>(&buf, infos_[i].version);
    PutFixed<32>(&buf, infos_[i].nrow);
    PutFixed<32>(&buf, infos_[i].col_offset.size());
    PutFixed<32>(&buf, infos_[i].var_offset);
    PutFixed<32>(&buf, infos_[i].var_len);
    // PutFixed<32>(&buf, infos_[i].ndevice);
    for (int j = 0; j < infos_[i].col_offset.size(); ++j) {
      PutFixed<32>(&buf, infos_[i].col_offset[j]);
    }
  }
  assert(buf.size() == length_);
  auto s = file->Append(buf);
  return s.ok() ? SUCCESS : FAIL;
}

void TsLastSegmentBuilder::IndexHandle::RecordBlockInfo(size_t info_length, const BlockInfo& info) {
  assert(!finished_);
  indices_.push_back({cursor_, info.table_id, info.version, info.ndevice, info.min_ts, info.max_ts,
                      info.min_entity_id, info.max_entity_id});
  cursor_ += info_length;
}

void TsLastSegmentBuilder::IndexHandle::ApplyInfoBlockOffset(size_t offset) {
  finished_ = true;
  for (auto& i : indices_) {
    i.offset += offset;
  }
}

KStatus TsLastSegmentBuilder::IndexHandle::WriteIndex(TsFile* file) {
  assert(finished_);
  std::string buf;
  for (const auto idx : indices_) {
    PutFixed<64>(&buf, idx.offset);
    PutFixed<64>(&buf, idx.table_id);
    PutFixed<32>(&buf, idx.table_version);
    PutFixed<32>(&buf, idx.n_entity);
    PutFixed<64>(&buf, idx.min_ts);
    PutFixed<64>(&buf, idx.max_ts);
    PutFixed<64>(&buf, idx.min_entity_id);
    PutFixed<64>(&buf, idx.max_entity_id);
  }
  auto s = file->Append(buf);
  return s.ok() ? SUCCESS : FAIL;
}

KStatus TsLastSegmentManager::NewLastSegment(std::shared_ptr<TsLastSegment>& last_segment) {
  char buffer[64];
  ver_++;
  std::snprintf(buffer, sizeof(buffer), "last.ver-%04u", ver_);
  auto filename = dir_path_ / buffer;
  last_segment = std::make_shared<TsLastSegment>(ver_, new TsMMapFile(filename, false /*read_only*/));
  last_segments_.push_back(last_segment);
  return KStatus::SUCCESS;
}

std::vector<std::shared_ptr<TsLastSegment>> TsLastSegmentManager::GetCompactLastSegments() {
  std::vector<std::shared_ptr<TsLastSegment>> result;
  if (last_segments_.empty()) {
    return result;
  }
  size_t offset = compacted_ver_ - last_segments_[0]->GetVersion() + 1;
  assert(offset < last_segments_.size());
  if (ver_ - compacted_ver_ > MAX_COMPACT_NUM) {
    result.assign(last_segments_.begin() + offset, last_segments_.begin() + offset + MAX_COMPACT_NUM);
  }
  return result;
}

bool TsLastSegmentManager::NeedCompact() {
  assert(ver_ > compacted_ver_);
  return ver_ - compacted_ver_ > MAX_COMPACT_NUM;
}

void TsLastSegmentManager::SetCompactedVer(uint32_t ver) {
  compacted_ver_ = ver;
}

}  // namespace kwdbts
