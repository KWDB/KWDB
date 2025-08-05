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
#include <limits>
#include <tuple>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_block.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_lastsegment_endec.h"
#include "ts_metric_block.h"

namespace kwdbts {

KStatus TsLastSegmentBuilder::PutBlockSpan(std::shared_ptr<TsBlockSpan> span) {
  TableVersionInfo current_table_version{span->GetTableID(), span->GetTableVersion()};
  bloom_filter_->Add(span->GetEntityID());
  while (span != nullptr && span->GetRowNum() != 0) {
    if (metric_block_builder_ == nullptr || current_table_version != table_version_) {
      auto s = RecordAndWriteBlockToFile();
      if (s == FAIL) {
        return FAIL;
      }
      std::shared_ptr<TsTableSchemaManager> table_schema_mgr;
      s = engine_schema_manager_->GetTableSchemaMgr(span->GetTableID(), table_schema_mgr);
      if (s == FAIL) {
        return FAIL;
      }
      std::vector<AttributeInfo> col_schema;
      s = table_schema_mgr->GetColumnsExcludeDropped(col_schema, span->GetTableVersion());
      if (s == FAIL) {
        return FAIL;
      }
      metric_block_builder_ = std::make_unique<TsMetricBlockBuilder>(col_schema);
      block_index_collector_ = std::make_unique<BlockIndexCollector>(span->GetTableID(), span->GetTableVersion());
      entity_id_buffer_.clear();
      table_version_ = current_table_version;
    }

    std::shared_ptr<TsBlockSpan> back_span;
    int extra_rows = metric_block_builder_->GetRowNum() + span->GetRowNum() - TsLastSegment::kNRowPerBlock;
    if (extra_rows > 0) {
      // split blockspan
      span->SplitBack(extra_rows, back_span);
    }

    block_index_collector_->Collect(span.get());
    auto s = metric_block_builder_->PutBlockSpan(span);
    if (s == FAIL) {
      return FAIL;
    }
    entity_id_buffer_.reserve(entity_id_buffer_.size() + span->GetRowNum());
    std::fill_n(std::back_inserter(entity_id_buffer_), span->GetRowNum(), span->GetEntityID());

    if (metric_block_builder_->GetRowNum() == TsLastSegment::kNRowPerBlock) {
      s = RecordAndWriteBlockToFile();
    }
    span = back_span;
  }
  return SUCCESS;
}

KStatus TsLastSegmentBuilder::Finalize() {
  if (metric_block_builder_ != nullptr && metric_block_builder_->GetRowNum() != 0) {
    auto s = RecordAndWriteBlockToFile();
    if (s == FAIL) {
      return FAIL;
    }
  }
  uint64_t current_offset = last_segment_file_->GetFileSize();
  assert(block_info_buffer_.size() == block_index_buffer_.size());
  uint32_t nblock = block_index_buffer_.size();
  std::string buffer;
  for (uint32_t i = 0; i < nblock; ++i) {
    auto offset = buffer.size();
    EncodeBlockInfo(&buffer, block_info_buffer_[i]);
    auto length = buffer.size() - offset;
    block_index_buffer_[i].info_offset = current_offset + offset;
    block_index_buffer_[i].length = length;
  }
  TsLastSegmentFooter footer_;
  footer_.magic_number = FOOTER_MAGIC;
  footer_.n_data_block = nblock;
  footer_.file_version = 1;
  footer_.block_info_idx_offset = current_offset + buffer.size();

  [[maybe_unused]] std::tuple<TSEntityID, timestamp64> prev{0, INT64_MIN};
  for (uint32_t i = 0; i < nblock; ++i) {
    const auto& index = block_index_buffer_[i];
    EncodeBlockIndex(&buffer, index);

    std::tuple<TSEntityID, timestamp64> current{index.min_entity_id, index.min_ts};
    // assert(current >= prev);
    prev = current;
  }

  std::vector<size_t> meta_block_offset;
  std::vector<size_t> meta_block_size;

  for (int i = 0; i < meta_blocks_.size(); ++i) {
    std::string tmp;
    meta_blocks_[i]->Serialize(&tmp);
    if (!tmp.empty()) {
      meta_block_offset.push_back(current_offset + buffer.size());
      meta_block_size.push_back(tmp.size());
    }
    buffer.append(tmp);
  }

  footer_.meta_block_idx_offset = current_offset + buffer.size();
  for (int i = 0; i < meta_block_offset.size(); ++i) {
    PutFixed64(&buffer, meta_block_offset[i]);
    PutFixed64(&buffer, meta_block_size[i]);
  }
  footer_.n_meta_block = meta_block_offset.size();
  EncodeFooter(&buffer, footer_);
  auto s = last_segment_file_->Append(buffer);
  last_segment_file_->Sync();
  last_segment_file_.reset();
  return s;
}

TS_LSN TsLastSegmentBuilder::GetMaxLSN() const {
  auto it = std::max_element(
      block_index_buffer_.begin(), block_index_buffer_.end(),
      [](const TsLastSegmentBlockIndex& a, const TsLastSegmentBlockIndex& b) { return a.max_lsn < b.max_lsn; });
  return it == block_index_buffer_.end() ? 0 : it->max_lsn;
}

KStatus TsLastSegmentBuilder::RecordAndWriteBlockToFile() {
  if (metric_block_builder_ == nullptr || metric_block_builder_->GetRowNum() == 0) {
    return SUCCESS;
  }
  assert(metric_block_builder_ != nullptr);
  assert(entity_id_buffer_.size() == metric_block_builder_->GetRowNum());
  auto metric_block = metric_block_builder_->GetMetricBlock();

  TsLastSegmentBlockInfo block_info;
  block_info.ncol = metric_block->GetColNum();
  block_info.nrow = metric_block->GetRowNum();
  block_info.block_offset = last_segment_file_->GetFileSize();

  auto index = block_index_collector_->GetIndex();

  // 1. compress entityid first;
  TSSlice entity_id_slice{reinterpret_cast<char*>(entity_id_buffer_.data()),
                          entity_id_buffer_.size() * sizeof(TSEntityID)};
  const auto& mgr = CompressorManager::GetInstance();
  std::string compressed_data;
  bool ok = mgr.CompressData(entity_id_slice, nullptr, entity_id_buffer_.size(), &compressed_data, TsCompAlg::kPlain,
                             GenCompAlg::kPlain);
  if (!ok) {
    return FAIL;
  }
  auto s = last_segment_file_->Append(compressed_data);
  if (s == FAIL) {
    return s;
  }

  block_info.entity_id_len = compressed_data.size();

  TsMetricCompressInfo compress_info;
  compressed_data.clear();
  ok = metric_block->GetCompressedData(&compressed_data, &compress_info, false, false);
  if (!ok) {
    return FAIL;
  }
  s = last_segment_file_->Append(compressed_data);
  if (s == FAIL) {
    return s;
  }

  index.info_offset = -1;
  index.length = -1;
  block_index_buffer_.push_back(std::move(index));

  block_info.lsn_len = compress_info.lsn_len;
  block_info.col_infos.resize(block_info.ncol);
  for (int i = 0; i < block_info.ncol; ++i) {
    block_info.col_infos[i].offset = compress_info.column_data_segments[i].offset;
    block_info.col_infos[i].bitmap_len = compress_info.column_compress_infos[i].bitmap_len;
    block_info.col_infos[i].fixdata_len = compress_info.column_compress_infos[i].fixdata_len;
    block_info.col_infos[i].vardata_len = compress_info.column_compress_infos[i].vardata_len;
  }
  block_info_buffer_.push_back(std::move(block_info));

  metric_block_builder_->Reset();
  block_index_collector_->Reset();
  entity_id_buffer_.clear();
  return SUCCESS;
}

void TsLastSegmentBuilder::BlockIndexCollector::Collect(TsBlockSpan* span) {
  max_entity_id_ = std::max(max_entity_id_, span->GetEntityID());
  min_entity_id_ = std::min(min_entity_id_, span->GetEntityID());
  max_ts_ = std::max(max_ts_, span->GetLastTS());
  min_ts_ = std::min(min_ts_, span->GetFirstTS());
  if (first_ts_ == std::numeric_limits<timestamp64>::max()) {
    first_ts_ = span->GetFirstTS();
  }
  last_ts_ = span->GetLastTS();

  TS_LSN min_lsn = std::numeric_limits<TS_LSN>::max();
  TS_LSN max_lsn = std::numeric_limits<TS_LSN>::min();
  for (int i = 0; i < span->GetRowNum(); ++i) {
    TS_LSN* lsn_addr = span->GetLSNAddr(i);
    min_lsn = std::min(min_lsn, *lsn_addr);
    max_lsn = std::max(max_lsn, *lsn_addr);
  }
  min_lsn_ = std::min(min_lsn_, min_lsn);
  max_lsn_ = std::max(max_lsn_, max_lsn);
  if (first_lsn_ == std::numeric_limits<TS_LSN>::max()) {
    first_lsn_ = span->GetFirstLSN();
  }
  last_lsn_ = span->GetLastLSN();
}

TsLastSegmentBlockIndex TsLastSegmentBuilder::BlockIndexCollector::GetIndex() const {
  TsLastSegmentBlockIndex index;
  index.info_offset = 0;
  index.length = 0;
  index.table_id = table_id_;
  index.table_version = version_;
  index.n_entity = -1;
  index.min_ts = min_ts_;
  index.max_ts = max_ts_;
  index.first_ts = first_ts_;
  index.last_ts = last_ts_;
  index.min_lsn = min_lsn_;
  index.max_lsn = max_lsn_;
  index.first_lsn = first_lsn_;
  index.last_lsn = last_lsn_;
  index.min_entity_id = min_entity_id_;
  index.max_entity_id = max_entity_id_;
  return index;
}

}  // namespace kwdbts
