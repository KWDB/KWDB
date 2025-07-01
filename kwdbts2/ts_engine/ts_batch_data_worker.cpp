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

#include "ts_batch_data_worker.h"
#include "ts_engine.h"

namespace kwdbts {

TsReadBatchDataWorker::TsReadBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id,
                                             uint32_t table_version, KwTsSpan ts_span, uint64_t job_id,
                                             std::queue<std::pair<uint32_t, uint32_t>> vgroup_entity_ids)
                                             : TsBatchDataWorker(job_id), ts_engine_(ts_engine), table_id_(table_id),
                                               table_version_(table_version), ts_span_(ts_span), actual_ts_span_(ts_span),
                                               vgroup_entity_ids_(std::move(vgroup_entity_ids)) {}

KStatus TsReadBatchDataWorker::GetTagData(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span) {
  cur_block_span_data_.clear();
  std::shared_ptr<TagTable> tag_table;
  KStatus ret = schema_->GetTagSchema(ctx, &tag_table);
  if (ret != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  roachpb::CreateTsTable meta;
  KStatus s = tag_table->GetMeta(table_id_, table_version_, &meta);
  if (s != KStatus::SUCCESS) {
    return KStatus::FAIL;
  }
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < meta.k_column_size(); ++i) {
    scan_tags.push_back(meta.k_column(i).column_id());
  }
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::GetTsBlockSpanInfo(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span) {
  // block span length
  uint32_t block_span_data_len = 0;
  cur_block_span_data_.append((char*)&block_span_data_len, sizeof(uint32_t));
  // first row ts
  timestamp64 first_row_ts = block_span->GetTS(0);
  cur_block_span_data_.append((char*)&first_row_ts, sizeof(timestamp64));
  // end tow ts
  timestamp64 end_row_ts = block_span->GetTS(block_span->GetRowNum() - 1);
  cur_block_span_data_.append((char*)&end_row_ts, sizeof(timestamp64));
  // ncols
  cur_block_span_data_.append((char*)&n_cols_, sizeof(uint32_t));
  // nrows
  uint32_t n_rows = block_span->GetRowNum();
  cur_block_span_data_.append((char*)&n_rows, sizeof(uint32_t));
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::NextBlockSpansIterator() {
  if (vgroup_entity_ids_.empty()) {
    is_finished_ = true;
    return KStatus::SUCCESS;
  }
  // init iterator
  std::pair<uint32_t, uint32_t> vgroup_entity_id = vgroup_entity_ids_.front();
  vgroup_entity_ids_.pop();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  KStatus s = ts_engine_->GetTsVGroup(vgroup_entity_id.first)->GetBlockSpans(table_id_, vgroup_entity_id.second,
                                                                             actual_ts_span_, ts_col_type_, schema_,
                                                                             table_version_, &block_spans);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsReadBatchDataWorker::Init failed, failed to get block span, "
              "table_id[%lu], table_version[%u], entity_id[%u]",
              table_id_, table_version_, vgroup_entity_id.second);
    return s;
  }
  // filter block span
  auto it = block_spans.begin();
  while (it != block_spans.end()) {
    if (it->get()->GetTableVersion() > table_version_) {
      it = block_spans.erase(it);
    }
  }
  // init block span iterator
  block_spans_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(block_spans, EngineOptions::g_dedup_rule);
  s = block_spans_iterator_->Init();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsReadBatchDataWorker::Init failed, failed to init block span iterator, "
              "table_id[%lu], table_version[%u], entity_id[%u]",
              table_id_, table_version_, vgroup_entity_id.second);
  }
  return s;
}

KStatus TsReadBatchDataWorker::Init(kwdbContext_p ctx) {
  KStatus s = ts_engine_->GetTableSchemaMgr(ctx, table_id_, schema_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr[%lu] failed", table_id_);
    return KStatus::FAIL;
  }
  // update ts span with lifetime
  auto life_time = schema_->GetLifeTime();
  if (life_time.ts != 0) {
    int64_t acceptable_ts = INT64_MIN;
    auto now = std::chrono::time_point_cast<std::chrono::seconds>(std::chrono::system_clock::now());
    acceptable_ts = now.time_since_epoch().count() - life_time.ts;
    actual_ts_span_.begin = acceptable_ts;
  }
  // convert second to actual timestamp
  std::shared_ptr<MMapMetricsTable> metric_schema;
  s = schema_->GetMetricSchema(table_version_, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get metric schema, table id[%lu], version[%u]", table_id_, table_version_);
    return KStatus::FAIL;
  }
  const vector<AttributeInfo>& attrs = metric_schema->getSchemaInfoExcludeDropped();
  assert(!attrs.empty());
  n_cols_ = attrs.size() + 1; // add lsn
  ts_col_type_ = static_cast<DATATYPE>(attrs[0].type);
  actual_ts_span_.begin = convertSecondToPrecisionTS(actual_ts_span_.begin, ts_col_type_);
  actual_ts_span_.end = convertSecondToPrecisionTS(actual_ts_span_.end, ts_col_type_);
  if (actual_ts_span_.begin > actual_ts_span_.end) {
    is_finished_ = true;
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
  std::snprintf(buffer, sizeof(buffer), "%lu-%d-%lu-%lu-%ld-%ld", table_id, table_version,
                begin_hash, end_hash, ts_span.begin, ts_span.end);
  return buffer;
}

KStatus TsReadBatchDataWorker::Read(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) {
  if (is_finished_) {
    *row_num = 0;
    return KStatus::SUCCESS;
  }
  // get next ts block span
  std::shared_ptr<TsBlockSpan> cur_block_span;
  bool block_span_read_finished = false;
  while (!block_span_read_finished) {
    block_spans_iterator_->Next(cur_block_span, &block_span_read_finished);
    if (!block_span_read_finished) {
      KStatus s = NextBlockSpansIterator();
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("NextBlockSpansIterator failed");
        return s;
      }
      if (is_finished_) {
        *row_num = 0;
        return KStatus::SUCCESS;
      }
    }
  }
  // add tag value
  KStatus s = GetTagData(ctx, cur_block_span);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagData failed");
    return s;
  }
  // add TsBlockSpan info
  s = GetTsBlockSpanInfo(ctx, cur_block_span);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagData failed");
    return s;
  }
  // get compress data
  *row_num = cur_block_span->GetRowNum();
  s = cur_block_span->GetCompressData(cur_block_span_data_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BuildCompressData failed");
    return s;
  }
  // set ts block span data length
  uint32_t ts_block_span_len = cur_block_span_data_.size() - ts_block_span_len_offset;
  memcpy(cur_block_span_data_.data() + ts_block_span_len_offset, &ts_block_span_len, sizeof(ts_block_span_len));
  return s;
}

TsWriteBatchDataWorker::TsWriteBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id,
                                               uint32_t table_version, uint64_t job_id)
                                               : TsBatchDataWorker(job_id), ts_engine_(ts_engine),
                                               table_id_(table_id), table_version_(table_version) {}

KStatus TsWriteBatchDataWorker::Init(kwdbContext_p ctx) {
  KStatus s = ts_engine_->GetTableSchemaMgr(ctx, table_id_, schema_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr[%lu] failed", table_id_);
    return KStatus::FAIL;
  }
  auto vgroups = ts_engine_->GetTsVGroups();
  for (uint32_t vgroup_id = 1; vgroup_id < vgroups->size(); ++vgroup_id) {
    vgroups_lsn_[vgroup_id] = (*vgroups)[vgroup_id]->GetWALManager()->FetchCurrentLSN();
  }
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::GetTagPayload(TSSlice* data, std::shared_ptr<TsRawPayload>& payload_only_tag) {
  // table_version hash_point tags ptag
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::UpdateLSN(uint32_t vgroup_id, TSSlice* input, std::string& result) {
  // header
  uint32_t block_data_header_size = 2 * sizeof(timestamp64) + 3 * sizeof(uint32_t);
  result.append(input->data, block_data_header_size);
  // block data
  uint32_t n_cols = *(uint32_t*)(input->data + sizeof(uint32_t) + 2 * sizeof(timestamp64));
  uint32_t n_rows = *(uint32_t*)(input->data + 2 * sizeof(uint32_t) + 2 * sizeof(timestamp64));
  std::vector<uint32_t> block_col_offsets;
  for (uint32_t idx = 0; idx < n_cols; ++idx) {
    block_col_offsets.push_back(*(uint32_t*)(input->data + block_data_header_size + idx * sizeof(uint32_t)));
  }
  // column_block_data without lsn
  TSSlice data = {input->data + block_data_header_size + n_cols * sizeof(uint32_t) + block_col_offsets[0],
                  block_col_offsets[n_cols - 1] - block_col_offsets[0]};
  // lsn
  std::string lsn_data;
  lsn_data.resize(sizeof(uint64_t) * n_rows);
  for (uint32_t row_idx = 0; row_idx < n_rows; ++row_idx) {
    memcpy(lsn_data.data() + row_idx * sizeof(uint64_t), &(vgroups_lsn_[vgroup_id]), sizeof(uint64_t));
  }
  DATATYPE d_type = DATATYPE::INT64;
  size_t d_size = sizeof(uint64_t);
  std::string compressed;
  const auto& mgr = CompressorManager::GetInstance();
  auto [first, second] = mgr.GetDefaultAlgorithm(d_type);
  TSSlice plain{lsn_data.data(), n_rows * d_size};
  mgr.CompressData(plain, nullptr, n_rows, &compressed, first, second);
  // update offset
  uint32_t old_lsn_size = block_col_offsets[0];
  uint32_t new_lsn_size = compressed.size();
  int32_t offset = new_lsn_size - old_lsn_size;
  for (uint32_t idx = 0; idx < n_cols; ++idx) {
    block_col_offsets[idx] += offset;
    result.append(reinterpret_cast<const char *>(&block_col_offsets[idx]), sizeof(uint32_t));
  }
  // append lsn
  result.append(compressed.data(), compressed.size());
  // append other column block data
  result.append(data.data, data.len);
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::Write(kwdbContext_p ctx, TSSlice* data, int32_t* row_num) {
  std::shared_ptr<TsRawPayload> payload_only_tag;
  KStatus s = GetTagPayload(data, payload_only_tag);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagPayload failed");
    return s;
  }

  uint32_t vgroup_id;
  TSEntityID entity_id;
  bool new_tag;
  s = ts_engine_->GetEngineSchemaManager()->GetVGroup(ctx, table_id_, payload_only_tag->GetPrimaryTag(),
                                                      &vgroup_id, &entity_id, &new_tag);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetVGroup failed, table_id[%lu], ptag[%s]", table_id_, payload_only_tag->GetPrimaryTag().data);
    return s;
  }

  // insert tag record
  if (new_tag) {
    std::shared_ptr<TagTable> tag_table = schema_->GetTagTable();
    if (tag_table->InsertTagRecord(*payload_only_tag, vgroup_id, entity_id) < 0) {
      LOG_ERROR("InsertTagRecord failed, table_id[%lu], ptag[%s]", table_id_, payload_only_tag->GetPrimaryTag().data);
      return KStatus::FAIL;
    }
  }

  // update lsn
  std::string result;
  UpdateLSN(vgroup_id, data, result);
  TSSlice new_block_data = {result.data(), result.size()};
  // write payload data to entity segment
  timestamp64 ts = *(timestamp64*)(new_block_data.data + sizeof(timestamp64));
  std::shared_ptr<MMapMetricsTable> metric_schema;
  s = schema_->GetMetricSchema(table_version_, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get metric schema, table id[%lu], version[%u]", table_id_, table_version_);
    return KStatus::FAIL;
  }
  const vector<AttributeInfo>& attrs = metric_schema->getSchemaInfoExcludeDropped();
  assert(!attrs.empty());
  DATATYPE ts_col_type = static_cast<DATATYPE>(attrs[0].type);
  s = ts_engine_->GetTsVGroup(vgroup_id)->WriteBatchData(ctx, table_id_, table_version_, entity_id, ts, ts_col_type, new_block_data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("WriteBatchData failed, table_id[%lu], entity_id[%lu]", table_id_, entity_id);
  }
  return s;
}

KStatus TsWriteBatchDataWorker::Finish(kwdbContext_p ctx) {
  auto vgroups = ts_engine_->GetTsVGroups();
  for (const auto& vgroup : *vgroups) {
    KStatus s = vgroup->FinishWriteBatchData();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("FinishWriteBatchData failed, table_id[%lu]", table_id_);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

void TsWriteBatchDataWorker::Cancel(kwdbContext_p ctx) {
  auto vgroups = ts_engine_->GetTsVGroups();
  for (const auto &vgroup: *vgroups) {
    KStatus s = vgroup->ClearWriteBatchData();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("ClearWriteBatchData failed, table_id[%lu]", table_id_);
    }
  }
}
}  // namespace kwdbts
