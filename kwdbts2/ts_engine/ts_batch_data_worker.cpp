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
#include "ee_tag_row_batch.h"
#include "ts_engine.h"

namespace kwdbts {

TsReadBatchDataWorker::TsReadBatchDataWorker(TSEngineV2Impl* ts_engine, TSTableID table_id,
                                             uint64_t table_version, KwTsSpan ts_span, uint64_t job_id,
                                             vector<EntityResultIndex> entity_indexes)
                                             : TsBatchDataWorker(job_id), ts_engine_(ts_engine), table_id_(table_id),
                                               table_version_(table_version), ts_span_(ts_span), actual_ts_span_(ts_span),
                                               entity_indexes_(std::move(entity_indexes)) {}

KStatus TsReadBatchDataWorker::GetTagValue(kwdbContext_p ctx) {
  // get tag schema
  std::vector<TagInfo> tags_info;
  KStatus s = schema_->GetTagMeta(table_version_, tags_info);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagMeta failed");
    return KStatus::FAIL;
  }
  std::vector<uint32_t> scan_tags;
  for (int i = 0; i < tags_info.size(); ++i) {
    scan_tags.push_back(i);
  }

  // init tag iterator
  ResultSet res(scan_tags.size());
  uint32_t count;
  s = ts_table_->GetTagList(ctx, {cur_entity_index_}, scan_tags, &res, &count, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTagIterator failed");
    return KStatus::FAIL;
  }
  if (count != 1) {
    LOG_ERROR("GetTagData failed, count=%d", count);
    return KStatus::FAIL;
  }

  // tag data
  uint32_t tag_value_bitmap_len_ = (tags_info.size() + 7) / 8;  // bitmap
  uint32_t tag_value_len_ = tag_value_bitmap_len_;
  for (const auto& tag : tags_info) {
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
  std::string tag_data;
  tag_data.resize(tag_value_len_);
  assert(res.col_num_ == tags_info.size());
  uint32_t tag_data_start_offset = cur_batch_data_.tags_data_offset_;
  for (int tag_idx = 0; tag_idx < res.col_num_; ++tag_idx) {
    const Batch* col_batch = res.data[tag_idx][0];
    bool is_null = false;
    if (!tags_info[tag_idx].isPrimaryTag()) {
      s = col_batch->isNull(0, &is_null);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("tag col value isNull failed");
        return s;
      }
    }
    if (is_null) {
      set_null_bitmap(reinterpret_cast<unsigned char *>(tag_data.data()), tag_idx);
      continue;
    } else {
      unset_null_bitmap(reinterpret_cast<unsigned char *>(tag_data.data()), tag_idx);
    }
    if (!tags_info[tag_idx].isPrimaryTag() && isVarLenType(tags_info[tag_idx].m_data_type)) {
      uint64_t offset = tag_data.size();
      memcpy(tag_data.data() + tag_value_bitmap_len_ + tags_info[tag_idx].m_offset, &offset, sizeof(uint64_t));
      uint16_t var_data_len = col_batch->getVarColDataLen(0);
      tag_data.append(reinterpret_cast<const char *>(&var_data_len), sizeof(uint16_t));
      tag_data.append(reinterpret_cast<const char *>(col_batch->getVarColData(0)), var_data_len);
    } else {
      int null_bitmap_size = tags_info[tag_idx].isPrimaryTag() ? 0 : 1;
      memcpy(tag_data.data() + tag_value_bitmap_len_ + tags_info[tag_idx].m_offset,
             reinterpret_cast<char*>(col_batch->mem) + null_bitmap_size, tags_info[tag_idx].m_size);
    }
  }

  cur_batch_data_.AddTags({tag_data.data(), tag_data.size()});
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::AddTsBlockSpanInfo(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan>& block_span) {
  timestamp64 first_row_ts = block_span->GetTS(0);
  timestamp64 end_row_ts = block_span->GetTS(block_span->GetRowNum() - 1);
  uint32_t n_rows = block_span->GetRowNum();
  cur_batch_data_.AddBlockSpanDataHeader(0, first_row_ts, end_row_ts, n_cols_, n_rows);
  return KStatus::SUCCESS;
}

KStatus TsReadBatchDataWorker::NextBlockSpansIterator() {
  if (entity_indexes_.empty()) {
    is_finished_ = true;
    return KStatus::SUCCESS;
  }
  // init iterator
  cur_entity_index_ = entity_indexes_[entity_indexes_.size() - 1];
  entity_indexes_.pop_back();
  std::list<std::shared_ptr<TsBlockSpan>> block_spans;
  KStatus s = ts_engine_->GetTsVGroup(cur_entity_index_.subGroupId)->GetBlockSpans(table_id_, cur_entity_index_.entityId,
                                                                                   actual_ts_span_, ts_col_type_, schema_,
                                                                                   table_version_, &block_spans);
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
    block_spans_iterator_ = std::make_shared<TsBlockSpanSortedIterator>(block_spans, EngineOptions::g_dedup_rule);
    s = block_spans_iterator_->Init();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsReadBatchDataWorker::Init failed, failed to init block span iterator, "
                "table_id[%lu], table_version[%lu], entity_id[%u]",
                table_id_, table_version_, cur_entity_index_.entityId);
    }
  }
  return s;
}

KStatus TsReadBatchDataWorker::Init(kwdbContext_p ctx) {
  ErrorInfo err_info;
  KStatus s = ts_engine_->GetTsTable(ctx, table_id_, ts_table_, true, err_info, table_version_);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTsTable[%lu] failed, %s", table_id_, err_info.toString().c_str());
    return KStatus::FAIL;
  }
  s = ts_engine_->GetTableSchemaMgr(ctx, table_id_, schema_);
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
  const vector<AttributeInfo>& attrs = metric_schema->getSchemaInfoExcludeDropped();
  assert(!attrs.empty());
  n_cols_ = attrs.size() + 1;  // add lsn
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
  memset(buffer, 0, sizeof(buffer));
  std::snprintf(buffer, sizeof(buffer), "%lu-%d-%lu-%lu-%ld-%ld", table_id, table_version,
                begin_hash, end_hash, ts_span.begin, ts_span.end);
  return buffer;
}

KStatus TsReadBatchDataWorker::GenerateBatchData(kwdbContext_p ctx, std::shared_ptr<TsBlockSpan> block_span) {
  cur_batch_data_.Clear();
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
    AddTsBlockSpanInfo(ctx, block_span);
    // get compress data
    s = block_span->GetCompressData(cur_batch_data_.data_);
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
  *row_num = 0;
  data->data = nullptr;
  data->len = 0;
  if (is_finished_) {
    return KStatus::SUCCESS;
  }
  // get next ts block span
  std::shared_ptr<TsBlockSpan> cur_block_span;
  bool block_span_read_finished = false;
  bool cur_entity_tag_only = false;
  while (!is_finished_) {
    if (block_spans_iterator_ == nullptr) {
      cur_entity_tag_only = true;
      // generate batch data
      KStatus s = GenerateBatchData(ctx, nullptr);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GenerateBatchData failed");
        return s;
      }
      *row_num = 1;
      data->data = cur_batch_data_.data_.data();
      data->len = cur_batch_data_.data_.size();
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
  if (is_finished_) {
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
  data->data = cur_batch_data_.data_.data();
  data->len = cur_batch_data_.data_.size();
  LOG_INFO("current batch data read success, job_id[%lu], table_id[%lu], table_version[%lu], row_num[%u]",
           job_id_, table_id_, table_version_, *row_num);
  return s;
}

TsWriteBatchDataWorker::TsWriteBatchDataWorker(TSEngineV2Impl* ts_engine, uint64_t job_id)
                                               : TsBatchDataWorker(job_id), ts_engine_(ts_engine) {}

KStatus TsWriteBatchDataWorker::Init(kwdbContext_p ctx) {
  auto vgroups = ts_engine_->GetTsVGroups();
  for (uint32_t vgroup_id = 0; vgroup_id < vgroups->size(); ++vgroup_id) {
    vgroups_lsn_[vgroup_id] = (*vgroups)[vgroup_id]->GetWALManager()->FetchCurrentLSN();
  }
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::GetTagPayload(uint32_t table_version, TSSlice* data, std::string& tag_payload_str) {
  tag_payload_str.clear();
  tag_payload_str.append(data->data, data->len);
  // update table version
  memcpy(tag_payload_str.data() + TsBatchData::ts_version_offset_, &table_version, TsBatchData::ts_version_size_);
  // update tag row num
  uint32_t row_num = 1;
  memcpy(tag_payload_str.data() + TsBatchData::row_num_offset_, &row_num, TsBatchData::row_num_size_);
  // update tag type
  uint8_t tag_type = DataTagFlag::TAG_ONLY;
  memcpy(tag_payload_str.data() + TsBatchData::row_type_offset_, &tag_type, TsBatchData::row_type_size_);
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::UpdateLSN(uint32_t vgroup_id, TSSlice* input, std::string& result) {
  // header
  result.append(input->data, TsBatchData::block_span_data_header_size_);
  // block data
  uint32_t n_cols = *reinterpret_cast<uint32_t*>(input->data + TsBatchData::n_cols_offset_in_span_data_);
  uint32_t n_rows = *reinterpret_cast<uint32_t*>(input->data + TsBatchData::n_rows_offset_in_span_data_);
  std::vector<uint32_t> block_col_offsets;
  for (uint32_t idx = 0; idx < n_cols; ++idx) {
    block_col_offsets.push_back(*reinterpret_cast<uint32_t*>(input->data + TsBatchData::block_span_data_header_size_ +
                                idx * sizeof(uint32_t)));
  }
  // agg block length
  uint32_t agg_block_length = *reinterpret_cast<uint32_t*>(input->data + TsBatchData::block_span_data_header_size_
                    + n_cols * sizeof(uint32_t) + block_col_offsets[n_cols - 1] + (n_cols - 2) * sizeof(uint32_t));
  assert(input->len == TsBatchData::block_span_data_header_size_ + (n_cols * 2 - 1) * sizeof(uint32_t)
                    + block_col_offsets[n_cols - 1] + agg_block_length);
  // column_block_data without lsn
  uint32_t column_block_offset_without_lsn = TsBatchData::block_span_data_header_size_ + n_cols * sizeof(uint32_t)
                                             + block_col_offsets[0];
  TSSlice data = {input->data + column_block_offset_without_lsn, input->len - column_block_offset_without_lsn};
  // lsn
  std::string lsn_data;
  lsn_data.resize(sizeof(uint64_t) * n_rows);
  for (uint32_t row_idx = 0; row_idx < n_rows; ++row_idx) {
    memcpy(lsn_data.data() + row_idx * sizeof(uint64_t), &(vgroups_lsn_[vgroup_id - 1]), sizeof(uint64_t));
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
  int32_t offset = (int32_t)new_lsn_size - old_lsn_size;
  for (uint32_t idx = 0; idx < n_cols; ++idx) {
    block_col_offsets[idx] += offset;
    result.append(reinterpret_cast<const char *>(&block_col_offsets[idx]), sizeof(uint32_t));
  }
  // append lsn
  result.append(compressed.data(), compressed.size());
  // append other column block data
  result.append(data.data, data.len);
  // block span length
  uint32_t length = (uint32_t)result.size();
  memcpy(result.data(), &length, sizeof(uint32_t));
  assert(length == TsBatchData::block_span_data_header_size_ + (n_cols * 2 - 1) * sizeof(uint32_t)
                   + block_col_offsets[n_cols - 1] + agg_block_length);
  return KStatus::SUCCESS;
}

KStatus TsWriteBatchDataWorker::Write(kwdbContext_p ctx, TSTableID table_id, uint32_t table_version,
                                      TSSlice* data, uint32_t* row_num) {
  // get or create ts table
  ErrorInfo err_info;
  std::shared_ptr<TsTable> ts_table;
  KStatus s = ts_engine_->GetTsTable(ctx, table_id, ts_table, true, err_info, table_version);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTsTable[%lu] failed, %s", table_id, err_info.toString().c_str());
    return KStatus::FAIL;
  }
  // get table schema && create ts table
  std::shared_ptr<TsTableSchemaManager> schema = nullptr;
  s = ts_engine_->GetTableSchemaMgr(ctx, table_id, schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetTableSchemaMgr[%lu] failed", table_id);
    return KStatus::FAIL;
  }
  // parser tag info
  uint16_t p_tag_size = KUint16(data->data + TsBatchData::header_size_);
  uint32_t p_tag_offset = TsBatchData::header_size_ + sizeof(p_tag_size);
  uint32_t tags_data_offset = p_tag_offset + p_tag_size + sizeof(uint32_t);
  uint32_t tags_data_size = KUint32(data->data + tags_data_offset - sizeof(uint32_t));
  uint8_t row_type = *reinterpret_cast<uint8_t*>(data->data + TsBatchData::row_type_offset_);
  uint32_t block_span_data_offset = tags_data_offset + tags_data_size;
  uint32_t block_span_data_size = 0;
  if (block_span_data_offset == data->len) {
    row_type = DataTagFlag::TAG_ONLY;
  }
  if (row_type == DataTagFlag::DATA_AND_TAG) {
    block_span_data_size = KUint32(data->data + block_span_data_offset);
  }
  assert(data->len == block_span_data_offset + block_span_data_size);

  TSSlice tag_slice = {data->data, tags_data_offset + tags_data_size};
  std::string tag_payload_str;
  GetTagPayload(table_version, &tag_slice, tag_payload_str);
  // insert tag record
  uint32_t vgroup_id;
  TSEntityID entity_id;
  uint16_t entity_cnt;
  s = ts_engine_->InsertTagData(ctx, table_id, 0, {tag_payload_str.data(), tag_payload_str.size()}, false,
                                vgroup_id, entity_id, &entity_cnt);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("InsertTagData[%lu] failed, %s", table_id, err_info.toString().c_str());
    return KStatus::FAIL;
  }

  if (row_type == TAG_ONLY) {
    *row_num = KUint32(data->data + TsBatchData::row_num_offset_);
    LOG_INFO("current batch data write success, job_id[%lu], table_id[%lu], vgroup_id[%u], entity_id[%lu], row_num[%u]",
             job_id_, table_id, vgroup_id, entity_id, *row_num);
    return KStatus::SUCCESS;
  }

  if (schema == nullptr) {
    s = ts_engine_->GetTableSchemaMgr(ctx, table_id, schema);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetTableSchemaMgr[%lu] failed", table_id);
      return KStatus::FAIL;
    }
  }
  // update lsn
  TSSlice block_span_slice = {data->data + block_span_data_offset, block_span_data_size};
  std::string block_span_data;
  UpdateLSN(vgroup_id, &block_span_slice, block_span_data);
  TSSlice new_block_data = {block_span_data.data(), block_span_data.size()};
  // write payload data to entity segment
  timestamp64 ts = *reinterpret_cast<timestamp64*>(new_block_data.data + sizeof(uint32_t));
  std::shared_ptr<MMapMetricsTable> metric_schema;
  s = schema->GetMetricSchema(table_version, &metric_schema);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("Failed to get metric schema, table id[%lu], version[%u]", table_id, table_version);
    return KStatus::FAIL;
  }
  const vector<AttributeInfo>& attrs = metric_schema->getSchemaInfoExcludeDropped();
  assert(!attrs.empty());
  DATATYPE ts_col_type = static_cast<DATATYPE>(attrs[0].type);
  s = ts_engine_->GetTsVGroup(vgroup_id)->WriteBatchData(ctx, table_id, table_version,
                                                         entity_id, ts, ts_col_type, new_block_data);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("WriteBatchData failed, table_id[%lu], entity_id[%lu]", table_id, entity_id);
    return KStatus::FAIL;
  }
  *row_num = KUint32(data->data + TsBatchData::row_num_offset_);
  LOG_INFO("current batch data write success, job_id[%lu], table_id[%lu], vgroup_id[%u], entity_id[%lu], row_num[%u]",
           job_id_, table_id, vgroup_id, entity_id, *row_num);
  return s;
}

KStatus TsWriteBatchDataWorker::Finish(kwdbContext_p ctx) {
  auto vgroups = ts_engine_->GetTsVGroups();
  for (const auto& vgroup : *vgroups) {
    KStatus s = vgroup->FinishWriteBatchData();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("FinishWriteBatchData failed, job_id[%lu]", job_id_);
      return s;
    }
  }
  return KStatus::SUCCESS;
}

void TsWriteBatchDataWorker::Cancel(kwdbContext_p ctx) {
  auto vgroups = ts_engine_->GetTsVGroups();
  for (const auto &vgroup : *vgroups) {
    KStatus s = vgroup->ClearWriteBatchData();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("ClearWriteBatchData failed, job_id[%lu]", job_id_);
    }
  }
}
}  // namespace kwdbts
