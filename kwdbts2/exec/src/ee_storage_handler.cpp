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
// Created by liguoliang on 2022/07/18.

#include "ee_storage_handler.h"

#include "cm_func.h"
#include "ee_field.h"
#include "ee_global.h"
#include "ee_kwthd_context.h"
#include "ee_scan_row_batch.h"
#include "ee_table.h"
#include "ee_tag_scan_op.h"
#include "engine.h"
#include "iterator.h"
#include "lg_api.h"
#include "tag_iterator.h"
#include "ts_table.h"
#include "ee_hash_tag_row_batch.h"

namespace kwdbts {

StorageHandler::~StorageHandler() {
  table_ = nullptr;
  Close();
}

EEIteratorErrCode StorageHandler::Init(kwdbContext_p ctx) {
  EnterFunc();
  ErrorInfo err_info;
  KStatus ret = KStatus::FAIL;
  TSEngine *ts_engine = static_cast<TSEngine *>(ctx->ts_engine);
  if (ts_engine)
    ret = ts_engine->GetTsTable(ctx, table_->object_id_, ts_table_, true, err_info, table_->table_version_);
  if (ret == KStatus::FAIL) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail when getting ts table");
    LOG_ERROR("GetTsTable Failed, table id: %lu", table_->object_id_);
    Return(EEIteratorErrCode::EE_ERROR);
  }
  Return(EEIteratorErrCode::EE_OK);
}

k_uint32 StorageHandler::GetStorageOffset() {
  return ts_iterator->GetFilterCount();
}

void StorageHandler::SetSpans(std::vector<KwTsSpan> *ts_spans) {
  ts_spans_ = ts_spans;
}

EEIteratorErrCode StorageHandler::HandleTsItrAndGetTagData(
    kwdbContext_p ctx, ScanRowBatch *row_batch, bool init_itr) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  if (init_itr) {
    code = NewTsIterator(ctx);
    if (code != EEIteratorErrCode::EE_OK) {
      return code;
    }
  }
  return GetNextTagData(ctx, row_batch);
}

EEIteratorErrCode StorageHandler::TsNextAndFilter(
    kwdbContext_p ctx, Field *filter, k_uint32 *cur_offset, k_int32 limit,
    ScanRowBatch *row_batch, k_uint32 *total_read_row, k_uint32 *examined_rows) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  bool null_filter = (thd->wtyp_ == WindowGroupType::EE_WGT_EVENT) ||
                     (thd->wtyp_ == WindowGroupType::EE_WGT_STATE);
  while (true) {
    code = this->TsNext(ctx);
    if (EEIteratorErrCode::EE_OK != code) {
      break;
    }

    if (row_batch->count_ < 1) continue;

    if (!null_filter && filter == NULL && *cur_offset == 0 && limit == 0) {
      *total_read_row += row_batch->count_;
      *examined_rows += row_batch->count_;
      break;
    }
    if (null_filter) {
      thd->window_field_->backup();
    }
    // filter || offset || limit
    for (int i = 0; i < row_batch->count_; ++i) {
      if (filter != NULL) {
        k_int64 ret = filter->ValInt();
        if (0 == ret) {
          row_batch->NextLine();
          ++(*total_read_row);
          continue;
        }
      }
      if (null_filter) {
        if (thd->window_field_->is_nullable()) {
          row_batch->NextLine();
          ++(*total_read_row);
          continue;
        }
        thd->window_field_->ValInt();
      }
      if (*cur_offset > 0) {
        --(*cur_offset);
        row_batch->NextLine();
        ++(*total_read_row);
        continue;
      }

      if (limit && *examined_rows >= limit) {
        break;
      }

      row_batch->AddSelection();
      row_batch->NextLine();
      ++(*examined_rows);
      ++(*total_read_row);
    }
    if (null_filter) {
      thd->window_field_->restore();
    }
    if (0 != row_batch->GetSelection()->size()) {
      break;
    }
  }
  return code;
}

EEIteratorErrCode StorageHandler::TsNext(kwdbContext_p ctx) {
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  bool need_reset = (thd->wtyp_ == WindowGroupType::EE_WGT_EVENT);
  while (true) {
    ScanRowBatch* row_batch =
        static_cast<ScanRowBatch *>(thd->GetRowBatch());
    if (nullptr == ts_iterator) {
      code = HandleTsItrAndGetTagData(ctx, row_batch, true);
      if (code != EEIteratorErrCode::EE_OK) {
        return code;
      }
    }

    row_batch->Reset();
    KStatus ret = ts_iterator->Next(&row_batch->res_, &row_batch->count_, row_batch->ts_);
    if (KStatus::FAIL == ret) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail");
      LOG_ERROR("TsTableIterator::Next() Failed\n");
      code = EEIteratorErrCode::EE_ERROR;
      break;
    }
    total_read_rows_ += row_batch->count_;

    if (0 == row_batch->count_) {
      current_line_++;
      code = HandleTsItrAndGetTagData(ctx, row_batch,
                                      current_line_ >= entities_.size());
      if (code != EEIteratorErrCode::EE_OK) {
        return code;
      }
      if (need_reset) {
        thd->window_field_->reset();
      }
    } else {
      while (!entities_[current_line_].equalsWithoutMem(
             row_batch->res_.entity_index)) {
        current_line_++;
        code = HandleTsItrAndGetTagData(ctx, row_batch,
                                        current_line_ >= entities_.size());
        if (code != EEIteratorErrCode::EE_OK) {
          return code;
        }
        if (need_reset) {
          thd->window_field_->reset();
        }
      }
      row_batch->ResetLine();
      break;
    }
  }

  return code;
}

EEIteratorErrCode StorageHandler::TsOffsetNext(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KStatus ret = KStatus::FAIL;
  if (nullptr == ts_iterator) {
    KWThdContext *thd = current_thd;
    read_mode_ = static_cast<TSTableReadMode>(table_->GetAccessMode());
    if (TSTableReadMode::tagIndex == read_mode_ ||
          TSTableReadMode::tagIndexTable == read_mode_ ||
          TSTableReadMode::metaTable == read_mode_ ||
          (TSTableReadMode::tableTableMeta == read_mode_ && IsHasTagFilter())) {
      ScanRowBatch data_handle(table_);
      std::vector<EntityResultIndex> entities;
      while (true) {
        ret = tag_scan_->GetEntities(ctx, &entities, &(data_handle.tag_rowbatch_));
        if (KStatus::FAIL == ret) {
          break;
        }

        entities_.insert(entities_.end(), entities.begin(), entities.end());
      }

      if (entities_.empty()) {
        Return(EEIteratorErrCode::EE_END_OF_RECORD);
      }
    }

    ret = ts_table_->GetOffsetIterator(ctx, entities_, *ts_spans_, table_->scan_cols_,
                                table_->table_version_, &ts_iterator, table_->offset_, table_->limit_,
                                table_->is_reverse_);
    if (KStatus::FAIL == ret) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  ScanRowBatch* row_batch = static_cast<ScanRowBatch *>(current_thd->GetRowBatch());
  row_batch->Reset();
  ret = ts_iterator->Next(&row_batch->res_, &row_batch->count_, row_batch->ts_);
  if (KStatus::FAIL == ret) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                "scanning column data fail");
    LOG_ERROR("TsTableIterator::Next() Failed\n");
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }

  if (0 == row_batch->count_) {
    code = EEIteratorErrCode::EE_END_OF_RECORD;
    Return(code);
  }

  // 获取tag值
  TagRowBatchPtr tag_rowbatch = std::make_shared<TagRowBatch>();
  tag_rowbatch->Init(table_);
  ret = ts_table_->GetTagList(ctx, {row_batch->res_.entity_index}, table_->scan_tags_,
                                      &tag_rowbatch->res_, &tag_rowbatch->count_, table_->table_version_);
  if (KStatus::FAIL == ret) {
    LOG_ERROR("TsTable::GetTagList() Failed\n");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "get tag list fail");
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }
  row_batch->tag_rowbatch_ = tag_rowbatch;
  ret = tag_rowbatch->GetTagData(&(row_batch->tagdata_), &(row_batch->tag_bitmap_), row_batch->res_.entity_index.index);
  if (KStatus::FAIL == ret) {
    LOG_ERROR("GetTagData Failed\n");
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning tag data fail");
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }

  row_batch->ResetLine();

  Return(code);
}

EEIteratorErrCode StorageHandler::TsStatisticCacheNext(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  if (entities_.empty()) {
    EntityResultIndex entity;
    KStatus ret = ts_table_->GetLastRowEntity(entity);
    if (KStatus::FAIL == ret) {
      code = EEIteratorErrCode::EE_ERROR;
      Return(code);
    }
    entities_.push_back(entity);
  }

  if (entities_[0].entityId == 0) {
    code = EEIteratorErrCode::EE_END_OF_RECORD;
    Return(code);
  }
  if (nullptr == ts_iterator) {
    std::vector<KwTsSpan> ts_spans;
    ts_spans = *ts_spans_;
    KStatus ret = ts_table_->GetIterator(ctx, entities_, ts_spans, table_->scan_cols_,
                                 table_->scan_real_agg_types_, table_->table_version_,
                                 &ts_iterator, table_->scan_real_last_ts_points_,
                                 table_->is_reverse_, table_->ordered_scan_);
    if (KStatus::FAIL == ret) {
      code = EEIteratorErrCode::EE_ERROR;
      Return(code);
    }
  }

  ScanRowBatch* row_batch = static_cast<ScanRowBatch *>(current_thd->GetRowBatch());
  row_batch->Reset();
  KStatus ret = ts_iterator->Next(&row_batch->res_, &row_batch->count_, row_batch->ts_);
  if (KStatus::FAIL == ret) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                "scanning column data fail");
    LOG_ERROR("TsTableIterator::Next() Failed\n");
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }

  if (0 == row_batch->count_) {
    code = EEIteratorErrCode::EE_END_OF_RECORD;
    Return(code);
  }

  if (!table_->contain_tag_for_statistic) {
    Return(code);
  }

  // 获取tag值
  TagRowBatchPtr tag_rowbatch = std::make_shared<TagRowBatch>();
  tag_rowbatch->Init(table_);
  ret = ts_table_->GetTagList(ctx, {row_batch->res_.entity_index}, table_->scan_tags_,
                                      &tag_rowbatch->res_, &tag_rowbatch->count_, table_->table_version_);
  if (KStatus::FAIL == ret) {
    LOG_ERROR("TsTable::GetTagList() Failed\n");
    code = EEIteratorErrCode::EE_ERROR;
    Return(code);
  }
  row_batch->tag_rowbatch_ = tag_rowbatch;
  tag_rowbatch->GetTagData(&(row_batch->tagdata_), &(row_batch->tag_bitmap_),
                           row_batch->res_.entity_index.index);
  Return(code);
}

EEIteratorErrCode StorageHandler::NextTagDataChunk(kwdbContext_p ctx,
  TSTagReaderSpec *spec,
  Field *tag_filter,
  std::vector<void *> &primary_tags,
  std::vector<void *> &secondary_tags,
  const vector<k_uint32> tag_other_join_cols,
  Field ** renders,
  ColumnInfo* col_info,
  k_int32 col_info_size,
  DataChunkPtr &data_chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  if (primary_tags.size() > 0) {
    if (tag_iterator) {
      tag_iterator = nullptr;
      code = GetTagDataChunkWithPrimaryTags(ctx, spec, tag_filter, primary_tags, secondary_tags,
                                            tag_other_join_cols, renders, col_info, col_info_size, data_chunk);
    } else {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
    }
  } else {
    code = NextTagDataChunk(ctx, tag_filter, renders, col_info, col_info_size, data_chunk);
  }
  Return(code);
}

EEIteratorErrCode StorageHandler::NextTagDataChunk(kwdbContext_p ctx, Field *tag_filter, Field ** renders,
                                                   ColumnInfo* col_info,
                                                   k_int32 col_info_size, DataChunkPtr &data_chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  // Create tag_rowbatch for tag data
  tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
  tag_rowbatch_->Init(table_);
  RowBatch* ptr = current_thd->GetRowBatch();
  current_thd->SetRowBatch(tag_rowbatch_.get());
  Defer defer{[&]() {
    current_thd->SetRowBatch(ptr);
  }};

  while (true) {
    tag_rowbatch_->Reset();
    KStatus ret = tag_iterator->Next(&(tag_rowbatch_->entity_indexs_),
                                     &(tag_rowbatch_->res_),
                                     &(tag_rowbatch_->count_));
    if (KStatus::FAIL == ret) {
      break;
    }

    code = EEIteratorErrCode::EE_OK;
    if (0 == tag_rowbatch_->count_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      break;
    }

    if (nullptr == tag_filter) {
      break;
    }
    tagFilter(ctx, tag_filter);
    if (tag_rowbatch_->effect_count_ > 0) {
      break;
    }
  }
  tag_rowbatch_->SetPipeEntityNum(ctx, 1);

  // Init a data chunk
  data_chunk = std::make_unique<DataChunk>(col_info, col_info_size, tag_rowbatch_->Count());
  if (data_chunk->Initialize() != true) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    data_chunk = nullptr;
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // Transform tag row batch to data chunk
  KStatus status = data_chunk->AddRowBatchData(ctx, tag_rowbatch_.get(), renders);
  if (status != KStatus::SUCCESS) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Failed to transform tag row batch to data chunk.");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // Add entity id to data_chunk
  status = data_chunk->InsertEntities(tag_rowbatch_.get());
  if (status != KStatus::SUCCESS) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Failed to insert entities to data chunk.");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(code);
}

EEIteratorErrCode StorageHandler::TagNext(kwdbContext_p ctx, Field *tag_filter) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  RowBatch* ptr = thd->GetRowBatch();
  thd->SetRowBatch(tag_rowbatch_.get());
  while (true) {
    tag_rowbatch_->Reset();
    KStatus ret = tag_iterator->Next(&(tag_rowbatch_->entity_indexs_),
                                     &(tag_rowbatch_->res_),
                                     &(tag_rowbatch_->count_));
    if (KStatus::FAIL == ret) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail when getting tag value");
      LOG_ERROR("scanning column data fail when getting tag value.");
      break;
    }

    code = EEIteratorErrCode::EE_OK;
    // LOG_DEBUG("Handler::TagNext count:%d", tag_rowbatch_->count_);
    if (0 == tag_rowbatch_->count_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      break;
    }

    if (nullptr == tag_filter) {
      break;
    }
    tagFilter(ctx, tag_filter);
    if (tag_rowbatch_->effect_count_ > 0) {
      break;
    }
  }
  if (tag_rowbatch_->Count() > 1) {
    // sort by entityIndex
    tag_rowbatch_->SortByEntityIndex();
  }
  tag_rowbatch_->SetPipeEntityNum(ctx, current_thd->GetDegree());
  thd->SetRowBatch(ptr);
  Return(code);
}

KStatus StorageHandler::Close() {
  KStatus ret = KStatus::SUCCESS;
  SafeDeletePointer(ts_iterator);

  if (nullptr != tag_iterator) {
    tag_iterator->Close();
    SafeDeletePointer(tag_iterator);
  }

  return ret;
}
EEIteratorErrCode StorageHandler::GetNextTagData(kwdbContext_p ctx, ScanRowBatch *row_batch) {
  KStatus ret = FAIL;
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  ret = row_batch->tag_rowbatch_->GetTagData(&(row_batch->tagdata_),
                                                 &(row_batch->tag_bitmap_),
                                                 entities_[current_line_].index);
  if (KStatus::FAIL == ret) {
    code = EE_END_OF_RECORD;
  }
  return code;
}

EEIteratorErrCode StorageHandler::NewTsIterator(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = FAIL;
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  KWThdContext *thd = current_thd;
  if (thd->auto_quit_) {
    Return(EE_END_OF_RECORD);
  }
  ScanRowBatch* data_handle =
      static_cast<ScanRowBatch *>(thd->GetRowBatch());

  do {
    entities_.clear();
    ret = tag_scan_->GetEntities(ctx, &entities_,
                                 &(data_handle->tag_rowbatch_));
    if (KStatus::FAIL == ret) {
      code = EE_END_OF_RECORD;
      break;
    }
    current_line_ = 0;
    data_handle->SetTagToColOffset(table_->GetMinTagId());
    if (ts_iterator) {
      SafeDeletePointer(ts_iterator);
    }
    std::vector<KwTsSpan> ts_spans;
    if (EngineOptions::isSingleNode()) {
      ts_spans = *ts_spans_;
    } else {
      auto it = table_->hash_points_spans_.find(entities_[0].hash_point);
      if (it != table_->hash_points_spans_.end()) {
        for (auto const &ts_span : it->second) {
          ts_spans.push_back(ts_span);
          // LOG_DEBUG(
          //     "TSTable::GetIterator() entityID is %d, hashPoint is %d , ts_span.begin is %ld, "
          //     "ts_span.end is %ld  \n",
          //     entities[0].entityId, entities[0].hash_point, ts_span.begin, ts_span.end);
        }
      }
    }
    // LOG_DEBUG("TSTable::GetIterator() entity_size %ld", sizeof(entities_));

    // LOG_DEBUG("TSTable::GetIterator() ts_span_size:%ld", sizeof(ts_spans));
    if (this->table_->GetRelTagJoinColumnIndexes().size() > 0) {
      ret = ts_table_->GetIteratorInOrder(ctx, entities_, ts_spans, table_->scan_cols_,
                                   table_->scan_real_agg_types_, table_->table_version_,
                                   &ts_iterator, table_->scan_real_last_ts_points_, table_->is_reverse_, false);
    } else {
      ret = ts_table_->GetIterator(ctx, entities_, ts_spans, table_->scan_cols_,
                                 table_->scan_real_agg_types_, table_->table_version_,
                                 &ts_iterator, table_->scan_real_last_ts_points_,
                                 table_->is_reverse_, table_->ordered_scan_);
    }
    if (KStatus::FAIL == ret) {
      code = EEIteratorErrCode::EE_ERROR;
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail when getting ts iterator");
      LOG_ERROR("TsTable::GetIterator() error\n");
      break;
    }
  } while (0);
  Return(code);
}

EEIteratorErrCode StorageHandler::NewTagIterator(kwdbContext_p ctx) {
  EnterFunc();
  KStatus ret = FAIL;
  if (EngineOptions::isSingleNode()) {
      if (read_mode_ == TSTableReadMode::metaTable) {
        MetaIterator *meta = nullptr;
        ret = ts_table_->GetMetaIterator(ctx, &meta, table_->table_version_);
        tag_iterator = meta;
      } else {
        TagIterator *tag = nullptr;
        ret = ts_table_->GetTagIterator(ctx, table_->scan_tags_, &tag, table_->table_version_);
        tag_iterator = tag;
      }
  } else {
    TagIterator *tag = nullptr;
    ret = ts_table_->GetTagIterator(ctx, table_->scan_tags_, table_->hash_points_, &tag, table_->table_version_);
    tag_iterator = tag;
  }
  if (ret == KStatus::FAIL) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail when getting ts tag iterator");
    LOG_ERROR("TsTable::GetTagIterator() error\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  Return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode StorageHandler::GetEntityIdList(kwdbContext_p ctx,
                                           TSTagReaderSpec *spec,
                                           Field *tag_filter) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;
  KWThdContext *thd = current_thd;
  RowBatch* old_ptr = thd->GetRowBatch();
  thd->SetRowBatch(tag_rowbatch_.get());

  std::vector<void*> primary_tags;
  std::vector<k_uint64> tags_index_id;
  std::vector<void *> tags;
  do {
    k_int32 sz = spec->primarytags_size();
    k_int32 sz_tag = spec->tagindexes_size();
    if (sz <= 0 && sz_tag <= 0) {
      break;
    }
    size_t malloc_size = 0;
    for (int i = 0; i < sz; ++i) {
      k_uint32 tag_id = spec->mutable_primarytags(i)->colid();
      malloc_size += table_->fields_[tag_id]->get_storage_length();
    }
    if (sz) {
      KStatus ret =
          GeneratePrimaryTags(spec, table_, malloc_size, sz, &primary_tags);
      if (ret != SUCCESS) {
        break;
      }
    }

    TSTagOpType tp = (TSTagOpType)spec->uniontype();
    KStatus ret = GenerateTags(spec, table_, &tags_index_id, &tags);
    if (ret != SUCCESS) {
      break;
    }
    ret = ts_table_->GetEntityIdList(
        ctx, primary_tags, tags_index_id, tags, tp, table_->scan_tags_, &tag_rowbatch_->entity_indexs_,
        &tag_rowbatch_->res_, &tag_rowbatch_->count_, table_->table_version_);
    if (ret != SUCCESS) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_FETCH_DATA_FAILED,
                                  "scanning column data fail when getting ts entity list");
      LOG_ERROR("TsTable::GetEntityIdList() error\n");
      break;
    }
    if (tag_filter) {
      tagFilter(ctx, tag_filter);
      if (0 == tag_rowbatch_->effect_count_) {
        code = EEIteratorErrCode::EE_END_OF_RECORD;
        break;
      }
    } else if (0 == tag_rowbatch_->count_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      break;
    }

    if (tag_rowbatch_->Count() > 1) {
      // sort by entityIndex
      tag_rowbatch_->SortByEntityIndex();
    }
    tag_rowbatch_->SetPipeEntityNum(ctx, current_thd->GetDegree());
    code = EEIteratorErrCode::EE_OK;
  } while (0);
  for (auto& it : primary_tags) {
    SafeFreePointer(it);
  }
  for (auto &it : tags) {
    SafeFreePointer(it);
  }
  thd->SetRowBatch(old_ptr);
  Return(code);
}

std::vector<void *> StorageHandler::findCommonTags(const std::vector<void *> &primary_tag1,
                    const std::vector<void *> &primary_tag2, int data_size) {
  std::vector<void *> commonElements;

  // Iterate through the first vector
  for (void* element1 : primary_tag1) {
    // Iterate through the second vector and compare content
    for (void* element2 : primary_tag2) {
      // Compare the binary data using memcmp
      if (std::memcmp(element1, element2, data_size) == 0) {
        commonElements.push_back(element1);
        break;  // Once we find a match, we can stop searching for this element
      }
    }
  }
  return commonElements;
}

EEIteratorErrCode StorageHandler::GetTagDataChunkWithPrimaryTags(kwdbContext_p ctx,
                                            TSTagReaderSpec *spec,
                                            Field *tag_filter,
                                            std::vector<void *> &primary_tags,
                                            std::vector<void *> &secondary_tags,
                                            const vector<k_uint32> tag_other_join_cols,
                                            Field ** renders,
                                            ColumnInfo *col_info,
                                            k_int32 col_info_size,
                                            DataChunkPtr &data_chunk) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_ERROR;

  // Create tag_rowbatch for tag data
  tag_rowbatch_ = std::make_shared<HashTagRowBatch>();
  tag_rowbatch_->Init(table_);
  RowBatch* ptr = current_thd->GetRowBatch();
  current_thd->SetRowBatch(tag_rowbatch_.get());
  Defer defer{[&]() {
    current_thd->SetRowBatch(ptr);
  }};

  std::vector<uint64_t> tags_index_id;
  std::vector<void*> tags;

  do {
    KStatus ret = ts_table_->GetEntityIdList(
        ctx, primary_tags, tags_index_id, tags, TSTagOpType::opUnKnow, table_->scan_tags_, &tag_rowbatch_->entity_indexs_,
        &tag_rowbatch_->res_, &tag_rowbatch_->count_);
    if (ret != SUCCESS) {
      break;
    }
    if (tag_filter) {
      tagFilter(ctx, tag_filter);
      if (0 == tag_rowbatch_->effect_count_) {
        code = EEIteratorErrCode::EE_END_OF_RECORD;
        break;
      }
    } else if (0 == tag_rowbatch_->count_) {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
      break;
    }
    if (secondary_tags.size() > 0) {
      tagRelFilter(ctx, secondary_tags, tag_other_join_cols);
    }
    code = EEIteratorErrCode::EE_OK;
  } while (0);
  tag_rowbatch_->SetPipeEntityNum(ctx, 1);

  // Init a data chunk
  data_chunk = std::make_unique<DataChunk>(col_info, col_info_size, tag_rowbatch_->Count());
  if (data_chunk->Initialize() != true) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    data_chunk = nullptr;
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // Transform tag row batch to data chunk
  KStatus status = data_chunk->AddRowBatchData(ctx, tag_rowbatch_.get(), renders);
  if (status != KStatus::SUCCESS) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Failed to transform tag row batch to data chunk.");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // Add entity id to data_chunk
  status = data_chunk->InsertEntities(tag_rowbatch_.get());
  if (status != KStatus::SUCCESS) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Failed to insert entities to data chunk.");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  Return(code);
}

KStatus StorageHandler::GeneratePrimaryTags(TSTagReaderSpec *spec, TABLE *table,
                                      size_t malloc_size,
                                      kwdbts::k_int32 sz,
                                      std::vector<void *> *primary_tags) {
  char *ptr = nullptr;
  k_int32 ns = spec->mutable_primarytags(0)->tagvalues_size();
  for (k_int32 i = 0; i < ns; ++i) {
    void *buffer = malloc(malloc_size);
    if (buffer == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      return FAIL;
    }

    memset(buffer, 0, malloc_size);
    ptr = static_cast<char *>(buffer);
    for (k_int32 j = 0; j < sz; ++j) {
      TSTagReaderSpec_TagValueArray *tagInfo = spec->mutable_primarytags(j);
      k_uint32 tag_id = tagInfo->colid();
      const std::string &str = tagInfo->tagvalues(i);
      roachpb::DataType d_type = table->fields_[tag_id]->get_storage_type();
      k_int32 len = table->fields_[tag_id]->get_storage_length();
      switch (d_type) {
        case roachpb::DataType::BOOL: {
          k_bool val = 0;
          if (str == "true" || str == "TRUE") {
            val = 1;
          } else if (str == "false" || str == "FALSE") {
            val = 0;
          } else {
            val = std::stoi(str);
          }
          memcpy(ptr, &val, len);
        } break;
        case roachpb::DataType::SMALLINT: {
          k_int32 val = std::stoi(str);
          if (!CHECK_VALID_SMALLINT(val)) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_WRONG_OBJECT_TYPE,
              ("integer \"" +std::to_string(val) + "\" out of range for type INT2").c_str());
            return FAIL;
          }
          memcpy(ptr, &val, len);
        } break;
        case roachpb::DataType::INT: {
          k_int64 val = std::stoll(str);
          if (!CHECK_VALID_INT(val)) {
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_WRONG_OBJECT_TYPE,
              ("integer \"" +std::to_string(val) + "\" out of range for type INT4").c_str());
            return FAIL;
          }
          memcpy(ptr, &val, sizeof(k_int32));
        } break;
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO: {
          k_uint64 val = std::stoll(str);
          memcpy(ptr, &val, sizeof(KTimestamp));
        } break;
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO: {
          k_uint64 val = std::stoll(str);
          memcpy(ptr, &val, sizeof(KTimestampTz));
        }
        case roachpb::DataType::DATE: {
          k_uint64 val = std::stoll(str);
          memcpy(ptr, &val, sizeof(k_uint32));
        }
        case roachpb::DataType::BIGINT: {
          k_int64 val = std::stoll(str);
          memcpy(ptr, &val, sizeof(k_int64));
        } break;
        case roachpb::DataType::FLOAT: {
          k_float32 val = std::stof(str);
          memcpy(ptr, &val, sizeof(k_float32));
        } break;
        case roachpb::DataType::DOUBLE: {
          k_double64 val = std::stod(str);
          memcpy(ptr, &val, sizeof(k_double64));
        } break;
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NVARCHAR:
          memcpy(ptr, str.c_str(), str.length());
          break;
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          k_uint32 buf_len = str.length() - 1;
          if (buf_len > 2 * len + 3) {
            buf_len = 2 * len + 3;
          }
          k_int32 n = 2;
          for (k_uint32 i = 3; i < buf_len; i = i + 2) {
            if (str[i] >= 'a' && str[i] >= 'f') {
              ptr[n] = str[i] - 'a' + 10;
            } else {
              ptr[n] = str[i] - '0';
            }
            if (str[i + 1] >= 'a' && str[i + 1] >= 'f') {
              ptr[n] = ptr[n] << 4 | (str[i + 1] - 'a' + 10);
            } else {
              ptr[n] = ptr[n] << 4 | (str[i + 1] - '0');
            }
            n++;
          }
          *(static_cast<k_int16 *>(static_cast<void *>(ptr))) = n - 2;
          break;
        }
        default: {
          free(buffer);
          LOG_ERROR("unsupported data type:%d", d_type);
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
          return FAIL;
        }
      }
      ptr += table->fields_[tag_id]->get_storage_length();
    }
    primary_tags->push_back(buffer);
  }
  return SUCCESS;
}

KStatus StorageHandler::GenerateTags(TSTagReaderSpec *spec, TABLE *table,
                                     std::vector<k_uint64> *tags_index_id,
                                     std::vector<void *> *tags) {
  k_int32 sz_tag = spec->tagindexes_size();
  for (int i = 0; i < sz_tag; ++i) {
    char *ptr = nullptr;
    TSTagReaderSpec_TagIndexInfo tagIndexInfo = spec->tagindexes(i);
    k_int32 tz = tagIndexInfo.tagvalues_size();
    size_t malloc_size = 0;
    for (int tmp = 0; tmp < tz; ++tmp) {
      k_uint32 tag_id = tagIndexInfo.mutable_tagvalues(tmp)->colid();
      malloc_size += table->fields_[tag_id]->get_storage_length();
    }
    k_int32 ns = tagIndexInfo.mutable_tagvalues(0)->tagvalues_size();
    for (k_int32 k = 0; k < ns; ++k) {
      void *buffer = malloc(malloc_size);
      if (buffer == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                      "Insufficient memory");
        return FAIL;
      }

      memset(buffer, 0, malloc_size);
      ptr = static_cast<char *>(buffer);
      for (k_int32 j = 0; j < tz; ++j) {
        TSTagReaderSpec_TagValueArray *tagInfo =
            tagIndexInfo.mutable_tagvalues(j);
        k_uint32 tag_id = tagInfo->colid();
        const std::string &str = tagInfo->tagvalues(k);
        roachpb::DataType d_type = table->fields_[tag_id]->get_storage_type();
        k_int32 len = table->fields_[tag_id]->get_storage_length();
        switch (d_type) {
          case roachpb::DataType::BOOL: {
            k_bool val = 0;
            if (str == "true" || str == "TRUE") {
              val = 1;
            } else if (str == "false" || str == "FALSE") {
              val = 0;
            } else {
              val = std::stoi(str);
            }
            memcpy(ptr, &val, len);
          } break;
          case roachpb::DataType::SMALLINT: {
            k_int32 val = std::stoi(str);
            if (!CHECK_VALID_SMALLINT(val)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");
              return FAIL;
            }
            memcpy(ptr, &val, len);
          } break;
          case roachpb::DataType::INT: {
            k_int64 val = std::stoll(str);
            if (!CHECK_VALID_INT(val)) {
              EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                            "out of range");
              return FAIL;
            }
            memcpy(ptr, &val, sizeof(k_int32));
          } break;
          case roachpb::DataType::TIMESTAMP: {
            k_uint64 val = std::stoll(str);
            memcpy(ptr, &val, sizeof(KTimestamp));
          } break;
          case roachpb::DataType::TIMESTAMPTZ: {
            k_uint64 val = std::stoll(str);
            memcpy(ptr, &val, sizeof(KTimestampTz));
          }
          case roachpb::DataType::DATE: {
            k_uint64 val = std::stoll(str);
            memcpy(ptr, &val, sizeof(k_uint32));
          }
          case roachpb::DataType::BIGINT: {
            k_int64 val = std::stoll(str);
            memcpy(ptr, &val, sizeof(k_int64));
          } break;
          case roachpb::DataType::FLOAT: {
            k_float32 val = std::stof(str);
            memcpy(ptr, &val, sizeof(k_float32));
          } break;
          case roachpb::DataType::DOUBLE: {
            k_double64 val = std::stod(str);
            memcpy(ptr, &val, sizeof(k_double64));
          } break;
          case roachpb::DataType::CHAR:
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::VARCHAR:
          case roachpb::DataType::NVARCHAR:
            memcpy(ptr, str.c_str(), str.length());
            break;
          case roachpb::DataType::BINARY:
          case roachpb::DataType::VARBINARY: {
            k_uint32 buf_len = str.length() - 1;
            if (buf_len > 2 * len + 3) {
              buf_len = 2 * len + 3;
            }
            k_int32 n = 2;
            for (k_uint32 i = 3; i < buf_len; i = i + 2) {
              if (str[i] >= 'a' && str[i] >= 'f') {
                ptr[n] = str[i] - 'a' + 10;
              } else {
                ptr[n] = str[i] - '0';
              }
              if (str[i + 1] >= 'a' && str[i + 1] >= 'f') {
                ptr[n] = ptr[n] << 4 | (str[i + 1] - 'a' + 10);
              } else {
                ptr[n] = ptr[n] << 4 | (str[i + 1] - '0');
              }
              n++;
            }
            *(static_cast<k_int16 *>(static_cast<void *>(ptr))) = n - 2;
            break;
          }
          default: {
            free(buffer);
            LOG_ERROR("unsupported data type:%d", d_type);
            EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE,
                                          "unsupported data type");
            return FAIL;
          }
        }
        ptr += table->fields_[tag_id]->get_storage_length();
      }
      tags->push_back(buffer);
      tags_index_id->push_back(spec->tagindexids(i));
    }
  }
  return SUCCESS;
}

void StorageHandler::tagFilter(kwdbContext_p ctx, Field *tag_filter) {
  EnterFunc();

  for (k_uint32 i = 0; i < tag_rowbatch_->count_; ++i) {
    if (0 == tag_filter->ValInt()) {
      tag_rowbatch_->NextLine();
      continue;
    }

    tag_rowbatch_->AddSelection();
    tag_rowbatch_->NextLine();
  }
  tag_rowbatch_->isFilter_ = true;
  tag_rowbatch_->ResetLine();

  ReturnVoid();
}

void StorageHandler::tagRelFilter(kwdbContext_p ctx,
                                  std::vector<void *> secondary_tags,
                                  const vector<k_uint32> tag_other_join_cols) {
  EnterFunc();
  bool isAlreadyFilter = tag_rowbatch_->isFilter_;
  // Check if the sizes of secondary_tags and tag_other_join_indexes are equal; log an error and return if not
  // secondary_tags shoudl be the same as tag_other_join_indexes since they are prepared by InitRelJointIndexes()
  if (secondary_tags.size() != tag_other_join_cols.size()) {
    LOG_ERROR("tagRelFilter() Failed. The size of secondary tags and rel columns are not equal \n");
    ReturnVoid();
  }
  k_uint32 count = isAlreadyFilter ? tag_rowbatch_->effect_count_ : tag_rowbatch_->count_;
  // Iterate over each row in tag_rowbatch_
  for (k_uint32 i = 0; i < count; ++i) {
    TagData cur_data;
    void *bmp = nullptr;
    // Retrieve the tag data and bitmap for the current row
    // cur_data should return all tag columns
    tag_rowbatch_->GetTagData(&cur_data, &bmp, tag_rowbatch_->GetCurrentEntity());

    bool isEqual = true;  // Flag to check if the current row matches the criteria
    k_uint32 tag_col_offset = table_->GetMinTagId();
    for (int i = 0; i < secondary_tags.size(); i++) {
      char* other_data = static_cast<char*>(secondary_tags[i]);
      // Get the join index for the current column
      k_uint32 cur_join_index = tag_other_join_cols[i];
      // Check if the values are equal, no match if not
      if (cur_data[cur_join_index].is_null) {
        if (other_data != nullptr) {
          // nullptr means value is null
          isEqual = false;
          break;
        }
      } else {
        k_uint32 index = table_->scan_tags_[cur_join_index] + tag_col_offset;
        switch (table_->fields_[index]->get_storage_type()) {
          case roachpb::DataType::CHAR:
          case roachpb::DataType::VARCHAR:
            if (std::strcmp(cur_data[cur_join_index].tag_data, other_data) != 0) {
              isEqual = false;
            }
            break;
          case roachpb::DataType::NCHAR:
          case roachpb::DataType::NVARCHAR:
          case roachpb::DataType::VARBINARY:
          case roachpb::DataType::BINARY:
            if (std::memcmp(cur_data[cur_join_index].tag_data, other_data,
                    tag_rowbatch_->GetDataLen(i, cur_join_index, table_->fields_[index]->get_column_type())) != 0) {
              isEqual = false;
            }
            break;
          default:
            if (std::memcmp(cur_data[cur_join_index].tag_data, other_data,
                    table_->fields_[index]->get_storage_length()) != 0) {
              isEqual = false;
            }
            break;
        }
        if (!isEqual) {
          break;
        }
      }
    }
    if (isAlreadyFilter) {
      // If the row does not match, move to the next row
      if (!isEqual) {
        // remove current selected
        tag_rowbatch_->RemoveSelection();
      } else {
        tag_rowbatch_->NextLine();
      }
      continue;
    } else {
      if (!isEqual) {
        tag_rowbatch_->NextLine();
        continue;
      } else {
        tag_rowbatch_->AddSelection();
        tag_rowbatch_->NextLine();
      }
    }
  }
  tag_rowbatch_->isFilter_ = true;
  tag_rowbatch_->ResetLine();

  ReturnVoid();
}

bool StorageHandler::isDisorderedMetrics() {
  if (ts_iterator == nullptr) {
    return false;
  } else {
    return ts_iterator->IsDisordered();
  }
}

}  // namespace kwdbts
