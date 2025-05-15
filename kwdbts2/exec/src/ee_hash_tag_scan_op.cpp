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

#include "ee_hash_tag_scan_op.h"

#include <sstream>
#include <cmath>
#include <memory>
#include <string>
#include <vector>

#include "cm_func.h"
#include "ee_row_batch.h"
#include "ee_flow_param.h"
#include "ee_global.h"
#include "ee_storage_handler.h"
#include "ee_pb_plan.pb.h"
#include "ee_table.h"
#include "lg_api.h"
#include "ee_kwthd_context.h"
#include "ee_dml_exec.h"
#include "ee_cancel_checker.h"

namespace kwdbts {

HashTagScanOperator::HashTagScanOperator(TsFetcherCollection *collection,
                                         TSTagReaderSpec* spec, TSPostProcessSpec* post,
                                         TABLE* table, int32_t processor_id)
    : TagScanBaseOperator(collection, table, processor_id),
      spec_(spec),
      post_(post),
      param_(post, table) {
  if (spec) {
    table->SetAccessMode(spec->accessmode());
    table_->ptag_size_ = 0;
    for (int i = 0; i < table_->field_num_; ++i) {
      if (table_->fields_[i]->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
        ++table_->ptag_size_;
      }
    }
    object_id_ = spec->tableid();
    table->SetTableVersion(spec->tableversion());
  }
}

HashTagScanOperator::~HashTagScanOperator() = default;

KStatus HashTagScanOperator::Close(kwdbContext_p ctx) {
  EnterFunc();
  Reset(ctx);
  if (dynamic_hash_index_) {
    delete dynamic_hash_index_;
    dynamic_hash_index_ = nullptr;
  }

  if (tag_renders_) {
    free(tag_renders_);
    tag_renders_ = nullptr;
  }
  SafeDeleteArray(tag_col_info_);
  Return(KStatus::SUCCESS);
}

EEIteratorErrCode HashTagScanOperator::Init(kwdbContext_p ctx) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (is_init_) {
    Return(init_code_);
  }
  EEIteratorErrCode ret = EEIteratorErrCode::EE_ERROR;
  do {
    // resolve tag
    param_.ResolveScanTags(ctx);
    for (k_uint32 i = 0; i < table_->scan_tags_.size(); ++i) {
      scan_tag_to_output[table_->scan_tags_[i]] = i;
    }
    // resolve relational cols
    param_.ResolveScanRelCols(ctx);
    // post->filter;
    ret = param_.ResolveFilter(ctx, &filter_, true);
    if (EEIteratorErrCode::EE_OK != ret) {
      LOG_ERROR("ReaderPostResolve::ResolveFilter() failed");
      break;
    }
    if (object_id_ > 0) {
      // renders num
      param_.RenderSize(ctx, &num_);
      ret = param_.ResolveRender(ctx, &renders_, num_);
      if (ret != EEIteratorErrCode::EE_OK) {
        LOG_ERROR("ResolveRender() failed");
        break;
      }
      // Output Fields
      ret = param_.ResolveOutputFields(ctx, renders_, num_, output_fields_);
      if (EEIteratorErrCode::EE_OK != ret) {
        LOG_ERROR("ResolveOutputFields() failed");
        break;
      }
    }
  } while (0);

  // Create a relational batch queue to receive relational batches from ME.
  KStatus status = ctx->dml_exec_handle->CreateRelBatchQueue(ctx, table_->GetRelFields());
  if (status != KStatus::SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  rel_batch_queue_ = ctx->dml_exec_handle->GetRelBatchQueue();

  // Create batch_data_container_ to keep relational batches, linked list
  // and tag data.
  batch_data_container_ = std::make_shared<BatchDataContainer>();

  is_init_ = true;
  init_code_ = ret;

  Return(ret);
}

// sort the list using tag index
inline bool SortByTagIndex(pair<k_uint32, k_uint32>& a, pair<k_uint32, k_uint32>& b) {
    return a.second < b.second;
}

EEIteratorErrCode HashTagScanOperator::InitRelJointIndexes() {
  std::vector<pair<k_uint32, k_uint32>>& rel_tag_join_column_indexes = table_->GetRelTagJoinColumnIndexes();
  std::vector<pair<k_uint32, k_uint32>> primary_cols;

  for (pair<k_uint32, k_uint32>& join_columns : rel_tag_join_column_indexes) {
    if (table_->fields_[join_columns.second]->get_column_type() == roachpb::KWDBKTSColumn::TYPE_PTAG) {
      primary_cols.push_back(join_columns);
    } else {
      rel_other_join_cols_.push_back(join_columns.first);
      tag_other_join_cols_.push_back(scan_tag_to_output[join_columns.second - table_->GetMinTagId()]);
    }
  }
  // all primary tags should be involved in join columns for primaryHashTagScan
  if (primary_cols.size() != table_->ptag_size_) {
    LOG_ERROR("The number of primary tags involved (%d) is not equal to ptag size (%d).\n",
                primary_cols.size(), table_->ptag_size_);
    return EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED;
  }

  sort(primary_cols.begin(), primary_cols.end(), SortByTagIndex);
  for (pair<k_uint32, k_uint32>& join_columns : primary_cols) {
    primary_rel_cols_.push_back(join_columns.first);
    // always use tag storage length to generate key value
    k_uint32 length = table_->fields_[join_columns.second]->get_storage_length();
    join_column_lengths_.push_back(length);
    total_join_column_length_ += length;
  }
  return(EEIteratorErrCode::EE_OK);
}

EEIteratorErrCode HashTagScanOperator::Start(kwdbContext_p ctx) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  // Check for cancellation at the start
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  std::unique_lock l(tag_lock_);
  // Involving parallelism, ensuring that it is only called once
  if (started_) {
    Return(start_code_);
  }
  // Get TagScan ready
  started_ = true;
  start_code_ = EEIteratorErrCode::EE_ERROR;
  handler_ = new StorageHandler(table_);
  if (nullptr == handler_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("handler_ new failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  start_code_ = handler_->Init(ctx);
  if (start_code_ == EEIteratorErrCode::EE_ERROR) {
    Return(start_code_);
  }

  // Prepare column info for conversion from tag row batch to data chunk
  k_uint32 tag_col_size = table_->scan_tags_.size();

  tag_renders_ = static_cast<Field **>(malloc(tag_col_size * sizeof(Field *)));
  if (nullptr == tag_renders_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("tag_renders_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  memset(tag_renders_, 0, tag_col_size * sizeof(Field *));
  tag_col_size_ = tag_col_size;
  tag_col_info_ = KNEW ColumnInfo[tag_col_size];
  if (nullptr == tag_col_info_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("tag_col_info_ malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  for (k_int32 i = 0; i < tag_col_size; ++i) {
    k_uint32 tab = table_->scan_tags_[i];
    Field *field = table_->GetFieldWithColNum(tab + table_->GetMinTagId());
    if (nullptr == field) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "field is null");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    tag_col_info_[i] = ColumnInfo(field->get_storage_length(), field->get_storage_type(), field->get_return_type());
    tag_renders_[i] = field;
  }

  k_int32 sz = spec_->primarytags_size();
  if (sz > 0) {
    size_t malloc_size = 0;
    for (int i = 0; i < sz; ++i) {
      k_uint32 tag_id = spec_->mutable_primarytags(i)->colid();
      malloc_size += table_->fields_[tag_id]->get_storage_length();
    }
    KStatus ret = StorageHandler::GeneratePrimaryTags(spec_, table_, malloc_size, sz, &primary_tags_);
    if (ret != SUCCESS) {
      return(EEIteratorErrCode::EE_ERROR);
    }
  }

  k_uint32 access_mode = table_->GetAccessMode();
  switch (access_mode) {
    case TSTableReadMode::hashTagScan:
    case TSTableReadMode::hashRelScan:
      // Build dynamic hash indexes
      start_code_ = CreateDynamicHashIndexes(ctx);
      break;
    case TSTableReadMode::primaryHashTagScan:
      start_code_ = InitRelJointIndexes();
      break;
    default: {
      start_code_ = EEIteratorErrCode::EE_ERROR;
      LOG_ERROR("access mode must be primaryHashTagScan, hashTagScan or hashRelScan, %d", access_mode);
      break;
    }
  }

  // update stats
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<int64_t, std::nano> duration = end - start;
  fetcher_.Update(0, 0, 0, 0, 0, 0, duration.count());

  Return(start_code_);
}

EEIteratorErrCode HashTagScanOperator::CreateDynamicHashIndexes(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_END_OF_RECORD;
  // Check for cancellation at the start
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }
  // Initialize relational join columns
  std::vector<pair<k_uint32, k_uint32>>& rel_tag_join_column_indexes = table_->GetRelTagJoinColumnIndexes();
  vector<Field*>& rel_fields = table_->GetRelFields();
  for (auto join_index : rel_tag_join_column_indexes) {
    primary_rel_cols_.push_back(join_index.first);
    primary_tag_cols_.push_back(scan_tag_to_output[join_index.second - table_->GetMinTagId()]);
    k_uint32 length = min(rel_fields[join_index.first]->get_storage_length(),
                          table_->GetFieldWithColNum(join_index.second)->get_storage_length());
    join_column_lengths_.push_back(length);
    total_join_column_length_ += length;
  }

  code = handler_->NewTagIterator(ctx);
  if (code != EE_OK) {
    Return(code);
  }

  // Create a in-memory hash index
  dynamic_hash_index_ = new DynamicHashIndex();
  if (nullptr == dynamic_hash_index_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("dynamic_hash_index_ new failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }

  int ret = dynamic_hash_index_->init(total_join_column_length_);
  if (ret != 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Failed to init dynamic hash index.");
    LOG_ERROR("dynamic_hash_index_ init failed with %d.\n", ret);
    Return(EEIteratorErrCode::EE_ERROR);
  }

  // Time to bulid the dynamic hash index, it's up to optimizer
  // to decide which one to build.
  if (table_->GetAccessMode() == TSTableReadMode::hashRelScan) {
    // Use relation data to build hash index
    code = BuildRelIndex(ctx);
  } else if (table_->GetAccessMode() == TSTableReadMode::hashTagScan) {
    // Use tag data to build hash index
    code = BuildTagIndex(ctx);
  } else {
    code = EEIteratorErrCode::EE_ERROR;
  }
  Return(code)
}

EEIteratorErrCode HashTagScanOperator::BuildRelIndex(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // Move the relational batch to rel_tag_rowbatch and build linked list
  DataChunkPtr rel_data_chunk;

  char* rel_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
  if (nullptr == rel_join_column_value) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("rel_join_column_value malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  Defer defer{[&]() {
    free(rel_join_column_value);
  }};

  while ((code = rel_batch_queue_->Next(ctx, rel_data_chunk)) != EEIteratorErrCode::EE_END_OF_RECORD) {
    // Check for cancellation within the loop
    if (code != EEIteratorErrCode::EE_OK) {
      Return(code);
    }
    if (batch_data_container_->AddRelDataChunkAndBuildLinkedList(dynamic_hash_index_, rel_data_chunk,
                                                            primary_rel_cols_, join_column_lengths_,
                                                            rel_join_column_value, total_join_column_length_)
                                                            == KStatus::FAIL) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode HashTagScanOperator::BuildTagIndex(kwdbContext_p ctx) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  DataChunkPtr tag_data_chunk;

  char* tag_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
  if (nullptr == tag_join_column_value) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("tag_join_column_value malloc failed\n");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  Defer defer{[&]() {
    free(tag_join_column_value);
  }};

  std::vector<void*> other_join_columns_values;
  // Scan all tag data with filter
  while ((code = handler_->NextTagDataChunk(ctx, spec_, filter_,
                              primary_tags_, other_join_columns_values, tag_other_join_cols_,
                              tag_renders_, tag_col_info_, tag_col_size_, tag_data_chunk))
            != EEIteratorErrCode::EE_END_OF_RECORD) {
    // Check for cancellation within the loop
    if (CheckCancel(ctx) != SUCCESS) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
    if (code != EEIteratorErrCode::EE_OK) {
      Return(code);
    }

    // Move the data chunk into hyper container and build the linked list
    if (batch_data_container_->AddRelDataChunkAndBuildLinkedList(dynamic_hash_index_, tag_data_chunk,
                                                            primary_tag_cols_, join_column_lengths_,
                                                            tag_join_column_value, total_join_column_length_)
                                                            == KStatus::FAIL) {
      Return(EEIteratorErrCode::EE_ERROR);
    }
  }

  Return(EEIteratorErrCode::EE_OK)
}

EEIteratorErrCode HashTagScanOperator::GetJoinColumnValues(kwdbContext_p ctx,
                                                      DataChunkPtr& rel_data_chunk,
                                                      k_uint32 row_index,
                                                      std::vector<void*>& primary_column_values,
                                                      std::vector<void*>& other_join_columns_values,
                                                      k_bool& has_null) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  char* p = static_cast<char*>(primary_column_values[0]);
  for (int i = 0; i < primary_rel_cols_.size(); ++i) {
    if (rel_data_chunk->IsNull(row_index, primary_rel_cols_[i])) {
      has_null = true;
      Return(code);
    }
    memcpy(p, rel_data_chunk->GetDataPtr(row_index, primary_rel_cols_[i]),
          min(rel_data_chunk->GetColumnInfo()[primary_rel_cols_[i]].storage_len, join_column_lengths_[i]));
    p += join_column_lengths_[i];
  }
  for (int i = 0; i < rel_other_join_cols_.size(); ++i) {
    if (rel_data_chunk->IsNull(row_index, rel_other_join_cols_[i])) {
      has_null = true;
      Return(code);
    }
    other_join_columns_values[i] = rel_data_chunk->GetDataPtr(row_index, rel_other_join_cols_[i]);
  }
  Return(code);
}

EEIteratorErrCode HashTagScanOperator::GetJoinColumnValues(kwdbContext_p ctx,
                                                      DataChunkPtr& data_chunk,
                                                      k_uint32 row_index,
                                                      vector<k_uint32> key_cols,
                                                      char* key_column_values,
                                                      k_bool& has_null) {
  EnterFunc();
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;
  char* p = key_column_values;
  for (int i = 0; i < key_cols.size(); ++i) {
    if (data_chunk->IsNull(row_index, key_cols[i])) {
      has_null = true;
      Return(code);
    }
    roachpb::DataType type = data_chunk->GetColumnInfo()[key_cols[i]].storage_type;
    switch (type) {
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NVARCHAR:
        strncpy(p, data_chunk->GetDataPtr(row_index, key_cols[i]), join_column_lengths_[i]);
        break;
      default:
        memcpy(p, data_chunk->GetDataPtr(row_index, key_cols[i]), join_column_lengths_[i]);
        break;
    }
    p += join_column_lengths_[i];
  }
  Return(code);
}

EEIteratorErrCode HashTagScanOperator::Next(kwdbContext_p ctx) {
  EnterFunc();
  auto start = std::chrono::high_resolution_clock::now();
  int64_t read_bytes = 0;
  // check when enter
  if (CheckCancel(ctx) != SUCCESS) {
    Return(EEIteratorErrCode::EE_ERROR);
  }

  if (!rel_batch_queue_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "rel_batch_queue_ was not created.");
    Return(EEIteratorErrCode::EE_ERROR);
  }
  EEIteratorErrCode code = EEIteratorErrCode::EE_OK;

  // Create rel_tag_rowbatch for output data with tag and relational columns
  rel_tag_rowbatch_ = std::make_shared<RelTagRowBatch>();
  rel_tag_rowbatch_->Init(table_);

  if (table_->GetAccessMode() == TSTableReadMode::hashRelScan) {
    // Probe tag table with rel hash index
    DataChunkPtr tag_data_chunk;
    char* rel_join_column_value = static_cast<char*>(malloc(total_join_column_length_));
    if (nullptr == rel_join_column_value) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("rel_join_column_value malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    Defer defer{[&]() {
      free(rel_join_column_value);
    }};

    std::vector<void*> other_join_columns_values;
    // Scan all tag data with filter
    while ((code = handler_->NextTagDataChunk(ctx, spec_, filter_,
                              primary_tags_, other_join_columns_values, tag_other_join_cols_,
                              tag_renders_, tag_col_info_, tag_col_size_, tag_data_chunk))
              != EEIteratorErrCode::EE_END_OF_RECORD) {
      // Check for cancellation within the loop
      if (CheckCancel(ctx) != SUCCESS) {
        Return(EEIteratorErrCode::EE_ERROR);
      }
      if (code != EEIteratorErrCode::EE_OK) {
        Return(code);
      }

      k_uint32 row_count = tag_data_chunk->Count();
      for (k_uint32 i = 0; i < row_count; ++i) {
        k_bool has_null = false;
        code = GetJoinColumnValues(ctx, tag_data_chunk, i, primary_tag_cols_, rel_join_column_value, has_null);
        if (code != EEIteratorErrCode::EE_OK) {
          Return(code);
        }
        if (has_null) {
          continue;
        }
        // search for the linked list
        RowIndice row_indice;
        if (dynamic_hash_index_->get(rel_join_column_value, total_join_column_length_, row_indice) == 0) {
          // for each matched record, combine them to output
          if (rel_tag_rowbatch_->AddRelTagJoinRecord(ctx, batch_data_container_, tag_data_chunk, i, row_indice)
                == KStatus::FAIL) {
            Return(EEIteratorErrCode::EE_ERROR);
          }
        }
      }
    }
    rel_tag_rowbatch_->SetPipeEntityNum(ctx, current_thd->GetDegree());
    if (rel_tag_rowbatch_->Count() > 0) {
      code = EEIteratorErrCode::EE_OK;
    } else {
      code = EEIteratorErrCode::EE_END_OF_RECORD;
    }
    // tmp bytes
    if (rel_tag_rowbatch_ != nullptr) {
      read_bytes += (sizeof(rel_tag_rowbatch_));
    }
  } else {
    // Get primary column values and other join column values.
    std::vector<void*> primary_column_values(1);
    primary_column_values[0] = malloc(total_join_column_length_);
    if (nullptr == primary_column_values[0]) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      LOG_ERROR("primary_column_values[0] malloc failed\n");
      Return(EEIteratorErrCode::EE_ERROR);
    }
    memset(primary_column_values[0], 0, total_join_column_length_);
    std::vector<void*> other_join_columns_values(rel_other_join_cols_.size());
    std::vector<void *> intersect_primary_tags;
    Defer defer{[&]() {
      free(primary_column_values[0]);
    }};

    DataChunkPtr rel_data_chunk;
    do {
      code = rel_batch_queue_->Next(ctx, rel_data_chunk);
      if (code != EEIteratorErrCode::EE_OK) {
        Return(code);
      }
      if (table_->GetAccessMode() == TSTableReadMode::primaryHashTagScan) {
        k_uint32 row_count = rel_data_chunk->Count();
        for (k_uint32 i = 0; i < row_count; ++i) {
          k_bool has_null = false;
          code = GetJoinColumnValues(ctx, rel_data_chunk, i, primary_column_values, other_join_columns_values, has_null);
          if (code != EEIteratorErrCode::EE_OK) {
            Return(code);
          }
          if (has_null) {
            continue;
          }
          if (primary_tags_.size() > 0) {
            if (primary_rel_cols_.size() > 0) {
              intersect_primary_tags = handler_->findCommonTags(primary_column_values, primary_tags_,
                                                                total_join_column_length_);
            } else {
              intersect_primary_tags = primary_tags_;
            }
          } else  {
            intersect_primary_tags = primary_column_values;
          }

          if (intersect_primary_tags.size() == 0) {
            continue;
          }
          DataChunkPtr tag_data_chunk;
          // Call GetTagDataChunkWithPrimaryTags to search hash index, tagFilter & join match.
          code = handler_->GetTagDataChunkWithPrimaryTags(ctx, spec_, filter_,
                              intersect_primary_tags, other_join_columns_values, tag_other_join_cols_,
                              tag_renders_, tag_col_info_, tag_col_size_, tag_data_chunk);

          if (code != EE_OK && code != EE_END_OF_RECORD) {
            Return(code);
          }

          if (code == EE_OK) {
            if (rel_tag_rowbatch_->AddPrimaryTagRelJoinRecord(ctx, rel_data_chunk, i, tag_data_chunk)
                  == KStatus::FAIL) {
              Return(EEIteratorErrCode::EE_ERROR);
            }
          }
        }
        if (rel_data_chunk != nullptr) {
          read_bytes += int64_t(rel_data_chunk->Capacity()) * int64_t(rel_data_chunk->RowSize());
        }
      } else if (table_->GetAccessMode() == TSTableReadMode::hashTagScan) {
        k_uint32 row_count = rel_data_chunk->Count();
        for (k_uint32 i = 0; i < row_count; ++i) {
          k_bool has_null = false;
          code = GetJoinColumnValues(ctx, rel_data_chunk, i, primary_column_values, other_join_columns_values, has_null);
          if (code != EEIteratorErrCode::EE_OK) {
            Return(code);
          }
          if (has_null) {
            continue;
          }
          // search for the linked list
          RowIndice row_indice;
          if (dynamic_hash_index_->get(static_cast<char*>(primary_column_values[0]),
                                    total_join_column_length_, row_indice) == 0) {
            // for each matched record, combine them to output
            if (rel_tag_rowbatch_->AddTagRelJoinRecord(ctx, batch_data_container_, rel_data_chunk, i, row_indice)
                  == KStatus::FAIL) {
              Return(EEIteratorErrCode::EE_ERROR);
            }
          }
        }
        read_bytes += int64_t(rel_data_chunk->Capacity()) * int64_t(rel_data_chunk->RowSize());
      } else {
          LOG_ERROR("access mode must be primaryHashTagScan or hashTagScan, %d", table_->GetAccessMode());
          Return(EEIteratorErrCode::EE_ERROR);
      }
    } while (rel_tag_rowbatch_->Count() == 0);
    code = EEIteratorErrCode::EE_OK;
    rel_tag_rowbatch_->SetPipeEntityNum(ctx, current_thd->GetDegree());
    primary_column_values.clear();  // Clear the vector after cleanup
    other_join_columns_values.clear();  // Clear the vector after cleanup
  }
  total_read_row_ += rel_tag_rowbatch_->Count();

  // Fetcher performance analysis
  auto end = std::chrono::high_resolution_clock::now();
  std::chrono::duration<int64_t, std::nano> duration = end - start;
  if (rel_tag_rowbatch_ != nullptr) {
    fetcher_.Update(rel_tag_rowbatch_->Count(), duration.count(), read_bytes, 0, 0, 0);
  } else {
    fetcher_.Update(0, duration.count(), 0, 0, 0, 0);
  }

  Return(code);
}

EEIteratorErrCode HashTagScanOperator::Next(kwdbContext_p ctx, DataChunkPtr& chunk) {
  EnterFunc();
  // not supported yet
  Return(EEIteratorErrCode::EE_ERROR);
}

RowBatch* HashTagScanOperator::GetRowBatch(kwdbContext_p ctx) {
  EnterFunc();

  Return(rel_tag_rowbatch_.get());
}

EEIteratorErrCode HashTagScanOperator::Reset(kwdbContext_p ctx) {
  EnterFunc();

  if (handler_) {
    SafeDeletePointer(handler_);
  }
  total_read_row_ = 0;
  tag_index_once_ = false;
  started_ = false;
  tag_index_once_ = true;

  Return(EEIteratorErrCode::EE_OK)
}

KStatus HashTagScanOperator::GetEntities(kwdbContext_p ctx,
                                     std::vector<EntityResultIndex> *entities,
                                     TagRowBatchPtr *row_batch_ptr) {
  EnterFunc();
  std::unique_lock l(tag_lock_);
  if (*row_batch_ptr == nullptr) {
    *row_batch_ptr = rel_tag_rowbatch_;
  }
  if (is_first_entity_ || (*row_batch_ptr != nullptr &&
                           (row_batch_ptr->get()->isAllDistributed()))) {
    if (is_first_entity_ || *row_batch_ptr == rel_tag_rowbatch_ || rel_tag_rowbatch_->isAllDistributed()) {
      is_first_entity_ = false;
      EEIteratorErrCode code = Next(ctx);
      if (code != EE_OK) {
        Return(FAIL);
      }
    } else if (rel_tag_rowbatch_.get()->Count() == 0) {
      Return(FAIL);
    }

    // construct ts_iterator
    *row_batch_ptr = rel_tag_rowbatch_;
  }
  KStatus ret = row_batch_ptr->get()->GetEntities(entities);
  Return(ret);
}

}  // namespace kwdbts
