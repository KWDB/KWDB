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

#include <unistd.h>  // For getpid()
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <utility>
#include <random>
#include <unordered_map>

#include "ee_base_op.h"
#include "ee_tag_scan_parser.h"
#include "ee_tag_row_batch.h"
#include "ee_rel_tag_row_batch.h"
#include "ee_hash_tag_row_batch.h"
#include "kwdb_consts.h"
#include "kwdb_type.h"
#include "ee_tag_scan_op.h"
#include "mmap/mmap_tag_column_table.h"
#include "ee_rel_batch_queue.h"
#include "tag_iterator.h"
#include "ee_dynamic_hash_index.h"

namespace kwdbts {

// scan tag data and hash join with relational data from ME for multiple model processing
class HashTagScanOperator : public TagScanBaseOperator {
 public:
  HashTagScanOperator(TsFetcherCollection *collection,
                      TSTagReaderSpec* spec,
                      TSPostProcessSpec* post,
                      TABLE* table,
                      int32_t processor_id);

  ~HashTagScanOperator() override;

  EEIteratorErrCode Init(kwdbContext_p ctx) override;

  EEIteratorErrCode Start(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx) override;

  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk) override;

  EEIteratorErrCode Reset(kwdbContext_p ctx) override;

  enum OperatorType Type() override {return OperatorType::OPERATOR_HASH_TAG_SCAN;}

  RowBatch* GetRowBatch(kwdbContext_p ctx) override;

  EEIteratorErrCode Close(kwdbContext_p ctx) override;

  KStatus GetEntities(kwdbContext_p ctx,
                      std::vector<EntityResultIndex>* entities,
                      TagRowBatchPtr* row_batch_ptr) override;

 protected:
  /**
   * @brief Create dynamic hash index with build side data
   * @param[in] ctx kwdb context
   * @return Error code
   */
  EEIteratorErrCode CreateDynamicHashIndexes(kwdbContext_p ctx);
  /**
   * @brief Build dynamic hash index with tag data
   * @param[in] ctx kwdb context
   * @return Error code
   */
  EEIteratorErrCode BuildTagIndex(kwdbContext_p ctx);
  /**
   * @brief Build dynamic hash index with relation data
   * @param[in] ctx kwdb context
   * @return Error code
   */
  EEIteratorErrCode BuildRelIndex(kwdbContext_p ctx);
  /**
   * @brief Get join column values
   * @param[in] ctx kwdb context
   * @param[in] rel_data_chunk  relational data chunk
   * @param[out] primary_column_values join column values joining with primary tags
   * @param[out] other_join_columns_values other join column values except the columns joining with primary tags
   * @param[out] has_null whether has null values or not
   */
  EEIteratorErrCode GetJoinColumnValues(kwdbContext_p ctx,
                                        DataChunkPtr& rel_data_chunk,
                                        k_uint32 row_index,
                                        std::vector<void*>& primary_column_values,
                                        std::vector<void*>& other_join_columns_values,
                                        k_bool& has_null);
  /**
   * @brief Get join column values
   * @param[in] ctx kwdb context
   * @param[in] data_chunk  data chunk
   * @param[in] row_index the index of row in data chunk used to get join column values
   * @param[in] key_cols the key/join columns
   * @param[out] key_column_values key/join column values
   * @param[out] has_null whether has null values or not
   */
  EEIteratorErrCode GetJoinColumnValues(kwdbContext_p ctx,
                                        DataChunkPtr& data_chunk,
                                        k_uint32 row_index,
                                        vector<k_uint32> key_cols,
                                        char* key_column_values,
                                        k_bool& has_null);
  // Initialize the primary join columns and other join columns.
  EEIteratorErrCode InitRelJointIndexes();

 protected:
  TSTagReaderSpec* spec_{nullptr};
  k_uint64 object_id_{0};
  k_uint32 total_read_row_{0};  // total count
  TsTagScanParser param_;
  // Filter for tag data scan.
  Field* filter_{nullptr};
  /**
   * Relational tag row batch for HashTagScan output
   * which can be considered as a virtual tag scan
   * data result.
   */
  RelTagRowBatchPtr rel_tag_rowbatch_{nullptr};
  /**
   * Batch container used to keep the build side data chunk.
   */
  BatchDataContainerPtr batch_data_container_{nullptr};
  // Storage handle to scan tag data.
  StorageHandler* handler_{nullptr};
  // Relational key/join columns used to do hash join.
  vector<k_uint32> primary_rel_cols_;
  // Tag key/join columns used to do hash join.
  vector<k_uint32> primary_tag_cols_;
  /**
   * Relation other join columns except the columns used to join primary tags,
   * this is only for primaryHashTagScan mode.
   */
  vector<k_uint32> rel_other_join_cols_;
  /**
   * Tag other join columns except the primary tags,
   * this is only for primaryHashTagScan mode.
   */
  vector<k_uint32> tag_other_join_cols_;
  /**
   * Relational batch container used to keep the relational
   * data chunk pushed down from ME.
   */
  RelBatchQueue* rel_batch_queue_;
  // Dynamic hash index built with build side data.
  DynamicHashIndex* dynamic_hash_index_{nullptr};
  // The map from tags to output columns.
  std::unordered_map<k_uint32, k_uint32> scan_tag_to_output;
  // Primary tag values used to scan tag data.
  std::vector<void*> primary_tags_;
  // Renders to convert tag row batch into data chunk.
  Field **tag_renders_{nullptr};
  // Column infos to convert tag row batch into data chunk.
  ColumnInfo *tag_col_info_{nullptr};
  k_int32 tag_col_size_{0};
  // The length of each join column.
  std::vector<k_uint32> join_column_lengths_;
  // The total length of join columns.
  k_uint32 total_join_column_length_{0};

 private:
  mutable std::mutex tag_lock_;
  bool is_init_{false};
  bool started_{false};
  EEIteratorErrCode init_code_;
  EEIteratorErrCode start_code_;
  bool tag_index_once_{true};
  bool is_first_entity_{true};
};

}  // namespace kwdbts
