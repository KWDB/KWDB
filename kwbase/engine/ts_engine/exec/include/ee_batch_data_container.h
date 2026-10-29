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

#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "ee_row_batch.h"
#include "ee_hash_tag_row_batch.h"
#include "kwdb_type.h"
#include "ts_common.h"
#include "ee_dynamic_hash_index.h"
#include "ee_data_chunk_container.h"
#include "ee_linkedlist_container.h"

namespace kwdbts {

class BatchDataContainer;
typedef std::shared_ptr<BatchDataContainer> BatchDataContainerPtr;
class BatchDataLinkedList;

// relational batch data linked list of hash index for multiple model processing
class BatchDataLinkedList {
 public:
  BatchDataLinkedList();
  ~BatchDataLinkedList();

  int init(k_uint16 row_count);
  void inline ResetDataPtr(RowIndice* newPtr) {
    if (row_indice_list_ && is_data_owner_) {
      free(row_indice_list_);
    }
    row_indice_list_ = newPtr;
    is_data_owner_ = false;
  }

  RowIndice* row_indice_list_{nullptr};
  k_uint16  row_count_{0};
 private:
  bool is_data_owner_{true};
};

// relational batch container to keep relational data from ME for multiple model processing
class BatchDataContainer {
 public:
  BatchDataContainer() {}
  ~BatchDataContainer() {
    for (auto data : physical_tag_data_batches_) {
      for (const auto& it : data) {
        for (auto batch : it) {
          delete batch;
        }
      }
    }
  }
  // get the data chunk with batch number
  DataChunkPtr& GetRelDataChunk(uint16_t batch_no);
  // add relational data chunk and build the linked list for each relational row
  KStatus AddRelDataChunkAndBuildLinkedList(DynamicHashIndex* rel_hash_index,
                                            DataChunkPtr &rel_data_chunk,
                                            std::vector<k_uint32>& rel_key_cols,
                                            std::vector<k_uint32>& join_column_lengths,
                                            char* rel_join_column_value,
                                            k_uint32 total_join_column_length);
  // add tag data result and keep it from being deleted
  void AddPhysicalTagData(ResultSet& res);

 public:
  // data chunk container to keep relational data
  DataChunkContainer rel_data_container_;
  std::vector<HashTagRowBatchPtr> tag_rowbatches_;
  LinkedListContainer rel_data_linked_lists_;
  std::vector<std::vector<std::vector<const Batch*>>> physical_tag_data_batches_;
};

};  // namespace kwdbts
