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

#include "ee_batch_data_container.h"
#include "ee_data_chunk.h"
#include "ee_field.h"
#include "ee_table.h"

namespace kwdbts {

KStatus BatchDataContainer::AddRelDataChunkAndBuildLinkedList(DynamicHashIndex* rel_hash_index,
                                                        DataChunkPtr &rel_data_chunk,
                                                        std::vector<k_uint32>& rel_key_cols,
                                                        std::vector<k_uint32>& join_column_lengths,
                                                        char* rel_join_column_value,
                                                        k_uint32 total_join_column_length) {
  // Build linked list for the coming relational batch
  k_uint32 row_count = rel_data_chunk->Count();
  RowIndice last_row_indice;
  RowIndice new_row_indice;
  LinkedListPtr linkedList = std::make_unique<BatchDataLinkedList>();
  if (nullptr == linkedList) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("linkedList new failed\n");
    return KStatus::FAIL;
  }
  int ret = linkedList->init(row_count);
  if (ret != 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INTERNAL_ERROR, "Linked list init failure");
    LOG_ERROR("linkedList init failed with %d.\n", ret);
    return KStatus::FAIL;
  }
  rel_data_linked_lists_.AddLinkedList(std::move(linkedList));
  new_row_indice.batch_no = rel_data_container_.GetBatchCount() + 1;
  for (new_row_indice.offset_in_batch = 1; new_row_indice.offset_in_batch <= row_count; ++new_row_indice.offset_in_batch) {
    // Get the join key value from relational batch
    memset(rel_join_column_value, 0, total_join_column_length);
    char* p = rel_join_column_value;
    k_bool has_null = false;
    for (int i = 0; i < rel_key_cols.size(); ++i) {
      if (rel_data_chunk->IsNull(new_row_indice.offset_in_batch - 1, rel_key_cols[i])) {
        has_null = true;
        break;
      }
      roachpb::DataType type = rel_data_chunk->GetColumnInfo()[rel_key_cols[i]].storage_type;
      switch (type) {
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::CHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::VARBINARY: {
          char* ptr = rel_data_chunk->GetData(new_row_indice.offset_in_batch - 1, rel_key_cols[i]);
          k_uint16 len = *reinterpret_cast<k_uint16 *>(ptr);
          memcpy(p, ptr + sizeof(k_uint16), len);
          break;
        }
        default:
          memcpy(p, rel_data_chunk->GetDataPtr(new_row_indice.offset_in_batch - 1, rel_key_cols[i]),
                  join_column_lengths[i]);
          break;
      }
      p += join_column_lengths[i];
    }
    if (has_null) {
      continue;
    }
    // Add or update hash table key value
    if (rel_hash_index->addOrUpdate(rel_join_column_value, total_join_column_length,
                                    new_row_indice, last_row_indice) != 0) {
      return KStatus::FAIL;
    }
    if (last_row_indice.batch_no > 0) {
      // Update linked list
      rel_data_linked_lists_.GetLinkedList(new_row_indice.batch_no - 1)->row_indice_list_[new_row_indice.offset_in_batch - 1]
        = last_row_indice;
    }
  }
  // Keep the relational data chunk in memory for probe and table reader later.
  return rel_data_container_.AddDataChunk(rel_data_chunk);
}

DataChunkPtr& BatchDataContainer::GetRelDataChunk(uint16_t batch_no) {
  return rel_data_container_.GetDataChunk(batch_no);
}

void BatchDataContainer::AddPhysicalTagData(ResultSet& res) {
  physical_tag_data_batches_.push_back(std::move(res.data));
}

BatchDataLinkedList::BatchDataLinkedList() {
}

int BatchDataLinkedList::init(k_uint16 row_count) {
  row_count_ = row_count;
  row_indice_list_ = static_cast<RowIndice*>(malloc(row_count_ * sizeof(RowIndice)));
  if (nullptr == row_indice_list_) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
    LOG_ERROR("row_indice_list_ malloc failed\n");
    return KWENOMEM;
  }
  memset(row_indice_list_, 0, row_count_ * sizeof(RowIndice));
  return 0;
}

BatchDataLinkedList::~BatchDataLinkedList() {
  if (row_indice_list_ && is_data_owner_) {
    free(row_indice_list_);
    row_indice_list_ = nullptr;
    row_count_ = 0;
  }
}

}  // namespace kwdbts
