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

#include "ts_table_snapshot.h"

namespace kwdbts {


TSSlice SnapshotPayloadDataBlockPart::GenData() {
  if (res_list->size() == 0) {
    return TSSlice{nullptr, 0};
  }
  // statistics var type value size in this res_list.
  size_t var_data_length = 0;
  for (auto& block : (*res_list)) {
    for (auto& var_col : block->var_col_values) {
      var_data_length += KUint16(var_col.get()) + 2;
    }
  }
  auto& one_block = *(res_list->front());
  size_t one_block_fix_size = sizeof(one_block.block_item);
  for (auto& col : one_block.col_block_addr) {
    one_block_fix_size += col.len;
  }
  size_t all_block_fixed_part_size = one_block_fix_size * res_list->size();

  //              keylen + key + blockmaxrow + blkitem len +
  size_t data_len = 4 + primary_key.len + 4 +    4 +
        // blkitem num + blockitemvars + col num + col block size
              4 + res_list->size() * 4  + 4 + one_block.col_block_addr.size() * 4 +
              all_block_fixed_part_size + var_data_length;
  char* data_mem = reinterpret_cast<char*>(malloc(data_len));
  memset(data_mem, 0, data_len);
  char* mem_write_loc = data_mem;
  KUint32(mem_write_loc) = primary_key.len;
  mem_write_loc += 4;
  memcpy(mem_write_loc, primary_key.data, primary_key.len);
  mem_write_loc += primary_key.len;
  KUint32(mem_write_loc) = one_block.rows;
  mem_write_loc += 4;
  KUint32(mem_write_loc) = sizeof(one_block.block_item);  // one block item size.
  mem_write_loc += 4;
  KUint32(mem_write_loc) = res_list->size();  // block item number in this data.
  mem_write_loc += 4;
  for (auto& block : *res_list) {
    KUint32(mem_write_loc) = block->var_col_values.size();
    mem_write_loc += 4;
  }
  KUint32(mem_write_loc) = one_block.col_block_addr.size();  // col number following blockitem.
  mem_write_loc += 4;
  for (auto& col : one_block.col_block_addr) {  // every col block size
    KUint32(mem_write_loc) = col.len;
    mem_write_loc += 4;
  }
  char* start_block_item_addr = mem_write_loc;
  size_t fix_addr_offset = 0;
  char* start_var_type_value_addr = start_block_item_addr + all_block_fixed_part_size;
  size_t var_mem_offset = 0;
  for (auto& block : (*res_list)) {
    memcpy(start_block_item_addr + fix_addr_offset, &(block->block_item), sizeof(block->block_item));
    fix_addr_offset += sizeof(block->block_item);
    for (auto& col : one_block.col_block_addr) {
      memcpy(start_block_item_addr + fix_addr_offset, col.data, col.len);
      fix_addr_offset += col.len;
    }
    for (auto& var_col : block->var_col_values) {
      uint16_t value_len = KUint16(var_col.get()) + 2;
      memcpy(start_var_type_value_addr + var_mem_offset, var_col.get(), value_len);
      var_mem_offset += value_len;
    }
  }
  return TSSlice{data_mem, data_len};
}

SnapshotPayloadDataBlockPart SnapshotPayloadDataBlockPart::ParseData(TSSlice data) {
  char* mem_read_loc = data.data;
  uint32_t primary_key_len = KUint32(mem_read_loc);
  mem_read_loc += 4;
  TSSlice primary_key{mem_read_loc, primary_key_len};
  mem_read_loc += primary_key_len;
  uint32_t block_max_row_num = KUint32(mem_read_loc);
  mem_read_loc += 4;
  uint32_t block_item_size = KUint32(mem_read_loc);
  mem_read_loc += 4;
  uint32_t block_item_num = KUint32(mem_read_loc);  // block item numbers.
  mem_read_loc += 4;
  std::vector<uint32_t> block_var_values;           // every block item has how many var-type values.
  for (size_t i = 0; i < block_item_num; i++) {
    block_var_values.push_back(KUint32(mem_read_loc));
    mem_read_loc += 4;
  }
  uint32_t col_num = KUint32(mem_read_loc);       // column number
  mem_read_loc += 4;
  std::vector<uint32_t> col_block_size;           // every column block size
  for (size_t i = 0; i < col_num; i++) {
    col_block_size.push_back(KUint32(mem_read_loc));
    mem_read_loc += 4;
  }
  char* start_block_item_addr = mem_read_loc;
  auto res_list = new std::list<TsBlockFullData*>();
  BlockItem blk_item;
  char* blk_item_reading_loc = start_block_item_addr;
  for (size_t i = 0; i < block_item_num; i++) {           // scan blockitem one by one
    std::vector<TSSlice> col_block_addr;
    memcpy(&blk_item, blk_item_reading_loc, block_item_size);
    blk_item_reading_loc += block_item_size;
    for (size_t j = 0; j < col_block_size.size(); j++) {    // get column blocks of current blockitem
      col_block_addr.push_back({blk_item_reading_loc, col_block_size[j]});
      blk_item_reading_loc += col_block_size[j];
    }
    res_list->push_back(new TsBlockFullData{block_max_row_num, blk_item, col_block_addr});
  }
  // parse all var-type column values
  char* var_type_vaule_addr = blk_item_reading_loc;
  size_t cur_offset = var_type_vaule_addr - data.data;
  assert(cur_offset <= data.len);
  std::list<TSSlice> var_col_values;
  while (true) {
    if (cur_offset >= data.len) {
      break;
    }
    size_t val_len = KUint16(data.data + cur_offset);
    var_col_values.push_back(TSSlice{data.data + cur_offset, val_len + 2});
    cur_offset += val_len + 2;
  }
  // fill var_col_values of TsBlockFullData
  auto var_list_iter = var_col_values.begin();
  int block_idx = 0;
  for (auto& block : *res_list) {
    for (size_t i = 0; i < block_var_values[block_idx]; i++) {
      assert(var_list_iter != var_col_values.end());
      // NOTICE: this var-type column value memory is in data, so cannot deleted by std::shard_ptr.
      block->var_col_values.push_back(std::shared_ptr<void>(var_list_iter->data, DoNothingDeleter()));
      var_list_iter++;
    }
    block_idx++;
  }
  return SnapshotPayloadDataBlockPart(primary_key, block_max_row_num, res_list, true);
}

KStatus TsSnapshotProductorByPayload::SerializeData(kwdbContext_p ctx, const std::list<SnapshotBlockDataInfo>& res_list,
                                                    TSSlice* data, TsSnapshotDataType* type, bool is_only_tag_data) {
  *type = TsSnapshotDataType::PAYLOAD_COL_BASED_DATA;
  size_t  cur_rows_in_res = 0;
  for (auto& res : res_list) {
    cur_rows_in_res += res.count;
  }
  // create payload builder with tag info.
  auto pl_template = getPlBuilderTemplate(res_list.front().res->entity_index, is_only_tag_data);
  if (UNLIKELY(pl_template == nullptr)) {
    LOG_ERROR("pl_template is null pointer");
    return KStatus::FAIL;
  }
  PayloadBuilder py_builder(*pl_template);
  py_builder.SetDataRows(cur_rows_in_res);
  // put all rows in res_list into payload.
  size_t start_pos = 0;
  KStatus s;
  for (auto& res : res_list) {
    s = putResIntoPayload(py_builder, *(res.res), res.count, start_pos);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("put res into payload failed.");
      return KStatus::FAIL;
    }
    start_pos += res.count;
  }
  LOG_DEBUG("SerializeData payload row data: %lu.", cur_rows_in_res);
  // construct payload
  if (!py_builder.Build(data, snapshot_info_.table->GetHashNum())) {
    LOG_ERROR("Payload build failed when build snapshot, snapshot id[%lu].", snapshot_info_.id);
    return KStatus::FAIL;
  }
  total_rows_ += cur_rows_in_res;
  return KStatus::SUCCESS;
}

PayloadBuilder* TsSnapshotProductorByPayload::genPayloadWithTag(const TagRowNum& tag_row) {
  int error_code;
  ResultSet tag_res{(k_uint32)tag_attribute_info_exclude_dropped_.size()};
  if (cur_tag_table_version_ != tag_row.tag_version) {
    // need calc new scan idxs
    error_code = subgrp_data_scan_ctx_.cur_entity_group->GetSubEntityGroupNewTagbt()->
                 CalculateSchemaIdxs(tag_row.tag_version,
                 result_scan_tag_idxs_,
                 tag_schema_include_dropped_,
                 &cur_src_scan_tag_idxs_);
    if (error_code < 0) {
      LOG_ERROR("CalculateSchemaIdxs failed.");
      return nullptr;
    }
    cur_tag_table_version_ = tag_row.tag_version;
  }
  error_code = subgrp_data_scan_ctx_.cur_entity_group->GetSubEntityGroupNewTagbt()->
                GetColumnsByRownumLocked(tag_row.tag_version, tag_row.tag_row_id,
                                         cur_src_scan_tag_idxs_, tag_schema_include_dropped_, &tag_res);
  if (error_code < 0) {
    LOG_ERROR("GetColumnsByRownumLocked failed. error_code= %d tag_row_id: %u ", error_code, tag_row.tag_row_id);
    return nullptr;
  }
  auto cur_entity_pl_builder = new PayloadBuilder(tag_attribute_info_exclude_dropped_, pl_metric_attribute_info_);
  for (size_t i = 0; i < tag_attribute_info_exclude_dropped_.size(); i++) {
    if (tag_res.data[i].size() == 0) {
      continue;
    }
    const Batch* tag_col_batch = tag_res.data[i].at(0);
    if (tag_col_batch == nullptr) {
      continue;
    }
    TagInfo* tag_schema_ptr = &tag_attribute_info_exclude_dropped_[i];
    char* value_addr = nullptr;
    int value_len = 0;
    // Different tags are stored differently in payload
    if (tag_schema_ptr->isPrimaryTag()) {
      value_addr = reinterpret_cast<char*>(tag_col_batch->mem);
      value_len = tag_schema_ptr->m_size;
    } else {
      if (tag_schema_ptr->m_data_type == DATATYPE::VARSTRING ||
          tag_schema_ptr->m_data_type == DATATYPE::VARBINARY) {
        value_addr = reinterpret_cast<char*>(tag_col_batch->getVarColData(0));
        value_len = tag_col_batch->getVarColDataLen(0);
      } else {
        value_addr = reinterpret_cast<char*>(tag_col_batch->mem) + 1;
        value_len = tag_schema_ptr->m_size;
      }
    }
    bool is_null_value = false;

    if (tag_col_batch->isNull(0, &is_null_value) != KStatus::SUCCESS) {
      continue;
    }
    if (!is_null_value || tag_schema_ptr->isPrimaryTag()) {  // null bitmap.
      cur_entity_pl_builder->SetTagValue(i, value_addr, value_len);
    }
  }

  return cur_entity_pl_builder;
}

PayloadBuilder* TsSnapshotProductorByPayload::getPlBuilderTemplate(const EntityResultIndex& idx,
                                                                   bool is_only_tag_data) {
  if (!is_only_tag_data) {
    assert(egrp_iter_->first == idx.entityGroupId);
    assert(subgrp_iter_->first == idx.subGroupId);
  }
  if (cur_entity_pl_builder_ != nullptr) {
    if (cur_pl_builder_entity_.equalsWithoutMem(idx)) {
      // same entity id as payload builder.
      return cur_entity_pl_builder_;
    }
    delete cur_entity_pl_builder_;
    cur_entity_pl_builder_ = nullptr;
  }

  TagRowNum tag_row = {0, 0};
  for (auto& entity : entity_map_[idx.entityGroupId][idx.subGroupId]) {
    if (entity.first == idx.entityId) {
      tag_row = entity.second;
      break;
    }
  }
  assert(tag_row.tag_row_id >= 0);
  cur_pl_builder_entity_ = idx;
  cur_entity_pl_builder_ = genPayloadWithTag(tag_row);
  return cur_entity_pl_builder_;
}

KStatus TsSnapshotProductorByPayload::putResIntoPayload(PayloadBuilder& pl_builder, const ResultSet &res,
                                                        uint32_t count, uint32_t start_pos) {
  uint32_t batch_num = res.data[0].size();
  for (int batch_index = 0; batch_index < batch_num; batch_index++) {
    // uint32_t batch_data_count = res.data[0][batch_index]->count;
    // Set the metrics data to payload builder
    for (size_t i = 0; i < kw_scan_cols_.size(); ++i) {
      size_t column = kw_scan_cols_[i];
      auto batch = res.data[column][batch_index];
      if (batch->count == 0 && batch->bitmap == nullptr) {
        continue;
      }
      int actual_col = ts_scan_cols_[column];
      for (int batch_data_index = 0; batch_data_index < batch->count; batch_data_index++) {
        bool is_null = false;
        batch->isNull(batch_data_index, &is_null);
        if (is_null) {
          // LOG_INFO("The data is null at column[%ld:%s] batch[%d]",
          //           column, metrics_schema_include_dropped_[column].name, batch_data_index);
          pl_builder.SetColumnNull(batch_data_index + start_pos, column);
          continue;
        }
        char* value_addr = nullptr;
        int value_len = 0;
        if (isVarLenType(metrics_schema_include_dropped_[actual_col].type)) {
          // Variable-length type
          value_addr = reinterpret_cast<char*>(batch->getVarColData(batch_data_index));
          value_len = batch->getVarColDataLen(batch_data_index);
        } else {
          value_len = metrics_schema_include_dropped_[actual_col].size;
          value_addr = reinterpret_cast<char*>(batch->mem) + value_len * batch_data_index;
        }
        pl_builder.SetColumnValue(batch_data_index + start_pos, column, value_addr, value_len);
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductorByPayload::getEnoughRows(kwdbContext_p ctx,
        std::list<SnapshotBlockDataInfo>* res_list, bool* need_change_subgroup) {
  *need_change_subgroup = false;
  res_list->clear();
  uint32_t cur_rows_in_res = 0;
  if (subgrp_data_scan_ctx_.entity_first_batch.res != nullptr) {
    res_list->push_back(subgrp_data_scan_ctx_.entity_first_batch);
    cur_rows_in_res = subgrp_data_scan_ctx_.entity_first_batch.count;
    subgrp_data_scan_ctx_.entity_first_batch = {nullptr, 0, false};
  }
  KStatus s;
  uint32_t count = 0;
  // bool need_change_entity = false;
  uint32_t col_num = ts_scan_cols_.size();
  while (true) {
    ResultSet *res = new ResultSet(col_num);
    count = 0;
    s = subgrp_data_scan_ctx_.sub_grp_data_iter->Next(res, &count);
    if (s != KStatus::SUCCESS) {
      delete res;
      LOG_ERROR("next failed during build snapshot, subgroup_id[%u]", subgrp_iter_->first);
      return s;
    }
    if (count == 0) {
      delete res;
      *need_change_subgroup = true;
      LOG_DEBUG("NextDataUsingPayload need_change_subgroup[%u].", subgrp_iter_->first);
      break;
    }
    auto last_entity_info = subgrp_data_scan_ctx_.cur_block_entity_info;
    subgrp_data_scan_ctx_.cur_block_entity_info = res->entity_index;
    if (last_entity_info.entityGroupId == 0 || last_entity_info.equalsWithoutMem(res->entity_index)) {
      last_entity_info = res->entity_index;
      res_list->push_back({res, count, false});
      cur_rows_in_res += count;
      if (cur_rows_in_res >= snapshot_payload_rows_num) {
        break;
      }
    } else {
      // change entity, we need store current res and convert entity info below.
      subgrp_data_scan_ctx_.entity_first_batch = {res, count, false};
      // need_change_entity = true;
      break;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsSnapshotProductorByBlock::getEnoughRows(kwdbContext_p ctx, std::list<SnapshotBlockDataInfo>* res_list,
                                                  bool* need_change_subgroup) {
  *need_change_subgroup = false;
  // bool need_change_entity = false;
  KStatus s;
  uint32_t count = 0;
  uint32_t cur_rows_in_res = 0;
  if (subgrp_data_scan_ctx_.entity_first_batch.res != nullptr) {
    cur_rows_in_res = subgrp_data_scan_ctx_.entity_first_batch.count;
    res_list->push_back(std::move(subgrp_data_scan_ctx_.entity_first_batch));
    subgrp_data_scan_ctx_.entity_first_batch = {nullptr, 0, false};
  }
  uint32_t col_num = ts_scan_cols_.size();
  while (true) {
    bool is_finished = false;
    ResultSet* res = new ResultSet();
    TsBlockFullData* block_data = new TsBlockFullData();
    s = subgrp_data_scan_ctx_.sub_grp_data_iter->NextFullBlock(res, block_data, &is_finished);
    if (s != KStatus::SUCCESS) {
      delete res;
      delete block_data;
      LOG_ERROR("next failed during build snapshot, subgroup_id[%u]", subgrp_iter_->first);
      return s;
    }
    count += block_data->rows;
    if (is_finished) {
      delete res;
      delete block_data;
      *need_change_subgroup = true;
      LOG_DEBUG("NextDataUsingPayload need_change_subgroup[%u].", subgrp_iter_->first);
      break;
    }
    auto last_entity_info = subgrp_data_scan_ctx_.cur_block_entity_info;
    subgrp_data_scan_ctx_.cur_block_entity_info = res->entity_index;
    if (last_entity_info.entityGroupId == 0 || last_entity_info.equalsWithoutMem(res->entity_index)) {
      res_list->push_back(std::move(SnapshotBlockDataInfo{res, block_data->rows, true, block_data}));
      cur_rows_in_res += count;
      if (cur_rows_in_res >= snapshot_payload_rows_num) {
        break;
      }
    } else {
      // change entity, we need store current res and convert entity info below.
      subgrp_data_scan_ctx_.entity_first_batch = std::move(SnapshotBlockDataInfo{res, count, true, block_data});
      // need_change_entity = true;
      break;
    }
  }
  return KStatus::SUCCESS;
}

TSSlice TsSnapshotProductorByBlock::getPrimaryKey(EntityResultIndex* entity_id) {
  if (cur_primary_key_idx_.equalsWithoutMem(*entity_id)) {
    return cur_primary_key_;
  }
  if (cur_primary_key_.data != nullptr) {
    free(cur_primary_key_.data);
    cur_primary_key_ = {nullptr, 0};
  }
  // get tage row id of tag table.
  TagRowNum tag_row = {0, 0};
  for (auto& entity : entity_map_[entity_id->entityGroupId][entity_id->subGroupId]) {
    if (entity.first == entity_id->entityId) {
      tag_row = entity.second;
      break;
    }
  }
  assert(tag_row.tag_row_id >= 0);
  int error_code;
  ResultSet tag_res{(k_uint32)tag_attribute_info_exclude_dropped_.size()};
  // subgrp_data_scan_ctx_.cur_entity_group->GetSubEntityGroupNewTagbt()->
  //                     GetColumnsByRownumLocked(tag_row_id, tag_schema_infos_, &tag_res);
  if (cur_tag_table_version_ != tag_row.tag_version) {
    // need calc new scan idxs
    error_code = subgrp_data_scan_ctx_.cur_entity_group->GetSubEntityGroupNewTagbt()->
                 CalculateSchemaIdxs(tag_row.tag_version,
                 result_scan_tag_idxs_,
                 tag_schema_include_dropped_,
                 &cur_src_scan_tag_idxs_);
    if (error_code < 0) {
      LOG_ERROR("CalculateSchemaIdxs failed.");
      return cur_primary_key_;
    }
    cur_tag_table_version_ = tag_row.tag_version;
  }
  error_code = subgrp_data_scan_ctx_.cur_entity_group->GetSubEntityGroupNewTagbt()->
                GetColumnsByRownumLocked(tag_row.tag_version, tag_row.tag_row_id,
                                         cur_src_scan_tag_idxs_, tag_schema_include_dropped_, &tag_res);
  if (error_code < 0) {
    LOG_ERROR("GetColumnsByRownumLocked failed. error_code= %d tag_row_id: %u ", error_code, tag_row.tag_row_id);
    return cur_primary_key_;
  }
  for (size_t i = 0; i < tag_attribute_info_exclude_dropped_.size(); i++) {
    const Batch* tag_col_batch = tag_res.data[i].at(0);
    TagInfo* tag_schema_ptr = &tag_attribute_info_exclude_dropped_[i];
    // Different tags are stored differently in payload
    if (tag_schema_ptr->isPrimaryTag()) {
      auto value_addr = reinterpret_cast<char*>(tag_col_batch->mem);
      auto value_len = tag_schema_ptr->m_size;
      if (cur_primary_key_.data == nullptr) {
        cur_primary_key_.data = static_cast<char*>(malloc(value_len));
        cur_primary_key_.len = value_len;
        memcpy(cur_primary_key_.data, value_addr, value_len);
      } else {
        cur_primary_key_.data = static_cast<char*>(realloc(cur_primary_key_.data, value_len + cur_primary_key_.len));
        memcpy(cur_primary_key_.data + cur_primary_key_.len, value_addr, value_len);
        cur_primary_key_.len = cur_primary_key_.len + value_len;
      }
    }
  }
  cur_primary_key_idx_ = *entity_id;
  return cur_primary_key_;
}

KStatus TsSnapshotProductorByBlock::SerializeData(kwdbContext_p ctx, const std::list<SnapshotBlockDataInfo>& res_list,
                                                  TSSlice* data, TsSnapshotDataType* type, bool is_only_tag_data) {
  *type = TsSnapshotDataType::BLOCK_ITEM_BASED_DATA;
  if (res_list.size() == 0) {
    LOG_WARN("SerializeData empty list.");
    return SUCCESS;
  }
  TSSlice pkey = getPrimaryKey(&(res_list.front().res->entity_index));
  std::list<TsBlockFullData*> res_list_full_blk;
  for (auto& data : res_list) {
    res_list_full_blk.push_back(std::move(data.blk_item));
  }
  uint32_t blk_rows = res_list.front().count;
  SnapshotPayloadDataBlockPart cur_part(pkey, blk_rows, &res_list_full_blk);
  *data = cur_part.GenData();
  total_rows_ += blk_rows * res_list.size();
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
