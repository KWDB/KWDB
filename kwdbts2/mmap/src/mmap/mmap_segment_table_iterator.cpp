// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "mmap/mmap_segment_table_iterator.h"
#include "cm_func.h"
#include "st_config.h"

int convertFixedToNum(DATATYPE old_type, DATATYPE new_type, char* src, char* dst, ErrorInfo& err_info) {
  switch (old_type) {
    case DATATYPE::INT16 : {
      switch (new_type) {
        case DATATYPE::INT32:
          return convertNumToNum<int16_t, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<int16_t, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int16_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int16_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::INT32 : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<int32_t, int16_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<int32_t, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int32_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int32_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::INT64 : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<int64_t, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<int64_t, int32_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<int64_t, float>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<int64_t, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::FLOAT : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<float, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<float, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<float, int64_t>(src, dst);
        case DATATYPE::DOUBLE:
          return convertNumToNum<float, double>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::DOUBLE : {
      switch (new_type) {
        case DATATYPE::INT16:
          return convertNumToNum<double, int16_t>(src, dst);
        case DATATYPE::INT32:
          return convertNumToNum<double, int32_t>(src, dst);
        case DATATYPE::INT64:
          return convertNumToNum<double, int64_t>(src, dst);
        case DATATYPE::FLOAT:
          return convertNumToNum<double, float>(src, dst);
        default:
          break;
      }
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      return convertStrToFixed(std::string(src), new_type, dst, strlen(src), err_info);
    }

    default:
      break;
  }
  return 0;
}

int convertFixedToStr(DATATYPE old_type, char* old_data, char* new_data, ErrorInfo& err_info) {
  std::string res;
  switch (old_type) {
    case DATATYPE::INT16 : {
      res = std::to_string(KInt16(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(old_data));
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::FLOAT : {
      std::ostringstream oss;
      oss.clear();
      oss.precision(7);
      oss.setf(std::ios::fixed);
      oss << KFloat32(old_data);
      res = oss.str();
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::DOUBLE : {
      std::stringstream ss;
      ss.precision(15);
      ss.setf(std::ios::fixed);
      ss << KDouble64(old_data);
      res = ss.str();
      strcpy(new_data, res.data());
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      strcpy(new_data, old_data);
      break;
    }
    default:
      err_info.setError(KWEPERM, "Fixed type invalid");
      break;
  }
  return 0;
}

KStatus convertValueType2Newblk(std::shared_ptr<MMapSegmentTable> segment_tbl, BlockItem* cur_block_item,
                          const AttributeInfo& old_type, uint32_t col_id,
                          const AttributeInfo& new_type, TSSlice* new_block_addr, std::list<std::shared_ptr<void>>* var_values) {
  assert(old_type.type != new_type.type || old_type.size != new_type.size || isVarLenType(new_type.type));
  ErrorInfo err_info;
  // allocate new data type block memroy
  size_t blk_size = segment_tbl->GetColBlockSize(new_type.size);
  char* value = reinterpret_cast<char*>(malloc(blk_size));
  memset(value, 0, blk_size);
  *new_block_addr = {value, blk_size};
  // initial block header
  segment_tbl->CopyNullBitmap(cur_block_item->block_id, col_id, value);
  value += segment_tbl->GetColBlockHeaderSize(new_type.size);
  std::list<std::shared_ptr<void>> var_values_cur;
  if (!isVarLenType(new_type.type)) {
    if (old_type.type == new_type.type) {
      // fixed-len column type with diff length, alter column just increased column length
      for (int idx = 0; idx < cur_block_item->publish_row_count; idx++) {
        memcpy(value + idx * new_type.size, segment_tbl->columnAddrByBlk(cur_block_item->block_id, idx, col_id), old_type.size);
      }
      return KStatus::SUCCESS;
    } else {
      ErrorInfo err_info;
      if (!isVarLenType(old_type.type)) {
        // fixed-len column type to fixed-len column type
        for (k_uint32 i = 0; i < cur_block_item->publish_row_count; ++i) {
          void* old_mem = segment_tbl->columnAddrByBlk(cur_block_item->block_id, i, col_id);
          if (new_type.type == DATATYPE::CHAR || new_type.type == DATATYPE::BINARY) {
            err_info.errcode = convertFixedToStr((DATATYPE)old_type.type, (char*) old_mem, value + i * new_type.size, err_info);
          } else {
            err_info.errcode = convertFixedToNum((DATATYPE)old_type.type, (DATATYPE)new_type.type, (char*) old_mem, value + i * new_type.size, err_info);
          }
          if (err_info.errcode < 0) {
            return KStatus::FAIL;
          }
        }
      } else {
        // vartype cloumn to fixed-len column type
        for (k_uint32 i = 0; i < cur_block_item->publish_row_count; ++i) {
          std::shared_ptr<void> old_mem = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, i, col_id);
          std::string v_value((char*) old_mem.get() + kStringLenLen, KUint16(old_mem.get()));
          convertStrToFixed(v_value, (DATATYPE)new_type.type, value + i * new_type.size, KUint16(old_mem.get()), err_info);
        }
      }
    }
  } else {
    for (k_uint32 j = 0; j < cur_block_item->publish_row_count; ++j) {
      std::shared_ptr<void> data = nullptr;
      // row not deleted, and col not null.
      if (!isColDeleted(cur_block_item->rows_delete_flags, new_block_addr->data, j + 1)) {
        if (new_type.type != old_type.type) {
          // to vartype column
          data = ConvertToVarLen(segment_tbl, cur_block_item->block_id, static_cast<DATATYPE>(old_type.type),
                                static_cast<DATATYPE>(new_type.type), j, col_id);
        } else {
          data = segment_tbl->varColumnAddrByBlk(cur_block_item->block_id, j, col_id);
        }
        var_values->push_back(data);
        var_values_cur.push_back(data);
      }
    }
  }
  // reset agg result in block.
  if (cur_block_item->is_agg_res_available) {
    segment_tbl->ResetBlockAgg(cur_block_item, new_block_addr->data, new_type, var_values_cur);
  }
  return KStatus::SUCCESS;
}

// Online DDL canceled the validity check of data, alter column from string type to numeric type can succeed
// even if data is invalid. If convert a string column value to numeric type failed when queried, we should mark
// the value to be NULL. The column bitmap at the block header may be read only, so we should malloc and copy a
// new bitmap to return.
KStatus ConvertToFixedLen(std::shared_ptr<MMapSegmentTable> segment_tbl, char* value, BLOCK_ID block_id,
                          DATATYPE old_type, DATATYPE new_type, int32_t new_len, size_t start_row, k_uint32 count,
                          k_int32 col_idx, void** bitmap, bool& need_free_bitmap) {
  bool is_bitmap_new = false;
  if (!isVarLenType(old_type)) {
    // fixed-len column type to fixed-len column type
    for (k_uint32 i = 0; i < count; ++i) {
      ErrorInfo err_info;
      void* old_mem = segment_tbl->columnAddrByBlk(block_id, start_row + i - 1, col_idx);
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        convertFixedToStr(old_type, (char*) old_mem, value + i * new_len, err_info);
      } else {
        if (convertFixedToNum(old_type, new_type, (char*) old_mem, value + i * new_len, err_info) < 0) {
          if (!is_bitmap_new) {
            void* new_bitmap = malloc(BLOCK_ITEM_BITMAP_SIZE);
            memcpy(new_bitmap, *bitmap, BLOCK_ITEM_BITMAP_SIZE);
            *bitmap = new_bitmap;
            is_bitmap_new = true;
            need_free_bitmap = true;
          }
          SetObjectColNull(reinterpret_cast<char*>(*bitmap), start_row + i - 1);
        }
      }
    }
  } else {
    // variable-length column to fixed-len column type
    for (k_uint32 i = 0; i < count; ++i) {
      if (IsObjectColNull(reinterpret_cast<const char*>(*bitmap), start_row + i - 1)) {
        continue;
      }
      std::shared_ptr<void> old_mem = segment_tbl->varColumnAddrByBlk(block_id, start_row + i - 1, col_idx);
      std::string v_value;
      if (old_type == DATATYPE::VARBINARY) {
        v_value = std::string((char*) old_mem.get() + kStringLenLen, KUint16(old_mem.get()));
      } else {
        v_value = std::string((char*) old_mem.get() + kStringLenLen, KUint16(old_mem.get()) - 1);
      }
      ErrorInfo err_info;
      if (convertStrToFixed(v_value, new_type, value + i * new_len, KUint16(old_mem.get()), err_info) < 0) {
        if (!is_bitmap_new) {
          void* new_bitmap = malloc(BLOCK_ITEM_BITMAP_SIZE);
          memcpy(new_bitmap, *bitmap, BLOCK_ITEM_BITMAP_SIZE);
          *bitmap = new_bitmap;
          is_bitmap_new = true;
          need_free_bitmap = true;
        }
        SetObjectColNull(reinterpret_cast<char*>(*bitmap), start_row + i - 1);
      }
    }
  }
  return KStatus::SUCCESS;
}

std::shared_ptr<void> ConvertToVarLen(std::shared_ptr<MMapSegmentTable> segment_tbl, BLOCK_ID block_id,
                                      DATATYPE old_type, DATATYPE new_type, size_t row_idx, k_int32 col_idx) {
  ErrorInfo err_info;
  std::shared_ptr<void> data = nullptr;
  if (!isVarLenType(old_type)) {
    void* old_mem = segment_tbl->columnAddrByBlk(block_id, row_idx, col_idx);
    data = convertFixedToVar(old_type, new_type, (char*) old_mem, err_info);
  } else {
    if (old_type == VARSTRING) {
      auto old_data = segment_tbl->varColumnAddrByBlk(block_id, row_idx, col_idx);
      auto old_len = KUint16(old_data.get()) - 1;
      char* var_data = static_cast<char*>(std::malloc(old_len + kStringLenLen));
      memset(var_data, 0, old_len + kStringLenLen);
      KUint16(var_data) = old_len;
      memcpy(var_data + kStringLenLen,
             (char*) old_data.get() + kStringLenLen, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    } else {
      auto old_data = segment_tbl->varColumnAddrByBlk(block_id, row_idx, col_idx);
      auto old_len = KUint16(old_data.get());
      char* var_data = static_cast<char*>(std::malloc(old_len + kStringLenLen + 1));
      memset(var_data, 0, old_len + kStringLenLen + 1);
      KUint16(var_data) = old_len + 1;
      memcpy(var_data + kStringLenLen,
             (char*) old_data.get() + kStringLenLen, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    }
  }
  return data;
}

KStatus getActualColBitmap(std::shared_ptr<MMapSegmentTable> segment_tbl, BLOCK_ID block_id, size_t start_row,
                           AttributeInfo attr, uint32_t col_idx, k_uint32 count, void** bitmap, bool& need_free_bitmap) {
  *bitmap = segment_tbl->columnNullBitmapAddr(block_id, col_idx);
  auto& schema_info = segment_tbl->getSchemaInfo();
  if (!isVarLenType(attr.type)) {
    if (schema_info[col_idx].type != attr.type) {
      // Conversion from other types to fixed length types.
      char* value = static_cast<char*>(malloc(attr.size * count));
      memset(value, 0, attr.size * count);
      KStatus s = ConvertToFixedLen(segment_tbl, value, block_id,
                                    (DATATYPE)(schema_info[col_idx].type), (DATATYPE)(attr.type),
                                    attr.size, start_row, count, col_idx, bitmap, need_free_bitmap);
      if (s != KStatus::SUCCESS) {
        free(value);
        return s;
      }
      free(value);
    }
  }
  return KStatus::SUCCESS;
}

KStatus MMapSegmentTableIterator::GetFullBlock(BlockItem* cur_block_item, TsBlockFullData* blk_info) {
  blk_info->col_block_addr.resize(kw_scan_cols_.size());
  blk_info->block_item = *cur_block_item;
  // add all column data to res
  auto& schema_info = segment_table_->getSchemaInfo();
  ErrorInfo err_info;
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 ts_col = -1;
    if (i < ts_scan_cols_.size()) {
      ts_col = ts_scan_cols_[i];
    }
    assert(ts_col >= 0);
    TSSlice block_addr;
    if (!segment_table_->isColExist(ts_col)) {
      // column not exist in segment table. so return nullptr.
      block_addr.len =  segment_table_->GetColBlockSize(attrs_[i].size);
      block_addr.data = reinterpret_cast<char*>(malloc(block_addr.len));
      memset(block_addr.data, 0xFF, block_addr.len);   // set all data bitmap invalid.
      // set agg count = 0
      KUint16(block_addr.data + (segment_table_->getBlockMaxRows() + 7) / 8) = 0;
      LOG_DEBUG("cannot found col [%d], maybe no exists in this segment.", ts_col);
    } else {
      if (!isVarLenType(attrs_[ts_col].type) &&
          schema_info[ts_col].type == attrs_[ts_col].type && schema_info[ts_col].size == attrs_[ts_col].size) {
        // normal operation.
        block_addr.len = segment_table_->getDataBlockSize(ts_col);
        block_addr.data = reinterpret_cast<char*>(malloc(block_addr.len));
        void* blk_addr = segment_table_->getBlockHeader(blk_info->block_item.block_id, ts_col);
        memcpy(block_addr.data, blk_addr, block_addr.len);
      } else {
        // we need new block and copy original data to new one.
        // new block, input data, update agg result
        auto s = convertValueType2Newblk(segment_table_, &(blk_info->block_item), schema_info[ts_col],
                                      ts_col, attrs_[ts_col], &block_addr, &(blk_info->var_col_values));
        if (s != KStatus::SUCCESS) {
          LOG_ERROR("convertValueType2Newblk failed.");
          return KStatus::FAIL;
        }
      }
      // reset agg result in block.
      if (!blk_info->block_item.is_agg_res_available) {
        segment_table_->ResetBlockAgg(&blk_info->block_item, block_addr.data, schema_info[ts_col],
                                      blk_info->var_col_values);
      }
    }
    blk_info->col_block_addr[i] = block_addr;
    blk_info->need_del_mems.push_back(block_addr);
  }
  // recaculate agg result and store into new allocated block.
  blk_info->block_item.is_agg_res_available = true;
  return KStatus::SUCCESS;
}

KStatus MMapSegmentTableIterator::GetBatch(BlockItem* cur_block_item, size_t block_start_idx,
                                         ResultSet* res, k_uint32 count) {
  // add all column data to res
  auto& schema_info = segment_table_->getSchemaInfo();
  ErrorInfo err_info;
  for (k_uint32 i = 0; i < kw_scan_cols_.size(); ++i) {
    k_int32 col_idx = -1;
    if (i < ts_scan_cols_.size()) {
      col_idx = ts_scan_cols_[i];
    }
    Batch* b;
    if (col_idx >= 0 && segment_table_->isColExist(col_idx)) {
      void* bitmap_addr = segment_table_->getBlockHeader(cur_block_item->block_id, col_idx);
      if (!isVarLenType(attrs_[col_idx].type)) {
        if (schema_info[col_idx].type != attrs_[col_idx].type) {
          // other column type to fixed-len column type
          char* value = static_cast<char*>(malloc(attrs_[col_idx].size * count));
          memset(value, 0, attrs_[col_idx].size * count);
          bool need_free_bitmap = false;
          ConvertToFixedLen(segment_table_, value, cur_block_item->block_id, static_cast<DATATYPE>(schema_info[col_idx].type),
                            static_cast<DATATYPE>(attrs_[col_idx].type), attrs_[col_idx].size,
                            block_start_idx, count, col_idx, &bitmap_addr, need_free_bitmap);
          b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_table_);
          b->is_new = true;
          b->need_free_bitmap = need_free_bitmap;
        } else {
          if (schema_info[col_idx].size != attrs_[col_idx].size) {
            // fixed-len column type with diff len
            char* value = static_cast<char*>(malloc(attrs_[col_idx].size * count));
            memset(value, 0, attrs_[col_idx].size * count);
            for (int idx = block_start_idx - 1; idx < count; idx++) {
              memcpy(value + idx * attrs_[col_idx].size,
                     segment_table_->columnAddrByBlk(cur_block_item->block_id, idx, col_idx),
                     schema_info[col_idx].size);
            }
            b = new Batch(static_cast<void *>(value), count, bitmap_addr, block_start_idx, segment_table_);
            b->is_new = true;
          } else {
            b = new Batch(segment_table_->columnAddrByBlk(cur_block_item->block_id, block_start_idx - 1, col_idx),
                          count, bitmap_addr, block_start_idx, segment_table_);
          }
        }
      } else {
        b = new VarColumnBatch(count, bitmap_addr, block_start_idx, segment_table_);
        for (k_uint32 j = 0; j < count; ++j) {
          std::shared_ptr<void> data = nullptr;
          bool is_null;
          if (b->isNull(j, &is_null) != KStatus::SUCCESS) {
            delete b;
            b = nullptr;
            return KStatus::FAIL;
          }
          if (is_null) {
            data = nullptr;
          } else {
            if (schema_info[col_idx].type != attrs_[col_idx].type) {
              // to vartype column
              data = ConvertToVarLen(segment_table_, cur_block_item->block_id, static_cast<DATATYPE>(schema_info[col_idx].type),
                                     static_cast<DATATYPE>(attrs_[col_idx].type), block_start_idx + j - 1, col_idx);
            } else {
              data = segment_table_->varColumnAddrByBlk(cur_block_item->block_id, block_start_idx + j - 1, col_idx);
            }
          }
          b->push_back(data);
        }
      }
    } else {
      void* bitmap = nullptr;  // column not exist in segment table. so return nullptr.
      b = new Batch(bitmap, count, bitmap, block_start_idx, segment_table_);
    }
    res->push_back(i, b);
  }
  return KStatus::SUCCESS;
}

KStatus MMapSegmentTableIterator::Next(BlockItem* cur_block_item, k_uint32* start_offset,
                                     ResultSet* res, k_uint32* count) {
  *count = 0;
  res->clear();
  bool has_first = false;
  size_t block_start_idx = 0;
  // scan all data in blockitem.
  while (*start_offset <= cur_block_item->alloc_row_count) {
    bool is_row_valid = segment_table_->IsRowVaild(cur_block_item, *start_offset);
    // if cur_blockdata_offset_  row not valid or not in ts_span or not in lsn, we should check next row continue
    timestamp64 cur_ts = KTimestamp(segment_table_->columnAddrByBlk(cur_block_item->block_id, *start_offset - 1, 0));
    // LOG_DEBUG("ts: %ld, ts-span size %u, [%ld - %ld]", cur_ts, ts_spans_.size(), ts_spans_[0].begin, ts_spans_[0].end);
    if (!is_row_valid || !CheckIfTsInSpan(cur_ts)) {
      ++(*start_offset);
      // LOG_DEBUG("del or no match. %d, startoffset: %u", has_first, *start_offset);
      if (has_first) {
        break;
      }
      continue;
    }
    if (!has_first) {
      has_first = true;
      block_start_idx = *start_offset;
    }
    ++(*count);
    ++(*start_offset);
  }
  // LOG_DEBUG("has first: %d, block_start_idx: %lu, count: %u", has_first, block_start_idx, *count);
  if (has_first) {
    return GetBatch(cur_block_item, block_start_idx, res, *count);
  }
  return SUCCESS;
}
