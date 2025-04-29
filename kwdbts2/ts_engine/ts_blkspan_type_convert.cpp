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
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include "ts_blkspan_type_convert.h"
#include "ts_agg.h"
#include "ts_block.h"

namespace kwdbts {

// copyed from TsTimePartition::ConvertDataTypeToMem
int ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size, void* old_mem,
                                             std::shared_ptr<void> old_var_mem, std::shared_ptr<void>* new_mem) {
  ErrorInfo err_info;
  if (!isVarLenType(new_type)) {
    void* temp_new_mem = malloc(new_type_size + 1);
    memset(temp_new_mem, 0, new_type_size + 1);
    if (!isVarLenType(old_type)) {
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, (char*)old_mem, (char*)temp_new_mem, err_info);
      } else {
        err_info.errcode = convertFixedToNum(old_type, new_type, (char*) old_mem, (char*) temp_new_mem, err_info);
      }
      if (err_info.errcode < 0) {
        free(temp_new_mem);
        return err_info.errcode;
      }
    } else {
      uint16_t var_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
      std::string var_value((char*)old_var_mem.get() + MMapStringColumn::kStringLenLen);
      convertStrToFixed(var_value, new_type, (char*) temp_new_mem, var_len, err_info);
    }
    std::shared_ptr<void> ptr(temp_new_mem, free);
    *new_mem = ptr;
  } else {
    if (!isVarLenType(old_type)) {
      auto cur_var_data = convertFixedToVar(old_type, new_type, (char*)old_mem, err_info);
      *new_mem = cur_var_data;
    } else {
      if (old_type == VARSTRING) {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get()) - 1;
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen);
        *reinterpret_cast<uint16_t*>(var_data) = old_len;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      } else {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen + 1));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen + 1);
        *reinterpret_cast<uint16_t*>(var_data) = old_len + 1;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               (char*) old_var_mem.get() + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      }
    }
  }
  return 0;
}

KStatus TSBlkSpanDataTypeConvert::GetFixLenColAddr(uint32_t col_id, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& desc_type, char** value, TsBitmap& bitmap) {
  assert(!isVarLenType(desc_type.type));
  assert(col_id < schema.size());
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(col_id, schema, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", col_id);
    return s;
  }
  char* blk_value;
  s = block_->GetColAddr(col_id, schema, &blk_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColAddr failed. col id [%u]", col_id);
    return s;
  }
  bitmap.SetCount(row_num_);
  if (schema[col_id].type == desc_type.type) {
    for (size_t i = 0; i < row_num_; i++) {
      bitmap[i] = blk_bitmap[start_row_idx_+ i];
    }
    *value = blk_value + desc_type.size * start_row_idx_;
  } else {
    char* allc_mem = reinterpret_cast<char*>(malloc(desc_type.size * row_num_));
    if (allc_mem == nullptr) {
      LOG_ERROR("malloc failed. alloc size: %u", desc_type.size * row_num_);
      return KStatus::SUCCESS;
    }
    alloc_mems_.push_back(allc_mem);
    for (size_t i = 0; i < row_num_; i++) {
      bitmap[i] == blk_bitmap[start_row_idx_+ i];
      if (bitmap[i] != DataFlags::kValid) {
        continue;
      }
      void* old_mem = nullptr;
      std::shared_ptr<void> old_var_mem = nullptr;
      if (!isVarLenType(schema[col_id].type)) {
        old_mem = blk_value + schema[col_id].size * (start_row_idx_+ i);
      } else {
        // old_var_mem = segment_tbl->varColumnAddr(real_row, ts_col);
        LOG_ERROR("no implementtion");
        exit(1);
      }
      // table altered. column type changes.
      std::shared_ptr<void> new_mem;
      int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(schema[col_id].type),
                                                static_cast<DATATYPE>(desc_type.type),
                                                desc_type.size, old_mem, old_var_mem, &new_mem);
      if (err_code < 0) {
        LOG_WARN("failed ConvertDataType from %u to %u", schema[col_id].type, desc_type.type);
        // todo(liangbo01) make sure if convert failed. value is null.
        bitmap[i] = DataFlags::kNull;
      } else {
        memcpy(allc_mem + desc_type.size * i, new_mem.get(), desc_type.size);
      }
    }
    *value = allc_mem;
  }
  return KStatus::SUCCESS;
}

KStatus TSBlkSpanDataTypeConvert::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t col_idx, const std::vector<AttributeInfo>& schema,
 const AttributeInfo& desc_type, DataFlags& flag, TSSlice& data) {
  assert(isVarLenType(desc_type.type));
  assert(row_idx < row_num_);
  assert(col_idx < schema.size());
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(col_idx, schema, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", col_idx);
    return s;
  }
  flag = blk_bitmap[start_row_idx_+ row_idx];
  if (flag != DataFlags::kValid) {
    data = {nullptr, 0};
    return KStatus::SUCCESS;
  }
  TSSlice orig_value;
  s = block_->GetValueSlice(row_idx, col_idx, schema, orig_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, col_idx);
    return s;
  }
  if (schema[col_idx].type == desc_type.type) {
    data = orig_value;
  } else {
    assert(!isVarLenType(schema[col_idx].type));
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(schema[col_idx].type),
                                              static_cast<DATATYPE>(desc_type.type),
                                              desc_type.size, orig_value.data, nullptr, &new_mem);
    if (err_code < 0) {
      LOG_WARN("failed ConvertDataType from %u to %u", schema[col_idx].type, desc_type.type);
      // todo(liangbo01) make sure if convert failed. value is null.
      flag = DataFlags::kNull;
    } else {
      uint16_t col_len = KUint16(new_mem.get());
      char* allc_mem = reinterpret_cast<char*>(malloc(col_len));
      memcpy(allc_mem, reinterpret_cast<char*>(new_mem.get()) + MMapStringColumn::kStringLenLen, col_len);
      data.len = col_len;
      data.data = allc_mem;
      alloc_mems_.push_back(allc_mem);
    }
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
