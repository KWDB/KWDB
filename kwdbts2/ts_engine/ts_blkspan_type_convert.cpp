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

#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include "ts_blkspan_type_convert.h"
#include "ts_block.h"

namespace kwdbts {

TSBlkDataTypeConvert::TSBlkDataTypeConvert(TsBlockSpan& blk_span)
  : block_(blk_span.block_.get()),
    start_row_idx_(blk_span.start_row_),
    row_num_(blk_span.nrow_) {
}

// copyed from TsTimePartition::ConvertDataTypeToMem
int ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size, void* old_mem,
                                             std::shared_ptr<void> old_var_mem, std::shared_ptr<void>* new_mem) {
  ErrorInfo err_info;
  if (!isVarLenType(new_type)) {
    void* temp_new_mem = malloc(new_type_size + 1);
    memset(temp_new_mem, 0, new_type_size + 1);
    if (!isVarLenType(old_type)) {
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, reinterpret_cast<char*>(old_mem),
                                             reinterpret_cast<char*>(temp_new_mem), err_info);
      } else {
        err_info.errcode = convertFixedToNum(old_type, new_type, reinterpret_cast<char*>(old_mem),
                                             reinterpret_cast<char*>(temp_new_mem), err_info);
      }
      if (err_info.errcode < 0) {
        free(temp_new_mem);
        return err_info.errcode;
      }
    } else {
      uint16_t var_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
      std::string var_value(reinterpret_cast<char*>(old_var_mem.get()) + MMapStringColumn::kStringLenLen);
      convertStrToFixed(var_value, new_type, reinterpret_cast<char*>(temp_new_mem), var_len, err_info);
    }
    std::shared_ptr<void> ptr(temp_new_mem, free);
    *new_mem = ptr;
  } else {
    if (!isVarLenType(old_type)) {
      auto cur_var_data = convertFixedToVar(old_type, new_type, reinterpret_cast<char*>(old_mem), err_info);
      *new_mem = cur_var_data;
    } else {
      if (old_type == VARSTRING) {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get()) - 1;
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen);
        *reinterpret_cast<uint16_t*>(var_data) = old_len;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               reinterpret_cast<char*>(old_var_mem.get()) + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      } else {
        auto old_len = *reinterpret_cast<uint16_t*>(old_var_mem.get());
        char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen + 1));
        memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen + 1);
        *reinterpret_cast<uint16_t*>(var_data) = old_len + 1;
        memcpy(var_data + MMapStringColumn::kStringLenLen,
               reinterpret_cast<char*>(old_var_mem.get()) + MMapStringColumn::kStringLenLen, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      }
    }
  }
  return 0;
}

KStatus TSBlkDataTypeConvert::GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
                                               const AttributeInfo& dest_type, char** value, TsBitmap& bitmap) {
  assert(!isVarLenType(dest_type.type));
  if (blk_col_idx == UINT32_MAX) {
    *value = nullptr;
    return SUCCESS;
  }
  uint32_t dest_type_size = dest_type.size;
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, schema, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  char* blk_value;
  s = block_->GetColAddr(blk_col_idx, schema, &blk_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColAddr failed. col id [%u]", blk_col_idx);
    return s;
  }
  bitmap.SetCount(row_num_);
  if (isSameType(schema[blk_col_idx], dest_type)) {
    for (size_t i = 0; i < row_num_; i++) {
      DataFlags flag = blk_bitmap[start_row_idx_+ i];
      bitmap[i] = flag;
    }
    *value = blk_value + dest_type_size * start_row_idx_;
  } else {
    char* allc_mem = reinterpret_cast<char*>(malloc(dest_type_size * row_num_));
    if (allc_mem == nullptr) {
      LOG_ERROR("malloc failed. alloc size: %u", dest_type_size * row_num_);
      return KStatus::SUCCESS;
    }
    alloc_mems_.push_back(allc_mem);
    for (size_t i = 0; i < row_num_; i++) {
      bitmap[i] = blk_bitmap[start_row_idx_+ i];
      if (bitmap[i] != DataFlags::kValid) {
        continue;
      }
      void* old_mem = nullptr;
      std::shared_ptr<void> old_var_mem = nullptr;
      if (!isVarLenType(schema[blk_col_idx].type)) {
        old_mem = blk_value + schema[blk_col_idx].size * (start_row_idx_+ i);
      } else {
        // old_var_mem = segment_tbl->varColumnAddr(real_row, ts_col);
        LOG_ERROR("no implementtion");
        exit(1);
      }
      // table altered. column type changes.
      std::shared_ptr<void> new_mem;
      int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(schema[blk_col_idx].type),
                                                static_cast<DATATYPE>(dest_type.type),
                                                dest_type_size, old_mem, old_var_mem, &new_mem);
      if (err_code < 0) {
        LOG_WARN("failed ConvertDataType from %u to %u", schema[blk_col_idx].type, dest_type.type);
        // todo(liangbo01) make sure if convert failed. value is null.
        bitmap[i] = DataFlags::kNull;
      } else {
        memcpy(allc_mem + dest_type_size * i, new_mem.get(), dest_type_size);
      }
    }
    *value = allc_mem;
  }
  return KStatus::SUCCESS;
}

KStatus TSBlkDataTypeConvert::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t blk_col_idx,
                                                   const std::vector<AttributeInfo>& schema,
                                                   const AttributeInfo& dest_type, DataFlags& flag,
                                                   TSSlice& data) {
  assert(isVarLenType(dest_type.type));
  assert(row_idx < row_num_);
  assert(blk_col_idx < schema.size());
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, schema, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  flag = blk_bitmap[start_row_idx_+ row_idx];
  if (flag != DataFlags::kValid) {
    data = {nullptr, 0};
    return KStatus::SUCCESS;
  }
  TSSlice orig_value;
  s = block_->GetValueSlice(row_idx, blk_col_idx, schema, orig_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, blk_col_idx);
    return s;
  }
  if (isSameType(schema[blk_col_idx], dest_type)) {
    data = orig_value;
  } else {
    assert(!isVarLenType(schema[blk_col_idx].type));
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(schema[blk_col_idx].type),
                                              static_cast<DATATYPE>(dest_type.type),
                                              dest_type.size, orig_value.data, nullptr, &new_mem);
    if (err_code < 0) {
      LOG_WARN("failed ConvertDataType from %u to %u", schema[blk_col_idx].type, dest_type.type);
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
