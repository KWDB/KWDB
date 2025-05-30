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
int ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size,
                         void* old_mem, uint16_t old_var_len, std::shared_ptr<void>* new_mem,
                         TsBitmap::Proxy& bit_flag) {
  ErrorInfo err_info;
  if (!isVarLenType(new_type)) {
    void* temp_new_mem = malloc(new_type_size + 1);
    memset(temp_new_mem, 0, new_type_size + 1);
    if (!isVarLenType(old_type)) {
      if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
        err_info.errcode = convertFixedToStr(old_type, static_cast<char*>(old_mem),
                                             static_cast<char*>(temp_new_mem), err_info);
      } else {
        err_info.errcode = convertFixedToNum(old_type, new_type, static_cast<char*>(old_mem),
                                             static_cast<char*>(temp_new_mem), err_info);
      }
      if (err_info.errcode < 0) {
        free(temp_new_mem);
        return err_info.errcode;
      }
    } else {
      std::string var_value(static_cast<char*>(old_mem));
      if (convertStrToFixed(var_value, new_type, static_cast<char*>(temp_new_mem), old_var_len, err_info) < 0) {
        bit_flag = DataFlags::kNull;
      }
    }
    std::shared_ptr<void> ptr(temp_new_mem, free);
    *new_mem = ptr;
  } else {
    if (!isVarLenType(old_type)) {
      auto cur_var_data = convertFixedToVar(old_type, new_type, static_cast<char*>(old_mem), err_info);
      *new_mem = cur_var_data;
    } else {
      if (old_type == VARSTRING) {
        auto old_len = old_var_len - 1;
        char* var_data = static_cast<char*>(std::malloc(old_len + kStringLenLen));
        memset(var_data, 0, old_len + kStringLenLen);
        *reinterpret_cast<uint16_t*>(var_data) = old_len;
        memcpy(var_data + kStringLenLen, old_mem, old_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      } else {
        char* var_data = static_cast<char*>(std::malloc(old_var_len + kStringLenLen + 1));
        memset(var_data, 0, old_var_len + kStringLenLen + 1);
        *reinterpret_cast<uint16_t*>(var_data) = old_var_len + 1;
        memcpy(var_data + kStringLenLen, old_mem, old_var_len);
        std::shared_ptr<void> ptr(var_data, free);
        *new_mem = ptr;
      }
    }
  }
  return 0;
}

KStatus TSBlkDataTypeConvert::GetColBitmap(uint32_t blk_col_idx, const std::vector<AttributeInfo>& schema,
                                            TsBitmap& bitmap) {
  return block_->GetColBitmap(blk_col_idx, schema, bitmap);
}

KStatus TSBlkDataTypeConvert::GetFixLenColAddr(uint32_t blk_col_idx, const std::vector<AttributeInfo>& blk_schema,
                                               const AttributeInfo& dest_type, char** value, TsBitmap& bitmap) {
  assert(!isVarLenType(dest_type.type));
  if (blk_col_idx == UINT32_MAX) {
    *value = nullptr;
    return SUCCESS;
  }
  uint32_t dest_type_size = dest_type.size;
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, blk_schema, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  bitmap.SetCount(row_num_);

  if (isSameType(blk_schema[blk_col_idx], dest_type)) {
    char* blk_value;
    s = block_->GetColAddr(blk_col_idx, blk_schema, &blk_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColAddr failed. col id [%u]", blk_col_idx);
      return s;
    }
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
      TSSlice orig_value;
      s = block_->GetValueSlice(start_row_idx_+ i, blk_col_idx, blk_schema, orig_value);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", start_row_idx_+ i, blk_col_idx);
        return s;
      }
      std::shared_ptr<void> new_mem;
      TsBitmap::Proxy proxy = bitmap[i];
      int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(blk_schema[blk_col_idx].type),
                                          static_cast<DATATYPE>(dest_type.type),
                                          dest_type_size, orig_value.data, orig_value.len, &new_mem, proxy);
      if (err_code < 0) {
        LOG_WARN("failed ConvertDataType from %u to %u", blk_schema[blk_col_idx].type, dest_type.type);
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
                                                   const std::vector<AttributeInfo>& blk_schema,
                                                   const AttributeInfo& dest_type, DataFlags& flag,
                                                   TSSlice& data) {
  assert(isVarLenType(dest_type.type));
  assert(row_idx < row_num_);
  assert(blk_col_idx < blk_schema.size());
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, blk_schema, blk_bitmap);
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
  s = block_->GetValueSlice(start_row_idx_ + row_idx, blk_col_idx, blk_schema, orig_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, blk_col_idx);
    return s;
  }
  if (isSameType(blk_schema[blk_col_idx], dest_type)) {
    data = orig_value;
  } else {
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    TsBitmap::Proxy proxy = blk_bitmap[start_row_idx_ + row_idx];
    int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(blk_schema[blk_col_idx].type),
                                        static_cast<DATATYPE>(dest_type.type),
                                        dest_type.size, orig_value.data, orig_value.len, &new_mem, proxy);
    if (err_code < 0) {
      LOG_WARN("failed ConvertDataType from %u to %u", blk_schema[blk_col_idx].type, dest_type.type);
      flag = DataFlags::kNull;
    } else {
      uint16_t col_len = KUint16(new_mem.get());
      char* allc_mem = reinterpret_cast<char*>(malloc(col_len));
      memcpy(allc_mem, reinterpret_cast<char*>(new_mem.get()) + kStringLenLen, col_len);
      data.len = col_len;
      data.data = allc_mem;
      alloc_mems_.push_back(allc_mem);
    }
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
