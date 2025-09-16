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
#include <memory>
#include <string>
#include <vector>
#include <algorithm>
#include "ts_blkspan_type_convert.h"
#include "ts_bitmap.h"
#include "ts_block.h"
#include "ts_table_schema_manager.h"
#include "ts_agg.h"
#include "ts_compressor.h"

namespace kwdbts {

TSBlkDataTypeConvert::TSBlkDataTypeConvert(uint32_t block_version, uint32_t scan_version,
                                           const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr)
  : block_version_(block_version), scan_version_(scan_version), tbl_schema_mgr_(tbl_schema_mgr) { }

KStatus TSBlkDataTypeConvert::Init() {
  if (tbl_schema_mgr_) {
    key_ = (static_cast<uint64_t>(block_version_) << 32) + scan_version_;
    auto res = tbl_schema_mgr_->FindVersionConv(key_, &version_conv_);
    if (!res) {
      std::shared_ptr<MMapMetricsTable> blk_metric;
      KStatus s = tbl_schema_mgr_->GetMetricSchema(block_version_, &blk_metric);
      if (s != SUCCESS) {
        LOG_ERROR("GetMetricSchema failed. table version [%u]", block_version_);
        return FAIL;
      }
      std::shared_ptr<MMapMetricsTable> scan_metric;
      s = tbl_schema_mgr_->GetMetricSchema(scan_version_, &scan_metric);
      if (s != SUCCESS) {
        LOG_ERROR("GetMetricSchema failed. table version [%u]", scan_version_);
        return FAIL;
      }
      auto& scan_cols = scan_metric->getIdxForValidCols();
      auto scan_attrs = static_cast<const std::vector<AttributeInfo>*>(&scan_metric->getSchemaInfoExcludeDropped());
      auto blk_attrs = static_cast<const std::vector<AttributeInfo>*>(&blk_metric->getSchemaInfoExcludeDropped());

      const auto blk_cols = blk_metric->getIdxForValidCols();
      // calculate column index in current block
      std::vector<uint32_t> blk_cols_extended;
      blk_cols_extended.resize(scan_cols.size());
      for (size_t i = 0; i < scan_cols.size(); i++) {
        bool found = false;
        auto it = std::find(blk_cols.begin(), blk_cols.end(), scan_cols[i]);
        found = (it != blk_cols.end());
        uint32_t j = 0;
        if (found) {
          j = std::distance(blk_cols.begin(), it);
        }
        if (found) {
          blk_cols_extended[i] = j;
        } else {
          blk_cols_extended[i] = UINT32_MAX;
        }
      }
      version_conv_ = std::make_shared<SchemaVersionConv>(scan_version_, blk_cols_extended, scan_attrs, blk_attrs);
      tbl_schema_mgr_->InsertVersionConv(key_, version_conv_);
    }
  }
  return SUCCESS;
}

// copyed from TsTimePartition::ConvertDataTypeToMem
int TSBlkDataTypeConvert::ConvertDataTypeToMem(uint32_t scan_col, int32_t new_type_size,
                         void* old_mem, uint16_t old_var_len, std::shared_ptr<void>* new_mem) {
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_col];
  DATATYPE old_type = static_cast<DATATYPE>((*version_conv_->blk_attrs_)[blk_col_idx].type);
  DATATYPE new_type = static_cast<DATATYPE>((*version_conv_->scan_attrs_)[scan_col].type);
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
        free(temp_new_mem);
        return err_info.errcode;
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

KStatus TSBlkDataTypeConvert::GetColBitmap(TsBlockSpan* blk_span, uint32_t scan_idx,
                                           std::unique_ptr<TsBitmapBase>* bitmap) {
  assert(blk_span->GetTableVersion() == block_version_);
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  if (blk_col_idx == UINT32_MAX) {
    *bitmap = std::make_unique<TsUniformBitmap<DataFlags::kNull>>(blk_span->nrow_);
    return SUCCESS;
  }
  if (isVarLenType((*version_conv_->blk_attrs_)[blk_col_idx].type) &&
      !isVarLenType((*version_conv_->scan_attrs_)[scan_idx].type)) {
    char* value = nullptr;
    auto ret = getColBitmapConverted(blk_span, scan_idx, bitmap);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("getColBitmapConverted failed.");
      return ret;
    }
    return SUCCESS;
  }
  std::unique_ptr<TsBitmapBase> blk_bitmap;
  auto s = blk_span->block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, &blk_bitmap);
  if (s == FAIL) {
    return s;
  }
  *bitmap = blk_bitmap->Slice(blk_span->start_row_, blk_span->nrow_);
  return s;
}

KStatus TSBlkDataTypeConvert::getColBitmapConverted(TsBlockSpan* blk_span, uint32_t scan_idx,
                                                    std::unique_ptr<TsBitmapBase>* bitmap) {
  assert(blk_span->GetTableVersion() == block_version_);
  auto dest_attr = (*version_conv_->scan_attrs_)[scan_idx];
  uint32_t dest_type_size = dest_attr.size;
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  std::unique_ptr<TsBitmapBase> blk_bitmap;
  auto s = blk_span->block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, &blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }

  char* allc_mem = reinterpret_cast<char*>(malloc(dest_type_size * blk_span->nrow_));
  if (allc_mem == nullptr) {
    LOG_ERROR("malloc failed. alloc size: %u", dest_type_size * blk_span->nrow_);
    return KStatus::SUCCESS;
  }
  memset(allc_mem, 0, dest_type_size * blk_span->nrow_);
  Defer defer([&]() { free(allc_mem); });

  // bitmap.SetCount(blk_span->nrow_);
  auto tmp_bitmap = std::make_unique<TsBitmap>(blk_span->nrow_);
  for (size_t i = 0; i < blk_span->nrow_; i++) {
    int block_row_idx = blk_span->start_row_ + i;
    (*tmp_bitmap)[i] = blk_bitmap->At(block_row_idx);
    if (tmp_bitmap->At(i) != DataFlags::kValid) {
      continue;
    }
    TSSlice orig_value;
    s = blk_span->block_->GetValueSlice(block_row_idx, blk_col_idx, version_conv_->blk_attrs_, orig_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetValueSlice failed. rowidx[%lu] colid[%u]", i, blk_col_idx);
      return s;
    }
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(scan_idx, dest_type_size, orig_value.data, orig_value.len, &new_mem);
    if (err_code < 0) {
      (*tmp_bitmap)[i] = DataFlags::kNull;
    }
  }
  *bitmap = std::move(tmp_bitmap);
  return KStatus::SUCCESS;
}

KStatus TSBlkDataTypeConvert::GetPreCount(TsBlockSpan* blk_span, uint32_t scan_idx, uint16_t &count) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetPreCount(version_conv_->blk_cols_extended_[scan_idx], count);
}

KStatus TSBlkDataTypeConvert::GetPreSum(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size,
  void *&pre_sum, bool &is_overflow) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetPreSum(version_conv_->blk_cols_extended_[scan_idx], size, pre_sum, is_overflow);
}

KStatus TSBlkDataTypeConvert::GetPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, void *&pre_max) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetPreMax(version_conv_->blk_cols_extended_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size, void *&pre_min) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetPreMin(version_conv_->blk_cols_extended_[scan_idx], size, pre_min);
}

KStatus TSBlkDataTypeConvert::GetVarPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, TSSlice &pre_max) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetVarPreMax(version_conv_->blk_cols_extended_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetVarPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, TSSlice &pre_min) {
  assert(blk_span->GetTableVersion() == block_version_);
  return blk_span->block_->GetVarPreMin(version_conv_->blk_cols_extended_[scan_idx], pre_min);
}

KStatus TSBlkDataTypeConvert::GetFixLenColAddr(TsBlockSpan* blk_span, uint32_t scan_idx, char** value,
                                               std::unique_ptr<TsBitmapBase>* bitmap) {
  assert(blk_span->GetTableVersion() == block_version_);
  if (!IsColExist(scan_idx)) {
    *value = nullptr;
    *bitmap = std::make_unique<TsUniformBitmap<DataFlags::kNull>>(blk_span->nrow_);
    return SUCCESS;
  }

  auto dest_attr = (*version_conv_->scan_attrs_)[scan_idx];
  uint32_t dest_type_size = dest_attr.size;
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  std::unique_ptr<TsBitmapBase> blk_bitmap;
  if (!(*version_conv_->blk_attrs_)[scan_idx].isFlag(AINFO_NOT_NULL)) {
    auto s = blk_span->block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, &blk_bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
      return s;
    }
  }

  if (IsSameType(scan_idx)) {
    char* blk_value;
    auto s = blk_span->block_->GetColAddr(blk_col_idx, version_conv_->blk_attrs_, &blk_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColAddr failed. col id [%u]", blk_col_idx);
      return s;
    }
    if (!(*version_conv_->blk_attrs_)[scan_idx].isFlag(AINFO_NOT_NULL)) {
      if (blk_span->start_row_ == 0 && blk_span->nrow_ == blk_span->block_->GetRowNum()) {
        bitmap->swap(blk_bitmap);
      } else {
        *bitmap = blk_bitmap->Slice(blk_span->start_row_, blk_span->nrow_);
      }
    } else {
      *bitmap = std::make_unique<TsUniformBitmap<DataFlags::kValid>>(blk_span->nrow_);
    }
    *value = blk_value + dest_type_size * blk_span->start_row_;
  } else {
    char* allc_mem = reinterpret_cast<char*>(malloc(dest_type_size * blk_span->nrow_));
    if (allc_mem == nullptr) {
      LOG_ERROR("malloc failed. alloc size: %u", dest_type_size * blk_span->nrow_);
      return FAIL;
    }
    memset(allc_mem, 0, dest_type_size * blk_span->nrow_);
    alloc_mems_.push_back(allc_mem);

    auto tmp_bitmap = std::make_unique<TsBitmap>(blk_span->nrow_);
    for (size_t i = 0; i < blk_span->nrow_; i++) {
      if (!(*version_conv_->blk_attrs_)[scan_idx].isFlag(AINFO_NOT_NULL)) {
        (*tmp_bitmap)[i] = blk_bitmap->At(blk_span->start_row_ + i);
        if (tmp_bitmap->At(i) != DataFlags::kValid) {
          continue;
        }
      }
      TSSlice orig_value;
      auto s =
          blk_span->block_->GetValueSlice(blk_span->start_row_ + i, blk_col_idx, version_conv_->blk_attrs_, orig_value);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetValueSlice failed. rowidx[%lu] colid[%u]", blk_span->start_row_ + i, blk_col_idx);
        return s;
      }
      std::shared_ptr<void> new_mem;
      int err_code = ConvertDataTypeToMem(scan_idx, dest_type_size, orig_value.data, orig_value.len, &new_mem);
      if (err_code < 0) {
        if (!(*version_conv_->blk_attrs_)[scan_idx].isFlag(AINFO_NOT_NULL)) {
          (*tmp_bitmap)[i] = DataFlags::kNull;
        }
      } else {
        memcpy(allc_mem + dest_type_size * i, new_mem.get(), dest_type_size);
      }
    }
    *value = allc_mem;
    *bitmap = std::move(tmp_bitmap);
  }
  return KStatus::SUCCESS;
}

KStatus TSBlkDataTypeConvert::GetVarLenTypeColAddr(TsBlockSpan* blk_span, uint32_t row_idx, uint32_t scan_idx,
                                                   DataFlags& flag, TSSlice& data) {
  auto dest_type = (*version_conv_->scan_attrs_)[scan_idx];
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  assert(isVarLenType(dest_type.type));
  assert(row_idx < blk_span->nrow_);
  assert(blk_span->GetTableVersion() == block_version_);
  std::unique_ptr<TsBitmapBase> blk_bitmap;
  auto s = blk_span->block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, &blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  flag = blk_bitmap->At(blk_span->start_row_ + row_idx);
  if (flag != DataFlags::kValid) {
    data = {nullptr, 0};
    return KStatus::SUCCESS;
  }
  TSSlice orig_value;
  s = blk_span->block_->GetValueSlice(blk_span->start_row_ + row_idx, blk_col_idx, version_conv_->blk_attrs_,
                                      orig_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, blk_col_idx);
    return s;
  }
  if (IsSameType(scan_idx)) {
    data = orig_value;
  } else {
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(scan_idx, dest_type.size, orig_value.data, orig_value.len, &new_mem);
    if (err_code < 0) {
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
