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
#include <algorithm>
#include "ts_blkspan_type_convert.h"
#include "ts_block.h"
#include "ts_table_schema_manager.h"
#include "ts_agg.h"

namespace kwdbts {

TSBlkDataTypeConvert::TSBlkDataTypeConvert(TsBlockSpan& blk_span,
                                           const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                           uint32_t scan_version)
  : block_(blk_span.block_.get()), start_row_idx_(blk_span.start_row_), row_num_(blk_span.nrow_),
    tbl_schema_mgr_(tbl_schema_mgr) {
  Init(scan_version);
}

TSBlkDataTypeConvert::TSBlkDataTypeConvert(TsBlock* block, uint32_t row_idx, uint32_t row_num,
                                           const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                                           uint32_t scan_version) :
        block_(block), start_row_idx_(row_idx), row_num_(row_num), tbl_schema_mgr_(tbl_schema_mgr) {
  Init(scan_version);
}

KStatus TSBlkDataTypeConvert::Init(uint32_t scan_version) {
  if (tbl_schema_mgr_) {
    auto blk_version = block_->GetTableVersion();
    key_ = std::to_string(blk_version) + "_" + std::to_string(scan_version);
    auto iter = tbl_schema_mgr_->GetVersionConvMap().find(key_);
    if (iter == tbl_schema_mgr_->GetVersionConvMap().end()) {
      std::shared_ptr<MMapMetricsTable> blk_metric;
      KStatus s = tbl_schema_mgr_->GetMetricSchema(blk_version, &blk_metric);
      if (s != SUCCESS) {
        LOG_ERROR("GetMetricSchema failed. table version [%u]", blk_version);
      }
      std::shared_ptr<MMapMetricsTable> scan_metric;
      s = tbl_schema_mgr_->GetMetricSchema(scan_version, &scan_metric);
      if (s != SUCCESS) {
        LOG_ERROR("GetMetricSchema failed. table version [%u]", scan_version);
      }
      auto& scan_cols = scan_metric->getIdxForValidCols();
      auto& scan_attrs = scan_metric->getSchemaInfoExcludeDropped();
      auto& blk_attrs = blk_metric->getSchemaInfoExcludeDropped();

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
      version_conv_ = std::make_shared<SchemaVersionConv>(scan_version, blk_cols_extended, scan_attrs, blk_attrs);
      tbl_schema_mgr_->GetVersionConvMap().insert({key_, version_conv_});
    } else {
      version_conv_ = iter->second;
    }
  }
  return SUCCESS;
}

// copyed from TsTimePartition::ConvertDataTypeToMem
int TSBlkDataTypeConvert::ConvertDataTypeToMem(uint32_t scan_col, int32_t new_type_size,
                         void* old_mem, uint16_t old_var_len, std::shared_ptr<void>* new_mem) {
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_col];
  DATATYPE old_type = static_cast<DATATYPE>(version_conv_->blk_attrs_[blk_col_idx].type);
  DATATYPE new_type = static_cast<DATATYPE>(version_conv_->scan_attrs_[scan_col].type);
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

KStatus TSBlkDataTypeConvert::GetColBitmap(uint32_t scan_idx, TsBitmap& bitmap) {
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  if (blk_col_idx == UINT32_MAX) {
    bitmap.SetCount(block_->GetRowNum());
    bitmap.SetAll(DataFlags::kNull);
    return SUCCESS;
  }
  if (isVarLenType(version_conv_->blk_attrs_[blk_col_idx].type) &&
      !isVarLenType(version_conv_->scan_attrs_[scan_idx].type)) {
    char* value = nullptr;
    auto ret = getColBitmapConverted(scan_idx, bitmap);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
      return ret;
    }
    return SUCCESS;
  }
  return block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, bitmap);
}

KStatus TSBlkDataTypeConvert::getColBitmapConverted(uint32_t scan_idx, TsBitmap &bitmap) {
  auto dest_attr = version_conv_->scan_attrs_[scan_idx];
  uint32_t dest_type_size = dest_attr.size;
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }

  auto blk_row_count = block_->GetRowNum();
  bitmap.SetCount(blk_row_count);

  char* allc_mem = reinterpret_cast<char*>(malloc(dest_type_size * blk_row_count));
  if (allc_mem == nullptr) {
    LOG_ERROR("malloc failed. alloc size: %u", dest_type_size * blk_row_count);
    return KStatus::SUCCESS;
  }
  memset(allc_mem, 0, dest_type_size * blk_row_count);
  Defer defer([&]() { free(allc_mem); });


  for (size_t i = 0; i < blk_row_count; i++) {
    bitmap[i] = blk_bitmap[i];
    if (bitmap[i] != DataFlags::kValid) {
      continue;
    }
    TSSlice orig_value;
    s = block_->GetValueSlice(i, blk_col_idx, version_conv_->blk_attrs_, orig_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetValueSlice failed. rowidx[%lu] colid[%u]", i, blk_col_idx);
      return s;
    }
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(scan_idx, dest_type_size, orig_value.data, orig_value.len, &new_mem);
    if (err_code < 0) {
      bitmap[i] = DataFlags::kNull;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TSBlkDataTypeConvert::GetPreCount(uint32_t scan_idx, uint16_t &count) {
  return block_->GetPreCount(version_conv_->blk_cols_extended_[scan_idx], count);
}

KStatus TSBlkDataTypeConvert::GetPreSum(uint32_t scan_idx, int32_t size, void *&pre_sum, bool &is_overflow) {
  return block_->GetPreSum(version_conv_->blk_cols_extended_[scan_idx], size, pre_sum, is_overflow);
}

KStatus TSBlkDataTypeConvert::GetPreMax(uint32_t scan_idx, void *&pre_max) {
  return block_->GetPreMax(version_conv_->blk_cols_extended_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetPreMin(uint32_t scan_idx, int32_t size, void *&pre_min) {
  return block_->GetPreMin(version_conv_->blk_cols_extended_[scan_idx], size, pre_min);
}

KStatus TSBlkDataTypeConvert::GetVarPreMax(uint32_t scan_idx, TSSlice &pre_max) {
  return block_->GetVarPreMax(version_conv_->blk_cols_extended_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetVarPreMin(uint32_t scan_idx, TSSlice &pre_min) {
  return block_->GetVarPreMin(version_conv_->blk_cols_extended_[scan_idx], pre_min);
}

bool TSBlkDataTypeConvert::IsColExist(uint32_t scan_idx) {
  return version_conv_->blk_cols_extended_[scan_idx] != UINT32_MAX;
}

bool TSBlkDataTypeConvert::IsSameType(uint32_t scan_idx) {
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  return isSameType(version_conv_->blk_attrs_[blk_col_idx], version_conv_->scan_attrs_[scan_idx]);
}

bool TSBlkDataTypeConvert::IsVarLenType(uint32_t scan_idx) {
  return isVarLenType(version_conv_->scan_attrs_[scan_idx].type);
}

int32_t TSBlkDataTypeConvert::GetColSize(uint32_t scan_idx) {
  return version_conv_->scan_attrs_[scan_idx].size;
}

int32_t TSBlkDataTypeConvert::GetColType(uint32_t scan_idx) {
  return version_conv_->scan_attrs_[scan_idx].type;
}

bool TSBlkDataTypeConvert::IsColNotNull(uint32_t scan_idx) {
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  return version_conv_->blk_attrs_[blk_col_idx].isFlag(AINFO_NOT_NULL);
}

KStatus TSBlkDataTypeConvert::GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap) {
  if (!IsColExist(scan_idx)) {
    *value = nullptr;
    return SUCCESS;
  }

  auto dest_attr = version_conv_->scan_attrs_[scan_idx];
  uint32_t dest_type_size = dest_attr.size;
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  bitmap.SetCount(row_num_);

  if (IsSameType(scan_idx)) {
    char* blk_value;
    s = block_->GetColAddr(blk_col_idx, version_conv_->blk_attrs_, &blk_value);
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
    memset(allc_mem, 0, dest_type_size * row_num_);
    alloc_mems_.push_back(allc_mem);

    for (size_t i = 0; i < row_num_; i++) {
      bitmap[i] = blk_bitmap[start_row_idx_+ i];
      if (bitmap[i] != DataFlags::kValid) {
        continue;
      }
      TSSlice orig_value;
      s = block_->GetValueSlice(start_row_idx_+ i, blk_col_idx, version_conv_->blk_attrs_, orig_value);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", start_row_idx_+ i, blk_col_idx);
        return s;
      }
      std::shared_ptr<void> new_mem;
      int err_code = ConvertDataTypeToMem(scan_idx, dest_type_size, orig_value.data, orig_value.len, &new_mem);
      if (err_code < 0) {
        bitmap[i] = DataFlags::kNull;
      } else {
        memcpy(allc_mem + dest_type_size * i, new_mem.get(), dest_type_size);
      }
    }
    *value = allc_mem;
  }
  return KStatus::SUCCESS;
}

KStatus TSBlkDataTypeConvert::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx,
                                                   DataFlags& flag, TSSlice& data) {
  auto dest_type = version_conv_->scan_attrs_[scan_idx];
  auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
  assert(isVarLenType(dest_type.type));
  assert(row_idx < row_num_);
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, version_conv_->blk_attrs_, blk_bitmap);
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
  s = block_->GetValueSlice(start_row_idx_ + row_idx, blk_col_idx, version_conv_->blk_attrs_, orig_value);
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
