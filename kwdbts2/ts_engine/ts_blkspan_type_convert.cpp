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
#include "ts_table_schema_manager.h"
#include "ts_agg.h"

namespace kwdbts {

TSBlkDataTypeConvert::TSBlkDataTypeConvert(TsBlockSpan& blk_span, std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                           uint32_t scan_version, const std::vector<uint32_t>& ts_scan_cols)
  : block_(blk_span.block_.get()), start_row_idx_(blk_span.start_row_), row_num_(blk_span.nrow_),
    tbl_schema_mgr_(tbl_schema_mgr), scan_version_(scan_version), ts_scan_cols_(ts_scan_cols) {
  if (tbl_schema_mgr_) {
    auto s = GetBlkScanColsInfo(blk_span.GetTableVersion(), blk_scan_cols_, blk_schema_valid_);
    assert(s == SUCCESS);
    tbl_schema_mgr_->GetColumnsIncludeDropped(table_schema_all_, scan_version_);
  }
}

// copyed from TsTimePartition::ConvertDataTypeToMem
int ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size,
                         void* old_mem, uint16_t old_var_len, std::shared_ptr<void>* new_mem) {
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
  if (auto res = col_bitmaps.find(scan_idx); res != col_bitmaps.end()) {
    bitmap = res->second;
    return SUCCESS;
  }
  if (scan_idx == UINT32_MAX) {
    bitmap.SetCount(block_->GetRowNum());
    bitmap.SetAll(DataFlags::kNull);
    col_bitmaps.insert({scan_idx, bitmap});
    return SUCCESS;
  }

  if (isVarLenType(blk_schema_valid_[blk_scan_cols_[scan_idx]].type) &&
      !isVarLenType(table_schema_all_[ts_scan_cols_[scan_idx]].type)) {
    // if (attrs_[col_idx].type != CHAR && attrs_[col_idx].type != BINARY) {
    char* value = nullptr;
    auto ret = GetFixLenColAddr(scan_idx, &value, bitmap);
    if (ret != KStatus::SUCCESS) {
      LOG_ERROR("GetFixLenColAddr failed.");
    }
    return ret;
    // }
  }
  return block_->GetColBitmap(blk_scan_cols_[scan_idx], blk_schema_valid_, bitmap);
}

KStatus TSBlkDataTypeConvert::GetPreCount(uint32_t scan_idx, uint16_t &count) {
  return block_->GetPreCount(blk_scan_cols_[scan_idx], count);
}

KStatus TSBlkDataTypeConvert::GetPreSum(uint32_t scan_idx, int32_t size, void *&pre_sum, bool &is_overflow) {
  return block_->GetPreSum(blk_scan_cols_[scan_idx], size, pre_sum, is_overflow);
}

KStatus TSBlkDataTypeConvert::GetPreMax(uint32_t scan_idx, void *&pre_max) {
  return block_->GetPreMax(blk_scan_cols_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetPreMin(uint32_t scan_idx, int32_t size, void *&pre_min) {
  return block_->GetPreMin(blk_scan_cols_[scan_idx], size, pre_min);
}

KStatus TSBlkDataTypeConvert::GetVarPreMax(uint32_t scan_idx, TSSlice &pre_max) {
  return block_->GetVarPreMax(blk_scan_cols_[scan_idx], pre_max);
}

KStatus TSBlkDataTypeConvert::GetVarPreMin(uint32_t scan_idx, TSSlice &pre_min) {
  return block_->GetVarPreMin(blk_scan_cols_[scan_idx], pre_min);
}

KStatus TSBlkDataTypeConvert::SetConvertVersion(std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                                                uint32_t scan_version, std::vector<uint32_t> ts_scan_cols) {
  tbl_schema_mgr_ = tbl_schema_mgr;
  scan_version_ = scan_version;
  if (block_->GetTableVersion() == scan_version) {
    ts_scan_cols_ = tbl_schema_mgr->GetIdxForValidCols(scan_version_);
  } else {
    ts_scan_cols_ = ts_scan_cols;
  }
  auto s = GetBlkScanColsInfo(block_->GetTableVersion(), blk_scan_cols_, blk_schema_valid_);
  if (s != SUCCESS) {
    return s;
  }
  tbl_schema_mgr_->GetColumnsIncludeDropped(table_schema_all_, scan_version_);
}

KStatus TSBlkDataTypeConvert::GetBlkScanColsInfo(uint32_t version, std::vector<uint32_t>& scan_cols,
                                                 vector<AttributeInfo>& blk_schema) {
  std::shared_ptr<MMapMetricsTable> blk_version;
  KStatus s = tbl_schema_mgr_->GetMetricSchema(version, &blk_version);
  if (s != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed. table version [%u]", version);
    return s;
  }
  auto& blk_schema_all = blk_version->getSchemaInfoIncludeDropped();
  blk_schema = blk_version->getSchemaInfoExcludeDropped();
  const auto blk_valid_cols = blk_version->getIdxForValidCols();

  // calculate column index in current block
  std::vector<uint32_t> blk_scan_cols;
  blk_scan_cols.resize(ts_scan_cols_.size());
  for (size_t i = 0; i < ts_scan_cols_.size(); i++) {
    if (!blk_schema_all[ts_scan_cols_[i]].isFlag(AINFO_DROPPED)) {
      bool found = false;
      size_t j = 0;
      for (; j < blk_valid_cols.size(); j++) {
        if (blk_valid_cols[j] == ts_scan_cols_[i]) {
          found = true;
          break;
        }
      }
      if (!found) {
        blk_scan_cols[i] = UINT32_MAX;
      } else {
        blk_scan_cols[i] = j;
      }
    } else {
      blk_scan_cols[i] = UINT32_MAX;
    }
  }
  scan_cols = blk_scan_cols;
  return KStatus::SUCCESS;
}

  bool TSBlkDataTypeConvert::IsColExist(uint32_t scan_idx) {
    return blk_scan_cols_[scan_idx] != UINT32_MAX;
  }

bool TSBlkDataTypeConvert::IsSameType(uint32_t scan_idx) {
  return isSameType(blk_schema_valid_[blk_scan_cols_[scan_idx]], table_schema_all_[ts_scan_cols_[scan_idx]]);
}

bool TSBlkDataTypeConvert::IsColNotNull(uint32_t scan_idx) {
  return blk_schema_valid_[blk_scan_cols_[scan_idx]].isFlag(AINFO_NOT_NULL);
}

KStatus TSBlkDataTypeConvert::GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap) {
  if (!IsColExist(scan_idx)) {
    *value = nullptr;
    return SUCCESS;
  }
  auto dest_attr = table_schema_all_[ts_scan_cols_[scan_idx]];
  uint32_t dest_type_size = dest_attr.size;
  auto blk_col_idx = blk_scan_cols_[scan_idx];
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, blk_schema_valid_, blk_bitmap);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetColBitmap failed. col id [%u]", blk_col_idx);
    return s;
  }
  bitmap.SetCount(row_num_);

  if (IsSameType(scan_idx)) {
    char* blk_value;
    s = block_->GetColAddr(blk_col_idx, blk_schema_valid_, &blk_value);
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
      s = block_->GetValueSlice(start_row_idx_+ i, blk_col_idx, blk_schema_valid_, orig_value);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", start_row_idx_+ i, blk_col_idx);
        return s;
      }
      std::shared_ptr<void> new_mem;
      int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(blk_schema_valid_[blk_col_idx].type),
                                          static_cast<DATATYPE>(dest_attr.type),
                                          dest_type_size, orig_value.data, orig_value.len, &new_mem);
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
  auto dest_type = table_schema_all_[ts_scan_cols_[scan_idx]];
  auto blk_col_idx = blk_scan_cols_[scan_idx];
  assert(isVarLenType(dest_type.type));
  assert(row_idx < row_num_);
  TsBitmap blk_bitmap;
  auto s = block_->GetColBitmap(blk_col_idx, blk_schema_valid_, blk_bitmap);
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
  s = block_->GetValueSlice(start_row_idx_ + row_idx, blk_col_idx, blk_schema_valid_, orig_value);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, blk_col_idx);
    return s;
  }
  if (IsSameType(scan_idx)) {
    data = orig_value;
  } else {
    // table altered. column type changes.
    std::shared_ptr<void> new_mem;
    int err_code = ConvertDataTypeToMem(static_cast<DATATYPE>(blk_schema_valid_[blk_col_idx].type),
                                        static_cast<DATATYPE>(dest_type.type),
                                        dest_type.size, orig_value.data, orig_value.len, &new_mem);
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
