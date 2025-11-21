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
#include <list>
#include <limits>
#include <vector>
#include <map>
#include <memory>
#include "ts_bitmap.h"
#include "ts_table_schema_manager.h"

namespace kwdbts {

typedef int (*CONVERT_DATA_FUNC)(const char* src, char* dst, int dst_len);

CONVERT_DATA_FUNC getConvertFunc(int32_t old_data_type, int32_t new_data_type,
                                 int32_t new_length, bool& is_digit_data, ErrorInfo& err_info);

template <typename T1, typename T2, bool need_to_string>
int convertFixDataToData(const char* src, char* dst, int dst_len)  {
  T1 tmp;
  memcpy(&tmp, src, sizeof(tmp));
  int len = 0;
  if (need_to_string) {
    std::ostringstream oss;
    oss.clear();
    if (std::is_same<T1, float>::value) {
      oss.precision(7);
    } else if (std::is_same<T1, double>::value) {
      oss.precision(14);
    }
    oss.setf(std::ios::fixed);
    oss << tmp;
    if (oss.str().length() > dst_len) {
      return -1;
    }
    snprintf(dst, dst_len, "%s", oss.str().c_str());
    len = oss.str().length();
  } else {
    *reinterpret_cast<T2*>(dst) = tmp;
    len = dst_len;
  }
  return len;
}

template <int32_t to_type>
int convertStringToFixData(const char* src, char* dst, int src_len) {
  char* end_val = nullptr;
  const char* end = src + src_len;
  switch (to_type) {
    case DATATYPE::INT16 : {
      int32_t value = std::strtol(src, &end_val, 10);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > INT16_MAX || value < INT16_MIN) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<int16_t*>(dst) = static_cast<int16_t>(value);
      break;
    }
    case DATATYPE::INT32 : {
      int32_t value = std::strtol(src, &end_val, 10);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > INT32_MAX || value < INT32_MIN) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<int32_t*>(dst) = static_cast<int32_t>(value);
      break;
    }
    case DATATYPE::INT64 : {
      int64_t value;
      size_t end_pos = 0;
      try {
        // use stoll avoid no exceptions
        value = std::stoll(src, &end_pos);
      } catch (std::invalid_argument const& ex) {
        return -2;
      } catch (std::out_of_range const& ex) {
        return -1;
      }
      if (end_pos < src_len) {
        return -2;
      }

      *reinterpret_cast<int64_t*>(dst) = value;
      break;
    }
    case DATATYPE::FLOAT : {
      float value = std::strtof(src, &end_val);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > numeric_limits<float>::max() || value < numeric_limits<float>::lowest()) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<float*>(dst) = value;
      break;
    }
    case DATATYPE::DOUBLE : {
      double value = std::strtod(src, &end_val);
      if (end_val < end && *end_val !='\0') {
        // data truncated
        return -2;
      }
      if (value > numeric_limits<double>::max() || value < numeric_limits<double>::lowest()) {
        // Out of range value
        return -1;
      }
      *reinterpret_cast<double*>(dst) = value;
      break;
    }
    default:
      return -3;
  }
  return 0;
}

template<typename SrcType, typename DestType>
int convertNumToNum(char* src, char* dst) {
  *reinterpret_cast<DestType*>(dst) = *reinterpret_cast<SrcType*>(src);
  return 0;
}

// convert column value from fixed-len type to numeric types
int convertFixedToNum(DATATYPE old_type, DATATYPE new_type, char* src, char* dst, ErrorInfo& err_info);

// convert column value from fixed-len type to string type
int convertFixedToStr(DATATYPE old_type, char* old_data, char* new_data, ErrorInfo& err_info);

int convertStrToFixed(const std::string& str, DATATYPE new_type, char* data, int32_t old_len, ErrorInfo& err_info);

std::shared_ptr<void> convertFixedToVar(DATATYPE old_type, DATATYPE new_type, char* data, ErrorInfo& err_info);

class TsBlockSpan;
class TsBlock;

// notice: make sure block_ no free, while TSBlkSpanDataTypeConvert exists.
class TSBlkDataTypeConvert {
 private:
  std::list<char*> alloc_mems_;

 public:
  uint32_t block_version_ = 0;
  uint32_t scan_version_ = 0;
  std::shared_ptr<SchemaVersionConv> version_conv_;
  std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr_;
  uint64_t key_;
  const std::vector<AttributeInfo>* scan_attrs_ = nullptr;

 public:
  TSBlkDataTypeConvert() = default;

  explicit TSBlkDataTypeConvert(uint32_t block_version, uint32_t scan_version,
                                const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr);

  ~TSBlkDataTypeConvert() {
    for (auto mem : alloc_mems_) {
      free(mem);
    }
    alloc_mems_.clear();
  }

  TSBlkDataTypeConvert(const TSBlkDataTypeConvert& src, TsBlockSpan& desc_blk_span);

  KStatus Init();

  std::unique_ptr<TSBlkDataTypeConvert> NewConvertOnlyChangeBlockSpan();

  int ConvertDataTypeToMem(uint32_t scan_col, int32_t new_type_size, void* old_mem, uint16_t old_var_len,
                           std::shared_ptr<void>* new_mem);

  bool IsColExist(uint32_t scan_idx) { return version_conv_->blk_cols_extended_[scan_idx] != UINT32_MAX; }
  bool IsSameType(uint32_t scan_idx) {
    auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
    return isSameType((*version_conv_->blk_attrs_)[blk_col_idx], (*version_conv_->scan_attrs_)[scan_idx]);
  }
  bool IsColNotNull(uint32_t scan_idx) {
    auto blk_col_idx = version_conv_->blk_cols_extended_[scan_idx];
    return (*version_conv_->blk_attrs_)[blk_col_idx].isFlag(AINFO_NOT_NULL);
  }
  bool IsVarLenType(uint32_t scan_idx) { return isVarLenType((*version_conv_->scan_attrs_)[scan_idx].type); }
  int32_t GetColSize(uint32_t scan_idx) { return (*version_conv_->scan_attrs_)[scan_idx].size; }
  int32_t GetColType(uint32_t scan_idx) { return (*version_conv_->scan_attrs_)[scan_idx].type; }

  // dest type is fixed len datatype.
  KStatus GetFixLenColAddr(TsBlockSpan* blk_span, uint32_t scan_idx, char** value,
                           std::unique_ptr<TsBitmapBase>* bitmap, TsScanStats* ts_scan_stats = nullptr);
  // dest type is varlen datatype.
  KStatus GetVarLenTypeColAddr(TsBlockSpan* blk_span, uint32_t row_idx, uint32_t scan_idx, DataFlags& flag,
                                TSSlice& data, TsScanStats* ts_scan_stats = nullptr);
  KStatus GetColBitmap(TsBlockSpan* blk_span, uint32_t scan_idx, std::unique_ptr<TsBitmapBase>* bitmap,
                                TsScanStats* ts_scan_stats = nullptr);
  KStatus getColBitmapConverted(TsBlockSpan* blk_span, uint32_t scan_idx, std::unique_ptr<TsBitmapBase>* bitmap,
                                TsScanStats* ts_scan_stats = nullptr);
  KStatus GetPreCount(TsBlockSpan* blk_span, uint32_t scan_idx, TsScanStats* ts_scan_stats, uint16_t& count);
  KStatus GetPreSum(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size, TsScanStats* ts_scan_stats,
                    void* &pre_sum, bool& is_overflow);
  KStatus GetPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, TsScanStats* ts_scan_stats, void* &pre_max);
  KStatus GetPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, int32_t size, TsScanStats* ts_scan_stats, void* &pre_min);
  KStatus GetVarPreMax(TsBlockSpan* blk_span, uint32_t scan_idx, TsScanStats* ts_scan_stats, TSSlice& pre_max);
  KStatus GetVarPreMin(TsBlockSpan* blk_span, uint32_t scan_idx, TsScanStats* ts_scan_stats, TSSlice& pre_min);
};


}  // namespace kwdbts
