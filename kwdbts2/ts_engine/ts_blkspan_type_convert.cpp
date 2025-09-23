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
#include <iomanip>
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

void eraseZeroBeforeE(std::string& str) {
  while (true) {
    size_t e_pos = str.find('e');
    if (e_pos != std::string::npos) {
      if (str[e_pos - 1] == '0') {
        if (!str.empty()) {
          std::string str1 = str.substr(0, e_pos - 1);
          std::string str2 = str.substr(e_pos, str.length() - e_pos);
          str =  str1 + str2;
        }
      } else {
        break;
      }
    } else {
      break;
    }
  }
}

std::string floatToStr(float value) {
  std::ostringstream oss;
  int decimal_precision = 6;  // decimal part len
  int integer_part = 6;       // integer part len
  oss << std::fixed << std::setprecision(decimal_precision) << value;
  std::string str = oss.str();
  if (str[0] == '-') {
    integer_part = 7;  // '-' will use one len in integer.
  }

  while (true) {
    // delete end '0's if exists.
    if (str[str.length() - 1] == '0') {
      if (!str.empty())
        str.pop_back();
    } else {
      break;
    }
  }
  // if no decimal part, delete '.'
  if (str[str.length() - 1] == '.') {
    if (!str.empty())
      str.pop_back();
  }

  size_t dot_pos = str.find('.');
  if (dot_pos == std::string::npos) {
    // if no decimal part, we check integer len
    if (str.length() > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(16) << value;  // tmp use 16 len
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  } else {
    // check integer len
    if (dot_pos > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(16) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  }
  return str;
}

std::string doubleToStr(double value) {
  std::ostringstream oss;
  int decimal_precision = 16;  // decimal part len
  int integer_part = 6;        // integer part len
  int total = 17;              // value total len
  oss << std::fixed << std::setprecision(decimal_precision) << value;
  std::string str = oss.str();
  if (str[0] == '-') {
    integer_part = 7;  // '-' use occupy len of value
  }

  while (true) {
    // delete end '0's if exists.
    if (str[str.length() - 1] == '0') {
      if (!str.empty())
        str.pop_back();
    } else {
      break;
    }
  }
  // if no decimal part, delete '.'
  if (str[str.length() - 1] == '.') {
    if (!str.empty())
      str.pop_back();
  }

  size_t dot_pos = str.find('.');
  if (dot_pos == std::string::npos) {
    // if no decimal part, we check integer len
    if (str.length() > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(decimal_precision) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    }
  } else {
    // check integer len
    if (dot_pos > integer_part) {
      // integer len is more than 6, change to Scientific notation
      oss.clear();
      oss.str("");
      oss << std::scientific << std::setprecision(decimal_precision) << value;
      str = oss.str();
      // delete '0's before 'e'
      eraseZeroBeforeE(str);
    } else {
      if (str.length() > total + 1) {
        oss.clear();
        oss.str("");
        oss << std::fixed << std::setprecision(total - dot_pos) << value;
        str = oss.str();
      }
    }
  }

  return str;
}

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
      strcpy(new_data, res.data());  // NOLINT
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(old_data));
      strcpy(new_data, res.data());  // NOLINT
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(old_data));
      strcpy(new_data, res.data());  // NOLINT
      break;
    }
    case DATATYPE::FLOAT : {
      std::ostringstream oss;
      oss.clear();
      oss.precision(7);
      oss.setf(std::ios::fixed);
      oss << KFloat32(old_data);
      res = oss.str();
      strcpy(new_data, res.data());  // NOLINT
      break;
    }
    case DATATYPE::DOUBLE : {
      std::stringstream ss;
      ss.precision(15);
      ss.setf(std::ios::fixed);
      ss << KDouble64(old_data);
      res = ss.str();
      strcpy(new_data, res.data());  // NOLINT
      break;
    }
    case DATATYPE::BINARY :
    case DATATYPE::CHAR : {
      strcpy(new_data, old_data);  // NOLINT
      break;
    }
    default:
      err_info.setError(KWEPERM, "Fixed type invalid");
      break;
  }
  return 0;
}

int convertStrToFixed(const std::string& str, DATATYPE new_type, char* data, int32_t old_len, ErrorInfo& err_info) {
  std::size_t pos{};
  int res32 = 0;
  int64_t res64{};
  float res_f{};
  double res_d{};
  try {
    switch (new_type) {
      case DATATYPE::INT16 :
        res32 = std::stoi(str, &pos);
        break;
      case DATATYPE::INT32 : {
        res32 = std::stoi(str, &pos);
        break;
      }
      case DATATYPE::INT64 :
        res64 = std::stoll(str, &pos);
        break;
      case DATATYPE::FLOAT :
        res_f = std::stof(str, &pos);
        break;
      case DATATYPE::DOUBLE :
        res_d = std::stod(str, &pos);
        break;
      case DATATYPE::CHAR :
      case DATATYPE::BINARY : {
        memcpy(data, str.data(), old_len);
        return 0;
      }
      default:
        break;
    }
  }
  catch (std::invalid_argument const &ex) {
    return err_info.setError(KWEPERM, "Incorrect integer value '" + str + "'");
  }
  catch (std::out_of_range const &ex) {
    return err_info.setError(KWEPERM, "Out of range value '" + str + "'");
  }
  if (pos < str.size()) {
    return err_info.setError(KWEPERM, "Data truncated '" + str + "'");
  }
  if (new_type == DATATYPE::INT16) {
    if (res32 > INT16_MAX || res32 < INT16_MIN) {
      return err_info.setError(KWEPERM, "Out of range value '" + str + "'");
    }
  }
  switch (new_type) {
    case DATATYPE::INT16 :
      KInt16(data) = res32;
      break;
    case DATATYPE::INT32 :
      KInt32(data) = res32;
      break;
    case DATATYPE::INT64 : {
      KInt64(data) = res64;
      break;
    }
    case DATATYPE::FLOAT :
      KFloat32(data) = res_f;
      break;
    case DATATYPE::DOUBLE :
      KDouble64(data) = res_d;
      break;
    default:
      break;
  }
  return err_info.errcode;
}

std::shared_ptr<void> convertFixedToVar(DATATYPE old_type, DATATYPE new_type, char* data, ErrorInfo& err_info) {
  std::string res;
  char* var_data;
  switch (old_type) {
    case DATATYPE::INT16 : {
      res = std::to_string(KInt16(data));
      break;
    }
    case DATATYPE::INT32 : {
      res = std::to_string(KInt32(data));
      break;
    }
    case DATATYPE::INT64 : {
      res = std::to_string(KInt64(data));
      break;
    }
    case DATATYPE::FLOAT : {
      res = floatToStr(KFloat32(data));
      break;
    }
    case DATATYPE::DOUBLE : {
      res = doubleToStr(KDouble64(data));
      break;
    }
    case DATATYPE::CHAR:
    case DATATYPE::BINARY: {
      auto char_len = strlen(data);
      if (new_type == DATATYPE::VARSTRING) {
        char_len += 1;
      }
      k_int16 buffer_len = char_len + kStringLenLen;
      var_data = static_cast<char*>(std::malloc(buffer_len));
      memset(var_data, 0, buffer_len);
      KInt16(var_data) = static_cast<k_int16>(char_len);
      memcpy(var_data + kStringLenLen, data, strlen(data));
      break;
    }
    default:
      err_info.setError(KWEPERM, "Incorrect integer value");
      break;
  }
  if (old_type == DATATYPE::INT16 || old_type == DATATYPE::INT32 || old_type == DATATYPE::INT64 ||
      old_type == DATATYPE::FLOAT || old_type == DATATYPE::DOUBLE) {
    auto act_len = res.size() + 1;
    var_data = static_cast<char*>(std::malloc(act_len + kStringLenLen));
    memset(var_data, 0, act_len + kStringLenLen);
    KUint16(var_data) = act_len;
    strcpy(var_data + kStringLenLen, res.data());  // NOLINT
      }
  std::shared_ptr<void> ptr(var_data, free);
  return ptr;
}

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
