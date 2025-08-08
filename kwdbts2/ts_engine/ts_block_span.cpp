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

#include "ts_agg.h"
#include "ts_block.h"
#include "ts_blkspan_type_convert.h"
#include "ts_compressor.h"

namespace kwdbts {
bool TsBlock::HasPreAgg(uint32_t begin_row_idx, uint32_t row_num) {
  return false;
}

KStatus TsBlock::GetPreCount(uint32_t blk_col_idx, uint16_t& count) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreSum(uint32_t blk_col_idx, int32_t size, void* &pre_sum, bool& is_overflow) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreMax(uint32_t blk_col_idx, void* &pre_max) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetPreMin(uint32_t blk_col_idx, int32_t size, void* &pre_min) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetVarPreMax(uint32_t blk_col_idx, TSSlice& pre_max) {
  return KStatus::FAIL;
}

KStatus TsBlock::GetVarPreMin(uint32_t blk_col_idx, TSSlice& pre_min) {
  return KStatus::FAIL;
}

KStatus TsBlock::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return KStatus::SUCCESS;
}

TsBlockSpan::TsBlockSpan(TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
                         const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, uint32_t scan_version)
    : block_(block),
      entity_id_(entity_id),
      start_row_(start),
      nrow_(nrow),
      tbl_schema_mgr_(tbl_schema_mgr) {
  assert(nrow_ >= 1);
  if (block_->GetTableVersion() != scan_version) {
    convert_ = std::make_unique<TSBlkDataTypeConvert>(*this, tbl_schema_mgr,
                                                    scan_version == 0 ? block->GetTableVersion() : scan_version);
    auto s = convert_->Init();
    if (s != SUCCESS) {
      LOG_ERROR("convert_ Init failed!");
    }
  }
  std::shared_ptr<MMapMetricsTable> scan_metric;
  auto s = tbl_schema_mgr_->GetMetricSchema(scan_version, &scan_metric);
  if (s != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed. table id [%u], table version [%lu]", scan_version, block->GetTableId());
  }
  scan_attrs_ = scan_metric->getSchemaInfoExcludeDropped();
  has_pre_agg_ = block_->HasPreAgg(start_row_, nrow_);
}

TsBlockSpan::TsBlockSpan(uint32_t vgroup_id, TSEntityID entity_id, std::shared_ptr<TsBlock> block, int start, int nrow,
                         const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr, uint32_t scan_version)
    : block_(block),
      vgroup_id_(vgroup_id),
      entity_id_(entity_id),
      start_row_(start),
      nrow_(nrow),
      tbl_schema_mgr_(tbl_schema_mgr) {
  assert(nrow_ >= 1);
  if (block_->GetTableVersion() != scan_version) {
    convert_ = std::make_unique<TSBlkDataTypeConvert>(*this, tbl_schema_mgr,
                                                    scan_version == 0 ? block->GetTableVersion() : scan_version);
    auto s = convert_->Init();
    if (s != SUCCESS) {
      LOG_ERROR("convert_ Init failed!");
    }
  }
  std::shared_ptr<MMapMetricsTable> scan_metric;
  auto s = tbl_schema_mgr_->GetMetricSchema(scan_version, &scan_metric);
  if (s != SUCCESS) {
    LOG_ERROR("GetMetricSchema failed. table id [%u], table version [%lu]", scan_version, block->GetTableId());
  }
  scan_attrs_ = scan_metric->getSchemaInfoExcludeDropped();
  has_pre_agg_ = block_->HasPreAgg(start_row_, nrow_);
}

bool TsBlockSpan::operator<(const TsBlockSpan& other) const {
  if (entity_id_ != other.entity_id_) {
    return entity_id_ < other.entity_id_;
  } else {
    timestamp64 ts = block_->GetTS(start_row_);
    timestamp64 other_ts = other.block_->GetTS(other.start_row_);
    if (ts != other_ts) {
      return ts < other_ts;
    } else {
      uint64_t seq_no = *block_->GetLSNAddr(start_row_);
      uint64_t other_seq_no = *other.block_->GetLSNAddr(other.start_row_);
      return seq_no > other_seq_no;
    }
  }
}

timestamp64 TsBlockSpan::GetFirstTS() const {
  if (start_row_ == 0) {
    return block_->GetFirstTS();
  } else {
    return block_->GetTS(start_row_);
  }
}

timestamp64 TsBlockSpan::GetLastTS() const {
  if (start_row_ + nrow_ == block_->GetRowNum()) {
    return block_->GetLastTS();
  } else {
    return block_->GetTS(start_row_ + nrow_ - 1);
  }
}

TS_LSN TsBlockSpan::GetFirstLSN() const {
  if (start_row_ == 0) {
    return block_->GetFirstLSN();
  } else {
    return *block_->GetLSNAddr(start_row_);
  }
}

TS_LSN TsBlockSpan::GetLastLSN() const {
  if (start_row_ + nrow_ == block_->GetRowNum()) {
    return block_->GetLastLSN();
  } else {
    return *block_->GetLSNAddr(start_row_ + nrow_ - 1);
  }
}

KStatus TsBlockSpan::BuildCompressedData(std::string& data) {
  KStatus s = KStatus::SUCCESS;
  // compressor manager
  const auto& mgr = CompressorManager::GetInstance();
  // init col offsets
  uint32_t block_data_begin_offset = data.size();
  size_t col_offsets_len = (scan_attrs_.size() + 1) * sizeof(uint32_t);
  std::vector<uint32_t> col_offset((col_offsets_len / sizeof(uint32_t)), 0);
  data.append(reinterpret_cast<char*>(col_offset.data()), col_offsets_len);
  std::string agg_data;
  size_t agg_col_offsets_len = scan_attrs_.size() * sizeof(uint32_t);
  std::vector<uint32_t> agg_col_offset((col_offsets_len / sizeof(uint32_t)), 0);
  agg_data.append(reinterpret_cast<char*>(agg_col_offset.data()), agg_col_offsets_len);
  // init lsn col data
  {
    DATATYPE d_type = DATATYPE::INT64;
    size_t d_size = sizeof(uint64_t);
    std::string lsn_data;
    for (int row_idx = 0; row_idx < nrow_; ++row_idx) {
      lsn_data.append(reinterpret_cast<char*>(block_->GetLSNAddr(start_row_ + row_idx)), d_size);
    }
    std::string compressed;
    auto [first, second] = mgr.GetDefaultAlgorithm(d_type);
    TSSlice plain{lsn_data.data(), nrow_ * d_size};
    mgr.CompressData(plain, nullptr, nrow_, &compressed, first, second);
    data.append(compressed);
    // block data offset
    uint32_t column_block_offset = data.size() - block_data_begin_offset - col_offsets_len;
    memcpy(data.data() + block_data_begin_offset, &column_block_offset, sizeof(uint32_t));
  }
  // init column block data && column agg data
  for (uint32_t scan_idx = 0; scan_idx < scan_attrs_.size(); ++scan_idx) {
    bool has_bitmap = scan_idx > 0;
    DATATYPE d_type = scan_idx != 0 ? static_cast<DATATYPE>(scan_attrs_[scan_idx].type)
                      : DATATYPE::TIMESTAMP64;
    int32_t d_size = scan_attrs_[scan_idx].size;
    bool is_var_col = isVarLenType(d_type);
    TsBitmap* b = nullptr;
    TsBitmap bitmap;
    std::string ts_col_data;
    std::string null_col_data;
    char* fixed_col_value_addr;
    std::string var_offset_data;
    var_offset_data.resize(nrow_ * sizeof(uint32_t));
    std::string var_data;
    std::vector<string> var_rows;
    if (!is_var_col) {
      s = GetFixLenColAddr(scan_idx, &fixed_col_value_addr, bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetFixLenColAddr failed. col id [%u]", scan_idx);
        return s;
      }
      if (scan_idx == 0) {
        for (size_t i = 0; i < nrow_; ++i) {
          ts_col_data.append(fixed_col_value_addr + i * d_size, sizeof(timestamp64));
        }
        fixed_col_value_addr = ts_col_data.data();
      }
      if (fixed_col_value_addr == nullptr) {
        null_col_data.resize(nrow_ * d_size);
        fixed_col_value_addr = null_col_data.data();
        bitmap.SetCount(nrow_);
        for (size_t i = 0; i < nrow_; ++i) {
          bitmap[i] = DataFlags::kNull;
        }
      } else if (bitmap.GetCount() == 0) {
        bitmap.SetCount(nrow_);
      }
    } else {
      if (!IsColExist(scan_idx)) {
        bitmap.Reset(nrow_);
        bitmap.SetAll(DataFlags::kNull);
      } else {
        if (has_bitmap) {
          s = GetColBitmap(scan_idx, bitmap);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("GetColBitmap failed. col id [%u]", scan_idx);
            return s;
          }
        }
        for (size_t i = 0; i < nrow_; ++i) {
          DataFlags flag;
          TSSlice var_slice;
          s = GetVarLenTypeColAddr(i, scan_idx, flag, var_slice);
          if (s != KStatus::SUCCESS) {
            LOG_ERROR("GetVarLenTypeColAddr failed. rowidx[%lu] colid[%u]", i, scan_idx)
            return s;
          }
          if (flag == kValid) {
            var_data.append(var_slice.data, var_slice.len);
            var_rows.emplace_back(var_slice.data, var_slice.len);
          }
          uint32_t var_offset = var_data.size();
          memcpy(var_offset_data.data() + i * sizeof(uint32_t), &var_offset, sizeof(uint32_t));
        }
      }
    }
    // compress bitmap
    if (has_bitmap) {
      TSSlice bitmap_data = bitmap.GetData();
      // TODO(limeng04): compress bitmap
      char bitmap_compress_type = 0;
      data.append(&bitmap_compress_type);
      data.append(bitmap_data.data, bitmap_data.len);
      b = &bitmap;
    }
    auto [first, second] = mgr.GetDefaultAlgorithm(static_cast<DATATYPE>(d_type));
    if (is_var_col) {
      // varchar use Gorilla algorithm
      first = TsCompAlg::kChimp_32;
      // var offset data
      std::string compressed;
      bool ok = mgr.CompressData({var_offset_data.data(), var_offset_data.size()},
                                 nullptr, nrow_, &compressed, first, second);
      if (!ok) {
        LOG_ERROR("Compress var offset data failed");
        return KStatus::SUCCESS;
      }
      uint32_t compressed_len = compressed.size();
      data.append(reinterpret_cast<const char *>(&compressed_len), sizeof(uint32_t));
      data.append(compressed);
      // var data
      compressed.clear();
      ok = mgr.CompressVarchar({var_data.data(), var_data.size()}, &compressed, GenCompAlg::kSnappy);
      if (!ok) {
        LOG_ERROR("Compress var data failed");
        return KStatus::SUCCESS;
      }
      data.append(compressed);
    } else {
      // compress col data & write to buffer
      std::string compressed;
      size_t col_size = scan_idx == 0 ? 8 : d_size;
      TSSlice plain{fixed_col_value_addr, nrow_ * col_size};
      mgr.CompressData(plain, b, nrow_, &compressed, first, second);
      data.append(compressed);
    }
    // block data offset
    uint32_t column_block_offset = data.size() - block_data_begin_offset - col_offsets_len;
    memcpy(data.data() + block_data_begin_offset + sizeof(uint32_t) * (scan_idx + 1),
           &column_block_offset, sizeof(uint32_t));

    // column agg data
    string col_agg;
    if (!is_var_col) {
      uint16_t count = 0;
      string max, min, sum;
      int32_t col_size = scan_idx == 0 ? 8 : d_size;
      max.resize(col_size, '\0');
      min.resize(col_size, '\0');
      // count: 2 bytes
      // max/min: col size
      // sum: 1 byte is_overflow + 8 byte result (int64_t or double)
      sum.resize(9, '\0');

      DATATYPE type = static_cast<DATATYPE>(scan_attrs_[scan_idx].type);
      AggCalculatorV2 aggCalc(fixed_col_value_addr, b, type, d_size, nrow_);
      *reinterpret_cast<bool *>(sum.data()) = aggCalc.CalcAggForFlush(count, max.data(), min.data(), sum.data() + 1);
      if (0 != count) {
        col_agg.resize(sizeof(uint16_t) + 2 * col_size + 9, '\0');
        memcpy(col_agg.data(), &count, sizeof(uint16_t));
        memcpy(col_agg.data() + sizeof(uint16_t), max.data(), col_size);
        memcpy(col_agg.data() + sizeof(uint16_t) + col_size, min.data(), col_size);
        memcpy(col_agg.data() + sizeof(uint16_t) + col_size * 2, sum.data(), 9);
      }
    } else {
      VarColAggCalculatorV2 aggCalc(var_rows);
      string max;
      string min;
      uint64_t count = 0;
      aggCalc.CalcAggForFlush(max, min, count);
      if (0 != count) {
        col_agg.resize(sizeof(uint16_t) + 2 * sizeof(uint32_t), '\0');
        memcpy(col_agg.data(), &count, sizeof(uint16_t));
        col_agg.append(max);
        col_agg.append(min);
        *reinterpret_cast<uint32_t *>(col_agg.data() + sizeof(uint16_t)) = max.size();
        *reinterpret_cast<uint32_t *>(col_agg.data() + sizeof(uint16_t) + sizeof(uint32_t)) = min.size();
      }
    }
    agg_data.append(col_agg);
    uint32_t offset = agg_data.size()- agg_col_offsets_len;
    memcpy(agg_data.data() + scan_idx * sizeof(uint32_t), &offset, sizeof(uint32_t));
  }
  // append column agg data
  data.append(agg_data);
  return s;
}

KStatus TsBlockSpan::GetCompressData(std::string& data) {
  assert(nrow_ > 0);
  // compressed data
  uint32_t table_version;
  if (!convert_) {
    table_version = block_->GetTableVersion();
  } else {
    table_version = convert_->version_conv_->scan_version_;
  }
  KStatus s = block_->GetCompressDataFromFile(table_version, nrow_, data);
  if (s == KStatus::SUCCESS) {
    return s;
  }
  // build compressed data
  s = BuildCompressedData(data);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

void TsBlockSpan::GetTSRange(timestamp64* min_ts, timestamp64* max_ts) {
  *min_ts = block_->GetTS(start_row_);
  *max_ts = block_->GetTS(start_row_ + nrow_ - 1);
}

KStatus TsBlockSpan::GetColBitmap(uint32_t scan_idx, TsBitmap& bitmap) {
  if (!convert_) {
    TsBitmap blk_bitmap;
    auto s = block_->GetColBitmap(scan_idx, scan_attrs_, blk_bitmap);
    if (s != SUCCESS) {
      return s;
    }
    bitmap.SetCount(nrow_);
    for (int i = 0; i < nrow_; i++) {
      bitmap[i] = blk_bitmap[start_row_ + i];
    }
    return SUCCESS;
  }
  return convert_->GetColBitmap(scan_idx, bitmap);
}

KStatus TsBlockSpan::GetFixLenColAddr(uint32_t scan_idx, char** value, TsBitmap& bitmap, bool bitmap_required) {
  if (!convert_) {
    TsBitmap blk_bitmap;
    if (!scan_attrs_[scan_idx].isFlag(AINFO_NOT_NULL)) {
      auto s = block_->GetColBitmap(scan_idx, scan_attrs_, blk_bitmap);
      if (s != KStatus::SUCCESS) {
        LOG_ERROR("GetColBitmap failed. col id [%u]", scan_idx);
        return s;
      }
      bitmap.SetCount(nrow_);
    } else {
      if (bitmap_required) {
        bitmap.SetCount(nrow_);
      }
    }
    char* blk_value;
    auto s = block_->GetColAddr(scan_idx, scan_attrs_, &blk_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColAddr failed. col id [%u]", scan_idx);
      return s;
    }
    if (!scan_attrs_[scan_idx].isFlag(AINFO_NOT_NULL)) {
      if (scan_idx == 0 && nrow_ == block_->GetRowNum()) {
        bitmap = blk_bitmap;
      } else {
        for (size_t i = 0; i < nrow_; i++) {
          DataFlags flag = blk_bitmap[start_row_+ i];
          bitmap[i] = flag;
        }
      }
    }
    *value = blk_value + scan_attrs_[scan_idx].size * start_row_;
    return SUCCESS;
  }
  return convert_->GetFixLenColAddr(scan_idx, value, bitmap, bitmap_required);
}

KStatus TsBlockSpan::GetVarLenTypeColAddr(uint32_t row_idx, uint32_t scan_idx, DataFlags& flag, TSSlice& data) {
  if (!convert_) {
    TsBitmap blk_bitmap;
    auto s = block_->GetColBitmap(scan_idx, scan_attrs_, blk_bitmap);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetColBitmap failed. col id [%u]", scan_idx);
      return s;
    }
    flag = blk_bitmap[start_row_+ row_idx];
    if (flag != DataFlags::kValid) {
      data = {nullptr, 0};
      return KStatus::SUCCESS;
    }
    TSSlice orig_value;
    s = block_->GetValueSlice(start_row_ + row_idx, scan_idx, scan_attrs_, orig_value);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetValueSlice failed. rowidx[%u] colid[%u]", row_idx, scan_idx);
      return s;
    }
    data = orig_value;
    return KStatus::SUCCESS;
  }
  return convert_->GetVarLenTypeColAddr(row_idx, scan_idx, flag, data);
}

KStatus TsBlockSpan::GetCount(uint32_t scan_idx, uint32_t& count) {
  TsBitmap bitmap;
  auto s = GetColBitmap(scan_idx, bitmap);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  count = bitmap.GetValidCount();
  return KStatus::SUCCESS;
}

KStatus TsBlockSpan::GetSum(uint32_t scan_idx, void *&pre_sum, bool &is_overflow) {
  return SUCCESS;
}

KStatus TsBlockSpan::GetMax(uint32_t scan_idx, void *&pre_max) {
  return SUCCESS;
}

KStatus TsBlockSpan::GetMin(uint32_t scan_idx, void *&pre_min) {
  return SUCCESS;
}

KStatus TsBlockSpan::GetVarMax(uint32_t scan_idx, TSSlice &pre_max) {
  return SUCCESS;
}

KStatus TsBlockSpan::GetVarMin(uint32_t scan_idx, TSSlice &pre_min) {
  return SUCCESS;
}

bool TsBlockSpan::HasPreAgg() {
  return has_pre_agg_;
}

KStatus TsBlockSpan::GetPreCount(uint32_t scan_idx, uint16_t& count) {
  if (!convert_) {
    return block_->GetPreCount(scan_idx, count);
  }
  return convert_->GetPreCount(scan_idx, count);
}

KStatus TsBlockSpan::GetPreSum(uint32_t scan_idx, void* &pre_sum, bool& is_overflow) {
  if (!convert_) {
    int32_t size = scan_attrs_[scan_idx].size;
    return block_->GetPreSum(scan_idx, size, pre_sum, is_overflow);
  }
  int32_t size = convert_->version_conv_->blk_attrs_[scan_idx].size;
  return convert_->GetPreSum(scan_idx, size, pre_sum, is_overflow);
}

KStatus TsBlockSpan::GetPreMax(uint32_t scan_idx, void* &pre_max) {
  if (!convert_) {
    return block_->GetPreMax(scan_idx, pre_max);
  }
  return convert_->GetPreMax(scan_idx, pre_max);
}

KStatus TsBlockSpan::GetPreMin(uint32_t scan_idx, void* &pre_min) {
  if (!convert_) {
    int32_t size = scan_attrs_[scan_idx].size;
    return block_->GetPreMin(scan_idx, size, pre_min);
  }
  int32_t size = convert_->version_conv_->blk_attrs_[scan_idx].size;
  return convert_->GetPreMin(scan_idx, size, pre_min);
}

KStatus TsBlockSpan::GetVarPreMax(uint32_t scan_idx, TSSlice& pre_max) {
  if (!convert_) {
    return block_->GetVarPreMax(scan_idx, pre_max);
  }
  return convert_->GetVarPreMax(scan_idx, pre_max);
}

KStatus TsBlockSpan::GetVarPreMin(uint32_t scan_idx, TSSlice& pre_min) {
  if (!convert_) {
    return block_->GetVarPreMin(scan_idx, pre_min);
  }
  return convert_->GetVarPreMin(scan_idx, pre_min);
}

KStatus TsBlockSpan::UpdateFirstLastCandidates(const std::vector<k_uint32>& ts_scan_cols,
                                                const std::vector<AttributeInfo>& schema,
                                                std::vector<k_uint32>& first_col_idxs,
                                                std::vector<k_uint32>& last_col_idxs,
                                                std::vector<AggCandidate>& candidates) {
  return block_->UpdateFirstLastCandidates(ts_scan_cols, schema, first_col_idxs, last_col_idxs, candidates);
}

void TsBlockSpan::SplitFront(int row_num, shared_ptr<TsBlockSpan>& front_span) {
  assert(row_num <= nrow_);
  assert(block_ != nullptr);
  front_span = make_shared<TsBlockSpan>(vgroup_id_, entity_id_, block_, start_row_, row_num, tbl_schema_mgr_,
                        convert_ == nullptr ? block_->GetTableVersion() : convert_->version_conv_->scan_version_);
  // change current span info
  start_row_ += row_num;
  nrow_ -= row_num;
  if (convert_) {
    convert_->SetStartRowIdx(start_row_);
    convert_->SetRowNum(nrow_);
  }
}

void TsBlockSpan::SplitBack(int row_num, shared_ptr<TsBlockSpan>& back_span) {
  assert(row_num <= nrow_);
  assert(block_ != nullptr);
  back_span = make_shared<TsBlockSpan>(vgroup_id_, entity_id_, block_, start_row_ + nrow_ - row_num,
                                  row_num, tbl_schema_mgr_,
                      convert_ == nullptr ? block_->GetTableVersion() : convert_->version_conv_->scan_version_);
  // change current span info
  nrow_ -= row_num;
  if (convert_) {
    convert_->SetRowNum(nrow_);
  }
}

void TsBlockSpan::TrimBack(int row_num) {
  assert(row_num <= nrow_);
  assert(block_ != nullptr);
  nrow_ -= row_num;
  if (convert_) {
    convert_->SetRowNum(nrow_);
  }
}

void TsBlockSpan::TrimFront(int row_num) {
  assert(row_num <= nrow_);
  assert(block_ != nullptr);
  start_row_ += row_num;
  nrow_ -= row_num;
  if (convert_) {
    convert_->SetStartRowIdx(start_row_);
    convert_->SetRowNum(nrow_);
  }
}

}  // namespace kwdbts
