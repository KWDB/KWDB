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

#include <cstdio>
#include <algorithm>
#include <cstring>
#include <atomic>
#include <lg_api.h>
#include "dirent.h"
#include "mmap/mmap_segment_table.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "cm_func.h"
#include "utils/compress_utils.h"
#include "engine.h"
#include "ts_time_partition.h"
#include "lt_rw_latch.h"
#include "perf_stat.h"
#include "st_config.h"
#include "sys_utils.h"

extern bool g_engine_initialized;

KStatus push_back_payload_with_conv(kwdbts::Payload* payload, char* value, const AttributeInfo& payload_attr,
                                    DATATYPE new_type, int32_t new_len, size_t start_row, uint32_t count, int32_t col_idx) {
  ErrorInfo err_info;
  for (int idx = 0; idx < count; ++idx) {
    DATATYPE old_type = static_cast<DATATYPE>(payload_attr.type);
    int32_t old_len = payload_attr.size;
    void* old_mem = nullptr;
    // Check whether merge data exists
    auto it = payload->tmp_col_values_4_dedup_merge_.find(col_idx);
    if (unlikely(it != payload->tmp_col_values_4_dedup_merge_.end())) {
      auto data_it = it->second.find(start_row + idx);
      if (data_it != it->second.end()) {
        old_type = static_cast<DATATYPE>(data_it->second.attr.type);
        old_len = data_it->second.attr.size;
        old_mem = data_it->second.value.get();
      }
    }
    // If merge data does not exist, it is obtained from the payload
    if (!old_mem) {
      if (isVarLenType(old_type)) {
        old_mem = payload->GetVarColumnAddr(start_row + idx, col_idx);
      } else {
        old_mem = payload->GetColumnAddr(start_row + idx, col_idx);
      }
    }
    // Insert data
    if (old_type != new_type) {
      // Different data types
      if (!isVarLenType(old_type)) {
        // Convert fixed-length data to fixed-length data
        if (new_type == DATATYPE::CHAR || new_type == DATATYPE::BINARY) {
          err_info.errcode = convertFixedToStr(old_type, (char*) old_mem, value + idx * new_len, err_info);
        } else {
          err_info.errcode = convertFixedToNum(old_type, new_type, (char*) old_mem, value + idx * new_len, err_info);
        }
        if (err_info.errcode < 0) {
          return KStatus::FAIL;
        }
      } else if (isVarLenType(old_type)) {
        // Variable length data is converted to fixed length data
        if (payload->IsNull(col_idx, start_row + idx)) {
          continue;
        }
        void* old_var_mem = old_mem;
        int32_t old_var_len = KUint16(old_var_mem);  // payload->GetVarColumnLen(start_row + idx, col_idx);
        convertStrToFixed((char*) old_var_mem + MMapStringColumn::kStringLenLen,
                          new_type, value + idx * new_len, old_var_len, err_info);
      }
    } else {
      // Data of the same type is written
      size_t copy_size = std::min(old_len, new_len);
      memcpy((char*)value + idx * new_len, old_mem, copy_size);
    }
  }
  return KStatus::SUCCESS;
}

std::shared_ptr<void> ConvertToVarLenBeforePush(kwdbts::Payload* payload, DATATYPE old_type, DATATYPE new_type,
                                                size_t start_row, uint32_t col_idx, bool has_merge_data,
                                                std::shared_ptr<void> merge_data) {
  ErrorInfo err_info;
  std::shared_ptr<void> data = nullptr;
  if (!isVarLenType(old_type)) {
    void* old_mem;
    if (likely(!has_merge_data)) {
      old_mem = payload->GetColumnAddr(start_row, col_idx);
    } else {
      old_mem = merge_data.get();
    }
    data = convertFixedToVar(old_type, new_type, (char*) old_mem, err_info);
  } else {
    TSSlice old_value;
    if (unlikely(has_merge_data)) {
      char* var_addr_with_len = reinterpret_cast<char*>(merge_data.get());
      uint16_t var_c_len = KUint16(var_addr_with_len);
      old_value = {var_addr_with_len + sizeof(uint16_t), var_c_len};
    } else {
      old_value = {payload->GetVarColumnAddr(start_row, col_idx) + MMapStringColumn::kStringLenLen,
                   payload->GetVarColumnLen(start_row, col_idx)};
    }
    if (old_type == VARSTRING) {
      auto old_len = uint16_t(old_value.len) - 1;
      char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen));
      memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen);
      KUint16(var_data) = old_len;
      memcpy(var_data + MMapStringColumn::kStringLenLen, (char*) old_value.data, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    } else {
      auto old_len = uint16_t(old_value.len);
      char* var_data = static_cast<char*>(std::malloc(old_len + MMapStringColumn::kStringLenLen + 1));
      memset(var_data, 0, old_len + MMapStringColumn::kStringLenLen + 1);
      KUint16(var_data) = old_len + 1;
      memcpy(var_data + MMapStringColumn::kStringLenLen, (char*) old_value.data, old_len);
      std::shared_ptr<void> ptr(var_data, free);
      data = ptr;
    }
  }
  return data;
}

MMapSegmentTable::MMapSegmentTable() : rw_latch_(RWLATCH_ID_MMAP_SEGMENT_TABLE_RWLOCK) {
  reserved_rows_ = 1024;
}

MMapSegmentTable::~MMapSegmentTable() {
  string real_path;
  if(meta_data_) {
    real_path = bt_file_.realFilePath();
  }
  ErrorInfo err_info;
  if (close(err_info) < 0) {
    LOG_DEBUG("~MMapSegmentTable[%s] failed", real_path.c_str());
  }
}

impl_latch_virtual_func(MMapSegmentTable, &rw_latch_)

int MMapSegmentTable::create(EntityBlockMetaManager* meta_manager, const vector<AttributeInfo>& schema,
  const uint32_t& table_version, int encoding, ErrorInfo& err_info, uint32_t max_rows_per_block,
  uint32_t max_blocks_per_segment) {
  if (init(meta_manager, schema, encoding, err_info) < 0)
    return err_info.errcode;

  if (max_rows_per_block != 0 && max_blocks_per_segment != 0) {
    max_rows_per_block_ = max_rows_per_block;
    max_blocks_per_segment_ = max_blocks_per_segment;
    meta_data_->max_rows_per_block = max_rows_per_block;
    meta_data_->max_blocks_per_segment = max_blocks_per_segment;
    block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
  }

  meta_data_->magic = magic();
  meta_data_->struct_type |= (ST_COLUMN_TABLE);
  meta_data_->schema_version = table_version;
  meta_data_->has_data = true;
  if (initColumn(bt_file_.flags(), err_info) < 0)
    return err_info.errcode;

  actual_writed_count_.store(meta_data_->num_node);
  setObjectReady();
  return err_info.errcode;
}

int MMapSegmentTable::addColumnFile(int col, int flags, ErrorInfo& err_info) {
  string col_file_name = name_ + '.' + intToString(col);
  MMapFile* col_file = new MMapFile();
  col_files_[col] = col_file;
  err_info.errcode = col_file->open(col_file_name, db_path_ + tbl_sub_path_ + col_file_name, flags);
  return err_info.setError(err_info.errcode);
}

int MMapSegmentTable::initColumn(int flags, ErrorInfo& err_info) {
  col_files_.resize(cols_info_include_dropped_.size());
  col_block_header_size_.resize(cols_info_include_dropped_.size());
  col_block_size_.resize(cols_info_include_dropped_.size());
  for (size_t i = 0 ; i < cols_info_include_dropped_.size() ; ++i) {
    if (cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
      col_files_[i] = nullptr;
      continue;
    }
    addColumnFile(i, flags, err_info);
    // check if all file open success
    if (err_info.errcode < 0) {
      return err_info.errcode;
    }
    // calculte block header size and block size
    col_block_header_size_[i] = GetColBlockHeaderSize(cols_info_include_dropped_[i].size);
    col_block_size_[i] = GetColBlockSize(cols_info_include_dropped_[i].size);

    // vartype column will use string file to store real values.
    if (isVarLenType(cols_info_include_dropped_[i].type) && !m_str_file_) {
      m_str_file_ = new MMapStringColumn(LATCH_ID_METRICS_STRING_FILE_MUTEX, RWLATCH_ID_METRICS_STRING_FILE_RWLOCK);
      std::string col_str_file_name = name_ + ".s";
      int err_code = m_str_file_->open(col_str_file_name, db_path_ + tbl_sub_path_ + col_str_file_name, flags);
      if (err_code < 0) {
        return err_code;
      }
      err_code = m_str_file_->reserve(4, 1024 * 1024);
      if (err_code < 0) {
        return err_code;
      }
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::open_(int char_code, const string& file_path, const std::string& db_path,
                          const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  if ((err_info.errcode = TsTableObject::open(file_path, db_path, tbl_sub_path, char_code, flags))
      < 0) {
    return err_info.errcode;
  }
  name_ = getTsObjectName(file_path);
  if (metaDataLen() >= (off_t) sizeof(TSTableFileMetadata)) {
    // Initialize segment pre allocation space size configuration
    if (UNLIKELY(meta_data_->max_blocks_per_segment == 0 || meta_data_->max_rows_per_block == 0)) {
      if (meta_manager_->getEntityHeader()->max_blocks_per_segment != 0 &&
          meta_manager_->getEntityHeader()->max_rows_per_block != 0) {
        // Compatible with lower versions(2.0.3.x)
        max_blocks_per_segment_ = meta_manager_->getEntityHeader()->max_blocks_per_segment;
        max_rows_per_block_ = meta_manager_->getEntityHeader()->max_rows_per_block;
        block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
      } else {
        // Get the current segment configuration
        LOG_WARN("Segment[%s] pre allocation configuration exception", file_path.c_str())
        int64_t partition_interval = meta_manager_->maxTimestamp() - meta_manager_->minTimestamp() + 1;
        GetSegmentConfig(max_blocks_per_segment_, max_rows_per_block_, meta_manager_->GetTableId(),
                         meta_manager_->max_entities_per_subgroup, partition_interval);
        block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
      }
      // If uncompressed, update persistent metadata
      if (!is_compressed_) {
        meta_data_->max_blocks_per_segment = max_blocks_per_segment_;
        meta_data_->max_rows_per_block = max_rows_per_block_;
      }
    } else {
      max_blocks_per_segment_ = meta_data_->max_blocks_per_segment;
      max_rows_per_block_ = meta_data_->max_rows_per_block;
      block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
    }
    if (meta_data_->has_data) {
      if (initColumn(flags, err_info) < 0)
        return err_info.errcode;

      if (isTransient(meta_data_->struct_type))
        return 0;
    }
    actual_writed_count_.store(meta_data_->num_node);
    setObjectReady();
  } else {
    if (!(bt_file_.flags() & O_CREAT)) {
      err_info.errcode = KWECORR;
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::open(EntityBlockMetaManager* meta_manager, BLOCK_ID segment_id, const string& file_path,
                           const std::string& db_path, const string& tbl_sub_path, int flags, bool lazy_open,
                           ErrorInfo& err_info) {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });
  if (getObjectStatus() == OBJ_READY) {
    return 0;
  }
  meta_manager_ = meta_manager;
  segment_id_ = segment_id;

  db_path_ = db_path;
  tbl_sub_path_ = tbl_sub_path;
  bt_file_.filePath() = file_path;
  bt_file_.realFilePath() = db_path + tbl_sub_path + file_path;

  // check if we should mount sqfs file
  string segment_dir = db_path + tbl_sub_path;
  string sqfs_path = getCompressedFilePath();
  is_compressed_ = IsExists(sqfs_path);
  bool is_mounted = isMounted(segment_dir);
  if (!(flags & O_CREAT) && lazy_open) {
    if (is_compressed_ && !is_mounted && IsExists(segment_dir)) {
      RemoveDirContents(segment_dir);
    }
    return 0;
  }
  if (is_compressed_ && !mount(sqfs_path, segment_dir, err_info)) {
    LOG_ERROR("%s mount failed", sqfs_path.c_str());
    return err_info.errcode;
  }

  open_(magic(), file_path, db_path, tbl_sub_path, flags, err_info);
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode, tbl_sub_path + file_path);
    LOG_ERROR("%s open_ failed, err_msg: %s", tbl_sub_path.c_str(), err_info.errmsg.c_str());
    return err_info.errcode;
  }

  // check if compressed failed last time.
  if (!(flags & O_CREAT) && !is_compressed_ && getSegmentStatus() == ImmuSegment) {
    setSegmentStatus(InActiveSegment);
  }

  // if reserve space failed while create segment, we can reserve again here.
  if (meta_data_ != nullptr && _reservedSize() == 0) {
    LOG_DEBUG("reservedSize of table[ %s ] is 0.", file_path.c_str());
    setObjectReady();
    err_info.errcode = reserve(getReservedRows());
    if (err_info.errcode < 0) {
      err_info.setError(err_info.errcode, tbl_sub_path + file_path);
    }
    LOG_DEBUG("reserved size for table[%s],status: %s", file_path.c_str(), err_info.errmsg.c_str());
  }

  return err_info.errcode;
}

int MMapSegmentTable::init(EntityBlockMetaManager* meta_manager, const vector<AttributeInfo>& schema, int encoding,
                           ErrorInfo& err_info) {
  meta_manager_ = meta_manager;
  err_info.errcode = initMetaData();
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  time(&meta_data_->create_time);
  meta_data_->block_num_of_segment = 0;

  name_ = getTsObjectName(path());

  for (size_t i = 0 ; i < schema.size() ; ++i) {
    cols_info_include_dropped_.push_back(schema[i]);
  }

  if ((meta_data_->record_size = setAttributeInfo(cols_info_include_dropped_)) < 0) {
    return err_info.errcode;
  }

  off_t col_off = 0;
  col_off = addColumnInfo(cols_info_include_dropped_, err_info.errcode);
  if (err_info.errcode < 0) {
    return err_info.errcode;
  }
  assign(meta_data_->attribute_offset, col_off);

  meta_data_->cols_num = cols_info_include_dropped_.size();
  meta_data_->struct_type = (ST_VTREE | ST_NS_EXT);

  // Get the current segment configuration
  int64_t partition_interval = meta_manager_->maxTimestamp() - meta_manager_->minTimestamp() + 1;
  GetSegmentConfig(max_blocks_per_segment_, max_rows_per_block_, meta_manager_->GetTableId(),
                   meta_manager_->max_entities_per_subgroup, partition_interval);
  block_null_bitmap_size_ = (max_rows_per_block_ + 7) / 8;
  meta_data_->max_blocks_per_segment = max_blocks_per_segment_;
  meta_data_->max_rows_per_block = max_rows_per_block_;

  cols_info_exclude_dropped_.clear();
  idx_for_valid_cols_.clear();
  for (int i = 0; i < cols_info_include_dropped_.size(); ++i) {
    if (!cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
      cols_info_exclude_dropped_.emplace_back(cols_info_include_dropped_[i]);
      idx_for_valid_cols_.emplace_back(i);
    }
  }

  return err_info.errcode;
}

int MMapSegmentTable::close(ErrorInfo& err_info) {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });
  if (getObjectStatus() != OBJ_READY) {
    return err_info.errcode;
  }

  setObjectStatus(OBJ_NOT_READY);

  int flags = bt_file_.flags();

  if (actual_writed_count_.load() != meta_data_->num_node) {
    // LOG_WARN("MMapSegmentTable is writing while deleting.");
    meta_data_->num_node = actual_writed_count_.load();
  }

  SegmentStatus s_status = ActiveSegment;
  if (meta_data_ != nullptr) {
    s_status = getSegmentStatus();
  }

  for (auto& col_file : col_files_) {
    delete col_file;
    col_file = nullptr;
  }
  if (m_str_file_) {
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  TsTableObject::close();

  if (s_status == ImmuSegment) {
    string segment_dir = db_path_ + tbl_sub_path_;
    if (!isMounted(segment_dir)) {
      RemoveDirContents(segment_dir);
    }
  }

  if (g_engine_initialized && g_max_mount_cnt_ != 0 && g_cur_mount_cnt_ > g_max_mount_cnt_
      && s_status == ImmuSegment) {
    if (is_latest_opened_ && !umount(db_path_, tbl_sub_path_, err_info)) {
      LOG_WARN("at MMapSegmentTable::close %s", err_info.errmsg.c_str());
    }
  }
  return err_info.errcode;
}

int MMapSegmentTable::setAttributeInfo(vector<AttributeInfo>& info) {
  int offset = 0;

  for (vector<AttributeInfo>::iterator it = info.begin() ; it != info.end() ; ++it) {
    if (it->length <= 0)
      it->length = 1;
    if ((it->size = getDataTypeSize(*it)) == -1)
      return -1;
    if (it->max_len == 0)
      it->max_len = it->size;
    it->offset = offset;
    offset += it->size;
    if (it->type == STRING) {
#if defined(USE_SMART_INDEX)
      if ((encoding & SMART_INDEX) && !actual_dim.empty())
        it->encoding = SMART_INDEX;
      else
#endif
      it->encoding = DICTIONARY;
    }
  }
  return offset;
}

int MMapSegmentTable::reserve(size_t n) {
  int err_code;
  if (meta_data_ == nullptr) {
    err_code = KWENOOBJ;
    return err_code;
  }
  if (n < _reservedSize())
    return 0;

  err_code = reserveBase(n);

  return err_code;
}

int MMapSegmentTable::reserveBase(size_t n) {
  if (meta_data_ == nullptr) {
    return KWENOOBJ;
  }
  if (!meta_data_->has_data) {
    return 0;
  }
  if (n < _reservedSize())
    return 0;
  int err_code;

  if (TSObject::startWrite() < 0) {
    return KWENOOBJ;
  }

  for (size_t i = 0 ; i < cols_info_include_dropped_.size() ; ++i) {
    if (cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    size_t max_length = getBlockMaxNum() * col_block_size_[i];
    // 32 bit system may overflow here
    if (max_length / (size_t) getBlockMaxNum() != col_block_size_[i]) {
      LOG_ERROR("reserve overflow in column file: %s, block_max_num: %lu, col_block_size: %lu",
                col_files_[i]->realFilePath().c_str(), getBlockMaxNum(),
                getBlockMaxNum() * col_block_size_[i]);
      return KWECORR;
    }
    err_code = col_files_[i]->mremap(max_length);
    if (err_code < 0) {
      break;
    }
  }

  if (err_code >= 0) {
    _reservedSize() = n;
  }

  this->TSObject::stopWrite();
  return err_code;
}

int MMapSegmentTable::truncate() {
  if (TSObject::startWrite() < 0) {
    return KWENOOBJ;
  }

  int error_code = 0;
  int block_num = meta_data_->block_num_of_segment;
  for (size_t column = 0 ; column < cols_info_include_dropped_.size() ; ++column) {
    if (col_files_[column] == nullptr) {
      continue;
    }
    // Truncate the unused space in each column of data files
    size_t length = block_num * getDataBlockSize(column);
    error_code = col_files_[column]->resize(length);
    if (error_code < 0) {
      TSObject::stopWrite();
      return error_code;
    }
  }

  TSObject::stopWrite();
  return error_code;
}

void MMapSegmentTable::ResetBlockAgg(BlockItem* blk_item, char* block_header,
                                     const AttributeInfo& col, std::list<std::shared_ptr<void>> var_values) {
  AggDataAddresses addresses{};
  size_t offset = (getBlockMaxRows() + 7) / 8;  // bitmap size
  addresses.count = reinterpret_cast<void*>((intptr_t)block_header + offset);
  addresses.max = reinterpret_cast<void*>((intptr_t) addresses.count + BLOCK_AGG_COUNT_SIZE);
  addresses.min = reinterpret_cast<void*>((intptr_t) addresses.max + col.size);
  addresses.sum = reinterpret_cast<void*>((intptr_t) addresses.max + col.size * 2);

  if (isAllDeleted(block_header, 1, blk_item->publish_row_count)) {
    // todo(liangbo01)
    LOG_WARN("block has no column values. agg result maybe wrong.");
    KUint16(addresses.count) = 0;
    return;
  }

  char* block_first_data = block_header + GetColBlockHeaderSize(col.size);
  if (!isVarLenType(col.type)) {
    AggCalculator aggCal(block_first_data, block_header, 1, DATATYPE(col.type), col.size, blk_item->publish_row_count);
    if (aggCal.CalAllAgg(addresses.min, addresses.max, addresses.sum, addresses.count, true, {blk_item, 0, blk_item->publish_row_count})) {
      blk_item->is_overflow = true;
    }
  } else {
    KUint64(addresses.min) = -1;
    KUint64(addresses.max) = -1;
    KUint16(addresses.count) = var_values.size();
    int min_idx, max_idx;
    int valid_col_idx = 0;
    VarColAggCalculator::CalAllAgg(var_values, &min_idx, &max_idx);
    for (size_t i = 0; i < blk_item->publish_row_count; i++) {
      if (isColDeleted(blk_item->rows_delete_flags, block_header, i + 1)) {
        continue;
      }
      if (valid_col_idx == min_idx) {
        memcpy(addresses.min, block_first_data + i * col.size, col.size);
      }
      if (valid_col_idx == max_idx) {
        memcpy(addresses.max, block_first_data + i * col.size, col.size);
      }
      valid_col_idx++;
    }
  }
}

int MMapSegmentTable::pushBackToColumn(MetricRowID start_row, size_t segment_col_idx, size_t payload_col_idx,
                                   kwdbts::Payload* payload, size_t start_in_payload, const BlockSpan& span,
                                   kwdbts::DedupInfo& dedup_info) {
  int error_code = 0;
  size_t segment_col = idx_for_valid_cols_[segment_col_idx];
  MetricRowID block_first_row = span.block_item->getRowID(1);
  if (!isVarLenType(cols_info_exclude_dropped_[segment_col_idx].type)) {
    push_back_payload(payload, start_row, segment_col, payload_col_idx, start_in_payload, span.row_num);
  } else {
    // Variable length is written line by line and can be directly written into the .s file.
    // Before writing, it will check whether the current .s file needs to be resized.
    // The merge mode will determine whether to write the merged data based on the offset of the payload.
    // String file stores the actual value of the string, and stores the offset of the string file
    // in the data column file.
    error_code = push_back_var_payload(payload, start_row, segment_col, payload_col_idx, start_in_payload, span.row_num);
    if (error_code < 0) return error_code;
  }

  if (!payload->NoNullMetric(payload_col_idx)) {
    RW_LATCH_X_LOCK(&rw_latch_);
    push_back_null_bitmap(payload, start_row, segment_col, payload_col_idx, start_in_payload, span.row_num);
    RW_LATCH_UNLOCK(&rw_latch_);
  }
  return error_code;
}

void MMapSegmentTable::updateAggregateResult(const BlockSpan& span, bool include_k_timestamp)  {
  MetricRowID start_row = MetricRowID{span.block_item->block_id, 1};
  size_t segment_col_idx = include_k_timestamp ? 0 : 1;
  for (; segment_col_idx < cols_info_exclude_dropped_.size(); ++segment_col_idx) {
    if (!hasValue(start_row, span.block_item->publish_row_count, idx_for_valid_cols_[segment_col_idx])) {
      // block has column all value null, agg no available.
      return;
    }
    AggDataAddresses addresses{};
    columnAggCalculate(span, start_row, segment_col_idx, addresses, span.block_item->publish_row_count, true);
    if (segment_col_idx == 0) {
      span.block_item->non_null_row_count = *reinterpret_cast<uint16_t*>(addresses.count);
      span.block_item->max_ts_in_block = KTimestamp(addresses.max);
      span.block_item->min_ts_in_block = KTimestamp(addresses.min);
      // Update the maximum and minimum timestamp information of the current segment,
      // which will be used for subsequent compression and other operations
      if (KTimestamp(addresses.max) > maxTimestamp() || maxTimestamp() == INVALID_TS) {
        maxTimestamp() = KTimestamp(addresses.max);
      }
      if (KTimestamp(addresses.min) > minTimestamp() || minTimestamp() == INVALID_TS) {
        minTimestamp() = KTimestamp(addresses.min);
      }
    }
    
  }
  span.block_item->is_agg_res_available = true;
}

void MMapSegmentTable::columnAggCalculate(const BlockSpan& span, MetricRowID start_row, size_t segment_col_idx,
                                          AggDataAddresses& addresses, size_t row_num, bool max_rows) {
  size_t segment_col = idx_for_valid_cols_[segment_col_idx];
  calculateAggAddr(span.block_item->block_id, segment_col, addresses);
  bool is_block_first_row = true;
  // timestamp col update agg every putdata. need check this agg udpate  is putdata or block full agg.
  if (segment_col_idx == 0) {
    if (max_rows) {
      *reinterpret_cast<uint16_t*>(addresses.count) = 0;
    } else {
      is_block_first_row = *reinterpret_cast<uint16_t*>(addresses.count) == 0;
    }
  }
  if (!isVarLenType(cols_info_exclude_dropped_[segment_col_idx].type)) {
    void* src = columnAddr(start_row, segment_col);
    AggCalculator aggCal(src, columnNullBitmapAddr(start_row.block_id, segment_col),
                          start_row.offset_row, DATATYPE(cols_info_exclude_dropped_[segment_col_idx].type),
                          cols_info_exclude_dropped_[segment_col_idx].size, row_num);
    if (aggCal.CalAllAgg(addresses.min, addresses.max, addresses.sum, addresses.count, is_block_first_row, span)) {
      span.block_item->is_overflow = true;
    }
  } else {
    std::shared_ptr<void> min_base =
        is_block_first_row ? nullptr : varColumnAggAddr(start_row, segment_col, Sumfunctype::MIN);
    std::shared_ptr<void> max_base =
        is_block_first_row ? nullptr : varColumnAggAddr(start_row, segment_col, Sumfunctype::MAX);
    void* mem = columnAddr(start_row, segment_col);
    vector<shared_ptr<void>> var_mem;
    for (k_uint32 j = 0; j < row_num; ++j) {
      std::shared_ptr<void> data = nullptr;
      if (!isNullValue(start_row + j, segment_col)) {
        data = varColumnAddr(start_row + j, segment_col);
      }
      var_mem.push_back(data);
    }
    VarColAggCalculator aggCal(mem, var_mem, columnNullBitmapAddr(start_row.block_id, segment_col),
                                start_row.offset_row, cols_info_exclude_dropped_[segment_col_idx].size, row_num);
    aggCal.CalAllAgg(addresses.min, addresses.max, min_base, max_base, addresses.count, is_block_first_row, span);
  }


  // Update the maximum and minimum timestamp information of the current segment,
  // which will be used for subsequent compression and other operations
  if (segment_col_idx == 0) {
    if (KTimestamp(addresses.max) > maxTimestamp() || maxTimestamp() == INVALID_TS) {
      maxTimestamp() = KTimestamp(addresses.max);
    }
    if (KTimestamp(addresses.min) > minTimestamp() || minTimestamp() == INVALID_TS) {
      minTimestamp() = KTimestamp(addresses.min);
    }
  }
}

int MMapSegmentTable::putDataIntoVarFile(char* var_value, DATATYPE type, size_t* loc) {
  assert(isVarLenType(type));
  int err_code = 0;
  mutexLock();
  Defer defer{[&]() { mutexUnlock(); }};
  size_t var_value_len = KUint16(var_value);
  // insert var string to string_file. and get string file location.
  if (var_value_len + m_str_file_->size() >= m_str_file_->fileLen()) {
    // The default size of the string_file file is 4MB, and it automatically remaps and expands based on usage,
    // doubling within 1GB and expanding in increments of 50% if it exceeds 1GB.
    if (m_str_file_->fileLen() > 1024 * 1024 * 1024) {
      size_t new_size = m_str_file_->size() / 2;
      err_code = m_str_file_->reserve(m_str_file_->size(), 3 * new_size, 2);
    } else {
      err_code = m_str_file_->reserve(m_str_file_->size(), 2 * m_str_file_->fileLen(), 2);
    }
  }
  if (err_code < 0) {
    LOG_ERROR("reserve file failed.");
    return err_code;
  }
  // When writing data, variable-length data is first written to the string_file file to obtain the offset,
  // and then written to the data block in the data column using memcpy.
  void* data_start = var_value + MMapStringColumn::kStringLenLen;
  size_t data_len = var_value_len - MMapStringColumn::kStringLenLen;
  if (type == DATATYPE::VARSTRING) {
    *loc = m_str_file_->push_back(data_start, data_len);
  } else {
    *loc = m_str_file_->push_back_binary(data_start, data_len);
  }

  if (loc == 0) {
    LOG_ERROR("StringFile push back failed.");
    return -1;
  }
  return 0;
}


int MMapSegmentTable::CopyColBlocks(BlockItem* blk_item, const TsBlockFullData& block, KwTsSpan ts_span) {
  if (cols_info_exclude_dropped_.size() != idx_for_valid_cols_.size()) {
    LOG_ERROR("Schema mismatch: The number of columns does not match!");
    return -1;
  }
  if (cols_info_exclude_dropped_.size() != block.col_block_addr.size()) {
    LOG_ERROR("Schema mismatch: The number of columns does not match with payload!");
    return -1;
  }

  auto var_iter = block.var_col_values.begin();
  for (size_t i = 0; i < block.col_block_addr.size(); ++i) {
    size_t segment_col_id = idx_for_valid_cols_[i];
    char* desc_block_addr = static_cast<char*>(getBlockHeader(blk_item->block_id, segment_col_id));
    // copy total block data into segment block
    memcpy(desc_block_addr, block.col_block_addr[i].data, block.col_block_addr[i].len);
    if (isVarLenType(cols_info_include_dropped_[segment_col_id].type)) {
      // if var type column, we need change tuple offset value.
      uint64_t min_offset = KUint64(columnAggAddr(blk_item->block_id, segment_col_id, Sumfunctype::MIN));
      uint64_t max_offset = KUint64(columnAggAddr(blk_item->block_id, segment_col_id, Sumfunctype::MAX));
      for (k_uint32 j = 0; j < blk_item->publish_row_count; ++j) {
        std::shared_ptr<void> data = nullptr;
        // row not deleted, and col not deleted.
        if (!isColDeleted(blk_item->rows_delete_flags, desc_block_addr, j + 1)) {
          if (var_iter == block.var_col_values.end()) {
            LOG_ERROR("the num of var cols not match num of var in block.");
            return -1;
          }
          size_t loc;
          int err_code = putDataIntoVarFile(static_cast<char*>(var_iter->get()), (DATATYPE)(cols_info_include_dropped_[segment_col_id].type), &loc);
          if (err_code < 0) {
            LOG_ERROR("putDataIntoVarFile failed.");
            return err_code;
          }
          var_iter++;
          char* cur_col_value_addr = desc_block_addr + col_block_header_size_[segment_col_id] + cols_info_include_dropped_[segment_col_id].size * j;
          auto old_col_loc = KUint64(cur_col_value_addr);
          KUint64(cur_col_value_addr) = loc;
          if (blk_item->is_agg_res_available) {
            if (old_col_loc == min_offset) {
              KUint64(columnAggAddr(blk_item->block_id, segment_col_id, Sumfunctype::MIN)) = loc;
            }
            if (old_col_loc == max_offset) {
              KUint64(columnAggAddr(blk_item->block_id, segment_col_id, Sumfunctype::MAX)) = loc;
            }
          }
        }
      }
    }
  }
  assert(var_iter == block.var_col_values.end());

  // filter rows that out ts span.
  if (blk_item->is_agg_res_available) {
    timestamp64 max_ts = KInt64(columnAggAddr(blk_item->block_id, 0, Sumfunctype::MAX));
    timestamp64 min_ts = KInt64(columnAggAddr(blk_item->block_id, 0, Sumfunctype::MIN));
    if (ts_span.begin <= min_ts && max_ts <= ts_span.end) {
      LOG_DEBUG("entity [%u]`s block [%u] ts [ %ld - %ld] total in span", blk_item->entity_id, blk_item->block_id, min_ts, max_ts);
      return 0;
    }
  }
  LOG_DEBUG("entity [%u]`s block [%u] cross with span or agg no available [%d]",
             blk_item->entity_id, blk_item->block_id,  blk_item->is_agg_res_available);
  char* ts_col_block_addr = static_cast<char*>(getBlockHeader(blk_item->block_id, 0));
  for (k_uint32 j = 0; j < blk_item->publish_row_count; ++j) {
    if (isRowDeleted(blk_item->rows_delete_flags, j + 1)) {
      continue;
    }
    char* cur_col_value_addr = ts_col_block_addr + col_block_header_size_[0] + cols_info_include_dropped_[0].size * j;
    auto cur_ts = KInt64(cur_col_value_addr);
    if (ts_span.begin <= cur_ts && cur_ts <= ts_span.end) {
      continue;
    }
    setRowDeleted(blk_item->rows_delete_flags, j + 1);
  }
  return 0;
}

int MMapSegmentTable::PushPayload(uint32_t entity_id, MetricRowID start_row, kwdbts::Payload* payload,
                                size_t start_in_payload, const BlockSpan& span,
                                uint32_t* inc_unordered_cnt, kwdbts::DedupInfo& dedup_info) {
  int error_code = 0;
  // During writing, a determination is made as to whether the current payload contains out-of-order data.
  // If present, entity_item is marked as being out of order.
  // During subsequent queries, a judgment will be made. If the result set is unordered, a secondary HASH aggregation based on AGG SCAN is required.
  EntityItem* entity_item = meta_manager_->getEntityItem(entity_id);
  timestamp64 block_max_ts = KTimestamp(columnAggAddr(span.block_item->block_id, 0, Sumfunctype::MAX));
  if (payload->IsDisordered(start_in_payload, span.row_num) ||
      (span.block_item->publish_row_count != 0 && payload->GetTimestamp(start_in_payload) < block_max_ts)) {
    span.block_item->unordered_flag = true;
    (*inc_unordered_cnt) += span.row_num;
  }

  // compatibility process : payload version may lower segment version
  const std::vector<uint32_t>& payload_valid_cols = payload->GetValidCols();
  int payload_col_idx = 1;  // skip ts column
  for (size_t i = 1; i < idx_for_valid_cols_.size(); ++i) {
    // payload columns may have been dropped, skip them
    while (payload_col_idx < payload_valid_cols.size() &&
           idx_for_valid_cols_[i] > payload_valid_cols[payload_col_idx]) {
      payload_col_idx++;
    }
    // column not exists in payload, set to null
    if (payload_col_idx == payload_valid_cols.size()) {
      MetricRowID row_id = start_row;
      for (int idx = 0 ; idx < span.row_num; ++idx) {
        setNullBitmap(row_id, idx_for_valid_cols_[i]);
        row_id.offset_row++;
      }
      continue;
    }

    // Memcpy by column. If the column is of variable length, write it row by row. After completion, update the bitmap and aggregation results.
    error_code = pushBackToColumn(start_row, i, payload_col_idx, payload, start_in_payload, span, dedup_info);
    if (error_code < 0) {
      return error_code;
    }
    payload_col_idx++;
  }
  // set timestamp at last. timestamp not 0 used for checking data inserted fully.
  error_code = pushBackToColumn(start_row, 0, 0, payload, start_in_payload, span, dedup_info);
  actual_writed_count_.fetch_add(span.row_num);

  // Aggregate result update, the structure includes 4 addresses, namely min, max, sum, count,
  // calculateAggAddr will write the obtained aggregation address to the AggDataAddresses struct.
  {
    AggDataAddresses addresses{};
    RW_LATCH_X_LOCK(&rw_latch_);
    columnAggCalculate(span, start_row, 0, addresses, span.row_num, false);
    span.block_item->max_ts_in_block = KTimestamp(addresses.max);
    span.block_item->min_ts_in_block = KTimestamp(addresses.min);
    // Update the maximum and minimum timestamp information of the current segment,
    // which will be used for subsequent compression and other operations
    if (KTimestamp(addresses.max) > maxTimestamp() || maxTimestamp() == INVALID_TS) {
      maxTimestamp() = KTimestamp(addresses.max);
    }
    if (KTimestamp(addresses.min) > minTimestamp() || minTimestamp() == INVALID_TS) {
      minTimestamp() = KTimestamp(addresses.min);
    }
    span.block_item->non_null_row_count = *reinterpret_cast<uint16_t*>(addresses.count);
    RW_LATCH_UNLOCK(&rw_latch_);
  }
  return error_code;
}

void MMapSegmentTable::push_back_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                       size_t payload_column_idx, size_t start_row, size_t num) {
  AttributeInfo payload_column_info = payload->GetSchemaInfo()[payload_column_idx];
  if (likely(!payload->HasMergeData(payload_column_idx) && isSameType(payload_column_info, cols_info_include_dropped_[segment_column]))) {
    memcpy(columnAddr(row_id, segment_column), payload->GetColumnAddr(start_row, payload_column_idx),
           num * cols_info_include_dropped_[segment_column].size);
  } else {
    push_back_payload_with_conv(payload, (char*) columnAddr(row_id, segment_column), payload_column_info,
                                (DATATYPE) cols_info_include_dropped_[segment_column].type, cols_info_include_dropped_[segment_column].size, start_row,
                                num, payload_column_idx);
  }
  return;
}

int MMapSegmentTable::push_back_var_payload(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                     size_t payload_column_idx, size_t payload_start_row, size_t payload_num) {
  mutexLock();
  int err_code = 0;
  size_t loc = 0;
  uint16_t var_c_len = 0;
  bool need_convert = false;
  std::shared_ptr<void> data = nullptr;
  char* var_addr_with_len = nullptr;
  for (int i = 0 ; i < payload_num ; i++) {
    if (!payload->IsNull(payload_column_idx, payload_start_row)) {
      AttributeInfo payload_column_info = payload->GetSchemaInfo()[payload_column_idx];
      bool has_merge_value = false;
      MergeValueInfo merge_value;
      if (unlikely(payload->HasMergeData(payload_column_idx))) {
        auto it = payload->tmp_col_values_4_dedup_merge_.find(payload_column_idx);
        auto data_it = it->second.find(payload_start_row);
        if (data_it != it->second.end()) {
          has_merge_value = true;
          payload_column_info = data_it->second.attr;
          merge_value = data_it->second;
        }
      }
      if (payload_column_info.type == cols_info_include_dropped_[segment_column].type) {
        need_convert = false;
        if (has_merge_value) {  // reuse string in stringfile.
          var_addr_with_len = reinterpret_cast<char*>(merge_value.value.get());
          var_c_len = KUint16(var_addr_with_len);
          if (cols_info_include_dropped_[segment_column].type == DATATYPE::VARSTRING) {
            var_c_len -= 1;
          }
        } else {
          var_c_len = payload->GetVarColumnLen(payload_start_row, payload_column_idx);
          var_addr_with_len = payload->GetVarColumnAddr(payload_start_row, payload_column_idx);
        }
      } else {
        need_convert = true;
        data = ConvertToVarLenBeforePush(payload, (DATATYPE) payload_column_info.type,
                                         (DATATYPE) cols_info_include_dropped_[segment_column].type,
                                         payload_start_row, payload_column_idx, has_merge_value, merge_value.value);
        var_c_len = KUint16(data.get());
      }
      // insert var string to string_file. and get string file location.
      {
        if (var_c_len + m_str_file_->size() >= m_str_file_->fileLen()) {
          // The default size of the string_file file is 4MB, and it automatically remaps and expands based on usage,
          // doubling within 1GB and expanding in increments of 50% if it exceeds 1GB.
          if (m_str_file_->fileLen() > 1024 * 1024 * 1024) {
            size_t new_size = m_str_file_->size() / 2;
            err_code = m_str_file_->reserve(m_str_file_->size(), 3 * new_size, 2);
          } else {
            err_code = m_str_file_->reserve(m_str_file_->size(), 2 * m_str_file_->fileLen(), 2);
          }
        }
        // When writing data, variable-length data is first written to the string_file file to obtain the offset,
        // and then written to the data block in the data column using memcpy.

        if (!need_convert) {
          if (cols_info_include_dropped_[segment_column].type == DATATYPE::VARSTRING) {
            loc = m_str_file_->push_back(
                (void*) (var_addr_with_len + MMapStringColumn::kStringLenLen), var_c_len);
          } else {
            loc = m_str_file_->push_back_binary(
                (void*) (var_addr_with_len + MMapStringColumn::kStringLenLen), var_c_len);
          }
        } else {
          if (cols_info_include_dropped_[segment_column].type == DATATYPE::VARSTRING) {
            loc = m_str_file_->push_back(
                (void*) ((char*)data.get() + MMapStringColumn::kStringLenLen), var_c_len);
          } else {
            loc = m_str_file_->push_back_binary(
                (void*) ((char*)data.get() + MMapStringColumn::kStringLenLen), var_c_len);
          }
        }

        if (loc == 0 || err_code < 0) {
          LOG_ERROR("StringFile reserve failed.");
          return err_code;
        }
      }
    }
    // set record tuple column addr with string file location.
    memcpy(columnAddr(row_id, segment_column), &loc, cols_info_include_dropped_[segment_column].size);
    row_id.offset_row++;
    payload_start_row++;
  }
  mutexUnlock();
  return err_code;
}

void MMapSegmentTable::push_back_null_bitmap(kwdbts::Payload* payload, MetricRowID row_id, size_t segment_column,
                                             size_t payload_column, size_t payload_start_row, size_t payload_num) {
  for (int i = 0 ; i < payload_num ; i++) {
    // If the payload is empty, set the bitmap
    if (payload->IsNull(payload_column, payload_start_row)) {
      setNullBitmap(row_id, segment_column);
    }
    payload_start_row++;
    row_id.offset_row++;
  }
}

void MMapSegmentTable::sync(int flags) {
  if (is_compressed_ || meta_data_ == nullptr) {
    return;
  }
  meta_data_->num_node = actual_writed_count_.load();
  bt_file_.sync(flags);
  for (size_t i = 0 ; i < col_files_.size() ; ++i) {
    if (cols_info_include_dropped_[i].isFlag(AINFO_DROPPED)) {
      continue;
    }
    if (col_files_[i]) {
      col_files_[i]->sync(flags);
    }
  }
  if (m_str_file_ != nullptr) {
    m_str_file_->sync(flags);
  }
}

int MMapSegmentTable::remove() {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });

  ErrorInfo err_info;

  status_ = OBJ_INVALID;
  if (m_str_file_) {
    m_str_file_->remove();
    delete m_str_file_;
    m_str_file_ = nullptr;
  }

  for (auto it = col_files_.begin() ; it != col_files_.end() ; ++it) {
    auto& file = *it;
    if (LIKELY(file != nullptr)) {
      file->remove();
      delete file;
      file = nullptr;
    }
  }

  TsTableObject::close();
  string sqfs_file_path = getCompressedFilePath();
  if (is_compressed_) {
    // try umount sqfs file
    umount(db_path_, tbl_sub_path_, err_info);
    // try remove sqfs file
    Remove(sqfs_file_path);
  }
  Remove(db_path_ + tbl_sub_path_);
  return err_info.errcode;
}

int MMapSegmentTable::try_umount() {
  mutexLock();
  Defer defer([&](){
    mutexUnlock();
  });

  ErrorInfo err_info;

  status_ = OBJ_INVALID;
  if (m_str_file_) {
    m_str_file_->munmap();
    delete m_str_file_;
    m_str_file_ = nullptr;
  }

  for (auto it = col_files_.begin() ; it != col_files_.end() ; ++it) {
    auto& file = *it;
    if (LIKELY(file != nullptr)) {
      file->munmap();
      delete file;
      file = nullptr;
    }
  }

  TsTableObject::close();
  // try umount sqfs file
  umount(db_path_, tbl_sub_path_, err_info);
  return err_info.errcode;
}

int MMapSegmentTable::addColumn(AttributeInfo& col_info,
                                ErrorInfo& err_info) {
  return err_info.setError(KWEPERM, "cannot support to add column " + quoteString(name()));
}

uint64_t MMapSegmentTable::dataLength() const {
  return 0;
}

string MMapSegmentTable::GetPath() {
  return db_path_ + tbl_sub_path_;
}

string MMapSegmentTable::path() const {
  return filePath();
}

const vector<AttributeInfo>& MMapSegmentTable::getSchemaInfo() const {
  return cols_info_include_dropped_;
}

int convertStrToFixed(const std::string& str, DATATYPE new_type, char* data, int32_t old_len, ErrorInfo& err_info) {
  std::size_t pos{};
  int res32 = 0;
  long long res64{};
  float res_f;
  double res_d;
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
      }
      else {
        break;
      }
    }
    else {
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
    }
    else {
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
    }
    else {
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
      k_int16 buffer_len = char_len + MMapStringColumn::kStringLenLen;
      var_data = static_cast<char*>(std::malloc(buffer_len));
      memset(var_data, 0, buffer_len);
      KInt16(var_data) = static_cast<k_int16>(char_len);
      memcpy(var_data + MMapStringColumn::kStringLenLen, data, strlen(data));
      break;
    }
    default:
      err_info.setError(KWEPERM, "Incorrect integer value");
      break;
  }
  if (old_type == DATATYPE::INT16 || old_type == DATATYPE::INT32 || old_type == DATATYPE::INT64 ||
      old_type == DATATYPE::FLOAT || old_type == DATATYPE::DOUBLE) {
    auto act_len = res.size() + 1;
    var_data = static_cast<char*>(std::malloc(act_len + MMapStringColumn::kStringLenLen));
    memset(var_data, 0, act_len + MMapStringColumn::kStringLenLen);
    KUint16(var_data) = act_len;
    strcpy(var_data + MMapStringColumn::kStringLenLen, res.data());
  }
  std::shared_ptr<void> ptr(var_data, free);
  return ptr;
}
