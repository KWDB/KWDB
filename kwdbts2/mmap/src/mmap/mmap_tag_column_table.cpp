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

#include <sys/types.h>
#include <sys/mman.h>
#include <dirent.h>
#include <errno.h>
#include <cstdio>
#include <cstring>
#include <atomic>
#include <assert.h>
#include "mmap/mmap_tag_column_table.h"
#include "mmap/mmap_tag_column_table_aux.h"
#include "utils/big_table_utils.h"
#include "utils/date_time_util.h"
#include "utils/string_utils.h"
#include "sys_utils.h"
#include "data_value_handler.h"
#include "lt_rw_latch.h"
#include "cm_func.h"
#include "ts_table.h"

uint32_t k_entity_group_id_size = 8;
uint32_t k_per_null_bitmap_size = 1;

TagColumn::TagColumn(int32_t idx, const TagInfo& attr) {
  m_attr_ = attr;
  m_idx_ = idx;
  m_str_file_ = nullptr;
  m_is_primary_tag_ = false;
  m_store_offset_ = 0;
  m_store_size_ = m_attr_.m_size;
  if (m_idx_ != -1) {
     m_store_size_ += k_per_null_bitmap_size;
  }
}
TagColumn::~TagColumn() {
  if (m_str_file_) {
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  munmap();
}
int TagColumn::open(const std::string& col_file_name, const std::string &db_path,
                    const std::string &dbname, int flags) {
  int error_code;
  if ((error_code = MMapFile::open(col_file_name, db_path + dbname + col_file_name, flags)) < 0) {
    LOG_ERROR("failed to open the tag file %s%s%s, errcode: %d",
      db_path.c_str(), dbname.c_str(), col_file_name.c_str(), error_code);
    return error_code;
  }
  if (m_attr_.m_data_type == VARSTRING || m_attr_.m_data_type == VARBINARY) {
    m_str_file_ = new MMapStringColumn(LATCH_ID_TAG_STRING_FILE_MUTEX, RWLATCH_ID_TAG_STRING_FILE_RWLOCK);
    std::string col_str_file_name = col_file_name + ".s";
    if ((error_code = m_str_file_->open(col_str_file_name, db_path + dbname + col_str_file_name, flags)) < 0) {
      LOG_ERROR("failed to open the tag string file %s%s%s, errcode: %d",
        db_path.c_str(), dbname.c_str(), col_str_file_name.c_str(), error_code);
      return error_code;
    }
  }
  m_db_path_ = db_path;
  m_db_name_ = dbname;
  return 0;
}
uint32_t  TagColumn::avgeStringColumnLength(size_t n) {
  if (n != 0) {
    return static_cast<int>(m_str_file_->size() / n) + 1;
  }
  return m_attr_.m_size;
}

int TagColumn::extend(size_t old_record_count, size_t new_record_count) {
  int err_code = 0;
  if (m_is_primary_tag_) {
    return 0;
  }
  if ((err_code = mremap(sizeof(TagColumnMetaData) + new_record_count*m_store_size_)) < 0) {
    LOG_ERROR("failed to extend the tag file %s, new size is: %lu",
        file_path_.c_str(), new_record_count*m_store_size_);
    return err_code;
  }
  if (m_str_file_) {
    err_code = m_str_file_->reserve(old_record_count, new_record_count, avgeStringColumnLength(old_record_count));
  }
  return err_code;
}

int TagColumn::remove() {
  if (!mem_) {
    return 0;
  }

  setDrop();
  if (m_str_file_) {
    m_str_file_->remove();
    delete m_str_file_;
    m_str_file_ = nullptr;
  }
  MMapFile::remove();
  return 0;
}

int TagColumn::writeValue(size_t row, const char* data, uint32_t len) {
  if (m_str_file_) {
    // var tag
    size_t loc_str;
    if (m_attr_.m_data_type == DATATYPE::VARSTRING) {
      loc_str = m_str_file_->push_back_nolock(data, len);
    } else {
      // varbinary
      loc_str = m_str_file_->push_back_binary(data, len);
    }
    if (loc_str == 0) {
      LOG_ERROR("failed to write the tag file %s", file_path_.c_str());
      return -1;
    }
    memcpy(rowAddrNoNullBitmap(row), &loc_str, sizeof(uint64_t));
  } else {
    memcpy(rowAddrNoNullBitmap(row), data, m_attr_.m_size);
  }
  return 0;
}

int TagColumn::getColumnValue(size_t row,  void *data) const {
  if (m_str_file_) {
    size_t loc = *reinterpret_cast<size_t*>(rowAddrNoNullBitmap(row));
    char *rec_ptr = m_str_file_->getStringAddr(loc);
    uint16_t len = *reinterpret_cast<uint16_t*>(rec_ptr);
    memcpy(data, rec_ptr + MMapStringColumn::kStringLenLen, len);
  } else {
    memcpy(data, rowAddrNoNullBitmap(row), m_attr_.m_size);
  }
  return 0;
}

int TagColumn::rename(std::string& new_col_file_name) {
  int err_code = 0;
  MMapFile old_str_file;
  MMapFile old_col_file;
  if (m_str_file_) {
     std::string new_str_file_name = new_col_file_name + ".s";
     size_t lastSep = new_col_file_name.find_last_of('.');
     if (lastSep != new_col_file_name.npos) {
       std::string suffix = new_col_file_name.substr(lastSep + 1);
       if (suffix == "bak") {
	       new_str_file_name = new_col_file_name.substr(0, lastSep) + ".s.bak";
       }
     }
    // backup old info
    std::string new_str_file_path = m_db_path_ + m_db_name_ + new_str_file_name;
    old_str_file.copyMember(m_str_file_->strFile());
    // rename to new file name
    if ((err_code = m_str_file_->rename(new_str_file_path)) < 0) {
      LOG_ERROR("failed to rename the tag string file %s to %s .",
                old_str_file.realFilePath().c_str(), (m_db_path_ + m_db_name_ + new_str_file_name).c_str());
      //reopen old string file
      if ((err_code = m_str_file_->open(old_str_file.filePath(), m_db_path_ + m_db_name_ + old_str_file.filePath(),
                                        MMAP_OPEN)) < 0) {
        LOG_ERROR("failed to open the tag string file for rollback. %s", m_str_file_->realFilePath().c_str());
      }
      return -1;
    }
    // open new string file
    if ((err_code = m_str_file_->open(new_str_file_name, m_db_path_ + m_db_name_ + new_str_file_name, MMAP_OPEN)) < 0) {
      LOG_ERROR(" failed to open the tag string file %s after renaming", m_str_file_->realFilePath().c_str());
      goto str_file_error;
    }
  }
  // backup old col info
  old_col_file.copyMember(*this);
  // rename new col file
  if ((err_code = MMapFile::rename(m_db_path_ + m_db_name_ + new_col_file_name)) < 0) {
    LOG_ERROR("failed to rename mmap file %s to %s .",
              old_col_file.realFilePath().c_str(), (m_db_path_ + m_db_name_ + new_col_file_name).c_str());
    // reopen old col file
    if ((err_code = MMapFile::open(old_col_file.filePath(), old_col_file.realFilePath(), MMAP_OPEN)) < 0) {
    LOG_ERROR("failed to open mmap file %s after renaming failed.", realFilePath().c_str());
   }
   // rollback string file
   goto str_file_error;
  }
  this->setFlags(MMAP_OPEN);
  this->file_path_ = new_col_file_name;
  // open new col file
  if ((err_code = MMapFile::open()) < 0) {
    LOG_ERROR("failed open mmap file %s after renaming.", realFilePath().c_str());
    goto col_file_error;
  }
  return 0;

col_file_error:
  if ((err_code = MMapFile::rename(m_db_path_ + m_db_name_ + old_col_file.filePath())) < 0) {
    LOG_ERROR("mmap file rename rollback rename %s to %s failed",
              realFilePath().c_str(), old_col_file.realFilePath().c_str());
  }
  if ((err_code = MMapFile::open(old_col_file.filePath(), old_col_file.realFilePath(), MMAP_OPEN) < 0)) {
    LOG_ERROR("mmap file rename rollback open failed. %s ", realFilePath().c_str());
    return err_code;
  }
str_file_error:
 if(m_str_file_) {
  if ((err_code = m_str_file_->rename(m_db_path_ + m_db_name_ + old_str_file.filePath())) < 0) {
    LOG_ERROR("string file rename rollback rename %s to %s failed.",
              m_str_file_->realFilePath().c_str(), old_str_file.realFilePath().c_str());
  }
  if ((err_code = m_str_file_->open(old_str_file.filePath(), m_db_path_ + m_db_name_ + old_str_file.filePath(),
                                    MMAP_OPEN)) < 0) {
    LOG_ERROR("string file rename rollback open failed. %s", m_str_file_->realFilePath().c_str());
  }
 }
return -1;
}

int TagColumn::sync(int flags) {
  int err_code = 0;
  if (mem_) {
    if ((err_code = MMapFile::sync(flags)) < 0) {
      return err_code;
    }
  }
  if (m_str_file_) {
    if ((err_code = m_str_file_->sync(flags)) < 0) {
      return err_code;
    }
  }
  return err_code;
}

void TagColumn::writeNullVarOffset(size_t row) {
  if (m_str_file_) {
    // var tag
    m_str_file_->mutexLock();
    size_t loc_str = m_str_file_->size();
    m_str_file_->mutexUnlock();
    memcpy(rowAddrNoNullBitmap(row), &loc_str, sizeof(uint64_t));
  }
  return;
}

MMapTagColumnTable::MMapTagColumnTable() {
  enableWal_ = false;
  m_tag_table_mutex_ = new KLatch(LATCH_ID_TAG_TABLE_MUTEX);
  m_tag_table_rw_lock_ = new KRWLatch(RWLATCH_ID_TAG_TABLE_RWLOCK);
  m_ref_cnt_mtx_ = new TagTableCntMutex(LATCH_ID_TAG_REF_COUNT_MUTEX);
  m_ref_cnt_cv_  = new TagTableCondVal(COND_ID_TAG_REF_COUNT_COND);
  m_ntag_indexes_rw_lock_ = new NTagIndexsRWLatch(RWLATCH_ID_NTAG_INDEXES_RWLATCH);
}
MMapTagColumnTable::~MMapTagColumnTable() {
  delete m_tag_table_rw_lock_;
  delete m_tag_table_mutex_;
  delete m_ref_cnt_mtx_;
  delete m_ref_cnt_cv_;
  delete m_ntag_indexes_rw_lock_;
  m_ntag_indexes_rw_lock_ = nullptr;
  m_ref_cnt_cv_ = nullptr;
  m_ref_cnt_mtx_ = nullptr;
  m_tag_table_mutex_ = nullptr;
  m_tag_table_rw_lock_ = nullptr;
  if (!m_ptag_file_ || m_ptag_file_->memAddr() == nullptr) {
    return;
  }

  delete  m_bitmap_file_;
  delete  m_index_;
  delete  m_entity_row_index_;
  delete  m_meta_file_;
  delete m_hps_file_;

  m_bitmap_file_ = nullptr;
  m_index_ = nullptr;
  m_entity_row_index_ = nullptr;
  m_meta_file_ = nullptr;
  m_hps_file_ = nullptr;

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (m_cols_[i] != nullptr) {
      delete m_cols_[i];
      m_cols_[i] = nullptr;
    }
  }
  delete m_ptag_file_;
  m_ptag_file_ = nullptr;
}

int MMapTagColumnTable::initMetaData(ErrorInfo &err_info) {
  // check if need read backup file
  std::string file_name = m_name_ + ".mt";

  // Only do normal open, recovery do something to bring into alignment.
#if 0
  std::string real_file_path = db_path_ + tbl_sub_path_ + file_name + ".bak";
  bool is_need_recovery = (access(real_file_path.c_str(), F_OK) == 0) ? true : false;
  if (is_need_recovery) {
    file_name = m_name_ + ".mt" + ".bak";
  }
#endif

  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  ainfo.m_size = kwdbts::EngineOptions::pageSize();
  m_meta_file_ = new TagColumn(-1, ainfo);
  err_info.errcode = m_meta_file_->open(file_name, m_db_path_, m_tbl_sub_path_, m_flags_);
  if (err_info.errcode < 0) {
    err_info.setError(err_info.errcode);
    LOG_ERROR("failed to open the tag meta file %s%s, error: %s",
      m_db_name_.c_str(), file_name.c_str(), err_info.errmsg.c_str());
    delete m_meta_file_;
    m_meta_file_ = nullptr;
    return -1;
  }
  if (!m_meta_file_->isInited()) {
    if ((err_info.errcode = m_meta_file_->extend(0, 1)) < 0) {
      err_info.setError(err_info.errcode);
      LOG_ERROR("failed to extend the meta file %s%s, error: %s",
        m_db_name_.c_str(), file_name.c_str(), err_info.errmsg.c_str());
      delete m_meta_file_;
      return -1;
    }
  }
  setMetaData();

#if 0
  if (is_need_recovery) {
    std::string meta_file_name = m_name_ + ".mt";
    std::string real_file_path = db_path_ + tbl_sub_path_ + meta_file_name;
    ::remove(real_file_path.c_str());
    m_meta_file_->rename(meta_file_name);
  }
#endif

  return err_info.errcode;
}

int MMapTagColumnTable::open_(const string &table_path, const std::string &db_path,
                              const string &tbl_sub_path, int flags, ErrorInfo &err_info) {
  string file_path = getTsFilePath(table_path);
  m_ptag_file_ = new MMapFile();
  int error_code = m_ptag_file_->open(file_path, db_path + tbl_sub_path + file_path, flags);
  if (error_code < 0) {
    err_info.setError(error_code);
    LOG_ERROR("failed to open the tag file %s%s, error: %s",
              tbl_sub_path.c_str(), file_path.c_str(), err_info.errmsg.c_str());
    return error_code;
  }
  // set members
  m_db_path_ = db_path;
  m_flags_ = flags;
  m_tbl_sub_path_ = tbl_sub_path;
  m_name_ = getTsObjectName(table_path);
  // open init
  if ((error_code = initMetaData(err_info)) < 0) {
    return error_code;
  }
  uint32_t check_code = *reinterpret_cast<const uint32_t*>("MMTT");
  if (m_meta_data_->m_magic != check_code) {
    err_info.errcode = -1;
    err_info.errmsg = "tag magic check failed.";
    LOG_ERROR("failed to check megic for the tag table %s%s, expect %u, "
      "but %u in fact",
              tbl_sub_path.c_str(), m_name_.c_str(), check_code, m_meta_data_->m_magic);
    return -1;
  }
  if (m_ptag_file_->fileLen() > metaDataSize()) {
    if ((error_code = readTagInfo(err_info)) < 0) {
      err_info.setError(error_code);
      return error_code;
    }
  }

  // 3. init ntag hash index file
  if (!(flags & O_CREAT)) {
    initNTagHashIndex(err_info);
  }

  setObjectReady();
  LOG_DEBUG("open the tag table %s%s successfully", tbl_sub_path.c_str(), m_name_.c_str());
  return 0;
}

int MMapTagColumnTable::create_mmap_file(const string& path, const std::string& db_path,
                                         const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  // create file + mremap
  std::string file_name = getTsFilePath(path);
  m_name_ = getTsObjectName(path);
  m_ptag_file_ = new MMapFile();
  int error_code = m_ptag_file_->open(file_name, db_path + tbl_sub_path + file_name, flags, metaDataSize(), err_info);
  if (error_code < 0) {
    err_info.setError(error_code);
    delete m_ptag_file_;
    m_ptag_file_ = nullptr;
    LOG_ERROR("failed to create the tag file %s%s, error: %s",
              tbl_sub_path.c_str(), m_name_.c_str(), err_info.errmsg.c_str());
    return error_code;
  }
  // set members
  m_db_path_ = db_path;
  m_flags_ = flags;
  m_tbl_sub_path_ = tbl_sub_path;
  if ((error_code = initMetaData(err_info)) < 0) {
    m_ptag_file_->remove();
    delete m_ptag_file_;
    m_ptag_file_ = nullptr;
    err_info.setError(error_code);
    return error_code;
  }

  return err_info.errcode;
}

int MMapTagColumnTable::initBitMapColumn(ErrorInfo& err_info) {
  std::string bitmap_file_name = m_name_ + ".header";
  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  // ainfo.m_size =  1 + (m_cols_.size() + 7)/8;
  ainfo.m_size = 1;  // delete mark
  m_bitmap_file_ = new TagColumn(-1, ainfo);
  err_info.errcode = m_bitmap_file_->open(bitmap_file_name, m_db_path_, m_tbl_sub_path_, m_flags_);
  if (err_info.errcode < 0) {
    delete m_bitmap_file_;
    m_bitmap_file_ = nullptr;
    err_info.errmsg = "initBitMapColumn failed.";
    LOG_ERROR("failed to open the bitmap file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), bitmap_file_name.c_str(), err_info.errmsg.c_str());
  }
  return err_info.errcode;
}

int MMapTagColumnTable::initHashPointColumn(ErrorInfo& err_info) {
  string hash_file_name = m_name_ + ".hps";
  TagInfo ainfo = {0x00};
  ainfo.m_offset = 0;
  ainfo.m_size = sizeof(hashPointStorage);
  m_hps_file_ = new TagColumn(-1, ainfo);
  err_info.errcode = m_hps_file_->open(hash_file_name, m_db_path_, m_tbl_sub_path_, m_flags_);
  if (err_info.errcode < 0) {
    err_info.errmsg += "initHashPointColumn failed.";
    LOG_ERROR("failed to open the tag hash point file %s%s, error: %s",
              m_tbl_sub_path_.c_str(), hash_file_name.c_str(), err_info.errmsg.c_str())
    return err_info.errcode;
  }
  return err_info.errcode;
}

int MMapTagColumnTable::readTagInfo(ErrorInfo& err_info) {
  TagColumn* tag_col = nullptr;
  uint32_t store_offset = 0;
  m_cols_.resize(m_meta_data_->m_column_count);
  TagInfo* cols = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(m_meta_file_->startAddr()) +
                                             m_meta_data_->m_column_info_offset);
  for (int idx = 0; idx < m_meta_data_->m_column_count; ++idx) {
    TagInfo ainfo;
    memcpy(&ainfo, &(cols[idx]), sizeof(TagInfo));
    m_tag_info_include_dropped_.push_back(ainfo);
    if (ainfo.isDropped()) {
      m_cols_[idx] = nullptr;
      continue;
    }
    tag_col = new TagColumn(idx, ainfo);
    if (!tag_col) {
      LOG_ERROR("failed to new TagColumn object for the tag table %s%s",
        m_db_name_.c_str(), m_name_.c_str());
      return -1;
    }
    if (tag_col->attributeInfo().m_tag_type == GENERAL_TAG) {
      if (tag_col->open(m_name_ + "." + std::to_string(ainfo.m_id),
                                      m_db_path_, m_db_name_, m_flags_) < 0) {
	    LOG_WARN("faild to open the tag %s%s%s(%u), retry open it after recovery",
               m_db_path_.c_str(), m_db_name_.c_str(), m_name_.c_str(), ainfo.m_id);
	    delete tag_col;
	    continue;
      }
    } else if (tag_col->attributeInfo().m_tag_type == PRIMARY_TAG) {
      tag_col->setPrimaryTag(true);
      tag_col->setStoreOffset(store_offset);
      store_offset += tag_col->attributeInfo().m_size;
    }
    m_cols_[idx] = tag_col;
  }
  err_info.errcode = initBitMapColumn(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }

  if (!EngineOptions::isSingleNode()) {
    err_info.errcode = initHashPointColumn(err_info);
    if (err_info.errcode < 0) {
      return err_info.setError(err_info.errcode);
    }
  }
  return 0;
}

int MMapTagColumnTable::initColumn(const std::vector<TagInfo>& schema, ErrorInfo& err_info) {
  uint32_t rec_size = 0;
  m_cols_.resize(schema.size());
  TagColumn* tag_col = nullptr;
  int error_code;
  m_meta_data_->m_primary_tag_store_size = rec_size = k_entity_group_id_size;
  uint32_t col_offset = 0;
  uint32_t store_offset = 0;
  uint32_t actual_col_cnt = 0;
  for (size_t idx = 0; idx < schema.size(); ++idx) {
    if (schema[idx].isDropped()) {
      m_cols_[idx] = nullptr;
      continue;
    }
    if (schema[idx].m_tag_type != GENERAL_TAG && schema[idx].m_tag_type != PRIMARY_TAG) {
      LOG_ERROR("invalid tag type, expect %d or %d, but %d in fact about the "
        " tag table %s%s",
        GENERAL_TAG, PRIMARY_TAG, schema[idx].m_tag_type, m_db_name_.c_str(),
        m_name_.c_str());
      // TODO(zhuderun): add errorcode
      err_info.errmsg = "tag type is invalid";
      return -1;
    }
    tag_col = new TagColumn(idx, schema[idx]);
    if (!tag_col) {
      LOG_ERROR("faild to new TagColumn object");
      return -1;
    }
    // tag_col->attributeInfo().m_size = getDataTypeSize(tag_col->attributeInfo().m_data_type);
    if (tag_col->attributeInfo().m_data_type == VARSTRING ||
        tag_col->attributeInfo().m_data_type == DATATYPE::VARBINARY) {
      tag_col->attributeInfo().m_size = sizeof(intptr_t);
    }

    if (tag_col->attributeInfo().m_tag_type == PRIMARY_TAG) {
      tag_col->setPrimaryTag(true);
      tag_col->setStoreOffset(store_offset);
      if (tag_col->attributeInfo().m_data_type == VARSTRING) {
        tag_col->attributeInfo().m_size = tag_col->attributeInfo().m_length;
      }
      store_offset += tag_col->attributeInfo().m_size;
      m_meta_data_->m_primary_tag_size += tag_col->attributeInfo().m_size;
      m_meta_data_->m_primary_tag_store_size += tag_col->attributeInfo().m_size;
    } else if (tag_col->attributeInfo().m_tag_type == GENERAL_TAG) {
      if ((error_code = tag_col->open(m_name_ + "." + std::to_string(schema[idx].m_id),
                                      m_db_path_, m_db_name_, m_flags_)) < 0) {
        LOG_ERROR("failed to open the tag file %s, error_code: %d",
          tag_col->filePath().c_str(), error_code);
        return error_code;
      }
    }
    tag_col->attributeInfo().m_offset = col_offset;
    m_tag_info_include_dropped_[idx].m_offset = col_offset;
    m_tag_info_include_dropped_[idx].m_size = tag_col->attributeInfo().m_size;
    rec_size += tag_col->attributeInfo().m_size;
    col_offset += tag_col->attributeInfo().m_size;
    m_cols_[idx] = tag_col;
    ++actual_col_cnt;
  }
  m_meta_data_->m_record_store_size = rec_size;
  m_meta_data_->m_record_size = (rec_size - k_entity_group_id_size);
  m_meta_data_->m_header_size = 1 + ((actual_col_cnt + 7) / 8);  //  1 + [null bit map]
  m_meta_data_->m_bitmap_size = ((actual_col_cnt + 7) / 8);
  return 0;
}


int MMapTagColumnTable::writeTagInfo(uint64_t start_offset, const std::vector<TagInfo>& tag_schemas) {
  uint64_t len = tag_schemas.size() * sizeof(TagInfo);
  uint64_t new_mem_len = start_offset + len;

  if (m_meta_file_->fileLen() < new_mem_len) {
    size_t old_row_count = m_meta_file_->fileLen() / kwdbts::EngineOptions::pageSize();
    size_t new_row_count = (getPageOffset(new_mem_len)) / kwdbts::EngineOptions::pageSize();
    int err_code = m_meta_file_->extend(old_row_count, new_row_count);
    if (err_code < 0) {
      LOG_ERROR("failed to extend the meta file %s",
        m_meta_file_->filePath().c_str());
      return err_code;
    }
    setMetaData();
  }
  TagInfo* col_attr = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(m_meta_file_->startAddr()) + start_offset);
  for (size_t i = 0; i < tag_schemas.size(); ++i) {
    memcpy(&(col_attr[i]), &(tag_schemas[i]), sizeof(TagInfo));
  }
  return 0;
}

int MMapTagColumnTable::init(const vector<TagInfo>& schema, ErrorInfo& err_info) {

  // initColumn
  m_tag_info_include_dropped_ = schema;
  err_info.errcode = initColumn(schema, err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }
  // m_meta_data_->m_header_size = 1 + ((m_cols_.size() + 7) / 8);  //  1 + [null bit map]
  // m_meta_data_->m_bitmap_size = ((m_cols_.size() + 7) / 8);
  // initBitmap
  err_info.errcode = initBitMapColumn(err_info);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }

  // initHashPointFile
  if (!EngineOptions::isSingleNode()) {
    err_info.errcode = initHashPointColumn(err_info);
    if (err_info.errcode < 0) {
      return err_info.setError(err_info.errcode);
    }
  }

  m_meta_data_->m_record_store_size += m_meta_data_->m_header_size;
  m_meta_data_->m_record_size += m_meta_data_->m_bitmap_size;
  m_meta_data_->m_column_count = m_cols_.size();

  m_meta_data_->m_column_info_offset = metaDataSize();  // sizeof(TagTableMetaData);

  err_info.errcode = writeTagInfo(m_meta_data_->m_column_info_offset, m_tag_info_include_dropped_);
  if (err_info.errcode < 0) {
    return err_info.setError(err_info.errcode);
  }

  return err_info.errcode;
}

int MMapTagColumnTable::create(const vector<TagInfo>& schema, int32_t entity_group_id, uint32_t new_ts_version, ErrorInfo& err_info) {
  // 1. create mmap file
  if (init(schema, err_info) < 0) {
    LOG_ERROR("failed to init the tag table %s%s, error: %s",
      m_db_name_.c_str(), m_name_.c_str(), err_info.errmsg.c_str());
    return err_info.errcode;
  }

  m_meta_data_->m_magic = *reinterpret_cast<const uint32_t*>("MMTT");
  m_meta_data_->m_record_start_offset = m_ptag_file_->fileLen();  // one page size
  m_meta_data_->m_entitygroup_id = entity_group_id;
  m_meta_data_->m_ts_version = new_ts_version;
  setObjectReady();
  err_info.errcode = reserve(1024, err_info);  // reserve space for row#0

  LOG_INFO("create the tag table %s%s successfully",
    m_db_name_.c_str(), m_name_.c_str());
  return err_info.errcode;
}

int MMapTagColumnTable::open(const string& table_path, const std::string& db_path,
                             const string& tbl_sub_path, int flags, ErrorInfo& err_info) {
  m_db_name_ = tbl_sub_path;

  if (flags & O_CREAT) {
    return create_mmap_file(table_path, db_path, tbl_sub_path, flags, err_info);
  }
  return open_(table_path, db_path, tbl_sub_path, flags, err_info);
}

int MMapTagColumnTable::remove() {
  int err_code = 0;

  if (!m_ptag_file_ || !m_ptag_file_->memAddr()) {
    return 0;
  }

  if (m_ptag_file_->memAddr()) {
    (reinterpret_cast<TagColumnMetaData*>(m_ptag_file_->memAddr()))->m_droped = true;
  }

  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]) {
      continue;
    }
    m_cols_[i]->remove();
    delete m_cols_[i];
    m_cols_[i] = nullptr;
  }
  if (m_bitmap_file_) {
    m_bitmap_file_->remove();
    delete m_bitmap_file_;
    m_bitmap_file_ = nullptr;
  }
  if (m_meta_file_) {
    m_meta_file_->remove();
    delete m_meta_file_;
    m_meta_file_ = nullptr;
  }
  if (m_hps_file_) {
    m_hps_file_->remove();
    delete m_hps_file_;
    m_hps_file_ = nullptr;
  }
  // remove primary tags
  m_ptag_file_->remove();
  delete m_ptag_file_;
  m_ptag_file_ = nullptr;
  m_meta_data_ = nullptr;

  NtagIndexRWMutexXLock();
  m_ntag_indexes_.clear();
  NtagIndexRWMutexUnLock();

  return err_code;
}

int MMapTagColumnTable::extend(size_t new_record_count, ErrorInfo& err_info) {
  int err_code = 0;
  off_t new_mem_len = new_record_count * m_meta_data_->m_primary_tag_store_size;
  if (m_ptag_file_->fileLen() < (m_meta_data_->m_record_start_offset + new_mem_len)) {
    err_code = m_ptag_file_->mremap(m_meta_data_->m_record_start_offset + new_mem_len);
    if (err_code < 0) {
      return err_code;
    }
  }
  return err_code;
}

int MMapTagColumnTable::reserve(size_t n, ErrorInfo& err_info) {
  int err_code = 0;
  if (m_meta_data_ == nullptr) {
    LOG_ERROR("failed to get meta data for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    return -1;
  }

  setObjectStatus(OBJ_READY);
  startWrite();
  if (n < m_meta_data_->m_reserve_row_count) {
    stopWrite();
    return 0;
  }
  LOG_DEBUG("the tag table %s%s reserve new row_size: %lu ",
    m_db_name_.c_str(), m_name_.c_str(), n);
  // bitmap mremap
  err_code = m_bitmap_file_->extend(m_meta_data_->m_row_count, n);
  if (err_code < 0) {
    LOG_ERROR("failed to extend bitmap file for the tag table %s%s",
      m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    return err_code;
  }

  // hashpoint file extend
  if (m_hps_file_) {
   err_code = m_hps_file_->extend(m_hps_file_->fileLen(), n*sizeof(hashPointStorage));
  }
  // tagcolumn extend
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]) {
      continue;
    }
    err_code = m_cols_[i]->extend(m_meta_data_->m_row_count, n);
    if (err_code < 0) {
      LOG_ERROR("failed to extend column file for the tag table %s%s(%u)",
        m_db_name_.c_str(), m_name_.c_str(), m_cols_[i]->attributeInfo().m_id);
      stopWrite();
      return err_code;
    }
  }

  // primary tags extend
  err_code = this->extend(n, err_info);
  if (err_code < 0) {
      LOG_ERROR("failed to extend primary tags for the tag table %s%s",
        m_db_name_.c_str(), m_name_.c_str());
    stopWrite();
    return err_code;
  }

  m_meta_data_->m_reserve_row_count = n;
  err_info.errcode = err_code;
  stopWrite();
  return err_code;
}

int MMapTagColumnTable::linkNTagHashIndex(uint32_t table_id, MMapTagColumnTable *old_part, ErrorInfo& err_info) {
  if (old_part != nullptr) {
    old_part->startRead();
    old_part->NtagIndexRWMutexSLock();
    auto old_indexs = old_part->getMmapNTagHashIndex();
    if (!old_indexs.empty()) {
      for (auto old_index : old_indexs) {
        string new_index_file_name = std::to_string(table_id) + "_" + to_string(old_index->getIndexID()) + ".tag" + ".ht";
        string new_index_path = m_db_path_ + m_db_name_ + new_index_file_name;
        err_info.errcode = symlink(old_index->realFilePath().c_str(), new_index_path.c_str());
        if (err_info.errcode != 0) {
          LOG_ERROR("create hash index symlink failed")
          return err_info.errcode;
        }
        NtagIndexRWMutexXLock();
        m_ntag_indexes_.emplace_back(old_index);
        NtagIndexRWMutexUnLock();
      }
    }
    old_part->NtagIndexRWMutexUnLock();
    old_part->stopRead();
  }
  return 0;
}

int MMapTagColumnTable::initNTagHashIndex(ErrorInfo& err_info) {
  DIR *pDir;
  struct dirent* ptr;
  string file_path = m_db_path_ + m_db_name_;
  std::vector<string> ntag_index_files;
  if (!(pDir = opendir(file_path.c_str()))) {
    LOG_ERROR("open path [%s] failed.", file_path.c_str());
    return -1;
  }
  while ((ptr = readdir(pDir)) != nullptr) {
    if (::toString(ptr->d_name).find("_") != string::npos && ::toString(ptr->d_name).find(".ht") != string::npos) {
      ntag_index_files.emplace_back(::toString(ptr->d_name));
    }
  }
  closedir(pDir);

  // todo for range files ,read metadata.RecordSize , update key_len&RecordSize
  for (const string& index_file : ntag_index_files) {
    size_t beg = index_file.find('_');
    size_t end = index_file.find_first_of('.');
    int index_id = atoi(index_file.substr(beg + 1, end).c_str());

    MMapNTagHashIndex* index = new MMapNTagHashIndex(0, index_id, std::vector<uint32_t>{});
    err_info.errcode = index->open(index_file, m_db_path_, m_db_name_, MMAP_OPEN, err_info);
    if (err_info.errcode < 0) {
      delete index;
      index = nullptr;
      err_info.errmsg = "open Hash Index failed.";
      LOG_ERROR("failed to open the tag hash index file %s%s, error: %s",
                m_db_name_.c_str(), index_file.c_str(), err_info.errmsg.c_str())
      return err_info.errcode;
    }
    // todo update metadata and key_ley and tag col ids
    // index->updateKeyLen()
    index->updateKeyLen();
    NtagIndexRWMutexXLock();
    m_ntag_indexes_.emplace_back(index);
    NtagIndexRWMutexUnLock();
  }
  return 0;
}

int MMapTagColumnTable::insert(uint32_t entity_id, uint32_t subgroup_id, uint32_t hashpoint,
                               const char* rec, size_t* row_id) {
  size_t row_no;
  int err_code = 0;
  ErrorInfo err_info;
  *row_id = 0;

  if (!isValid()) {
    return KWENOOBJ;
  }
  mutexLock();
  // reserve 0 + last rows
  if (size() + 2 >= reserveRowCount()) {
    // if (ins_ref_cnt_ != 0)
    //   pthread_cond_wait(&obj_mtx_cv_, &obj_mutex_);
    if (ins_ref_cnt_ == 0) {
      err_code = reserve(reserveRowCount() * 2, err_info);
      if (err_code < 0) {
        mutexUnlock();
        return err_code;
      }
    } else {
      LOG_DEBUG("already resized");
    }
  }

  // ins_ref_cnt_++;
  ++m_meta_data_->m_row_count;
  row_no = m_meta_data_->m_row_count;

  startRead();
  mutexUnlock();
  *row_id = row_no;
  setDeleteMark(row_no);

  // put entity id
  push_back_entityid(row_no, entity_id, subgroup_id);
  if (!EngineOptions::isSingleNode()) {
   LOG_DEBUG("%s/%s insert set row %ld hashpoint %d",m_db_name_.c_str(), m_name_.c_str(), row_no, hashpoint);
   setHashPoint(row_no, {hashpoint});
  }
  // put tag table record
  if ((push_back(row_no, rec)) < 0) {
    setDeleteMark(row_no);
    stopRead();
    return -1;
  }

  stopRead();
  mutexLock();
  ++m_meta_data_->m_valid_row_count;
  mutexUnlock();
  return err_code;
}

int MMapTagColumnTable::push_back(size_t r, const char* data) {
  // 1. direct write into bitmap
  // memcpy(header_(r) + 1, data, m_meta_data_->m_bitmap_size);  // skip del_mark

  // 2. write data
  const char* rec = (data + m_meta_data_->m_bitmap_size);
  int err_code = 0;
  size_t payload_tag_idx = 0;
  for (size_t i = 0; i < m_cols_.size(); ++i) {
    if (!m_cols_[i]) {
      continue;
    }
    if (get_null_bitmap(reinterpret_cast<uchar*>((intptr_t) data), payload_tag_idx++)) {
      // col is NULL
      if (m_cols_[i]->isPrimaryTag()) {
        LOG_ERROR("the value to be written is null for primary tag of the tag "
          "table %s%s",
          m_db_name_.c_str(), m_name_.c_str());
        return -1;
      }
      m_cols_[i]->setNull(r);
      if (!m_cols_[i]->isPrimaryTag() && m_cols_[i]->isVarTag()) {
        m_cols_[i]->writeNullVarOffset(r);
      }
      continue;
    }
    if (!m_cols_[i]->isPrimaryTag()) {
      m_cols_[i]->setNotNull(r);
    }
    if (!m_cols_[i]->isPrimaryTag() && m_cols_[i]->isVarTag()) {
      // parse var column, relative to bitmap offset
      uint64_t tag_value_offset = *reinterpret_cast<uint64_t*>(offsetAddr(rec, m_cols_[i]->attributeInfo().m_offset));
      // get var len
      uint16_t tag_value_len = *reinterpret_cast<const uint16_t*>(data + tag_value_offset);

      err_code = m_cols_[i]->writeValue(r, (data + tag_value_offset + sizeof(tag_value_len)), tag_value_len);
      if (err_code < 0) {
        LOG_ERROR("failed to write the tag table %s%s(%u)",
          m_db_name_.c_str(), m_name_.c_str(), m_cols_[i]->attributeInfo().m_id);
        return -1;
      }
    } else {
      // fixed-len tag column
      memcpy(columnValueAddr(r, i),
             offsetAddr(rec, m_cols_[i]->attributeInfo().m_offset),
             m_cols_[i]->attributeInfo().m_size);
    }
  }  // end for
  return 0;
}

uint32_t MMapTagColumnTable::getTagColOff(int col_id) {
  for (auto & tagInfo : m_tag_info_include_dropped_) {
    if (col_id == tagInfo.m_id) {
      return m_meta_data_->m_bitmap_size + tagInfo.m_offset;
    }
  }
  LOG_ERROR("Can't found TagColOff,Col ID [%d]", col_id)
  return 0;
}

uint32_t MMapTagColumnTable::getTagColSize(int col_id) {
  for (auto & tagInfo : m_tag_info_include_dropped_) {
    if (col_id == tagInfo.m_id) {
      return tagInfo.m_size;
    }
  }
  LOG_ERROR("Can't found TagColSize,Col ID [%d]", col_id)
  return 0;
}

void MMapTagColumnTable::getEntityIdGroupId(TagTableRowID row, uint32_t& entity_id, uint32_t& group_id) {
  startRead();
  char* rec_ptr = entityIdStoreAddr(row);
  memcpy(&entity_id, rec_ptr, sizeof(uint32_t));
  memcpy(&group_id, rec_ptr + sizeof(entity_id), sizeof(uint32_t));
  stopRead();
  return ;
}

int MMapTagColumnTable::getEntityIdByRownum(size_t row, std::vector<kwdbts::EntityResultIndex>* entityIdList) {
  uint32_t entity_id;
  uint32_t subgroup_id;
  char* record_ptr;
  record_ptr = entityIdStoreAddr(row);
  memcpy(&entity_id, record_ptr, sizeof(uint32_t));
  memcpy(&subgroup_id, record_ptr + sizeof(entity_id), sizeof(uint32_t));
  LOG_DEBUG("entityid: %u, groupid: %u", entity_id, subgroup_id);
  entityIdList->emplace_back(std::move(kwdbts::EntityResultIndex(m_meta_data_->m_entitygroup_id,
                                                                 entity_id,
                                                                 subgroup_id,
                                                                 record_ptr + k_entity_group_id_size)));
  return 0;
}

void MMapTagColumnTable::getHashpointByRowNum(size_t row, uint32_t *hash_point) {
  char *hashptr = hashpoint_pos_(row);
  hashPointStorage *hps = reinterpret_cast<hashPointStorage*>(hashptr);
  *hash_point = hps->hash_point;
  // LOG_DEBUG("hashPoint: %d", *hash_point);
  return ;
}

void MMapTagColumnTable::getHashedEntityIdByRownum(size_t row, uint32_t hps,
                         std::vector<kwdbts::EntityResultIndex>* entityIdList) {
  uint32_t entity_id;
  uint32_t subgroup_id;
  char* record_ptr;
  record_ptr = entityIdStoreAddr(row);
  memcpy(&entity_id, record_ptr, sizeof(uint32_t));
  memcpy(&subgroup_id, record_ptr + sizeof(entity_id), sizeof(uint32_t));
  // LOG_DEBUG("entityid: %u, groupid: %u, hashid: %u", entity_id, subgroup_id, hps);
  entityIdList->emplace_back(std::move(kwdbts::EntityResultIndex{m_meta_data_->m_entitygroup_id,
                                                                 entity_id,
                                                                 subgroup_id, hps,
                                                                 record_ptr + k_entity_group_id_size
                                                                 }));
  return ;
}

int MMapTagColumnTable::getColumnsByRownum(size_t row, const std::vector<uint32_t>& src_scan_tags_idx,
                        const std::vector<TagInfo>& result_scan_tag_infos, kwdbts::ResultSet* res) {
  if (res == nullptr) {
    return 0;
  }
  ErrorInfo err_info;
  res->setColumnNum(src_scan_tags_idx.size());
  for (int idx = 0; idx < src_scan_tags_idx.size(); idx++) {
    if (src_scan_tags_idx[idx] == INVALID_COL_IDX) {
      Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, 1);
      res->push_back(idx, batch);
      continue;
    }
    uint32_t col_idx = src_scan_tags_idx[idx];
    Batch* batch = this->GetTagBatchRecord(row, row + 1, col_idx,
                                    result_scan_tag_infos[col_idx], err_info);
    if (err_info.errcode < 0) {
      delete batch;
      LOG_ERROR("GetTagBatchRecord failed. error: %s ", err_info.errmsg.c_str());
      return err_info.errcode;
    }
    if (UNLIKELY(batch == nullptr)) {
      LOG_WARN("GetTagBatchRecord result is nullptr, skip this col[%u]", col_idx);
      continue;
    }
    res->push_back(idx, batch);
  }
  return 0;
}

int MMapTagColumnTable::getColumnsFromAll(const std::vector<uint32_t>& src_scan_tags,
                      const std::vector<TagInfo>& result_scan_tag_infos, kwdbts::ResultSet* res,
                      std::vector<TagTableRowID>* result_tag_rows) {
  if (res == nullptr) {
    return 0;
  }
  ErrorInfo err_info;
  res->setColumnNum(src_scan_tags.size());
  for (size_t row_num = 1; row_num <= m_meta_data_->m_row_count; row_num++) {
    if (isValidRow(row_num)) {
      for (size_t idx = 0; idx < src_scan_tags.size(); idx++) {
        if (src_scan_tags[idx] == INVALID_COL_IDX) {
          Batch* batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, 1);
          res->push_back(idx, batch);
          continue;
        }
        uint32_t col_idx = src_scan_tags[idx];
        Batch* batch = this->GetTagBatchRecord(row_num, row_num + 1, col_idx,
                                               result_scan_tag_infos[idx], err_info);
        if (err_info.errcode < 0) {
          delete batch;
          LOG_ERROR("GetTagBatchRecord failed. error: %s ", err_info.errmsg.c_str());
          return err_info.errcode;
        }
        if (UNLIKELY(batch == nullptr)) {
          LOG_WARN("GetTagBatchRecord result is nullptr, skip this col[%u]", col_idx);
          continue;
        }
        res->push_back(idx, batch);
      }
      result_tag_rows->emplace_back(row_num);
    }
  }
  return 0;
}

kwdbts::Batch* MMapTagColumnTable::GetTagBatchRecordWithNoConvert(size_t start_row, size_t end_row, uint32_t col, ErrorInfo& err_info) {
  kwdbts::Batch* batch = nullptr;
  assert(end_row > start_row);
  // check tag index is valid
  if (UNLIKELY(col >= this->getSchemaInfo().size())) {
    LOG_ERROR("failed to get tag(%u) in the tag table %s%s, "
      "but the max tag index no is %lu",
      col, this->sandbox().c_str(), this->name().c_str(), this->getSchemaInfo().size());
    err_info.errcode = -1;
    return nullptr;
  }
  uint32_t data_count = (end_row - start_row);
  void* data = nullptr;
  char* var_data = nullptr;
  if (!isVarTag(col)) {
    // fixed column data
    uint32_t col_data_len = this->getColumnSize(col);
    size_t total_size = col_data_len * data_count;
    data = std::malloc(total_size);
    if (UNLIKELY(data == nullptr)) {
      LOG_ERROR("failed to allocate memory for fetching tag(%u) from the tag "
        "table %s%s, it takes about %lu bytes for %u rows starting from %lu",
        col, this->sandbox().c_str(), this->name().c_str(), total_size,
        data_count, start_row);
      err_info.errcode = -4;
      return nullptr;
    }
    memset(data, 0x00, total_size);
    std::memcpy(data, this->getColumnAddr(start_row, col), total_size);
    batch = new(std::nothrow) kwdbts::TagBatch(col_data_len, data, data_count);
    if (UNLIKELY(batch == nullptr)) {
      LOG_ERROR("failed to new TagBatch for fetching tag(%u) from the tag table "
        "%s%s, start_row: %lu end_row: %lu, row_count: %lu, actual_size: %lu",
        col, this->sandbox().c_str(), this->name().c_str(), start_row, end_row,
        this->size(), this->actual_size());
        goto error_exit;
    }
  } else {
    // var tag data
    uint32_t var_total_len = std::max((m_cols_[col]->attributeInfo().m_length / 2 + 
                                       MMapStringColumn::kStringLenLen) * data_count,
                                       k_default_block_size);
    var_data = reinterpret_cast<char*>(std::malloc(var_total_len));
    memset(var_data, 0x00, var_total_len);
    VarTagBatch* var_batch = KNEW kwdbts::VarTagBatch(var_total_len, var_data, data_count);
    if (UNLIKELY(var_batch == nullptr)) {
      LOG_ERROR("failed to new TagBatch for fetching tag(%u) from the tag table "
        "%s%s, start_row: %lu end_row: %lu, row_count: %lu, actual_size: %lu",
        col, this->sandbox().c_str(), this->name().c_str(), start_row, end_row,
        this->size(), this->actual_size());
        goto error_exit;
    }
    uint32_t row_idx = 0;
    for(size_t row = start_row; row < end_row; ++row, ++row_idx) {
      if (this->isNull(row, col)) {
        var_batch->setNull(row_idx);
      } else {
        size_t var_start_offset = getVarOffset(row, col);
        if (UNLIKELY(var_start_offset < MMapStringColumn::startLoc())) {
          LOG_WARN("invalid start offset for tag(%u) in the tag tagle %s%s, "
          "start_offset: %lu, start_row: %lu, end_row: %lu, row_count: %lu, "
          "actual_size: %lu",
          col, this->sandbox().c_str(), this->name().c_str(), var_start_offset,
          row, end_row, this->size(), this->actual_size());
          var_batch->setNull(row_idx);
          continue;
        }

        m_cols_[col]->varRdLock();
        char* var_data_ptr = m_cols_[col]->getVarValueAddrByOffset(var_start_offset);
        uint16_t var_len = *reinterpret_cast<uint16_t*>(var_data_ptr);
        var_batch->writeDataIncludeLen(row_idx, var_data_ptr, var_len + MMapStringColumn::kStringLenLen);
        m_cols_[col]->varUnLock();
      }
    }  // end for
    batch = var_batch;
  }
  return batch;
error_exit:
  if (data != nullptr) {
    free(data);
  }
  if (var_data != nullptr) {
    free(var_data);
  }
  err_info.errcode = -4;
  return nullptr;
}

kwdbts::Batch* MMapTagColumnTable::convertToFixedLen(size_t start_row, size_t end_row, uint32_t col, const TagInfo& result_schema, ErrorInfo& err_info) {
  int err_code = 0;
  TagInfo old_tag_schema = m_cols_[col]->attributeInfo();
  DATATYPE old_data_type = static_cast<DATATYPE>(old_tag_schema.m_data_type);
  DATATYPE new_data_type = static_cast<DATATYPE>(result_schema.m_data_type);

  uint32_t row_count = end_row - start_row;
  uint32_t data_len = result_schema.m_size + k_per_null_bitmap_size;
  void* data = std::malloc(data_len * row_count);
  memset(data, 0x00, data_len * row_count);

  TagBatch* batch = KNEW TagBatch(data_len, data, row_count);
  if (UNLIKELY(batch == nullptr)) {
    LOG_ERROR("malloc TagBatch failed, out of memory");
    err_info.errcode = -4;
    err_info.errmsg = "malloc TagBatch failed, out of memory";
    return nullptr;
  }
  if (old_tag_schema.m_data_type != result_schema.m_data_type) {
    if (!isVarLenType(old_data_type)) {
      // fixed-len column type to fixed-len column type
      uint32_t row_idx = 0;
      for (size_t row = start_row; row < end_row; ++row, ++row_idx) {
        if (this->isNull(row, col)) {
          batch->setNull(row_idx);
          continue;
        }
        batch->setNotNull(row_idx);
        char* old_mem = m_cols_[col]->rowAddrNoNullBitmap(row);
        if (new_data_type == DATATYPE::CHAR || new_data_type == DATATYPE::BINARY) {
          err_code = convertFixedToStr(old_data_type, (char*) old_mem, batch->getRowAddr(row_idx), err_info);
          if (err_code < 0) {
            err_info.errcode = 0;
            batch->setNull(row_idx);
          }
        } else {
          if (convertFixedToNum(old_data_type, new_data_type, old_mem, batch->getRowAddr(row_idx), err_info) < 0) {
            LOG_WARN("convert [%lu] value failed. %s", row, err_info.errmsg.c_str());
            err_info.errcode = 0;
            batch->setNull(row_idx);
          }
        }
      } // end for
    } else {
      // variable-length column to fixed-len column type
      uint32_t row_idx = 0;
      for (size_t row = start_row; row < end_row; ++row, ++row_idx) {
        if (this->isNull(row, col)) {
          batch->setNull(row_idx);
          continue;
        }
        batch->setNotNull(row_idx);
        size_t var_offset = *(reinterpret_cast<size_t *>(m_cols_[col]->rowAddrNoNullBitmap(row)));
        m_cols_[col]->varRdLock();
        char* var_data_ptr = m_cols_[col]->getVarValueAddrByOffset(var_offset);
        uint16_t var_len = *reinterpret_cast<uint16_t*>(var_data_ptr);
        if (old_data_type == DATATYPE::VARSTRING) {
          var_len -= MMapStringColumn::kEndCharacterLen;
        }
        var_data_ptr += MMapStringColumn::kStringLenLen;
        if (convertStrToFixed(var_data_ptr, new_data_type, batch->getRowAddr(row_idx), var_len, err_info) < 0) {
          LOG_WARN("convert [%lu] value failed. %s", row, err_info.errmsg.c_str());
          err_info.errcode = 0;
          batch->setNull(row_idx);
        }
        m_cols_[col]->varUnLock();
      }
    }
  } else if (old_tag_schema.m_size <= result_schema.m_size) {
    // fixed-len column type with diff len
    uint32_t row_idx = 0;
    for (size_t row = start_row; row < end_row; ++row, ++row_idx) {
      batch->writeData(row_idx, m_cols_[col]->rowAddrHasNullBitmap(row), old_tag_schema.m_size + k_per_null_bitmap_size);
    }
  } else {
    LOG_ERROR("conversion from type %s, length %u to type %s, length %u "
    "is not supported in the tag table %s%s",
    getDataTypeName(old_tag_schema.m_data_type).c_str(), old_tag_schema.m_length,
    getDataTypeName(result_schema.m_data_type).c_str(), result_schema.m_length,
    m_db_name_.c_str(), m_name_.c_str());
    delete batch;
    err_info.errcode = 0;
    return nullptr;
  }
  return batch;
}

kwdbts::Batch* MMapTagColumnTable::convertToVarLen(size_t start_row, size_t end_row, uint32_t col, const TagInfo& result_schema, ErrorInfo& err_info) {

  TagInfo old_tag_schema = m_cols_[col]->attributeInfo();
  DATATYPE old_data_type = static_cast<DATATYPE>(old_tag_schema.m_data_type);
  DATATYPE new_data_type = static_cast<DATATYPE>(result_schema.m_data_type);

  uint32_t row_count = end_row - start_row;
  uint32_t var_total_len = std::max((m_cols_[col]->attributeInfo().m_length / 2 + 
                                       MMapStringColumn::kStringLenLen) * row_count,
                                       k_default_block_size);
  char* var_data = reinterpret_cast<char*>(std::malloc(var_total_len));
  memset(var_data, 0x00, var_total_len);
  VarTagBatch* var_batch = KNEW kwdbts::VarTagBatch(var_total_len, var_data, row_count);
  if (UNLIKELY(var_batch == nullptr)) {
    LOG_ERROR("malloc TagBatch failed, out of memory");
    err_info.errcode = -4;
    err_info.errmsg = "malloc TagBatch failed, out of memory";
    return nullptr;
  }
  if (old_data_type != new_data_type) {
    if (!isVarLenType(old_data_type)) {
      // fixed-len column type to var-len column type
      bool is_digit_data;
      CONVERT_DATA_FUNC convert_data_func = getConvertFunc(old_data_type, new_data_type,
                                                     result_schema.m_length, is_digit_data, err_info);
      if (err_info.errcode < 0) {
        LOG_ERROR("conversion from type %s, length %u to type %s, length %u "
          "is not supported in the tag table %s%s",
          getDataTypeName(old_data_type).c_str(), old_tag_schema.m_length,
          getDataTypeName(new_data_type).c_str(), result_schema.m_length,
          m_db_name_.c_str(), m_name_.c_str());
        err_info.errcode = 0;
        return nullptr;
      }
      char dest_data[result_schema.m_length + MMapStringColumn::kStringLenLen + 1] = {0x00};
      uint32_t row_idx = 0;
      int data_len = 0;
      char* rec_ptr = nullptr;
      for (size_t row = start_row; row < end_row; ++row,++row_idx) {
        if (this->isNull(row, col)) {
          var_batch->setNull(row_idx);
          continue;
        }
        if (is_digit_data) {
          data_len = convert_data_func(m_cols_[col]->rowAddrNoNullBitmap(row),
                                          dest_data, result_schema.m_length);
          rec_ptr = dest_data;
        } else {
          rec_ptr = m_cols_[col]->rowAddrNoNullBitmap(row);
          data_len = old_tag_schema.m_size;
          // char convert to varchar use real size
          if (old_tag_schema.m_data_type == DATATYPE::CHAR) {
            data_len = std::min(old_tag_schema.m_size, static_cast<uint32_t>(strlen(rec_ptr)));
          }
        }
        var_batch->writeDataExcludeLen(row_idx, rec_ptr, data_len);
        memset(dest_data, 0x00, sizeof(dest_data));
      }
    } else {
      // var-len column type to var-len column type
      uint32_t row_idx = 0;
      for(size_t row = start_row; row < end_row; ++row, ++row_idx) {
        if (this->isNull(row, col)) {
          var_batch->setNull(row_idx);
        } else {
          size_t var_start_offset = getVarOffset(row, col);
          if (UNLIKELY(var_start_offset < MMapStringColumn::startLoc())) {
            LOG_WARN("invalid start offset for tag(%u) in the tag tagle %s%s, "
            "start_offset: %lu, start_row: %lu, end_row: %lu, row_count: %lu, "
            "actual_size: %lu",
            col, this->sandbox().c_str(), this->name().c_str(), var_start_offset,
            row, end_row, this->size(), this->actual_size());
            var_batch->setNull(row_idx);
            continue;
          }
  
          m_cols_[col]->varRdLock();
          char* var_data_ptr = m_cols_[col]->getVarValueAddrByOffset(var_start_offset);
          uint16_t var_len = *reinterpret_cast<uint16_t*>(var_data_ptr);
          var_batch->writeDataIncludeLen(row_idx, var_data_ptr, var_len + MMapStringColumn::kStringLenLen);
          m_cols_[col]->varUnLock();
        }
      }  // end for
    }
  } else if (old_tag_schema.m_length <= result_schema.m_length) {
    uint32_t row_idx = 0;
    for(size_t row = start_row; row < end_row; ++row, ++row_idx) {
      if (this->isNull(row, col)) {
        var_batch->setNull(row_idx);
      } else {
        size_t var_start_offset = getVarOffset(row, col);
        if (UNLIKELY(var_start_offset < MMapStringColumn::startLoc())) {
          LOG_WARN("invalid start offset for tag(%u) in the tag tagle %s%s, "
          "start_offset: %lu, start_row: %lu, end_row: %lu, row_count: %lu, "
          "actual_size: %lu",
          col, this->sandbox().c_str(), this->name().c_str(), var_start_offset,
          row, end_row, this->size(), this->actual_size());
          var_batch->setNull(row_idx);
          continue;
        }

        m_cols_[col]->varRdLock();
        char* var_data_ptr = m_cols_[col]->getVarValueAddrByOffset(var_start_offset);
        uint16_t var_len = *reinterpret_cast<uint16_t*>(var_data_ptr);
        var_batch->writeDataIncludeLen(row_idx, var_data_ptr, var_len + MMapStringColumn::kStringLenLen);
        m_cols_[col]->varUnLock();
      }
    }  // end for
  } else {
    LOG_ERROR("conversion from type %s, length %u to type %s, length %u "
    "is not supported in the tag table %s%s",
    getDataTypeName(old_tag_schema.m_data_type).c_str(), old_tag_schema.m_length,
    getDataTypeName(result_schema.m_data_type).c_str(), result_schema.m_length,
    m_db_name_.c_str(), m_name_.c_str());
    delete var_batch;
    err_info.errcode = 0;
    return nullptr;
  }

  return var_batch;
}

kwdbts::Batch* MMapTagColumnTable::GetTagBatchRecordByConverted(size_t start_row,
                                                                size_t end_row,
                                                                uint32_t col,
                                                                const TagInfo& result_schema,
                                                                ErrorInfo& err_info) {
  // 1. check type
  TagInfo old_tag_schema = m_cols_[col]->attributeInfo();
  if (!isVarLenType(result_schema.m_data_type)) {
    // result tag schema is fixed-len type
    return this->convertToFixedLen(start_row, end_row, col, result_schema, err_info);
  }
  // result tag schema is var-len type
  return this->convertToVarLen(start_row, end_row, col, result_schema, err_info);
}

kwdbts::Batch* MMapTagColumnTable::GetTagBatchRecord(size_t start_row, size_t end_row, uint32_t col, const TagInfo& result_schema, ErrorInfo& err_info) {
  Batch* batch = nullptr;
  if (!m_cols_[col]) {
    LOG_WARN("Tag table:%s idx:%u id: %u is dropped.",m_name_.c_str(), col, m_tag_info_include_dropped_[col].m_id);
    batch = new(std::nothrow) kwdbts::TagBatch(0, nullptr, end_row - start_row);
    return batch;
  }
  const TagInfo& src_tag_schema = m_cols_[col]->attributeInfo();
  if (src_tag_schema.isEqual(result_schema)) {
    batch = GetTagBatchRecordWithNoConvert(start_row, end_row, col, err_info);
  } else {
    batch = GetTagBatchRecordByConverted(start_row, end_row, col, result_schema, err_info);
  }
  return batch;
}
