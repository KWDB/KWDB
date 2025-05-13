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

#include "mmap/mmap_tag_version_manager.h"

int TagTableVersionManager::Init(ErrorInfo& err_info) {
  return SUCCESS;
}

TagTableVersionManager::~TagTableVersionManager() {
  wrLock();
  for (auto& it : m_version_tables_) {
    if(it.second) {
      delete it.second;
      it.second = nullptr;
    }
  }
  m_version_tables_.clear();
  unLock();
}

TagVersionObject* TagTableVersionManager::CreateTagVersionObject(const std::vector<TagInfo>& schema, uint32_t ts_version,
                          ErrorInfo& err_info) {
  wrLock();
  auto version_obj = m_version_tables_.find(ts_version);
  if (version_obj != m_version_tables_.end()) {
     unLock();
     LOG_WARN("tag table version [%u] already exist.", ts_version);
     // err_info.errmsg = "tag table version: " + std::to_string(ts_version) + " already exist.";
     // if already exist,mean error
     return version_obj->second;
  }
  TagVersionObject* tmp_obj = KNEW TagVersionObject(m_db_path_, m_tbl_sub_path_, m_table_id_, ts_version);
  if (tmp_obj && !tmp_obj->create(schema, err_info)) {
    tmp_obj->setStatus(TAG_STATUS_CREATED);
    m_version_tables_.insert({ts_version, tmp_obj});
    unLock();
    return tmp_obj;
  }
  unLock();
  if (tmp_obj) {
    // failed && rollback
    tmp_obj->remove();
    delete tmp_obj;
  }

  LOG_ERROR("create tag metadata version[%u] failed, %s ",ts_version, err_info.errmsg.c_str());
  return nullptr;
}

TagVersionObject* TagTableVersionManager::OpenTagVersionObject(TableVersion table_version, ErrorInfo& err_info) {
  wrLock();
  auto version_obj = m_version_tables_.find(table_version);
  if (version_obj != m_version_tables_.end()) {
     unLock();
     LOG_WARN("tag table version [%u] already exist.", table_version);
     return version_obj->second;
  }
  TagVersionObject* tmp_obj = KNEW TagVersionObject(m_db_path_, m_tbl_sub_path_, m_table_id_, table_version);
  if (tmp_obj && !tmp_obj->open(MMAP_OPEN_NORECURSIVE, err_info)) {
    // status from meta data,cannot set status
    m_version_tables_.insert({table_version, tmp_obj});
    unLock();
    return tmp_obj;
  }
  unLock();
  LOG_ERROR("open TagVersionObject failed, %s ", (tmp_obj == nullptr) ? "out of memory" : err_info.errmsg.c_str());
  delete tmp_obj;
  return nullptr;
}
                          
TagVersionObject* TagTableVersionManager::GetVersionObject(uint32_t table_version) {
  rdLock();
  auto version_obj = m_version_tables_.find(table_version);
  if (version_obj != m_version_tables_.end()) {
     unLock();
     return version_obj->second;
  }
  unLock();
  LOG_ERROR("Get Tag MetaData Object failed. table_version: %u ", table_version);
  return nullptr;
}

int TagTableVersionManager::RemoveAll(ErrorInfo& err_info) {
  wrLock();
  // remove all objects
  for (auto& it : m_version_tables_) {
    if(it.second) {
      it.second->remove();
      delete it.second;
      it.second = nullptr;
    }
  }
  m_version_tables_.clear();
  unLock();
  return 0;
}

TagVersionObject* TagTableVersionManager::CloneTagVersionObject(const TagVersionObject* src_ver_obj, uint32_t new_version,
                          ErrorInfo& err_info) {
  wrLock();
  auto version_obj = m_version_tables_.find(new_version);
  if (version_obj != m_version_tables_.end()) {
     unLock();
     LOG_WARN("tag table version [%u] already exist.", new_version);
     return 0;
  }
  TagVersionObject* tmp_obj = KNEW TagVersionObject(m_db_path_, m_tbl_sub_path_, m_table_id_, new_version);
  if (tmp_obj && !tmp_obj->create(src_ver_obj->getIncludeDroppedSchemaInfos(), err_info)) {

    tmp_obj->metaData()->m_real_used_version_ = src_ver_obj->metaData()->m_real_used_version_;
    // tmp_obj->setStatus(TAG_STATUS_READY);
    m_version_tables_.insert({new_version, tmp_obj});
    unLock();
    return tmp_obj;
  }
  unLock();
  if (tmp_obj) {
    // failed && rollback
    tmp_obj->remove();
    delete tmp_obj;
  }

  LOG_ERROR("create TagVersionObject failed, %s ", (tmp_obj == nullptr) ? "out of memory" : "");
  return nullptr;
}

int TagTableVersionManager::SyncFromMetricsTableVersion(uint32_t cur_version, uint32_t new_version) {
  auto tbl_ver_obj = GetVersionObject(cur_version);
  if (nullptr == tbl_ver_obj) {
    LOG_ERROR("Get meta version object failed. cur_version[%u]", cur_version);
    return -1;
  }
  ErrorInfo err_info;
  auto tmp_new_obj = CloneTagVersionObject(tbl_ver_obj, new_version, err_info);
  if (tmp_new_obj == nullptr) {
    LOG_ERROR("CloneTagVersionObject failed. error: %s ", err_info.errmsg.c_str());
    return err_info.errcode;
  }
  tmp_new_obj->setStatus(TAG_STATUS_READY);
  UpdateNewestTableVersion(new_version);
  return 0;
}

int TagTableVersionManager::RollbackTableVersion(uint32_t need_rollback_version, ErrorInfo& err_info) {
  wrLock();
  auto version_obj = m_version_tables_.find(need_rollback_version);
  if (version_obj == m_version_tables_.end()) {
     unLock();
     LOG_WARN("tag table need rollback version [%u] does not exist.", need_rollback_version);
     return 0;
  }
  LOG_INFO("rollback remove file: %s%s ", version_obj->second->sub_path().c_str(),version_obj->second->name().c_str());
  version_obj->second->remove();
  delete version_obj->second;
  m_version_tables_.erase(version_obj);
  unLock();
  return 0;
}

TagVersionObject::~TagVersionObject() {
  if (m_data_file_) {
    delete m_data_file_;
    m_data_file_ = nullptr;
  }
}

int TagVersionObject::create(const std::vector<TagInfo> &schema, ErrorInfo &err_info) {
  // 1. create new file
  if (open_file(m_file_name_, m_db_path_, m_tbl_sub_path_, MMAP_CREAT_EXCL, err_info) < 0) {
    return err_info.errcode;
  }
  // 2. write schemas
  m_all_schemas_ = schema;
  size_t col_offset = 0;
  for (auto& tag_schema : m_all_schemas_) {
    if (tag_schema.isDropped()) {
      tag_schema.m_offset = 0;
      tag_schema.m_size = 0;
    }
    if (tag_schema.m_data_type == DATATYPE::VARSTRING ||
        tag_schema.m_data_type == DATATYPE::VARBINARY) {
      tag_schema.m_size = sizeof(intptr_t);
    }
    if (tag_schema.m_tag_type == PRIMARY_TAG && 
        tag_schema.m_data_type == DATATYPE::VARSTRING) {
      tag_schema.m_size = tag_schema.m_length;
    }
    tag_schema.m_offset = col_offset;
    col_offset += tag_schema.m_size;
  }
  if ((err_info.errcode = writeTagInfo(m_all_schemas_)) < 0) {
    return err_info.errcode;
  }
  m_meta_data_->m_column_count_ = schema.size();
  m_meta_data_->m_version_ = m_table_version_;
  m_meta_data_->m_real_used_version_ = m_table_version_;
  m_meta_data_->m_status_ = TAG_STATUS_INVALID;

  m_schema_info_exclude_dropped_.clear();
  m_valid_schema_idxs_.clear();
  for (int idx = 0; idx < m_all_schemas_.size(); ++idx) {
    if(!m_all_schemas_[idx].isDropped()) {
      m_schema_info_exclude_dropped_.push_back(m_all_schemas_[idx]);
      m_valid_schema_idxs_.push_back(idx);
    }
  }
  LOG_DEBUG("create tag metadata version success. %s/%s", m_tbl_sub_path_.c_str(), m_file_name_.c_str());
  return 0;
}

int TagVersionObject::writeTagInfo(const std::vector<TagInfo>& tag_schemas) {
  uint64_t len = tag_schemas.size() * sizeof(TagInfo);
  uint64_t new_mem_len = TagVersionObject::start_offset + len;

  if (m_data_file_->fileLen() < new_mem_len) {
    size_t new_size = getPageOffset(new_mem_len);
    int err_code = m_data_file_->resize(new_size);
    if (err_code < 0) {
      LOG_ERROR("failed to resize the file %s",
        m_data_file_->filePath().c_str());
      return err_code;
    }
    setMetaData();
  }
  TagInfo* col_attr = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(startAddr()));
  for (size_t i = 0; i < tag_schemas.size(); ++i) {
    memcpy(&(col_attr[i]), &(tag_schemas[i]), sizeof(TagInfo));
  }
  return 0;
}

void TagVersionObject::readTagInfo() {
  TagInfo* cols = reinterpret_cast<TagInfo*>(static_cast<uint8_t*>(startAddr()));
  TagInfo ainfo = {0x00};
  m_all_schemas_.clear();
  for (uint32_t idx = 0; idx < m_meta_data_->m_column_count_; ++idx) {
    memset(&ainfo, 0x00, sizeof(TagInfo));
    memcpy(&ainfo, &(cols[idx]), sizeof(TagInfo));
    m_all_schemas_.push_back(ainfo);
  }
  m_schema_info_exclude_dropped_.clear();
  m_valid_schema_idxs_.clear();
  for (int idx = 0; idx < m_all_schemas_.size(); ++idx) {
    if(!m_all_schemas_[idx].isDropped()) {
      m_schema_info_exclude_dropped_.push_back(m_all_schemas_[idx]);
      m_valid_schema_idxs_.push_back(idx);
    }
  }
}

int TagVersionObject::open_file(const std::string& table_name, const std::string &db_path, const std::string &tbl_sub_path,
           int flags, ErrorInfo &err_info) {
  // 1. open file
  m_data_file_ = new MMapFile();
  int error_code = 0;
  if (flags & O_CREAT) {
    error_code = m_data_file_->open(m_file_name_, db_path + tbl_sub_path + m_file_name_, flags, metaDataSize(), err_info);
  } else {
    error_code = m_data_file_->open(m_file_name_, db_path + tbl_sub_path + m_file_name_, flags);
  }
  if (error_code < 0) {
    err_info.setError(error_code);
    LOG_ERROR("failed to create the tag file %s%s, error: %s",
              tbl_sub_path.c_str(), m_file_name_.c_str(), err_info.errmsg.c_str());
    return error_code;
  }
  // 2. set metadata
  setMetaData();
  return 0;
}

int TagVersionObject::open(int flags, ErrorInfo &err_info) {
  // 1. open file
  if (open_file(m_file_name_, m_db_path_, m_tbl_sub_path_, flags, err_info) < 0) {
    return err_info.errcode;
  }

  // 2. read schema
  readTagInfo();
  return 0;
}

int TagVersionObject::remove() {
  if (m_data_file_) {
    m_data_file_->remove();
    delete m_data_file_;
    m_data_file_ = nullptr;
  }
  return 0;
}

int TagVersionObject::getTagColumnIndex(const TagInfo& tag_schema) {
  int col_idx = -1;
  for (int i = 0; i < m_all_schemas_.size(); ++i) {
    if ((m_all_schemas_[i].m_id == tag_schema.m_id) && (!m_all_schemas_[i].isDropped())) {
      col_idx = i;
      break;
    }
  }
  return col_idx;
}