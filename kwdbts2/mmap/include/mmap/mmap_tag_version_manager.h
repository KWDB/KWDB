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

#pragma once
#include <string>
#include <vector>
#include "utils/date_time_util.h"
#include "big_table.h"
#include "mmap_hash_index.h"
#include "ts_common.h"
#include "lg_api.h"
#include "payload.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"
#include "cm_func.h"
#include "mmap_tag_column_table.h"

using TableVersion = uint32_t;
class TagVersionObject;
struct  TagVersionMetaData;

class TagTableVersionManager {

protected:
  // Name of the table.
  string m_name_;
  // Storage path of the database.
  string m_db_path_;
  // Table of subpaths.
  string m_tbl_sub_path_;
  // ID of the table.
  uint32_t m_table_id_;
  TableVersion m_cur_table_version_{0};
  TableVersion m_newest_table_version_{0};
  // Stores mappings of all versions of the version table.
  std::unordered_map<uint32_t, TagVersionObject*> m_version_tables_;

private:
  // Read/write lock used to control concurrent access.
  KRWLatch rw_latch_;
  int rdLock() {return RW_LATCH_S_LOCK(&rw_latch_);}
  int wrLock() {return RW_LATCH_X_LOCK(&rw_latch_);}
  int unLock() {return RW_LATCH_UNLOCK(&rw_latch_);}

public:
  explicit TagTableVersionManager (const std::string& db_path, const std::string& tbl_sub_path, uint32_t table_id) :
      m_name_(to_string(table_id)), m_db_path_(db_path), m_tbl_sub_path_(tbl_sub_path), m_table_id_(table_id),
      m_cur_table_version_(0), rw_latch_(RWLATCH_ID_TAG_TABLE_VERSION_MGR_RWLOCK) {}

  virtual ~TagTableVersionManager();

  int Init(ErrorInfo& err_info);

  TagVersionObject* CreateTagVersionObject(const std::vector<TagInfo>& schema, uint32_t ts_version,
                          ErrorInfo& err_info);
                          
  TagVersionObject* GetVersionObject(uint32_t table_version);

  int UpdataTagTableVersionManager();

  int RemoveAll(ErrorInfo& err_info);

  TagVersionObject* OpenTagVersionObject(TableVersion table_version, ErrorInfo& err_info);

  inline TableVersion GetCurrentTableVersion() const {return m_cur_table_version_;}

  inline void SyncCurrentTableVersion() {
    if (m_cur_table_version_ < m_newest_table_version_) {
      LOG_INFO("Update table: %s cur version: %u to %u",
            m_name_.c_str(), m_cur_table_version_, m_newest_table_version_);
      m_cur_table_version_ = m_newest_table_version_;
    }
  }

  inline void UpdateNewestTableVersion(TableVersion new_version, bool is_update = true) {
    if (is_update) {
      if (m_newest_table_version_ < new_version) {
        LOG_INFO("Update table: %s newest version: %u to %u",
              m_name_.c_str(), m_newest_table_version_, new_version);
        m_newest_table_version_ = new_version;
      }
    } else {
      if (m_newest_table_version_ > new_version) {
        LOG_INFO("Rollback table: %s newest version: %u to %u",
              m_name_.c_str(), m_newest_table_version_, new_version);
        m_newest_table_version_ = new_version;
      }
    }
  }

  inline TableVersion GetNewestTableVersion() const {
    return m_newest_table_version_;
  }
  int SyncFromMetricsTableVersion(uint32_t cur_version, uint32_t new_version);

  int RollbackTableVersion(uint32_t need_rollback_version, ErrorInfo& err_info);

 private:
  TagVersionObject* CloneTagVersionObject(const TagVersionObject* src_ver_obj, uint32_t new_version, ErrorInfo& err_info);
};

 struct  TagVersionMetaData {
  TableVersion  m_version_;
  uint32_t      m_column_count_;
  TableVersion  m_real_used_version_;
  int           m_status_;
 };

enum TAG_STATUS {
  TAG_STATUS_INVALID = -1,
  TAG_STATUS_CREATED,
  TAG_STATUS_READY,
};

class TagVersionObject {
 protected:
  TagVersionMetaData* m_meta_data_;
  string m_file_name_;
  string m_db_path_;  // file path
  string m_tbl_sub_path_;
  std::vector<TagInfo>  m_all_schemas_;
  std::vector<TagInfo>  m_schema_info_exclude_dropped_;
  std::vector<uint32_t> m_valid_schema_idxs_;
  TableVersion  m_table_version_;
  uint32_t      m_table_id_;
  MMapFile*  m_data_file_{nullptr};

 public:
  explicit TagVersionObject(const std::string& db_path, const std::string& tbl_sub_path, uint32_t table_id,
                            TableVersion table_version)
      : m_db_path_(db_path), m_tbl_sub_path_(tbl_sub_path), m_table_version_(table_version), m_table_id_(table_id) {
    m_file_name_ = std::to_string(m_table_id_) + ".tag" + ".mt" + "_" + std::to_string(m_table_version_);
  }

  virtual ~TagVersionObject();
 
  int create(const std::vector<TagInfo> &schema, ErrorInfo &err_info);

  int open(int flags, ErrorInfo &err_info);

  int remove();
  
  const std::vector<TagInfo>& getIncludeDroppedSchemaInfos() const {return m_all_schemas_;}

  const std::vector<TagInfo>& getExcludeDroppedSchemaInfos() const {return m_schema_info_exclude_dropped_;}

  inline const std::vector<uint32_t>& getValidSchemaIdxs() const {return m_valid_schema_idxs_;}

  inline TagVersionMetaData* metaData() const {return m_meta_data_;}

  int getTagColumnIndex(const TagInfo& tag_schema);

  inline void setStatus(int status) {m_meta_data_->m_status_ = status;}

  TableVersion getTableVersion() {return m_table_version_;}

  inline bool isValid() const {return m_meta_data_->m_status_ == TAG_STATUS_READY;}

  inline std::string& name() { return m_file_name_;}

  inline std::string& sub_path() { return m_tbl_sub_path_;}
 private:

 static const uint32_t start_offset = 1024;  // 1K

 int open_file(const std::string& table_name, const std::string &db_path, const std::string &tbl_sub_path,
           int flags, ErrorInfo &err_info);

 inline size_t metaDataSize() {return sizeof(TagVersionMetaData);}

 inline void setMetaData() {
   m_meta_data_ = reinterpret_cast<TagVersionMetaData*>(m_data_file_->memAddr());
 }

inline void* startAddr() {
  return reinterpret_cast<void *>((intptr_t)m_data_file_->memAddr() + TagVersionObject::start_offset);
}

int writeTagInfo(const std::vector<TagInfo>& tag_schemas);

void readTagInfo();
};
