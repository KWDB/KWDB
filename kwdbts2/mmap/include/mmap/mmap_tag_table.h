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
#include "mmap_object.h"
#include "mmap_hash_index.h"
#include "mmap_ntag_hash_index.h"
#include "ts_common.h"
#include "lg_api.h"
#include "payload.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"
#include "cm_func.h"
#include "mmap_tag_column_table.h"
#include "mmap_tag_version_manager.h"
#include "mmap_tag_column_table_aux.h"

extern uint32_t k_entity_group_id_size;
extern uint32_t k_per_null_bitmap_size;

using TagHashIndex = MMapHashIndex;
using NTagHashINdex = MMapNTagHashIndex;
using TableVersion = uint32_t;
using TagPartitionTable = MMapTagColumnTable;

#define INVALID_COL_IDX UINT32_MAX

class TagPartitionTableManager;

class TagTable {
 private:
  KLatch*   m_mutex_;
  // for create/drop/undoCreate/undoDrop index and alter tag.
  KLatch*   m_version_mutex_;

  int mutexLock() {return MUTEX_LOCK(m_mutex_);}

  int mutexUnlock() {return MUTEX_UNLOCK(m_mutex_);}

  int versionMutexLock() {return MUTEX_LOCK(m_version_mutex_);}

  int versionMutexUnlock() {return MUTEX_UNLOCK(m_version_mutex_);}

 protected:
  TagTableVersionManager*        m_version_mgr_{nullptr};
  TagHashIndex*                  m_index_{nullptr};
  MMapEntityRowHashIndex*        m_entity_row_index_{nullptr};
  TagPartitionTableManager*      m_partition_mgr_{nullptr};
  std::string m_db_path_;
  std::string m_tbl_sub_path_;
  uint64_t m_table_id{0};
  uint32_t m_entity_group_id_{0};
  size_t   m_primary_tag_size{0};
 public:
  explicit TagTable(const std::string& db_path, const std::string& sub_path, uint64_t table_id, int32_t entity_group_id);

  virtual ~TagTable();

  int create(const vector<TagInfo> &schema, uint32_t table_version, ErrorInfo &err_info);

  int open(ErrorInfo &err_info);

  int remove(ErrorInfo &err_info);
  // check ptag exist & get entity id
  bool hasPrimaryKey(const char* primary_tag_val, int len, uint32_t& entity_id, uint32_t& group_id);

  // check ptag exist
  bool hasPrimaryKey(const char* primary_tag_val, int len);

  // insert tag record
  int InsertTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id);

  // update tag record
  int UpdateTagRecord(kwdbts::Payload &payload, int32_t sub_group_id, int32_t entity_id, ErrorInfo& err_info);

  /**
  * @brief Query tag through the index of the primary tag and normal tag.
  * @param primary_tags key value of the primary tag, which is queried by the index of the ptag.
  * @param tags_index_id normal tag Indicates the id of the index to be queried.
  * @param tags normal tag key value to be queried.
  * @param op_type Operators are used for multiple common tag index queries or (ptag or ntag) index queries.
  * @param scan_tags tag columns to be queried.
  * @param entity_id_list EntityResultIndex.
  * @param res tag column data.
  * @param count the number of rows queried.
  * @param table_version version number of the table.
  * @note Support ptag and ntag index query:When primary_tag is not empty, support ptag index query;
  * When tags_index_id is not empty, ntag index query is supported; Support only ptag and ntag index or operation query.
  */
  int GetEntityIdList(const std::vector<void*>& primary_tags, const std::vector<uint64_t/*index_id*/> &tags_index_id,
                      const std::vector<void*> tags, TSTagOpType op_type, const std::vector<uint32_t> &scan_tags,
                      std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                      kwdbts::ResultSet* res, uint32_t* count, uint32_t table_version = 0);

  // query tag by entityid
  int GetTagList(kwdbContext_p ctx, const std::vector<EntityResultIndex>& entity_id_list,
             const std::vector<uint32_t>& scan_tags, ResultSet* res, uint32_t* count, uint32_t table_version);
  // query tag by rownum
  int GetColumnsByRownumLocked(TableVersion src_table_version, uint32_t src_row_id,
                              const std::vector<uint32_t>& src_tag_idxes,
                              const std::vector<TagInfo>& result_tag_infos,
                              kwdbts::ResultSet* res);

  int CalculateSchemaIdxs(TableVersion src_table_version, const std::vector<uint32_t>& result_scan_idxs,
                                   const std::vector<TagInfo>& result_tag_infos,
                                  std::vector<uint32_t>* src_scan_idxs);

  // delete tag record by ptag
  int DeleteTagRecord(const char *primary_tags, int len, ErrorInfo& err_info);

  int AlterTableTag(AlterType alter_type, const AttributeInfo& attr_info,
                    uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

  std::vector<uint32_t> GetNTagIndexInfo(uint32_t ts_version, uint32_t index_id);

  std::vector<std::pair<uint32_t, std::vector<uint32_t>>> GetAllNTagIndexs(uint32_t ts_version);

  inline TagTableVersionManager* GetTagTableVersionManager() {return m_version_mgr_;}

  inline TagPartitionTableManager* GetTagPartitionTableManager() {return m_partition_mgr_;}

  // wal
  void sync_with_lsn(kwdbts::TS_LSN lsn);

  void sync(int flag);

  TagTuplePack* GenTagPack(const char* primarytag, int len);

  int UndoAlterTagTable(uint32_t cur_version, uint32_t new_version, ErrorInfo& err_info);

  int InsertForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag);
  int InsertForRedo(uint32_t group_id, uint32_t entity_id,
		    kwdbts::Payload &payload);
  int DeleteForUndo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, const TSSlice& tag_pack);

  int DeleteForRedo(uint32_t group_id, uint32_t entity_id,
		    const TSSlice& primary_tag, TSSlice& tags);
  int UpdateForRedo(uint32_t group_id, uint32_t entity_id,
                    const TSSlice& primary_tag, kwdbts::Payload &payload);
  int UpdateForUndo(uint32_t group_id, uint32_t entity_id, const TSSlice& primary_tag,
                    const TSSlice& old_tag);

  int UndoCreateHashIndex(uint32_t index_id, uint32_t cur_ts_version, uint32_t new_ts_version, ErrorInfo& err_info);

  int UndoDropHashIndex(const std::vector<uint32_t> &tags, uint32_t index_id, uint32_t cur_ts_version,
                        uint32_t new_ts_version, ErrorInfo& err_info);

  enum class HashIndex : int { Create = 1, Drop = 2, None = 0 };

  int addNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info,
                             const std::vector<uint32_t> &tags = {}, uint32_t index_id = 0,
                             HashIndex idx_flag = HashIndex::None);

  /**
* @brief Building a database version directory (with the designated version number) that includes the indexs, utilizing
*        the provided index metadata.
* @param schema the table schema corresponding the new_version.
* @param new_version the version num need to create.
* @param idx_info the table index info corresponding the new_version.
*/
  int AddNewPartitionVersion(const vector<TagInfo> &schema, uint32_t new_version, ErrorInfo &err_info,
                                       const std::vector<roachpb::NTagIndexInfo>& idx_info);

  int cleanPartition(uint32_t version, const uint32_t drop_index_id, ErrorInfo &err_info);

  int cleanPartition(uint32_t version, const std::vector<roachpb::NTagIndexInfo> ntagidxinfo, ErrorInfo &err_info);

  int createHashIndex(uint32_t new_version, ErrorInfo &err_info, const std::vector<uint32_t> &tags,
                      uint32_t index_id);

  int dropHashIndex(uint32_t new_version, ErrorInfo &err_info, uint32_t index_id);

  int CreateHashIndex(int flags, const std::vector<uint32_t> &tags, uint32_t index_id, const uint32_t cur_version,
                      const uint32_t new_version, ErrorInfo& err_info);

  int DropHashIndex(uint32_t index_id,  const uint32_t cur_version, const uint32_t new_version, ErrorInfo& err_info);

 private:

  int initHashIndex(int flags, ErrorInfo& err_info);

  int UpdateHashIndex(uint32_t index_id, const std::vector<uint32_t> &tags, uint32_t ts_version, ErrorInfo& err_info);

  // Get the column value of the tag using RowID.
  int getDataWithRowID(TagPartitionTable* tag_partition, std::pair<TableVersionID, TagPartitionTableRowID> ret,
                                 std::vector<kwdbts::EntityResultIndex>* entity_id_list,
                                 kwdbts::ResultSet* res, std::vector<uint32_t> &scan_tags, std::vector<uint32_t> &valid_scan_tags,
                                 TagVersionObject* tag_version_obj, uint32_t scan_tags_num, bool get_partition);

  // Query RowID using primary tag indexes.
  int getRowIDByPTag(const std::vector<void*>& primary_tags,
                     std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &ptag_value);

  // Query RowID using normal tag indexes.
  int getRowIDByNTag(const std::vector<uint64_t> &tags_index_id, std::vector<void*> tags,
                     TSTagOpType op_type, TagPartitionTable* tag_part_table,
                     std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result_val);

  // Get the intersection of results.
  int getIntersectionValue(uint32_t value_size, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> value[],
                           std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result);

  // Computes a union of result sets.
  int getUnionValue(uint32_t value_size, std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> value[],
                    std::vector<std::pair<TableVersionID, TagPartitionTableRowID>> &result);

  int initPrevEntityRowData(ErrorInfo& err_info);

  int initEntityRowHashIndex(int flags, ErrorInfo& err_info);

  int loadAllVersions(std::vector<TableVersion>& all_versions, ErrorInfo& err_info);

  static const uint32_t max_rows_per_res = 200000;
};

class TagPartitionTableManager {
private:
  // Read/write lock used to control concurrent access.
  KRWLatch rw_latch_;
  int rdLock() {return RW_LATCH_S_LOCK(&rw_latch_);}
  int wrLock() {return RW_LATCH_X_LOCK(&rw_latch_);}
  int unLock() {return RW_LATCH_UNLOCK(&rw_latch_);}
protected:
  // tag partition table name prefix
  std::string m_table_prefix_name_;
  // Storage path of the database.
  string m_db_path_;
  // Table of subpaths.
  string m_tbl_sub_path_;
  // ID of the table.
  uint32_t m_table_id_;
  // ID of entitygroup
  uint32_t m_entity_group_id_{0};
  // Primary tag size
  size_t   m_primary_tag_size_{0};
  // Stores ordered mappings of all versions of the version table.
  std::map<uint32_t, TagPartitionTable*> m_partition_tables_;

public:
  explicit TagPartitionTableManager (const std::string& db_path, const std::string& tbl_sub_path, uint32_t table_id, uint32_t entity_group_id) :
      m_db_path_(db_path), m_tbl_sub_path_(tbl_sub_path), m_table_id_(table_id),m_entity_group_id_(entity_group_id),
      rw_latch_(RWLATCH_ID_TAG_TABLE_PARTITION_MGR_RWLOCK) {
        m_table_prefix_name_ = std::to_string(table_id) + ".tag" + ".pt";
      }

  virtual ~TagPartitionTableManager();

  int Init(ErrorInfo& err_info);

  int CreateTagPartitionTable(const std::vector<TagInfo>& schema, uint32_t ts_version,
                          ErrorInfo& err_info, uint32_t old_part_file_version = 0);

  int OpenTagPartitionTable(TableVersion table_version, ErrorInfo& err_info);
                          
  TagPartitionTable* GetPartitionTable(uint32_t table_version);

  int RemoveAll(ErrorInfo& err_info);

  inline size_t getPrimarySize() {return m_primary_tag_size_;}

  void GetAllPartitionTablesLessVersion(std::vector<TagPartitionTable*>& tag_part_tables, TableVersion max_table_version);

  void GetAllPartitionTables(std::vector<std::pair<uint32_t, TagPartitionTable*>>& tag_part_tables);

  int RollbackPartitionTableVersion(TableVersion need_rollback_version, ErrorInfo& err_info);
};

TagTable* OpenTagTable(const std::string& db_path, const std::string &dir_path,
                                uint64_t table_id,  int32_t entity_group_id, ErrorInfo &err_info);

TagTable* CreateTagTable(const std::vector<TagInfo> &tag_schema,
                                   const std::string& db_path, const std::string &dir_path,
                                   uint64_t table_id, int32_t entity_group_id,
                                   uint32_t table_version, ErrorInfo &err_info);

int DropTagTable(TagTable* bt, ErrorInfo& err_info);

