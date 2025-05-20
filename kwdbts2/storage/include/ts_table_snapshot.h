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

#include <vector>
#include <string>
#include <memory>
#include <list>
#include <utility>
#include <unordered_map>
#include "libkwdbts2.h"
#include "kwdb_type.h"
#include "mmap/mmap_tag_column_table.h"
#include "ts_common.h"
#include "ts_table.h"
#include "payload_builder.h"
#include "mmap/ts_Iterator_4_sub_group.h"


namespace kwdbts {

// snapshot min rows in one payload.
const size_t snapshot_payload_rows_num = 4000;

class TsTable;

struct TsSnapshotInfo {
  uint64_t id;    // snapshot ID
  uint8_t type;   // type , 0: Build snapshot (source side read), 1: write snapshot (target side merge)
  uint64_t begin_hash;
  uint64_t end_hash;
  KwTsSpan ts_span;
  KTableKey table_id;
  std::shared_ptr<TsTable> table;
};

class TsEntityGroup;

enum TsSnapshotDataType : uint8_t {
  INVAILD_TYPE = 0,
  STORAGE_SCHEMA = 1,
  PAYLOAD_COL_BASED_DATA = 2,
  BLOCK_ITEM_BASED_DATA = 3,
};

enum TsSnapshotStatus : uint8_t {
  INVAILD_STATUS = 0,
  SENDING_USING_SCHEMA = 1,
  SENDING_TAG = 2,
  SENDING_METRIC = 3,
  SENDING_ALL_SCHEMAS = 4,
};

/*  snapshot data structure
_____________________________________________________
|   4     |       4       |         4        |   n  |
|---------|---------------|------------------|------|
|  type   | serial number |  data length     | data |
*/
struct SnapshotPayloadData {
  TsSnapshotDataType type{TsSnapshotDataType::INVAILD_TYPE};
  uint32_t sn{0};
  TSSlice data_part{nullptr, 0};
  TSSlice data{nullptr, 0};
  SnapshotPayloadData(TsSnapshotDataType t, uint32_t sn, TSSlice data) : type(t), sn(sn), data_part(data) {}

  static void DeleteData(TSSlice payload_data) {
    free(payload_data.data);
  }

  TSSlice GenData() {
    if (data.data != nullptr) {
      return data;
    }
    size_t data_len = 4 + 4 + 4 + data_part.len;
    char* data_mem = reinterpret_cast<char*>(malloc(data_len));
    memset(data_mem, 0, data_len);
    KUint32(data_mem) = type;
    KUint32(data_mem + 4) = sn;
    KUint32(data_mem + 8) = data_part.len;
    memcpy(data_mem + 12, data_part.data, data_part.len);
    data = {data_mem, data_len};
    return data;
  }

  static SnapshotPayloadData ParseData(TSSlice d) {
    assert(d.len > 12);
    assert(KUint32(d.data + 8) == d.len - 12);
    auto type = (TsSnapshotDataType)KUint32(d.data);
    uint32_t sn = KUint32(d.data + 4);
    size_t data_part_len = KUint32(d.data + 8);
    return SnapshotPayloadData(type, sn, {d.data + 12, data_part_len});
  }
};
// do not delete memory, used for std::shared_ptr
struct DoNothingDeleter {
  template<typename T>
  void operator()(T*) const {}
};

/*  data structure for data part of BLOCK_ITEM_BASED_DATA type snapshot. notice : block struct not Serialized, may work wrong in different OS machine.
|
|----------------------------------------- header -----------------------------------------------|
______________________________________________________________________________________________----___________________________________----___________________________________________ ----______________________________-----____________________________
|        4        |       n     |         4        |     4       |       4     |     4          |           4    |    4    |     4    |   4      |    n       |     n    |     n          n           n           n              4      |        n       |
|-----------------|-------------|------------------|-------------|-------------|----------------|----------------|---------|----------|----------|---------- -|-------- -|----------|----------|------------|----------|   |------------|----------------|
| primary key len | primary key |  block max row   | blkitem len | blkitem num | block1 var num | blockn var num | col num | col1 len | coln len | blockitem1 | c1 block | c2 block | cn block | blockitem2 | c1 block |   | var length | vartype values |
*/
struct SnapshotPayloadDataBlockPart {
  TSSlice primary_key;
  uint32_t block_has_max_rows_num;
  std::list<TsBlockFullData*>* res_list;
  bool del_res_list = false;

  SnapshotPayloadDataBlockPart(TSSlice pkey, uint32_t blk_rows, std::list<TsBlockFullData*>* list, bool del_list = false) :
        primary_key(pkey), block_has_max_rows_num(blk_rows), res_list(list), del_res_list(del_list) {}

  ~SnapshotPayloadDataBlockPart() {
    if (del_res_list) {
      for (auto& data : *res_list) {
        delete data;
      }
      delete res_list;
      res_list = nullptr;
    }
  }
  // generate serialized data with block part structure.
  TSSlice GenData();
  // parse data with block part structure.
  static SnapshotPayloadDataBlockPart ParseData(TSSlice data);
};

class TsTableEntitiesSnapshot {
 public:
  virtual ~TsTableEntitiesSnapshot() {
    LOG_DEBUG("~TsTableEntitiesSnapshot end. %s", Print().c_str());
  }

  virtual KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options);

  std::string Print();

 protected:
  TsSnapshotInfo snapshot_info_;
  TsSnapshotStatus status_{TsSnapshotStatus::SENDING_USING_SCHEMA};
  // current serial number.
  uint32_t cur_sn_{0};
  size_t total_rows_{0};
};

class TsSnapshotProductor : public TsTableEntitiesSnapshot {
 protected:
  struct SnapshotBlockDataInfo {
    ResultSet* res;
    uint32_t count;
    bool full_block;
    TsBlockFullData* blk_item;
  };
  struct SnapshotSubGroupContext {
    std::shared_ptr<TsEntityGroup> cur_entity_group{nullptr};
    TsSubEntityGroup* cur_sub_group{nullptr};
    TsSubGroupIteratorEntityBased* sub_grp_data_iter{nullptr};
    SnapshotBlockDataInfo entity_first_batch{nullptr, 0};
    EntityResultIndex cur_block_entity_info;
  };
  struct TagRowNum {
    TableVersion  tag_version{0};
    TagTableRowID tag_row_id{0};
  };

  class TagMapIter {
   public:
    TagMapIter(std::unordered_map<uint64_t, std::unordered_map<SubGroupID,
        std::vector<std::pair<EntityID, TagRowNum>>>>& entity_map) : entity_map_(entity_map) {
        if (!entity_map_.empty()) {
          entity_group_iter_ = entity_map_.begin();
          if (!entity_group_iter_->second.empty()) {
            subgroup_iter_ = entity_group_iter_->second.begin();
            if (!subgroup_iter_->second.empty()) {
              entity_iter_ = subgroup_iter_->second.begin();
              no_entity_ = false;
            }
          }
        }
    }

    EntityResultIndex NextEntityResultIndex() {
      if (no_entity_ || entity_group_iter_ == entity_map_.end()) return {};
      EntityResultIndex res = EntityResultIndex(entity_group_iter_->first, entity_iter_->first, subgroup_iter_->first);
      if (++entity_iter_ == subgroup_iter_->second.end()) {
        if (++subgroup_iter_ == entity_group_iter_->second.end()) {
          if (++entity_group_iter_ != entity_map_.end()) {
            subgroup_iter_ = entity_group_iter_->second.begin();
            entity_iter_ = subgroup_iter_->second.begin();
          }
        } else {
          entity_iter_ = subgroup_iter_->second.begin();
        }
      }
      return res;
    }

   private:
    bool no_entity_ = true;
    std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<std::pair<EntityID, TagRowNum>>>> entity_map_;
    std::unordered_map<uint64_t, std::unordered_map<SubGroupID,
                                 std::vector<std::pair<EntityID, TagRowNum>>>>::iterator entity_group_iter_;
    std::unordered_map<SubGroupID, std::vector<std::pair<EntityID, TagRowNum>>>::iterator subgroup_iter_;
    std::vector<std::pair<EntityID, TagRowNum>>::iterator entity_iter_;
  };

 public:
  TsSnapshotProductor() {}

  virtual ~TsSnapshotProductor();

  /**
   * @brief initialize snapshot
   *
   * @return KStatus
   */
  KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options) override;

    /**
   * @brief like iterator, get batch data from snaphost
   * @param[out] data   if data.len == 0 , means get over.
   *
   * @return KStatus
   */
  KStatus NextData(kwdbContext_p ctx, TSSlice* data);

  // gen data that can migrate by network.
  virtual KStatus SerializeData(kwdbContext_p ctx, const std::list<SnapshotBlockDataInfo>& res_list,
                                TSSlice* data, TsSnapshotDataType* type, bool is_only_tag_data = false) = 0;

 protected:
  virtual KStatus getEnoughRows(kwdbContext_p ctx, std::list<SnapshotBlockDataInfo>* res_list,
                                bool* need_change_subgroup) = 0;
  // construct tag data using payload struct.
  KStatus nextTagSerializedData(kwdbContext_p ctx, TSSlice* data, TsSnapshotDataType* type);
  // construct data using payload struct.
  KStatus nextSerializedData(kwdbContext_p ctx, TSSlice* data, TsSnapshotDataType* type);
  // generate subgroup iterator of next sub group
  KStatus nextSubGrpDataIter(kwdbContext_p ctx, bool &finished);
  // get next subgroup
  KStatus nextSubGroup(kwdbContext_p ctx, bool &finished);
  // get latest version of table schema.
  KStatus getSchemaInfo(kwdbContext_p ctx, uint32_t schema_version, TSSlice* schema);
  // get all schema versions.
  KStatus nextVersionSchema(kwdbContext_p ctx, TSSlice* schema);

 protected:
  // entity group --> sub entity group --> pair < entity_id, tag_row_id>
  std::unordered_map<uint64_t, std::unordered_map<SubGroupID, std::vector<std::pair<EntityID, TagRowNum>>>> entity_map_;
  TagMapIter* tag_map_iter_;
  std::unordered_map<uint64_t, std::unordered_map<kwdbts::SubGroupID,
                    std::vector<std::pair<kwdbts::EntityID, TagRowNum>>>>::iterator egrp_iter_{nullptr};
  std::unordered_map<kwdbts::SubGroupID,
                    std::vector<std::pair<kwdbts::EntityID, TagRowNum>>>::iterator subgrp_iter_{nullptr};
  SnapshotSubGroupContext subgrp_data_scan_ctx_;
  // used for iterator parameters.
  std::vector<Sumfunctype> scan_agg_types_;
  std::vector<k_uint32> kw_scan_cols_;
  std::vector<k_uint32> ts_scan_cols_;
  std::vector<KwTsSpan> ts_spans_;
  bool scan_over_{false};
  uint32_t using_storage_schema_version_{0};
  vector<AttributeInfo> metrics_schema_include_dropped_;
  vector<AttributeInfo> pl_metric_attribute_info_;
  std::vector<TagInfo> tag_attribute_info_exclude_dropped_;
  std::vector<TagInfo> tag_schema_include_dropped_;
  std::vector<k_uint32> result_scan_tag_idxs_;

  TableVersion    cur_tag_table_version_{0};
  std::vector<k_uint32> cur_src_scan_tag_idxs_;
  // sending all schema using below
  std::vector<uint32_t> schema_versions_;
  size_t cur_schema_version_idx_{0};

  bool send_complete_block_{false};
};

class TsSnapshotConsumer : public TsTableEntitiesSnapshot {
 public:
  TsSnapshotConsumer() {}

  virtual ~TsSnapshotConsumer();

  /**
   * @brief initialize snapshot
   *
   * @return KStatus
   */
  virtual KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options);

  KStatus WriteData(kwdbContext_p ctx, TSSlice data);

  virtual KStatus WriteMetricData(kwdbContext_p ctx, TSSlice data, TsSnapshotDataType type) = 0;

  virtual KStatus WriteCommit(kwdbContext_p ctx) = 0;

  virtual KStatus WriteRollback(kwdbContext_p ctx) = 0;

  bool IsTableExist() { return snapshot_info_.table != nullptr; }

  void SetTable(std::shared_ptr<TsTable> table) { snapshot_info_.table = table; }

  KTableKey GetTableId() { return snapshot_info_.table_id; }

 protected:
  KStatus WriteDataBlockBased(kwdbContext_p ctx, TSSlice data);

  KStatus updateTableSchema(kwdbContext_p ctx, TSSlice schema);

  KStatus beginMtrOfEntityGroup(kwdbContext_p ctx);

  // do while transaction over.
  KStatus setMtrOver(kwdbContext_p ctx, bool commit);

 protected:
  MMapMetricsTable* entity_bt_{nullptr};
  std::shared_ptr<TsEntityGroup> entity_group_{nullptr};
  // entitygroup and its mtr_id of current snapshot.
  std::unordered_map<uint64_t, uint64_t> entity_grp_mtr_id_;
  std::vector<EntityResultIndex> entities_result_idx_;
  MMapMetricsTable* version_schema_{nullptr};
  int committed_{0};
};

class TsSnapshotProductorByBlock : public TsSnapshotProductor {
 public:
  TsSnapshotProductorByBlock() {
    send_complete_block_ = true;
  }

  ~TsSnapshotProductorByBlock() override {
    if (cur_primary_key_.data != nullptr) {
      free(cur_primary_key_.data);
    }
  }

  KStatus SerializeData(kwdbContext_p ctx, const std::list<SnapshotBlockDataInfo>& res_list,
                        TSSlice* data, TsSnapshotDataType* type, bool is_only_tag_data = false) override;
  KStatus getEnoughRows(kwdbContext_p ctx, std::list<SnapshotBlockDataInfo>* res_list,
                        bool* need_change_subgroup) override;

 private:
  TSSlice getPrimaryKey(EntityResultIndex* entity_id);

 private:
  EntityResultIndex cur_primary_key_idx_{0, 0, 0};
  TSSlice cur_primary_key_{nullptr, 0};
};

class TsSnapshotProductorByPayload : public TsSnapshotProductor {
 public:
  TsSnapshotProductorByPayload() {
    send_complete_block_ = false;
  }

  ~TsSnapshotProductorByPayload() override {
    if (cur_entity_pl_builder_ != nullptr) {
      delete cur_entity_pl_builder_;
      cur_entity_pl_builder_ = nullptr;
    }
  }

  KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options) override {
    auto s = TsSnapshotProductor::Init(ctx, options);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("TsSnapshotProductor init failed.");
      return s;
    }
    cur_entity_pl_builder_ = nullptr;
    cur_pl_builder_entity_ = EntityResultIndex(0, 0, 0);
    return KStatus::SUCCESS;
  }

  KStatus SerializeData(kwdbContext_p ctx, const std::list<SnapshotBlockDataInfo>& res_list,
                        TSSlice* data, TsSnapshotDataType* type, bool is_only_tag_data = false) override;

  KStatus getEnoughRows(kwdbContext_p ctx, std::list<SnapshotBlockDataInfo>* res_list,
                        bool* need_change_subgroup) override;

 private:
  // parse data of res and put into payload.
  KStatus putResIntoPayload(PayloadBuilder& py_builder, const ResultSet &res, uint32_t count, uint32_t start_pos);
  // get payload builder using template pattern.
  PayloadBuilder* getPlBuilderTemplate(const EntityResultIndex& idx, bool is_only_tag_data = false);
  // generate payload builder with tag info filled.
  PayloadBuilder* genPayloadWithTag(const TagRowNum& tag_row);

 private:
  PayloadBuilder* cur_entity_pl_builder_{nullptr};
  EntityResultIndex cur_pl_builder_entity_;
};

class TsSnapshotConsumerByPayload : public TsSnapshotConsumer {
 public:
  ~TsSnapshotConsumerByPayload() override {
  }
  KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options) override;
  // check data struct. and  call PutData function
  KStatus WriteMetricData(kwdbContext_p ctx, TSSlice data, TsSnapshotDataType type) override;
  // transcation commit, with WAL of this snapshot commit.
  KStatus WriteCommit(kwdbContext_p ctx) override { return setMtrOver(ctx, true); }
  // transcation rollback, using WAL rollback function
  KStatus WriteRollback(kwdbContext_p ctx) override { return setMtrOver(ctx, false); }

 private:
  // write data into storage using PutData function.
  KStatus writeDataWithPayload(kwdbContext_p ctx, TSSlice data);

 private:
  // entitygroup and its mtr of current snapshot.
  std::unordered_map<uint64_t, uint64_t> entity_grp_mtr_id_;
  std::vector<EntityResultIndex> entities_result_idx_;
};

class TsSnapshotConsumerByBlock : public TsSnapshotConsumer {
 public:
  ~TsSnapshotConsumerByBlock() override;

  KStatus Init(kwdbContext_p ctx, const TsSnapshotInfo& options) override;
  // check data struct. and  call PutData function
  KStatus WriteMetricData(kwdbContext_p ctx, TSSlice data, TsSnapshotDataType type) override;
  // transcation commit, with WAL of this snapshot commit.
  KStatus WriteCommit(kwdbContext_p ctx) override;
  // transcation rollback, using WAL rollback function
  KStatus WriteRollback(kwdbContext_p ctx) override;

 private:
  // write data into storage using PutData function.
  KStatus writeBlocks(kwdbContext_p ctx, TSSlice data);

  KStatus GenEmptyTagForPrimaryKey(kwdbContext_p ctx, uint64_t entity_grp_id, TSSlice primary_key);

  KStatus CopyBlockToPartitionTable(kwdbContext_p ctx, TsTimePartition* p_table, EntityID entity_id,
                                    const TsBlockFullData& block);

 private:
  // entity group + sub entity group + partition --> tmp table.
  std::unordered_map<std::string, TsTimePartition*> tmp_partition_tables_;
};

class TsPayloadSnapshotFactory;
class TsFullBlockSnapshotFactory;

class SnapshotFactory {
 private:
  static int factory_type_;  // 1. using payload; 2: using fullblock.
  static TsPayloadSnapshotFactory payload_inst_;
  static TsFullBlockSnapshotFactory block_inst_;

 public:
  static SnapshotFactory* Get();
  static void TestSetType(int type) { factory_type_ = type; }
  virtual TsSnapshotProductor* NewProductor() { return nullptr; }
  virtual TsSnapshotConsumer* NewConsumer() { return nullptr; }
};


class TsPayloadSnapshotFactory : public SnapshotFactory {
 public:
  TsSnapshotProductor* NewProductor() override {
    return new TsSnapshotProductorByPayload();
  }

  TsSnapshotConsumer* NewConsumer() override {
    return new TsSnapshotConsumerByPayload();
  }
};
class TsFullBlockSnapshotFactory : public SnapshotFactory {
 public:
  TsSnapshotProductor* NewProductor() override {
    return new TsSnapshotProductorByBlock();
  }
  TsSnapshotConsumer* NewConsumer() override {
    return new TsSnapshotConsumerByBlock();
  }
};

}  //  namespace kwdbts
