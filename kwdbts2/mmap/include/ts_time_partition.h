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
#include <utility>
#include <queue>
#include <vector>
#include <atomic>
#include "cm_kwdb_context.h"
#include "utils/date_time_util.h"
#include "big_table.h"
#include "mmap/mmap_object.h"
#include "mmap/mmap_segment_table.h"
#include "mmap/mmap_root_table_manager.h"
#include "mmap/mmap_entity_block_meta.h"
#include "cm_func.h"
#include "payload.h"
#include "ts_common.h"
#include "utils/compress_utils.h"
#include "lg_api.h"
#include "lt_rw_latch.h"
#include "lt_cond.h"
#include "TSLockfreeOrderList.h"
#include "entity_block_meta_manager.h"
#include "sys_utils.h"

#define ENTITY_ITEM_LATCH_BUCKET_NUM 10

extern int64_t g_vacuum_interval;
extern uint64_t g_vacuum_sleep_time;

enum class ExclusiveStatus{
  NONE = 0,
  COMPRESSING,
  VACUUMING,
  MIGRATING
};

bool ReachMetaMaxBlock(BLOCK_ID cur_block_id);


class TsTimePartition : public TSObject {
 public:
  using TsTimePartitionCntMutex = KLatch;
  using TsTimePartitionCondVal = KCond_t;
  TsTimePartitionCntMutex* m_ref_cnt_mtx_;
  TsTimePartitionCondVal* m_ref_cnt_cv_;
 private:
  using TsTimePartitionRWLatch = KRWLatch;
  using TsTimePartitionLatch = KLatch;

  TsTimePartitionRWLatch* vacuum_query_lock_;
  TsTimePartitionLatch* vacuum_insert_lock_; // control insert & vacuum concurrency
  TsTimePartitionLatch* vacuum_delete_lock_; // control delete & vacuum concurrency

  TsTimePartitionLatch* active_segment_lock_;
  TsHashLatch entity_item_latch_;  // control entity item row written


  // collect all segments with status ActiveSegment and ImmuSegment
  std::vector<std::shared_ptr<MMapSegmentTable>> GetAllSegmentsForCompressing();

  void ImmediateCompress(uint32_t& compressed_num, ErrorInfo& err_info);

  void ScheduledCompress(timestamp64 ts, uint32_t& compressed_num, ErrorInfo& err_info);

  bool TrySetExclusiveStatus(ExclusiveStatus desired);

  void ResetExclusiveStatus();


 protected:
  string name_;
  string db_path_;
  string tbl_sub_path_;
  string file_path_;
  bool cancel_vacuum_ = false;
  std::atomic<int> writing_count_{0};
  std::atomic<ExclusiveStatus> comp_vacuum_status_{ExclusiveStatus::NONE};

  // root table with schema info in table root directory
  MMapRootTableManager*& root_table_manager_;

  mutable EntityBlockMetaManager meta_manager_;
  // partition list, key: min block id, value: MMapSegmentTable object
  TSLockfreeOrderList<BLOCK_ID, std::shared_ptr<MMapSegmentTable>> data_segments_;
  // current allocating space segment object.
  std::shared_ptr<MMapSegmentTable> active_segment_{nullptr};

  std::atomic<int>* update_time_count_{nullptr};

  // segment sub path
  inline std::string segment_tbl_sub_path(BLOCK_ID segment) {
    return tbl_sub_path_ + std::to_string(segment) + "/";
  }

  // segment dir file path
  inline std::string segment_sqfs(BLOCK_ID shard) {
    return db_path_ + tbl_sub_path_ + std::to_string(shard) + ".sqfs";
  }

  // initialize column with schema
  int init(const vector<AttributeInfo>& schema, int encoding, bool init_data, ErrorInfo& err_info);

  int loadSegment(BLOCK_ID segment_id, ErrorInfo& err_info);

  std::shared_ptr<MMapSegmentTable> reloadSegment(std::shared_ptr<MMapSegmentTable> old_segment,
                                                  bool lazy_open, ErrorInfo& err_info);

 public:
  explicit TsTimePartition(MMapRootTableManager*& root_table_manager, uint16_t config_subgroup_entities) :
    TSObject(), root_table_manager_(root_table_manager),
    entity_item_latch_(ENTITY_ITEM_LATCH_BUCKET_NUM, LATCH_ID_ENTITY_ITEM_MUTEX) {
    meta_manager_.max_entities_per_subgroup = config_subgroup_entities;
    vacuum_query_lock_ = new TsTimePartitionRWLatch(RWLATCH_ID_MMAP_PARTITION_TABLE_RWLOCK);
    active_segment_lock_ = new TsTimePartitionLatch(LATCH_ID_MMAP_PARTITION_TABLE_SEGMENTS_MUTEX);
    vacuum_insert_lock_ = new TsTimePartitionLatch(LATCH_ID_PARTITION_VACUUM_WRITE_MUTEX);
    vacuum_delete_lock_ = new TsTimePartitionLatch(LATCH_ID_PARTITION_VACUUM_DELETE_MUTEX);
    m_ref_cnt_mtx_ = new TsTimePartitionCntMutex(LATCH_ID_PARTITION_REF_COUNT_MUTEX);
    m_ref_cnt_cv_ = new TsTimePartitionCondVal(COND_ID_PARTITION_REF_COUNT_COND);
  }

  virtual ~TsTimePartition();

  int rdLock() override;

  int wrLock() override;

  int unLock() override;

  inline void RefWritingCount() {
    writing_count_.fetch_add(1);
  }

  inline void UnrefWritingCount() {
    writing_count_.fetch_sub(1);
  }

  inline bool IsWriting() {
    return writing_count_.load() > 0;
  }

  int refMutexLock() override {
    return MUTEX_LOCK(m_ref_cnt_mtx_);
  }

  int refMutexUnlock() override {
    return MUTEX_UNLOCK(m_ref_cnt_mtx_);
  }

  inline void updateLock() {
    rdLock();
    MUTEX_LOCK(vacuum_insert_lock_);
    MUTEX_LOCK(vacuum_delete_lock_);
  }

  inline void updateUnlock() {
    unLock();
    MUTEX_UNLOCK(vacuum_insert_lock_);
    MUTEX_UNLOCK(vacuum_delete_lock_);
  }

  inline void vacuumLock() {
    wrLock();
    MUTEX_LOCK(vacuum_insert_lock_);
    MUTEX_LOCK(vacuum_delete_lock_);
  }

  inline void vacuumUnlock() {
    unLock();
    MUTEX_UNLOCK(vacuum_insert_lock_);
    MUTEX_UNLOCK(vacuum_delete_lock_);
  }

  inline static void GetBlkMinMaxTs(BlockItem* cur_block, MMapSegmentTable* segment_tbl,
    timestamp64 &blk_min_ts, timestamp64 &blk_max_ts) {
    blk_min_ts = cur_block->min_ts_in_block;
    blk_max_ts = cur_block->max_ts_in_block;
    if (blk_min_ts == 0 && blk_max_ts == 0) {
      blk_min_ts = KTimestamp(segment_tbl->columnAggAddr(cur_block->block_id, 0, Sumfunctype::MIN));
      blk_max_ts = KTimestamp(segment_tbl->columnAggAddr(cur_block->block_id, 0, Sumfunctype::MAX));
    }
  }

  /**
 * @brief schema using MMapMetricsTable object. no need store schema at partition level.
 *
 * @param 	path			big object PATH to be opened.
 * @param 	flag		option to open a file; O_CREAT to create new file.
 * @return	0 succeed, otherwise -1.
 */
  int open(const string& path, const std::string& db_path, const string& tbl_sub_path,
           int flags, ErrorInfo& err_info) override;

  int openBlockMeta(const int flags, ErrorInfo& err_info);

  vector<AttributeInfo> getSchemaInfoIncludeDropped(uint32_t table_version = 0) const {
    std::vector<AttributeInfo> schema;
    root_table_manager_->GetSchemaInfoIncludeDropped(&schema, table_version);
    return schema;
  }

  vector<AttributeInfo> getSchemaInfoExcludeDropped(uint32_t table_version = 0) const {
    std::vector<AttributeInfo> schema;
    root_table_manager_->GetSchemaInfoExcludeDropped(&schema, table_version);
    return schema;
  }

  const vector<uint32_t>& getColsIdxExcludeDropped() const {
    return root_table_manager_->GetIdxForValidCols();
  }

  const string& tbl_sub_path() const { return tbl_sub_path_; }

  string name() const override { return name_; }

  string path() const override { return file_path_; }

  timestamp64& minTimestamp() { return meta_manager_.minTimestamp(); }

  timestamp64& maxTimestamp() { return meta_manager_.maxTimestamp(); }

  bool& isInitialized() { return meta_manager_.isInitialized(); }

  BLOCK_ID GetMaxBlockID() { return meta_manager_.getEntityHeader()->cur_block_id; }

  virtual int reserve(size_t size) { return KWEPERM; }

  virtual int remove(bool exclude_segment = false);

  void sync(int flags) override;

  virtual size_t size(uint32_t entity_id) const;

  // merge data from other into current partition, other table will be removed.
  bool JoinOtherPartitionTable(TsTimePartition* other);

  inline std::shared_ptr<MMapSegmentTable> getSegmentTable(BLOCK_ID segment_id, bool lazy_open = false) const {
    std::shared_ptr<MMapSegmentTable> value;
    BLOCK_ID key;
    // found segment that may contain this block id.
    bool ret = data_segments_.Seek(segment_id, key, value);
    if (ret) {
      // segment has different status, if not ready means we need load segment files.
      if (value->getObjectStatus() != OBJ_READY && !lazy_open) {
        ErrorInfo err_info;
        if (value->open(const_cast<EntityBlockMetaManager*>(&meta_manager_), key, name_ + ".bt",
                        db_path_, tbl_sub_path_ + std::to_string(key) + "/", MMAP_OPEN_NORECURSIVE,
                        false, err_info) < 0) {
          LOG_ERROR("getSegmentTable segment[%u] open failed", key);
          return nullptr;
        }
      }
      if (UNLIKELY(value->getObjectStatus() == OBJ_READY && segment_id >= value->segment_id() + value->getBlockMaxNum())) {
        // this segment may not satisfy, means we not found segment.
        LOG_ERROR("getSegmentTable segment[%u] does not exist", segment_id);
        return nullptr;
      }
      return value;
    }
    return nullptr;
  }

  // check if blockitem can store new data.
  inline bool isReadOnly(BlockItem* block_item, uint32_t payload_table_version = 0) const {
    assert(block_item != nullptr);
    if (block_item->block_id != 0) {
      std::shared_ptr<MMapSegmentTable> tbl = getSegmentTable(block_item->block_id);
      if (tbl == nullptr) {
        return true;
      }
      if (block_item->max_rows_in_block == 0) {
        block_item->max_rows_in_block = tbl->getBlockMaxRows();
      }
      if (!tbl->canWrite() || tbl->schemaVersion() < payload_table_version) {
        return true;
      }
      if (block_item->alloc_row_count >= tbl->getBlockMaxRows() ||
          block_item->publish_row_count >= tbl->getBlockMaxRows()) {
        return true;
      }
    }
    return block_item->read_only;
  }

  inline void releaseSegments() {
    data_segments_.Clear();
  }

  uint32_t getMaxRowsInBlock(BlockItem* block_item) {
    if (UNLIKELY(block_item->max_rows_in_block == 0)) {
      // historical version(2.0.3.x) compatibility
      // get max rows from entity header.
      block_item->max_rows_in_block = meta_manager_.getEntityHeader()->max_rows_per_block;
    }
    return block_item->max_rows_in_block;
  }

  /**
   * @brief createSegmentTable creates a Memory-Mapped Segment Table
   *
   * This function is responsible for creating a memory-mapped segment table based on the provided segment ID.
   * The segment table manages data block mappings and is crucial for efficient memory management.
   *
   * @param segment_id The unique identifier for the segment which the table is to be created for.
   * @param err_info A reference to an ErrorInfo object.
   * @param max_rows_per_block, max_blocks_per_segment used for vacuum
   *
   * @return A pointer to the newly created MMapSegmentTable on success, or nullptr if an error occurs.
   */
  MMapSegmentTable* createSegmentTable(BLOCK_ID segment_id, uint32_t table_version, ErrorInfo& err_info,
    uint32_t max_rows_per_block = 0, uint32_t max_blocks_per_segment = 0);

  inline bool GetBlockMinMaxTS(BlockItem* block, timestamp64* min_ts, timestamp64* max_ts) {
    std::shared_ptr<MMapSegmentTable> segment_tbl = getSegmentTable(block->block_id);
    if (segment_tbl == nullptr) {
      return false;
    }
    GetBlkMinMaxTs(block, segment_tbl.get(), *min_ts, *max_ts);
    return true;
  }

  inline EntityItem* getEntityItem(uint entity_id) {
    return meta_manager_.getEntityItem(entity_id);
  }

  inline void deleteEntityItem(uint entity_id) {
    meta_manager_.deleteEntity(entity_id);
  }

  inline BlockItem* GetBlockItem(uint item_id) {
    return meta_manager_.GetBlockItem(item_id);
  }

  inline TsHashLatch* GetEntityItemLatch() {
    return &entity_item_latch_;
  }

  inline uint32_t GetCurrentVersion() {
    return root_table_manager_->GetCurrentTableVersion();
  }
/**
 * @brief push_back_payload function is utilized for writing data into a partitioned table.
 * The function initially verifies the validity of the partition table, allocates data space,
 * and updates the maximum and minimum timestamps within the partition table.
 * Then, search for duplicate rows using deduplication logic and write the data into the pre-allocated space.
 * Finally, update the metadata and return.
 *
 * @param ctx Context information.
 * @param entity_id entity ID
 * @param payload Data to be written.
 * @param start_in_payload The starting position within the payload data.
 * @param num The number of rows of payload data to be written.
 * @param alloc_spans The assigned block list.
 * @param todo_markdel A list of MetricRowIDs that need to be marked for deletion.
 * @param err_info Error message.
 * @param dedup_result Results after eliminating duplicates.
 * @return The operation's result status code is returned.
 */
  virtual int64_t push_back_payload(kwdbts::kwdbContext_p ctx, uint32_t entity_id,
                                    kwdbts::Payload* payload, size_t start_in_payload, size_t num,
                                    std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
                                    ErrorInfo& err_info, uint32_t* inc_unordered_cnt, DedupResult* dedup_result);

  virtual ostream& printRecord(uint32_t entity_id, std::ostream& os, size_t row);

  /**
   * @brief Compress the segment whose maximum timestamp is smaller than ts in all segments of partition table
   * @param[in] compress_ts A timestamp that needs to be compressed
   * @param[out] err_info error info
   *
   * @return void
   */
  void Compress(const timestamp64& compress_ts, uint32_t& compressed_num, ErrorInfo& err_info);

  int Sync(kwdbts::TS_LSN check_lsn, ErrorInfo& err_info);

  int Sync(kwdbts::TS_LSN check_lsn, map<uint32_t, uint64_t>& rows, ErrorInfo& err_info);

  int DeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  int DeleteData(uint32_t entity_id, kwdbts::TS_LSN lsn, const std::vector<KwTsSpan>& ts_spans,
                 vector<kwdbts::DelRowSpan>* delete_rows, uint64_t* count,
                 ErrorInfo& err_info, bool evaluate_del = false);

  /**
   * @brief FindFirstBlockItem uses the start_payload_ts and lsn to search in reverse order for the
   * first matching block item within the written blocks, placing any subsequent block items into block_items.
   * If no matching block item is found, it checks whether there is a block item that has been allocated space
   * but has not yet written data.
   *
   * @param entity_id The entity ID to search for.
   * @param lsn The Log Sequence Number used for additional filtering of block items.
   * @param start_payload_ts The starting timestamp for the query.
   * @param block_items A deque of block items to be queried.
   * @param blk_item Output parameter where the found block item's pointer will be stored.
   * @param block_start_row Output parameter indicating the starting row number of the found block item.
   * @param partition_ts_map An optional map relating timestamps to partition row IDs.
   * @param p_time An optional partition timestamp for queries related to specific time partitions.
   * @return Returns 1 if a matching block item is found; otherwise, returns 0.
   */
  int FindFirstBlockItem(uint32_t entity_id, kwdbts::TS_LSN lsn, timestamp64 start_payload_ts,
                         std::deque<BlockItem*>& block_items, BlockItem** blk_item, uint32_t* block_start_row,
                         std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map, KTimestamp p_time);

  /**
   * @brief UndoPut undoes a previously performed put operation for a given entity.
   *
   * This function reverses the placement operation on a specific entity identified by its ID,
   * affecting a range of rows starting from the specified start row and covering the given number
   * of rows. It utilizes the provided LSN to ensure atomicity and consistency during the undo process.
   * The payload data is updated accordingly to reflect the changes made by this undo operation. In case
   * of failure, an error message is returned via the error info reference parameter.
   *
   * @param entity_id The ID of the entity for which the put operation is to be undone.
   * @param lsn The Log Sequence Number corresponding to this undo operation, ensuring transactional integrity.
   * @param start_row The starting row number from where the undo operation should commence.
   * @param num The number of rows to undo.
   * @param payload A pointer to the payload data that will be updated as part of the undo process.
   * @param err_info A reference to an ErrorInfo object where detailed error information is stored upon failure.
   *
   * @return Returns an integer indicating the result of the operation.
   */
  int UndoPut(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t start_row, size_t num, kwdbts::Payload* payload,
              ErrorInfo& err_info);

  /**
   * @brief UndoDelete undoes the deletion operation for a given entity ID and LSN.
   * This function is used to revert the deletion marks set on the rows of a data block,
   * typically called during data recovery or undo operations.
   *
   * @param entity_id The ID of the entity for which the deletion operation is to be undone.
   * @param lsn The logical sequence number of the deletion operation, used to match and undo the specific operation.
   * @param rows A pointer to a vector containing the details of the rows that were previously marked for deletion.
   * @param err_info A reference to an error information object.
   * @return Returns 0 if the operation is successful, indicating that the deletion undo operation has been completed.
   */
  int UndoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn, const vector<kwdbts::DelRowSpan>* rows, ErrorInfo& err_info);

  /**
   * @brief UndoDeleteEntity undoes the deletion of an entity within the partition table.
   *
   * This function reverses the deletion status of a specified entity by updating
   * its `is_deleted` attribute to false, indicating that the entity is no longer marked as deleted.
   *
   * @param entity_id The ID of the entity to undo deletion for.
   * @param lsn The Log Sequence Number (LSN) for logging purposes, ensuring data recovery and consistency.
   * @param count A pointer to a uint64_t
   * @param err_info A reference to ErrorInfo to return error details. It remains unmodified on success.
   * @return An error code indicating the result of the operation. Zero denotes success.
   */
  int UndoDeleteEntity(uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t* count, ErrorInfo& err_info);

  /**
   * @brief RedoPut redo the put operation for the mapped partition table.
   *        The data that has been written is re-written to the corresponding block,
   *        and the block that has not been applied is re-applied. Other logic is consistent with push_back_payload
   *
   * @param ctx The context of the database operation.
   * @param entity_id The ID of the entity to which the data belongs.
   * @param lsn The log sequence number for the operation, used for data consistency checks.
   * @param start_row The starting row number of the data to be written in the payload.
   * @param num The total number of rows of data to be written.
   * @param payload The data to be written, in the form of a payload.
   * @param alloc_spans Used to store the allocation results of the data blocks, i.e.,
   *                    the mapping between data and blocks.
   * @param todo_markdel Used to store the row IDs of data that need to be marked as deleted due to deduplication.
   * @param partition_ts_map A mapping of timestamps to row IDs, used for deduplication.
   * @param p_time The partition time, used to determine the partition to which the data belongs.
   * @param err_info Used to return error information if the operation fails.
   * @return Returns 0 on success, and a non-zero error code on failure.
   */
  int RedoPut(kwdbts::kwdbContext_p ctx, uint32_t entity_id, kwdbts::TS_LSN lsn, uint64_t start_row, size_t num,
              kwdbts::Payload* payload, std::vector<BlockSpan>* alloc_spans, std::vector<MetricRowID>* todo_markdel,
              std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map, KTimestamp p_time, ErrorInfo& err_info);

  /**
   * Replays delete operations from the redo log.
   * This function updates the deletion flags of block items based on provided delete flags and block item IDs.
   * It iterates through each delete flag and applies the corresponding flag to the block item's deletion flags array.
   *
   * @param entity_id Identifier for the entity targeted by the delete operation.
   * @param lsn LSN marking the position in the log for this delete operation, used during recovery.
   * @param rows Pointer to a vector of DelRowSpan, each containing a block item ID and a set of delete flags.
   * @param err_info Reference to an ErrorInfo object.
   * @return Returns 0 to indicate a successful operation.
   */
  int RedoDelete(uint32_t entity_id, kwdbts::TS_LSN lsn, const vector<kwdbts::DelRowSpan>* rows, ErrorInfo& err_info);

  int GetAllBlockItems(vector<uint32_t>& entity_ids, std::deque<BlockItem*>& block_items);

  int GetAllBlockItems(uint32_t entity_id, std::deque<BlockItem*>& block_items, bool reverse = false);

  int GetAllBlockSpans(uint32_t entity_id, std::vector<KwTsSpan>& ts_spans, std::deque<BlockSpan>& block_spans,
                       uint32_t max_block_id = INT32_MAX, bool reverse = false);

  /**
   * @brief	Allocates some free space within a block.
   *
   * @param[in]	entity_id	entity id.
   * @param[in]	batch_num	The number of rows applied for writing.
   * @param[out] segment_id	segment id
   * @param[out] span The space allocated to the application.
   * @return	0 succeed, otherwise -1.
   */
  int AllocateSpace(uint entity_id, size_t batch_num, BLOCK_ID* segment_id, BlockSpan* span);

  /**
   * @brief AllocateAllDataSpace allocates data space for writing to the corresponding entity and
   * saves the allocation result in the passed in spans array.
   * The function allocates the required BlockItems one by one by recursively calling the allocateBlockItem function,
   * Then, determine the actual number of rows allocated to each BlockItem based on the
   * remaining number of rows to be allocated and the maximum row count allowed for each BlockItem,
   * The function saves the allocation results to spans. If an error occurs during the allocation process,
   * the function will revert the previously allocated space and return an error code.
   * Finally, the function updates the counts of allocated and written rows, and returns the allocation results.
   *
   * @param entity_id entity ID, used to identify the entity that requires space allocation.
   * @param batch_num The number of batches that need to be allocated.
   * @param spans Pointer to the BlockSpan vector that stores the allocation result.
   *              The successfully allocated spans will be added to this vector.
   * @param payload_table_version Table version of payload
   * @return Return the error code, 0 indicates success, and a value less than 0 indicates an error.
   */
  int AllocateAllDataSpace(uint entity_id, size_t batch_num, std::vector<BlockSpan>* spans,  uint32_t payload_table_version);

  /**
   * @brief GetAndAllocateAllDataSpace reuse or allocates all data space required for an operation.
   *
   *
   * @param entity_id Unique identifier for the entity the data belongs to.
   * @param batch_num The number of batches to allocate space for.
   * @param start_row The starting row index for allocation.
   * @param payload Pointer to the payload data structure where allocated data space details will be stored.
   * @param lsn Log sequence number for synchronization and recovery purposes.
   * @param partition_ts_map A map that associates timestamps with metric row identifiers, used for partitioning data.
   * @param p_time Partitioning time used to determine which partition the data should belong to.
   * @param spans Pointer to a vector that will hold the span information of allocated block segments.
   *
   * @return err_code
   */
  int GetAndAllocateAllDataSpace(uint entity_id, uint64_t batch_num, size_t start_row, kwdbts::Payload* payload,
                                 kwdbts::TS_LSN lsn, std::unordered_map<KTimestamp, MetricRowID>* partition_ts_map,
                                 KTimestamp p_time, std::vector<BlockSpan>* spans);

  /**
   * @brief allocateBlockItem is used to allocate or get a BlockItem.
   * If the current entity has not been assigned any BlockItem or if the current BlockItem is full or unavailable,
   * then it is necessary to allocate a new BlockItem to the current entity.
   * If there is no active segment, then a new segment will be created.
   *
   * @param entity_id entity ID, Used to identify entities that need to be allocated BlockItems.
   * @param payload_table_version Table version of payload
   * @param blk_item Address of the pointer that points to the allocated BlockItem.
   * @return Success returns 0, failure returns error code.
   */
  int allocateBlockItem(uint entity_id, BlockItem** blk_item, uint32_t payload_table_version);

  void publish_payload_space(const std::vector<BlockSpan>& alloc_spans, const std::vector<MetricRowID>& delete_rows,
                             uint32_t entity_id, bool success);

  int PrepareDup(kwdbts::DedupRule dedup_rule, uint32_t entity_id, const std::vector<KwTsSpan>& ts_spans);

  int CopyFixedData(DATATYPE old_type, char* old_mem, std::shared_ptr<void>* new_mem);

  int ConvertDataTypeToMem(DATATYPE old_type, DATATYPE new_type, int32_t new_type_size,
                           void* old_mem, std::shared_ptr<void> old_var_mem, std::shared_ptr<void>* new_mem);

  std::string GetPath() { return db_path_ + tbl_sub_path_; }

  /**
   * updatePayloadUsingDedup is used to handle data deduplication based on different rules (KEEP, REJECT, MERGE),
   * and update the payload accordingly.
   * and check if there are any duplicate timestamps in the payload.
   * perform various processing on the Payload, including checking for duplicate rows and merging them.
   *
   * @param entity_id entity ID, used to identify the owner of the data.
   * @param first_span BlockSpan, used to locate the position of the data.
   * @param payload Data to be written.
   * @param start_in_payload Starting position of the data within the payload.
   * @param num Number of data rows to be processed.
   * @param dedup_info Structure containing information related to duplicate detection, which the function will update.
   * @return Return code for the operation, 0 indicates success, DEDUPREJECT means reject duplicates,
   *         and other negative values represent errors.
   */
  int updatePayloadUsingDedup(uint32_t entity_id, const BlockSpan& first_span, kwdbts::Payload* payload,
                              size_t start_in_payload, size_t num, kwdbts::DedupInfo& dedup_info);

  /**
   * updatePayload merges duplicate row data into the target row.
   * Based on whether the column values are null or not, select and merge the corresponding latest non-null values
   * from duplicate rows or a database table, and update the bitmap of the target row.
   * The function first retrieves the timestamp of the target row and finds all rows with
   * the same timestamp in the dedup_info.
   * It then iterates through these rows starting from the most recent one and makes a judgment based on whether the
   * column values are null or not. If the value of the corresponding column in the target row is not null, it
   * skips that column. If the value of the corresponding column in the target row is null and there are duplicate
   * rows, it selects the latest non-null value from the duplicate rows for merging. During this process, it checks
   * if there is a change in the data type of the merged data, and if there is, it converts the data type accordingly.
   *
   * @param payload The result of the merge will be written here.
   * @param dedup_info Deduplication information, including the line numbers for deduplication, etc
   * @return Returning 0 indicates success, non 0 indicates failure
   */
  int updatePayload(kwdbts::Payload* payload, kwdbts::DedupInfo& dedup_info);

  /**
   * waitBlockItemDataFilled is used to wait for the completion of writing all data for a specified
   * BlockItem. Once the data is fully written, the function will proceed with subsequent operations.
   * The function will determine whether to wait for data to be written based on the parameters.
   * If the data has already been written, the function will return immediately.
   * Otherwise, the function will block the current thread until the data is written.
   *
   * @param entity_id   entity ID, used to identify the entity that is waiting for data to be written.
   * @param block_item  Pointer to the block_item that requires waiting for data to be written.
   * @param read_count  The number of rows to be read.
   * @param has_lsn     Indicates whether block_items include LSN.
   */
  void waitBlockItemDataFilled(uint entity_id, BlockItem* block_item, int read_count, bool has_lsn);

  /**
   * GetDedupRows retrieves duplicate row data that falls within a specific timestamp range,
   * within the specified BlockSpan range.
   * First, calculate the valid timestamp range based on the minimum and maximum timestamps
   * of the payload line in the given dedup_info parameter
   * Then, traverse all BlockItems under entity, and for each BlockItem,
   * check if its timestamp range overlaps with the given valid timestamp range.
   * If it overlaps, attempt to obtain duplicate row information from it.
   *
   * @param entity_id   entity ID
   * @param first_span  The range of the first BlockItem.
   * @param dedup_info  A data structure containing deduplication information.
   * @param has_lsn     Indicates whether block_items include LSN.
   * @return            The operation returns 0 if successful, otherwise an error code will be returned.
   */
  int GetDedupRows(uint entity_id, const BlockSpan& first_span, kwdbts::DedupInfo& dedup_info, bool has_lsn);

  /**
   * GetDedupRowsInBlk retrieve the unique row count from the specified block_item.
   * @param entity_id  entity ID
   * @param block_item Pointer to the block containing the data to be read.
   * @param read_count The number of rows to be read.
   * @param dedup_info A reference that includes deduplication information,
   * where the function tracks the actual row count after deduplication.
   * @param has_lsn   Indicates whether block_items include LSN.
   * @return The operation returns 0 if successful, otherwise an error code will be returned.
   */
  int GetDedupRowsInBlk(uint entity_id, BlockItem* block_item, int read_count,
                        kwdbts::DedupInfo& dedup_info, bool has_lsn);

  static int tryAlterType(const std::string& str, DATATYPE new_type, ErrorInfo& err_info);

  int ProcessDuplicateData(kwdbts::Payload* payload, size_t start_in_payload, size_t count,
                           const BlockSpan span, kwdbts::DedupInfo& dedup_info,
                           DedupResult* dedup_result, ErrorInfo& err_info, int* deleted_rows);


  // When delete a table, check whether there is no other refcount other than the cache
  // and the table instance that called to delete
  inline bool isUsedWithCache() const { return ref_count_ > 2; }

  bool& DeleteFlag() {
    return meta_manager_.getEntityHeader()->partition_deleted;
  }

  // Set partition need to be reorganization.
  inline void SetDisordered() {
    meta_manager_.getEntityHeader()->data_disordered = true;
    cancel_vacuum_ = true;
  }

  inline void SetDeleted() {
    meta_manager_.getEntityHeader()->data_deleted = true;
    cancel_vacuum_ = true;
  }

  inline void ResetVacuumFlags() {
    meta_manager_.getEntityHeader()->data_disordered = false;
    meta_manager_.getEntityHeader()->data_deleted = false;
  }

  // Check if there have been any recent modifications to the partition
  bool IsModifiedRecent() {
    if (nullptr == active_segment_) {
      return false;
    }
    // get ts column file's modify time of active segment
    string ts_file_path = db_path_ + active_segment_->tbl_sub_path() + name_ + ".0";
    int64_t newest_modify_time = ModifyTime(ts_file_path);

    // get meta file's modify time
    int64_t meta_modify_time =  meta_manager_.GetModifyTime();
    if (newest_modify_time < meta_modify_time) {
      newest_modify_time = meta_modify_time;
    }

    if (newest_modify_time <= now() - g_vacuum_interval) {
      return false;
    }
    return true;
  }

  // Evaluate the partition can be vacuumed or not.
  bool ShouldVacuum(uint32_t ts_version);

  /**
   * Create a temporary partition using specified parameters and version
   */
  KStatus PrepareTempPartition(uint32_t max_rows_per_block, uint32_t max_blocks_pre_seg,
                               uint32_t ts_version, TsTimePartition** dest_pt);

  /**
   * Using ts_version to vacuum data, read ordered data from original partition and write into new tempory partition,
   * then use the temporary to replace the original.
   * @param ts_version vacuum data use specified ts version
   * @param vacuum_result failed/canceled/finished
   * @return KStatus
   */
  KStatus Vacuum(uint32_t ts_version, VacuumStatus &vacuum_result);

  /**
   * move partition data from high level disk to lower level disk.
   * @param from_level current disk level
   * @param to_level move to disk level.
   * @param vacuum_result failed/canceled/finished
   * @return KStatus
   */
  KStatus Migrate(int from_level, int to_level);

  /**
   * get files and directory that store data.
   * @param from_level current disk level
   * @param to_level move to disk level.
   * @param vacuum_result failed/canceled/finished
   * @return KStatus
   */
  KStatus GetMigrateFileList(std::vector<std::string>* files);

  /**
   * is all segments  compressed.
   * @return true/false
   */
  bool IsAllSegCompressed();

  /**
   * get current partition data in disk level.
   * @param level disk level.
   * @return KStatus
   */
  KStatus CurrentLevel(int* level);

  /**
   * Read partial data from the block item of the specified segment and convert it into ResultSet.
   * @param origin_segment_tbl Get data from this segment tabl
   * @param cur_block_item Get data from this block item
   * @param block_start_row Get data from this row of block item
   * @param row_count How many rows get from block item
   * @param ts_version Which version of data to get
   * @param res Get data and store in the ResultSet
   * @return KStatus
   */
  KStatus GetVacuumData(const std::shared_ptr<MMapSegmentTable>& origin_segment_tbl, BlockItem* cur_block_item,
                        size_t block_start_row, k_uint32 row_count, uint32_t ts_version, ResultSet* res);

  /**
   * Write the ResultSet data to the specified entity of the corresponding partition
   * @param dest_pt Write to which partition
   * @param entity_id Which entity does the written data belong to
   * @param res ResultSet data to be written
   * @param row_count Number of data rows to be written
   * @return
   */
  KStatus WriteVacuumData(TsTimePartition* dest_pt, uint32_t entity_id, ResultSet* res, uint32_t row_count);

  // Modify segment status
  void ChangeSegmentStatus();

  void CountUpdateTime() {
    if (update_time_count_ == nullptr) {
      return;
    }
    update_time_count_->fetch_add(1);
  }

  bool OpenUpateTimesCount(std::atomic<int>* update_time_count) {
    if (update_time_count_ == nullptr) {
      update_time_count_ = update_time_count;
      return true;
    }
    return false;
  }

  void CloseUpateTimesCount() {
    update_time_count_ = nullptr;
  }

  bool Unmount();

  // load all segments
  int loadSegments(ErrorInfo& err_info);

  int DropSegmentDir(const std::vector<BLOCK_ID>& segment_ids);

  bool IsSegmentsBusy(const std::vector<BLOCK_ID>& segment_ids);

  std::vector<uint32_t> GetEntities() { return meta_manager_.getEntities(); }

  uint64_t PartitionInterval() { return maxTimestamp() - minTimestamp() + 1; }
};
