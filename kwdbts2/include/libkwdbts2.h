// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#pragma once

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

// APIs used by CGO

#ifdef __cplusplus
extern "C" {
#endif

typedef struct TSEngine TSEngine;

// A TSSlice contains read-only data that does not need to be freed.
typedef struct {
  char* data;
  size_t len;
} TSSlice;

// A TSString is structurally identical to a DBSlice, but the data it
// contains must be freed via a call to free().
typedef struct {
  char* data;
  size_t len;
} TSString;

// A TSStatus is an alias for TSString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If TSStatus.data == NULL the operation succeeded.
typedef TSString TSStatus;
typedef uint64_t TSTableID;
typedef int64_t KTimestamp;

// distribute moudule RangeGroup
typedef struct {
  uint64_t range_group_id;
  int8_t typ;  // : 0 - LEADER, 1 - FOLLOWER
} RangeGroup;

typedef struct {
  RangeGroup* ranges;
  int32_t len;
} RangeGroups;

typedef struct {
    uint32_t* index_column;
    int32_t len;
} IndexColumns;

// timestamp span
typedef struct {
  int64_t begin;
  int64_t end;
} KwTsSpan;

// An AppliedRangeIndex is the applied index of a specified range.
typedef struct {
  uint64_t range_id;
  uint64_t applied_index;
} AppliedRangeIndex;

typedef struct {
  KwTsSpan* spans;
  int32_t len;
} KwTsSpans;

// hashID span
typedef struct {
  uint64_t begin;
  uint64_t end;
} HashIdSpan;

// deduplicate info
typedef struct {
  int dedup_rule;  // deduplicate policy
  int dedup_rows;  // deduplicate rows
  int payload_num;
  TSSlice discard_bitmap;  // discard rows bitmap
} DedupResult;

// hashID span
typedef struct {
  TSTableID table_id;
  HashIdSpan hash_span;
  KwTsSpan ts_span;
} SnapshotRange;

// Define a structure to store wait threads number
typedef struct {
  uint32_t wait_threads;
} ThreadInfo;


typedef enum LgSeverity {
  UNKNOWN_K = 0, INFO_K, WARN_K, ERROR_K, FATAL_K, NONE_K, DEFAULT_K
} LgSeverity;

typedef struct TsLogOptions {
  TSSlice Dir;
  int64_t LogFileMaxSize;
  int64_t LogFilesCombinedMaxSize;
  LgSeverity LogFileVerbosityThreshold;
  TSSlice Trace_on_off_list;
} TsLogOptions;

// TSOptions contains local database options.
typedef struct {
  uint8_t wal_level;
  uint16_t wal_file_size;
  uint16_t wal_file_in_group;
  uint16_t wal_buffer_size;
  bool must_exist;
  bool read_only;
  TSSlice extra_options;
  uint16_t thread_pool_size;
  uint16_t task_queue_size;
  uint32_t buffer_pool_size;
  TsLogOptions lg_opts;
  bool is_single_node;
} TSOptions;

typedef enum _EnMqType {
  MQ_TYPE_DML,
  MQ_TYPE_DML_SETUP,
  // MQ_TYPE_DML_PUSH only be used for sending push request to tse for multiple model processing
  // when the switch is on and the server starts with single node mode.
  MQ_TYPE_DML_PUSH,
  MQ_TYPE_DML_NEXT,
  MQ_TYPE_DML_CLOSE,
  MQ_TYPE_DML_PG_RESULT,
  MQ_TYPE_DML_VECTORIZE_NEXT,
  MQ_TYPE_DML_INIT,
  MQ_TYPE_MAX
} EnMqType;

// TsFetcher collect information in explain analyse
typedef struct {
  int32_t processor_id;
  int64_t row_num;
  int64_t stall_time;  // time of execute
  int64_t bytes_read;  // byte of rows
  int64_t max_allocated_mem;  // maximum number of memory
  int64_t max_allocated_disk;  // Maximum number of disk
  int64_t output_row_num;  // rows of aggregation
  // build_time only be used for showing build time in explain analyze of hash tag scan op
  // for multiple model processing when the switch is on and the server starts with single node mode.
  int64_t build_time;  // time of build
} TsFetcher;

typedef struct {
  bool collected;
  int8_t size;
  TsFetcher *TsFetchers;
  uint64_t goMutux;
} VecTsFetcher;

void __attribute__((weak)) goLock(uint64_t goMutux);
void __attribute__((weak)) goUnLock(uint64_t goMutux);

typedef struct _TsColumnInfo {
  uint32_t fixed_len_;
  int32_t return_type_;
  int32_t storage_len_;
  int32_t storage_type_;
}TsColumnInfo;

typedef struct _TsColumnData {
  void *data_ptr_;
  void *bitmap_ptr_;
  void *offset_;
}TsColumnData;

typedef struct _DataInfo {
  uint32_t column_num_;
  TsColumnInfo *column_;
  TsColumnData *column_data_;
  uint32_t bitmap_size_;
  uint32_t row_size_;
  uint32_t data_count_;
  uint32_t capacity_;
  void *data_;
  bool is_data_owner_;
} DataInfo;

typedef struct _QueryInfo {
  EnMqType tp;
  void* value;
  uint32_t len;
  int32_t row_num;
  int32_t code;
  int32_t id;
  int32_t unique_id;
  int32_t ret;
  void* handle;
  int32_t time_zone;
  uint64_t relation_ctx;
  // only pass the rel data chunk pointer and count info to tse for multiple model processing
  // when the switch is on and the server starts with single node mode.
  void* relBatchData;
  int32_t relRowCount;
  DataInfo vectorize_data;
} QueryInfo;

typedef QueryInfo RespInfo;

TSStatus TSOpen(TSEngine** engine, TSSlice dir, TSOptions options, AppliedRangeIndex* applied_indexes, size_t range_num);

TSStatus TSCreateTsTable(TSEngine* engine, TSTableID tableId, TSSlice meta, RangeGroups range_groups);

TSStatus TSGetRangeGroups(TSEngine* engine, TSTableID table_id, RangeGroups *range_groups);

TSStatus TSUpdateRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroups range_groups);

TSStatus TSCreateRangeGroup(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups);

TSStatus TSDropTsTable(TSEngine* engine, TSTableID tableId);

TSStatus TSDropResidualTsTable(TSEngine* engine);

TSStatus TSCreateNormalTagIndex(TSEngine* engine, TSTableID table_id, uint64_t index_id,
                                char* transaction_id, uint32_t cur_version, uint32_t new_version,
                                IndexColumns index_columns);

TSStatus TSDropNormalTagIndex(TSEngine* engine, TSTableID table_id, uint64_t index_id, char* transaction_id,
                              uint32_t cur_version, uint32_t new_version);

/**
 * @brief Compress the segment whose maximum timestamp in the time series table is less than ts
 * @param[in] table_id id of the time series table
 * @param[in] ts A timestamp that needs to be compressed
 *
 * @return TSStatus
 */
TSStatus TSCompressTsTable(TSEngine* engine, TSTableID table_id, KTimestamp ts);

/**
 * @brief Compress all segments in the time series table
 * @param[in] table_id id of the time series table
 * @param[in] goCtxPtr pointer points to the context object in go layer
 *
 * @return TSStatus
 */
TSStatus TSCompressImmediately(TSEngine* engine, uint64_t goCtxPtr, TSTableID table_id);

TSStatus TSVacuumTsTable(TSEngine* engine, TSTableID table_id, uint32_t ts_version);

/**
 * @brief Migrate table partition to another tiering level if hot and cold data tiering is configured
 * @param engine
 * @param table_id Id of the time series table
 * @return
 */
TSStatus TSMigrateTsTable(TSEngine* engine, TSTableID table_id);

TSStatus TSTableAutonomy(TSEngine* engine, TSTableID table_id);

TSStatus TSIsTsTableExist(TSEngine* engine, TSTableID tableId, bool* find);

TSStatus TSGetMetaData(TSEngine* engine, TSTableID table_id, RangeGroup range, TSSlice* schema);

TSStatus TSPutEntity(TSEngine* engine, TSTableID tableId, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                     uint64_t mtr_id);

TSStatus TSPutData(TSEngine* engine, TSTableID tableId, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                   uint64_t mtr_id, uint16_t* inc_entity_cnt, uint32_t* inc_unordered_cnt, DedupResult* dedup_result,
                   bool writeWAL);

TSStatus TSExecQuery(TSEngine* engine, QueryInfo* req, RespInfo* resp, TsFetcher* fetchers, void* fetcher);

TSStatus TsDeleteEntities(TSEngine* engine, TSTableID table_id, TSSlice* primary_tags, size_t primary_tags_num,
                          uint64_t range_group_id, uint64_t* count, uint64_t mtr_id);

TSStatus TsDeleteRangeData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      HashIdSpan hash_span, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id);

TSStatus TsDeleteData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      TSSlice primary_tag, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id);

TSStatus TSFlushBuffer(TSEngine* engine);

TSStatus TSCreateCheckpoint(TSEngine* engine);

TSStatus TSCreateCheckpointForTable(TSEngine* engine, TSTableID table_id);

TSStatus TSMtrBegin(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                    uint64_t range_id, uint64_t index, uint64_t* mtr_id);

TSStatus TSMtrCommit(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id);

TSStatus TSMtrRollback(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t mtr_id);

TSStatus TSxBegin(TSEngine* engine, TSTableID tableId, char* transaction_id);

TSStatus TSxCommit(TSEngine* engine, TSTableID tableId, char* transaction_id);

TSStatus TSxRollback(TSEngine* engine, TSTableID tableId, char* transaction_id);

/**
 * @brief TSDeleteExpiredData is the CGO interface that delete expired data which is older than the end_ts.
 * @param[in] table_id id of the time series table
 * @param[in] end_ts end timestamp of expired data
 * @return
 */
TSStatus TSDeleteExpiredData(TSEngine* engine, TSTableID table_id, KTimestamp end_ts);

/**
 * @brief calculate row size of this table. Approximate value
 * @param[in] table_id id of the time series table
 * @param[out] volume   range data
 * @return
 */
TSStatus TSGetAvgTableRowSize(TSEngine* engine, TSTableID table_id, uint64_t* row_size);

/**
 * @brief get range(hash-hash ts-ts) data volume, Approximate value
 * @param[in] table_id id of the time series table
 * @param[in] begin_hash,end_hash  hash range of primary key of entities 
 * @param[in] ts_span   timestamp span
 * @param[out] volume   range data
 * @return
 */
TSStatus TSGetDataVolume(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                         KwTsSpan ts_span, uint64_t* volume);

/**
 * @brief The timestamp when querying half of the total data within the range (an approximate value)
 * @param[in] table_id id of the time series table
 * @param[in] begin_hash,end_hash  hash range of primary key of entities 
 * @param[in] ts_span   timestamp span
 * @param[out] half_ts  timestamp that half split range
 * @return
 */
TSStatus TSGetDataVolumeHalfTS(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                               KwTsSpan ts_span, int64_t* half_ts);

/**
 * @brief Input data in Payload format based on row mode
 * @param[in] table_id id of the time series table
 * @param[in] payload_row  row-based payload
 * @param[in] range_group  hash-point
 * @param[out] dedup_result deduplicate info
 * @return
 */
TSStatus TSPutDataByRowType(TSEngine* engine, TSTableID table_id, TSSlice* payload_row, size_t payload_num,
                            RangeGroup range_group, uint64_t mtr_id, uint16_t* inc_entity_cnt,
                            uint32_t* inc_unordered_cnt, DedupResult* dedup_result, bool writeWAL);

TSStatus TsTestGetAndAddSchemaVersion(TSEngine* engine, TSTableID table_id, uint64_t version);

char* __attribute__((weak)) getTableMetaByVersion(TSTableID table_id, uint64_t ver, size_t* data_len, char** error);
bool __attribute__((weak)) checkTableMetaExist(TSTableID table_id);
/**
 * @brief delete data in range, used after snapshot finished. 
 * it maybe delete tstable in storage engine. in this case, before next input data, we should create table first.
 * @param[in] table_id id of the time series table
 * @param[in] begin_hash,end_hash  hash range of primary key of entities 
 * @param[in] ts_span   timestamp span
 * @return
 */
TSStatus TsDeleteTotalRange(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                              KwTsSpan ts_span, uint64_t mtr_id);

/**
 * @brief Create a snapshot object to read local data
 * @param[in] table_id id of the time series table
 * @param[in] begin_hash,end_hash  hash range of primary key of entities 
 * @param[in] ts_span   timestamp span
 * @param[out] snapshot_id  generated snapshot id
 * @return
 */
TSStatus TSCreateSnapshotForRead(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                                 KwTsSpan ts_span, uint64_t* snapshot_id);

/**
 * @brief Return the data that needs to be transmitted this time. If the data is 0, it means that all data has been queried
 * @param[in] table_id id of the time series table
 * @param[in] snapshot_id  generated snapshot id
 * @param[in] data   payload type data
 * @return
 */
TSStatus TSGetSnapshotNextBatchData(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id, TSSlice* data);

/**
 * @brief Create an object to receive data at the target node
 * @param[in] table_id id of the time series table
 * @param[in] begin_hash,end_hash  hash range of primary key of entities 
 * @param[in] ts_span   timestamp span
 * @param[out] snapshot_id  generated snapshot id
 * @return
 */
TSStatus TSCreateSnapshotForWrite(TSEngine* engine, TSTableID table_id, uint64_t begin_hash, uint64_t end_hash,
                                  KwTsSpan ts_span, uint64_t* snapshot_id);

/**
 * @brief Target node, after receiving data, writes the data to storage
 * @param[in] table_id id of the time series table
 * @param[in] snapshot_id  generated snapshot id
 * @param[in] data   payload type data
 * @return
 */
TSStatus TSWriteSnapshotBatchData(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id, TSSlice data);

/**
 * @brief All writes completed, this snapshot is successful, call this function
 * @param[in] table_id id of the time series table
 * @param[in] snapshot_id  generated snapshot id
 * @return
 */
TSStatus TSWriteSnapshotSuccess(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id);

/**
 * @brief The snapshot failed, or in other scenarios, the data written this time needs to be rolled back
 * @param[in] table_id id of the time series table
 * @param[in] snapshot_id  generated snapshot id
 * @return
 */
TSStatus TSWriteSnapshotRollback(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id);

/**
 * @brief Delete snapshot object
 * @param[in] table_id id of the time series table
 * @param[in] snapshot_id  generated snapshot id
 * @return
 */
TSStatus TSDeleteSnapshot(TSEngine* engine, TSTableID table_id, uint64_t snapshot_id);

TSStatus TSClose(TSEngine* engine);

void TSFree(void* ptr);

void TSRegisterExceptionHandler(char *dir);

TSStatus TSAddColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                     uint32_t cur_version, uint32_t new_version);

TSStatus TSDropColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column,
                      uint32_t cur_version, uint32_t new_version);

TSStatus TSAlterColumnType(TSEngine* engine, TSTableID table_id, char* transaction_id,
                           TSSlice new_column, TSSlice origin_column, uint32_t cur_version, uint32_t new_version);

TSStatus TSAlterPartitionInterval(TSEngine* engine, TSTableID table_id, uint64_t partition_interval);

void UpdateTsTraceConfig(TSSlice cfg);
/**
 * @brief Set AE cluster setting, save into map and notify modules.
 *        Function is called when server start and setting changed.
 * @param[in]  key      setting name slice
 * @param[in]  value    setting value slice
 * @return void
*/
void TSSetClusterSetting(TSSlice key, TSSlice value);
/**
 * @brief CGO interface, Dump all thread backtrace to file when receive signal SIGUSER1 
 * @param[in]  folder         dump folder path
 * @param[in]  nowTimpstamp   dump timestamp
 * @return  void
*/
bool TSDumpAllThreadBacktrace(char* folder, char* now_time_stamp);


TSStatus TSDeleteRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroup range);

/**
* @brief : Gets the number of remaining threads from the thread pool and available memory
*
* @param[out] : resp Return the execution result
*
* @return : TSStatus
*/
TSStatus TSGetWaitThreadNum(TSEngine* engine, void* resp);

/**
 * @brief Get current version of table
 * @param[in] table_id id of the time series table
 * @param[out] version current table version
 * @return
 */
TSStatus TsGetTableVersion(TSEngine* engine, TSTableID table_id, uint32_t* version);

void TsMemPoolFree(void *data);

char* TsGetStringPtr(void *data, uint32_t offset,  uint16_t *len);

/**
 * @brief Get current wal level of ts engine
 * @param[out] wal_level current wal level
 * @return
 */
TSStatus TsGetWalLevel(TSEngine* engine, uint8_t *wal_level);

bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr);

int __attribute__((weak)) goPrepareFlush();
int __attribute__((weak)) goFlushed();

#ifdef __cplusplus
}  // extern "C"
#endif
