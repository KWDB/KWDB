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


// distribute moudule RangeGroup
typedef struct {
  uint64_t range_group_id;
  int8_t typ;  // : 0 - LEADER, 1 - FOLLOWER
} RangeGroup;

typedef struct {
  RangeGroup* ranges;
  int32_t len;
} RangeGroups;

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

// Define a structure to store wait threads number
typedef struct {
  uint32_t wait_threads;
} ThreadInfo;

// A TSStatus is an alias for TSString and is used to indicate that
// the return value indicates the success or failure of an
// operation. If TSStatus.data == NULL the operation succeeded.
typedef TSString TSStatus;
typedef uint64_t TSTableID;
typedef int64_t KTimestamp;

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
  TsLogOptions lg_opts;
} TSOptions;

typedef enum _EnMqType {
  MQ_TYPE_DML,
  MQ_TYPE_DML_SETUP,
  MQ_TYPE_DML_NEXT,
  MQ_TYPE_DML_CLOSE,
  MQ_TYPE_DML_PG_RESULT,
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
} TsFetcher;

typedef struct {
  bool collected;
  int8_t size;
  TsFetcher *TsFetchers;
  uint64_t goMutux;
} VecTsFetcher;

void __attribute__((weak)) goLock(uint64_t goMutux);
void __attribute__((weak)) goUnLock(uint64_t goMutux);

typedef struct _QueryInfo {
  EnMqType tp;
  void* value;
  uint32_t len;
  int32_t code;
  int32_t id;
  int32_t unique_id;
  int32_t ret;
  void* handle;
  int32_t time_zone;
  uint64_t relation_ctx;
} QueryInfo;

typedef QueryInfo RespInfo;

TSStatus TSOpen(TSEngine** engine, TSSlice dir, TSOptions options, AppliedRangeIndex* applied_indexes, size_t range_num);

TSStatus TSCreateTsTable(TSEngine* engine, TSTableID tableId, TSSlice meta, RangeGroups range_groups);

TSStatus TSGetRangeGroups(TSEngine* engine, TSTableID table_id, RangeGroups *range_groups);

TSStatus TSUpdateRangeGroup(TSEngine* engine, TSTableID table_id, RangeGroups range_groups);

TSStatus TSCreateRangeGroup(TSEngine* engine, TSTableID table_id, TSSlice schema, RangeGroups range_groups);

TSStatus TSDropTsTable(TSEngine* engine, TSTableID tableId);

/**
 * @brief Compress the segment whose maximum timestamp in the time series table is less than ts
 * @param[in] table_id id of the time series table
 * @param[in] ts A timestamp that needs to be compressed
 *
 * @return TSStatus
 */
TSStatus TSCompressTsTable(TSEngine* engine, TSTableID table_id, KTimestamp ts);

TSStatus TSIsTsTableExist(TSEngine* engine, TSTableID tableId, bool* find);

TSStatus TSGetMetaData(TSEngine* engine, TSTableID table_id, RangeGroup range, TSSlice* schema);

TSStatus TSPutEntity(TSEngine* engine, TSTableID tableId, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                     uint64_t mtr_id);

TSStatus TSPutData(TSEngine* engine, TSTableID tableId, TSSlice* payload, size_t payload_num, RangeGroup range_group,
                   uint64_t mtr_id, DedupResult* dedup_result);

TSStatus TSExecQuery(TSEngine* engine, QueryInfo* req, RespInfo* resp, TsFetcher* fetchers, void* fetcher);

TSStatus TsDeleteEntities(TSEngine* engine, TSTableID table_id, TSSlice* primary_tags, size_t primary_tags_num,
                          uint64_t range_group_id, uint64_t* count, uint64_t mtr_id);

TSStatus TsDeleteRangeData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      HashIdSpan hash_span, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id);

TSStatus TsDeleteData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                      TSSlice primary_tag, KwTsSpans ts_spans, uint64_t* count, uint64_t mtr_id);

TSStatus TSFlushBuffer(TSEngine* engine);

TSStatus TSCreateCheckpoint(TSEngine* engine);

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

TSStatus TSCreateSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id,
                          uint64_t begin_hash, uint64_t end_hash, uint64_t* snapshot_id);

TSStatus TSDropSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id);

TSStatus TSGetSnapshotData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                           size_t offset, size_t limit, TSSlice* data, size_t* total);

TSStatus TSInitSnapshotForWrite(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                              size_t snapshot_size);

TSStatus TSWriteSnapshotData(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id,
                             size_t offset, TSSlice data, bool finished);

TSStatus TSEnableSnapshot(TSEngine* engine, TSTableID table_id, uint64_t range_group_id, uint64_t snapshot_id);

TSStatus TSClose(TSEngine* engine);

void TSFree(void* ptr);

void TSRegisterExceptionHandler();

TSStatus TSAddColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column);

TSStatus TSDropColumn(TSEngine* engine, TSTableID table_id, char* transaction_id, TSSlice column);

TSStatus TSAlterPartitionInterval(TSEngine* engine, TSTableID table_id, uint64_t partition_interval);

TSStatus TSAlterColumnType(TSEngine* engine, TSTableID table_id, char* transaction_id,
                           TSSlice new_column, TSSlice origin_column);

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

bool __attribute__((weak)) isCanceledCtx(uint64_t goCtxPtr);

#ifdef __cplusplus
}  // extern "C"
#endif