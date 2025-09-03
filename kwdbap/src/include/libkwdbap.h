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

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#include "libcommon.h"
#include "duckdb.h"
// APIs used by CGO

#ifdef __cplusplus
extern "C" {
#endif

/*
typedef struct {
  void* db;
} APEngine;

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
  TSSlice extra_options;
  TsLogOptions lg_opts;
  bool is_single_node;
  TSSlice cluster_id;
} APOptions;

TSStatus APOpen(APEngine** engine, TSSlice dir, APOptions options);

TSStatus APClose(APEngine* engine);
*/

typedef struct APEngine APEngine;
typedef struct APEngine *APEnginePtr;

typedef struct _APQueryInfo {
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
  TSSlice sql;
  void *db;
  void *connection;
  TSSlice db_path;
} APQueryInfo;

typedef struct _APString {
  char* value;
  uint32_t len;
} APString;

typedef struct _APAppender {
  void* value;
} * APAppender;

typedef struct _APConnection {
  void *internal_ptr;
} * APConnectionPtr;

typedef APQueryInfo APRespInfo;

TSStatus APOpen(APEngine** engine, APConnectionPtr *out, duckdb_database *out_db, APString *path);

TSStatus APClose(APEngine* engine);

TSStatus APExecQuery(APEngine* engine, APQueryInfo* req, APRespInfo* resp);

TSStatus APExecSQL(APEngine* engine, APString* sql, APRespInfo* resp);

TSStatus APDatabaseOperate(APEngine* engine, APString* name, EnDBOperateType type);

TSStatus APDropDatabase(APEngine* engine, APString* current_db_name, APString* drop_db_name);

TSStatus APCreateAppender(APEngine* engine, APString *catalog, APString *schema, APString *table,
                          APAppender *out_appender);

uint64_t APAppenderColumnCount(APAppender out_appender);

void TSFree(void* ptr);

#ifdef __cplusplus
}  // extern "C"
#endif