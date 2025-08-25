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

// APIs used by CGO

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  void* db;
} APEngine;

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

// TSStatus APOpen(APEngine** engine, TSSlice dir, APOptions options);

// TSStatus APClose(APEngine* engine);


struct APEngine {
  virtual ~APEngine() {}

  /**
 * @brief  calculate pushdown
 * @param[in] req
 * @param[out]  resp
 *
 * @return KStatus
 */
  virtual KStatus Execute(kwdbContext_p ctx, APQueryInfo* req, APRespInfo* resp) = 0;
};


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
  DataInfo vectorize_data;
  void *db;
  void *connection;
  TSSlice db_path;
} APQueryInfo;

typedef APQueryInfo APRespInfo;

TSStatus APOpen(APEngine** engine);

TSStatus APExecQuery(APEngine* engine, APQueryInfo* req, APRespInfo* resp);

#ifdef __cplusplus
}  // extern "C"
#endif