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

#ifdef __cplusplus
}  // extern "C"
#endif