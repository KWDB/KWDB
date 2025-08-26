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
//
//

#ifndef COMMON_SRC_INCLUDE_KWDB_TYPE_H_
#define COMMON_SRC_INCLUDE_KWDB_TYPE_H_

// define data types and constants in KWDB to mask out OS differences
#include <inttypes.h>
#include <cstring>
#include <string>
#include "cm_new.h"
#include "default_conf.h"

namespace kwdbts {

/*********************** KWDB widely useful type definitions ******************/
typedef char k_int8;
typedef char k_char;
typedef unsigned char k_uchar;
typedef size_t k_size_t;
typedef unsigned char k_uint8;
typedef int16_t k_int16;
typedef uint16_t k_uint16;
typedef int k_int32;
typedef unsigned int k_uint32;
typedef int64_t k_int64;
typedef uint64_t k_uint64;
typedef float k_float32;
typedef double k_float64;
typedef double k_double64;
typedef k_uint8 k_bool;
typedef k_uint8 k_bits8;
typedef k_uint16 k_bits16;
typedef k_uint32 k_bits32;
typedef k_uint64 k_bits64;
typedef struct DummyDecimal{} k_decimal;

typedef k_int64 KTimestamp;
typedef k_int64 KTimestampTz;
// Offset  in shared memory offset
typedef k_int64 KId;
// Type for connection Id
typedef k_uint64 KConnId;
// Type for string
typedef std::string KString;
// Type for KWDBThreadID
typedef k_uint64 KThreadID;
// The unique identifier of the KObjectSchema
typedef k_uint32 KSchemaKey;
// The unique identifier of the KObjectTable
typedef k_uint64 KTableKey;
// The id of database
typedef k_uint32 KDatabaseId;
// The id of table
typedef k_uint64 KTableId;
// The id of query id
typedef k_int64 KQueryId;
// The id of processor id
typedef k_int32 KProcessorId;

// the log level of log report
typedef enum LogSeverity_t { LG_DEBUG = 0, LG_INFO, LG_WARN, LG_ERROR, LG_FATAL } LogSeverity;

//  the max number of nested function calls
#define CTX_MAX_FRAME_LEVEL 32
#define CTX_MAX_FILE_NAME_LEN 32
#define CTX_MAX_FUNC_NAME_LEN 64

//  the max number of Log module calls
#define FULL_FILE_NAME_MAX_LEN (512)
#define LOG_FILE_NAME_MAX_LEN FULL_FILE_NAME_MAX_LEN
#define LOG_DIR_MAX_LEN (384)
#define TRACE_FILE_NAME_MAX_LEN (512)
#define TRACE_DIR_MAX_LEN (64)
#define HOST_NAME_MAX_LEN (64)
#define USER_NAME_MAX_LEN (64)

typedef struct _KContext_t {
  _KContext_t() :
    max_stack_size(0),
    frame_level(0),
    connection_id(0),
    timezone(8) {
    for (int i = 0; i < CTX_MAX_FRAME_LEVEL; i++) {
      memset(file_name[i], '\0', CTX_MAX_FILE_NAME_LEN);
      memset(func_name[i], '\0', CTX_MAX_FILE_NAME_LEN);
      func_start_time[i] = 0;
      func_end_time[i] = 0;
    }
    memset(msg_buf, '\0', 256);
    snprintf(assert_file_name, sizeof(assert_file_name), "./kwdb_assert.txt");
  }
  char file_name[CTX_MAX_FRAME_LEVEL][CTX_MAX_FILE_NAME_LEN];  // file name of current running function
  char func_name[CTX_MAX_FRAME_LEVEL][CTX_MAX_FUNC_NAME_LEN];  // current running function name
  KTimestamp func_start_time[CTX_MAX_FRAME_LEVEL];  // current running function start time
  KTimestamp func_end_time[CTX_MAX_FRAME_LEVEL];  // current running function end time
  k_uint32  max_stack_size;  // max stack size in current thread (debug stack overflow issue)
  k_uint8    frame_level;   //  current function frame level in file_name/func_name stack;
  KConnId connection_id;   // current connection id
  k_int8 timezone;  // current session timezone
  char msg_buf[256];  // temp buffer for snprintf or sprint or other purpose
  char assert_file_name[128] = "./kwdb_assert.txt";  // file name used to store assert dump info
  uint64_t relation_ctx;
} KContext;

typedef KContext* KContextP;

typedef enum {
  FAIL = 0,
  SUCCESS = 1
} KStatus;

enum KWDB_TYPE {
  KWDBC,
  KWDBE,
  KWDBT,
  KWDB_MAX
};


/*************************** KWDB widely useful macros ************************/
#ifndef KTRUE
#define KTRUE ((k_bool)1)
#endif

#ifndef KFALSE
#define KFALSE ((k_bool)0)
#endif
// Return the Maximum of two numbers.
#define KMax(x, y) ((x) > (y) ? (x) : (y))

// Return the Minimum of two numbers.
#define KMin(x, y) ((x) < (y) ? (x) : (y))

// Return the absolute value of the argument.
#define KAbs(x) ((x) >= 0 ? (x) : -(x))

// change char* memory to KTimestamp.
#define KTimestamp(buf) (*(static_cast<KTimestamp *>(static_cast<void *>(buf))))

// change char* memory to k_uint8.
#define KUint8(buf) (*(static_cast<k_uint8 *>(static_cast<void *>(buf))))

// change char* memory to k_uint16.
#define KUint16(buf) (*(static_cast<uint16_t *>(static_cast<void *>(buf))))

// change char* memory to k_uint32.
#define KUint32(buf) (*(static_cast<k_uint32 *>(static_cast<void *>(buf))))

// change char* memory to k_uint64.
#define KUint64(buf) (*(static_cast<uint64_t *>(static_cast<void *>(buf))))

// change char* memory to k_float32.
#define KFloat32(buf) (*(static_cast<float *>(static_cast<void *>(buf))))

// change char* memory to k_float64.
#define KFloat64(buf) (*(static_cast<k_float64 *>(static_cast<void *>(buf))))

// change char* memory to k_int16
#define KInt16(buf) (*(static_cast<int16_t *>(static_cast<void *>(buf))))

// change char* memory to k_int32
#define KInt32(buf) (*(static_cast<int32_t *>(static_cast<void *>(buf))))

// change char* memory to k_int64
#define KInt64(buf) (*(static_cast<int64_t *>(static_cast<void *>(buf))))

// change char* memory to k_double64
#define KDouble64(buf) (*(static_cast<double *>(static_cast<void *>(buf))))

// change char* memory to k_bool
#define KBool(buf) (*(static_cast<k_bool *>(static_cast<void *>(buf))))

// change char* memory to k_char
#define KChar(buf) (*(static_cast<k_char *>(static_cast<void *>(buf))))

// change char* memory to pointer
#define KVoidPtr(buf) (*((void **)(buf)))  // NOLINT

#define SINGLE_PROCESS 1

// definition of atomic operation macros related to x86 and arm platforms
}  // namespace kwdbts

#endif  // COMMON_SRC_INCLUDE_KWDB_TYPE_H_
