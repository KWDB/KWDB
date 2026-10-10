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

#ifndef COMMON_SRC_H_CM_FUNC_H_
#define COMMON_SRC_H_CM_FUNC_H_

#include <string.h>
#include <utility>
#include "cm_assert.h"
#include "cm_kwdb_context.h"
#include "kwdb_type.h"
#include "cm_trace_plugin.h"

namespace kwdbts {

// Get timestamp from KWDB routine thread.
KTimestamp getCurrentTime();

// Get file name only from file path
const char *GetFileName(const char *path);

// Convert time_t to gmtime.
void ToGMT(time_t ts, tm &tm);

#ifdef K_DEBUG
#define EnterFunc()                                                                              \
  {                                                                                              \
    Assert(ctx->frame_level < CTX_MAX_FRAME_LEVEL);                                              \
    strncpy(ctx->file_name[ctx->frame_level], GetFileName(__FILE__), CTX_MAX_FILE_NAME_LEN - 1); \
    strncpy(ctx->func_name[ctx->frame_level], __FUNCTION__, CTX_MAX_FUNC_NAME_LEN - 1);          \
    ctx->func_start_time[ctx->frame_level] = getCurrentTime();                                   \
    ctx->frame_level++;                                                                          \
  }                                                                                              \
  const k_uint8 my_frame_level = ctx->frame_level;                                               \
  void* scoped_span__ = START_SPAN(ctx);

#else
#ifdef K_OPENTRACING
#define EnterFunc()  void* scoped_span__ = START_SPAN(ctx);
#else
#define EnterFunc()
#endif
#endif

/* When a function exits, we do following:
 * 1) computing max stack size;
 * 2) computing function execution time(including children);
 * TODO:
 * 3) validate error_stack has errors when returning KFAIL
 *             error_stack has no error when returning KSUCCESS
 */

#ifdef K_DEBUG
// caller to make sure ctx is a valid pointer
void ReturnIntl(kwdbContext_p ctx);

#define Return(x)                               \
  {                                             \
    Assert(my_frame_level == ctx->frame_level); \
    END_SPAN(ctx, scoped_span__);               \
    ReturnIntl(ctx);                            \
    return (x);                                 \
  }

#define ReturnVoid()                            \
  {                                             \
    Assert(my_frame_level == ctx->frame_level); \
    END_SPAN(ctx, scoped_span__);               \
    ReturnIntl(ctx);                            \
    return;                                     \
  }

#else

#define Return(x) \
  { END_SPAN(ctx, scoped_span__); return (x); }

#define ReturnVoid() \
  { END_SPAN(ctx, scoped_span__); return; }

#endif

// Determine whether the condition is met. If it is, push the error to the ctx stack and return FAIL
#define PushErrAndReturnFail(err_code, ...)                \
  {                                                        \
    PUSH_ERR(err_code, ##__VA_ARGS__);                     \
    LOG_ERROR("err_code = %u", err_code);                  \
    LOG_ERROR(_CHECKER_##err_code, ##__VA_ARGS__);         \
    Return(KStatus::FAIL);                                 \
  }

// Check whether the condition is met. If it is, push the error to the ctx stack and return FAIL
#define PushErrAndReturnFailIfMatch(cond, err_code, ...)     \
  {                                                          \
    if ((cond)) {                                            \
      PushErrAndReturnFail(err_code, ##__VA_ARGS__);         \
    }                                                        \
  }

// Create a free thread ctx, whether it is possible to reuse the existing ctx of the thread needs further processing
#define CreateLocalContext() \
  kwdbContext_p ctx = ContextManager::GetThreadContext();

// Reuse the existing ctx, the current thread is a thread in the thread pool
#define CreateThreadContext()       \
  kwdbContext_p ctx = ContextManager::GetThreadContext();

#define DeleteCharArray(tmp)        \
  do {                              \
    if (tmp) {                      \
      delete[] tmp;                 \
      tmp = nullptr;                \
    }                               \
  } while (0);

/**
 * @brief bitmap format
 *        If there are 10 columns in the metadata, the bitmap of a record occupies 2 bytes.
 *        Assuming that the data written in columns 1, 3, 6, 9, and 10 of a record is not empty,
 *        the reading format of the bitmap is '0101101100'.
 *        The storage format is as follows, where N is equal to 2:
 *
 * +------------------+------------------+
 * | 0010 0101 (8bit) | 0000 0011 (8bit) |
 * +------------------+------------------+
 * |<-------- bitmap (N * 8bit) -------->|
 *
 */

/**
 * @brief Set the bit corresponding to the col_index column in the bitmap of a record to 0
 * @param[in] bitmap         Pointer to bitmap
 *                           Whether the value of a column is valid is indicated by the value
 *                           of a corresponding bit in the bitmap. The memory space pointed to by the
 *                           input parameter bitmap should be sufficient to represent all columns.
 *                           This requirement needs to be ensured by the caller,
 *                           and no internal judgment is made by the function
 * @param[in] col_index      It represents the column number, starting from 0.
 *                           The 0th column is the timestamp,
 *                           and the values in the timestamp column are all valid by default
 */
void SetObjectColNotNull(char* bitmap, k_uint32 col_index);

/**
 * @brief Set the bit corresponding to the col_index column in the bitmap of a record to 1
 * @param[in] bitmap         Pointer to bitmap
 *                           Whether the value of a column is valid is indicated by the value
 *                           of a corresponding bit in the bitmap. The memory space pointed to by the
 *                           input parameter bitmap should be sufficient to represent all columns.
 *                           This requirement needs to be ensured by the caller,
 *                           and no internal judgment is made by the function
 * @param[in] col_index      It represents the column number, starting from 0.
 *                           The 0th column is the timestamp,
 *                           and the values in the timestamp column are all valid by default
 */
void SetObjectColNull(char* bitmap, k_uint32 col_index);

/**
 * @brief Determine whether the bit corresponding to the col_index column in the bitmap of a record is 1
 * @param[in] bitmap         Pointer to bitmap
 *                           Whether the value of a column is valid is indicated by the value
 *                           of a corresponding bit in the bitmap. The memory space pointed to by the
 *                           input parameter bitmap should be sufficient to represent all columns.
 *                           This requirement needs to be ensured by the caller,
 *                           and no internal judgment is made by the function
 * @param[in] col_index      It represents the column number, starting from 0.
 *                           The 0th column is the timestamp,
 *                           and the values in the timestamp column are all valid by default
 *
 * @return k_bool
 */
// inline k_bool IsObjectColNull(const char* bitmap, k_uint32 col_index);
inline k_bool IsObjectColNull(const char* bitmap, k_uint32 col_index) {
  if (!bitmap) {
    return KTRUE;
  }
  const k_uint32 bit = col_index & 0x00000007;
  const k_uint32 idx = col_index >> 3;

  if (bitmap[idx] & (1 << bit)) {
    return KTRUE;
  }
  return KFALSE;
}


#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect((x), 1))
#define UNLIKELY(x) (__builtin_expect((x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif

#define STD_MOVE(a, b) { \
  try { \
  a = std::move(b); \
  } catch (...) { \
    PUSH_ERR_1(IN_EXEC_ERR, "std::move failed."); \
    Return(FAIL); \
  } \
}

#define STD_ATOMIC_EXCHANGE(a, b) { \
  try { \
  std::atomic_exchange(a, b); \
  } catch (...) { \
    PUSH_ERR_1(IN_EXEC_ERR, "std::atomic_exchange failed."); \
    Return(FAIL); \
  } \
}

#define KDP_CONTAINER_OK(a) { \
  if (!ok) { \
    PUSH_ERR_1(IN_EXEC_ERR, "KDP container operation faild."); \
    Return(FAIL); \
  } \
}


#define K_INT64_FMT PRId64
#define K_UINT64_FMT PRIu64
}  //  namespace kwdbts

#endif  // COMMON_SRC_H_CM_FUNC_H_
