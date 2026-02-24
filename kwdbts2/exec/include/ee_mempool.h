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

#include <atomic>
#include <mutex>
#include <vector>

#include "kwdb_type.h"

extern std::atomic<int64_t> g_malloc_memory_size;
namespace kwdbts {

#define NEWPOOL_MAX_SIZE (4096)  // default number of memory pool
#define ROW_BUFFER_SIZE (65536)  // default buffer size = 64k
// default string heap size = 1G
const int64_t MAX_STRING_HEAP_SIZE =  1024 * 1024 * 1024 * 6L;

typedef struct {
  k_uint32 iNumOfFreeBlock;  // amount of free block in the pool
  k_uint32 iNumOfSumBlock;   // amount of total block in the pool
  k_uint32 iBlockSize;       // block size
  k_uint32 iDataIndex;
  k_uint64 iSumOffset;
  k_uint64 iStringHeapSize{0};
  k_uint32 *iFreeList;
  k_char *data_;  // pointer to pool data
  bool is_pool_init_{false};
  std::mutex lock_;  // mutex to control buffer allocate and release
} EE_PoolInfoData;
typedef EE_PoolInfoData *EE_PoolInfoDataPtr;

kwdbts::EE_PoolInfoDataPtr EE_MemPoolInit(k_uint32 numOfBlock, k_uint32 blockSize);
k_char *EE_MemPoolMalloc(kwdbts::EE_PoolInfoDataPtr pstPoolMsg, k_size_t iMallocLen);
kwdbts::KStatus EE_MemPoolFree(kwdbts::EE_PoolInfoDataPtr pstPoolMsg, k_char *data);
kwdbts::KStatus EE_MemPoolCleanUp(kwdbts::EE_PoolInfoDataPtr pstPoolMsg);
extern kwdbts::EE_PoolInfoDataPtr g_pstBufferPoolInfo;
};  // namespace kwdbts
