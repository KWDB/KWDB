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

#include "ee_mempool.h"

#include "ee_global.h"
#include "lg_api.h"

namespace kwdbts {
kwdbts::EE_PoolInfoDataPtr g_pstBufferPoolInfo;
EE_PoolInfoDataPtr EE_MemPoolInit(k_uint32 numOfBlock, k_uint32 blockSize) {
  if (numOfBlock <= 0 || blockSize <= 0) {
    LOG_ERROR("invalid parameter numOfBlockL %d, blockSize: %d\n", numOfBlock, blockSize);
    return nullptr;
  }

  EE_PoolInfoDataPtr pstPoolMsg = nullptr;
  pstPoolMsg = KNEW(EE_PoolInfoData);
  if (pstPoolMsg == nullptr) {
    LOG_ERROR("ee pool new failed .\n");
    return nullptr;
  }
  pstPoolMsg->iFreeList = KNEW k_uint32[numOfBlock];
  if (pstPoolMsg->iFreeList == nullptr) {
    LOG_ERROR("failed to malloc memory, malloc numOfBlock: %d\n", numOfBlock);
    delete pstPoolMsg;
    return nullptr;
  }
  pstPoolMsg->iDataIndex = 0;

  pstPoolMsg->data_ = nullptr;
  k_uint64 iSumSize = (k_uint64)blockSize * (k_uint64)numOfBlock;
#ifdef MAP_HUGETLB
  pstPoolMsg->data_ = reinterpret_cast<char *>(mmap(NULL, iSumSize, PROT_READ | PROT_WRITE,
                                             MAP_ANONYMOUS | MAP_PRIVATE | MAP_HUGETLB | MAP_POPULATE, -1, 0));
#else
  pstPoolMsg->data_ = KNEW char[iSumSize];
#endif
  if (pstPoolMsg->data_ == nullptr) {
    LOG_ERROR("failed to malloc memory, malloc sum size: %ld\n", iSumSize);
    SafeDeleteArray(pstPoolMsg->iFreeList);
    delete pstPoolMsg;
    return nullptr;
  }

  pstPoolMsg->iBlockSize = blockSize;
  pstPoolMsg->iNumOfSumBlock = numOfBlock;
  pstPoolMsg->iSumOffset = iSumSize;

  std::memset(pstPoolMsg->data_, 0, iSumSize);
  for (k_uint32 i = 0; i < numOfBlock; ++i) {
    pstPoolMsg->iFreeList[i] = i;
  }

  pstPoolMsg->iNumOfFreeBlock = numOfBlock;

  pstPoolMsg->is_pool_init_ = true;

  return pstPoolMsg;
}

k_char *EE_MemPoolMalloc(kwdbts::EE_PoolInfoDataPtr pstPoolMsg, k_size_t iMallocLen) {
  k_char *data = nullptr;
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return nullptr;
  }
  if (iMallocLen > ROW_BUFFER_SIZE) {
    data = KNEW char[iMallocLen];
    // LOG_INFO("malloc non-anticipatory address: %p, iMallocLen: %ld\n", data, iMallocLen);
    return data;
  }

  std::lock_guard<std::mutex> lock(pstTemPtr->lock_);
  if (pstTemPtr->iNumOfFreeBlock > 0) {
    data = pstTemPtr->data_ + (k_uint64)pstTemPtr->iBlockSize * (k_uint64)(pstTemPtr->iFreeList[pstTemPtr->iDataIndex]);
    pstTemPtr->iDataIndex++;
    pstTemPtr->iDataIndex = pstTemPtr->iDataIndex % pstPoolMsg->iNumOfSumBlock;
    pstTemPtr->iNumOfFreeBlock--;
  } else {
    // LOG_ERROR("buffer pool malloc failed, malloc out of memory iMallocLen: %ld\n", iMallocLen);
  }

  return data;
}

kwdbts::KStatus EE_MemPoolFree(kwdbts::EE_PoolInfoDataPtr pstPoolMsg, k_char *data) {
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return kwdbts::FAIL;
  }
  if (data == nullptr) {
    return kwdbts::FAIL;
  }
  kwdbts::k_int64 iCheckOffset = data - pstTemPtr->data_;
  if ((iCheckOffset < 0) || (iCheckOffset > pstPoolMsg->iSumOffset)) {
    SafeDeleteArray(data);
    return kwdbts::SUCCESS;
  }

  // caclulate pool offset
  if ((iCheckOffset % pstTemPtr->iBlockSize) != 0) {
    LOG_ERROR("invalid free address:%p\n", data);
    return kwdbts::FAIL;
  }
  k_uint32 iOffset = 0;
  iOffset = (iCheckOffset / pstTemPtr->iBlockSize);
  if (iOffset < 0 || iOffset >= pstTemPtr->iNumOfSumBlock) {
    LOG_ERROR("iOffset: error, iOffset: %d invalid address:%p\n", iOffset, data);
    return kwdbts::FAIL;
  }

  memset(data, 0, pstTemPtr->iBlockSize);
  std::lock_guard<std::mutex> lock(pstTemPtr->lock_);
  pstTemPtr->iFreeList[(pstTemPtr->iDataIndex + pstTemPtr->iNumOfFreeBlock) % pstTemPtr->iNumOfSumBlock] = iOffset;
  pstTemPtr->iNumOfFreeBlock++;
  return kwdbts::SUCCESS;
}

kwdbts::KStatus EE_MemPoolCleanUp(kwdbts::EE_PoolInfoDataPtr pstPoolMsg) {
  EE_PoolInfoDataPtr pstTemPtr = pstPoolMsg;
  if (pstTemPtr == nullptr) {
    return kwdbts::FAIL;
  }
  {
    std::lock_guard<std::mutex> lock(pstTemPtr->lock_);
    if (pstPoolMsg->is_pool_init_ == false) {
      return kwdbts::FAIL;
    }
#ifdef MAP_HUGETLB
    if (pstTemPtr->data_ != nullptr) {
      munmap(pstTemPtr->data_, pstTemPtr->iSumOffset);
    }
#else
    SafeDeleteArray(pstTemPtr->data_);
#endif
    SafeDeleteArray(pstTemPtr->iFreeList);
  }
  delete pstTemPtr;
  pstTemPtr = nullptr;
  return kwdbts::SUCCESS;
}
}  // namespace kwdbts
