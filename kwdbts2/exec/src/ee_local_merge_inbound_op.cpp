// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_local_merge_inbound_op.h"
#include "lg_api.h"
#include "ee_sort_op.h"

namespace kwdbts {

KStatus LocalMergeInboundOperator::PullChunk(kwdbContext_p ctx,
                                             DataChunkPtr& chunk) {
  // lock
  std::unique_lock l(chunk_lock_);
  while (true) {
    // all children finish break, otherwise wait
    if (is_finished_) {
      break;
    }
    // wait for data
    wait_cond_.wait_for(l, std::chrono::seconds(2));
    if (local_inbound_code_ != EEIteratorErrCode::EE_OK) {
      chunk = nullptr;
      break;
    }
    continue;
  }

  if (chunks_.empty()) {
    chunk = nullptr;
    return KStatus::FAIL;
  }

  if (nullptr == chunk) {
    chunk = std::make_unique<DataChunk>(chunks_.front()->GetColumnInfo(), chunks_.front()->ColumnNum());
    if (chunk->Initialize() != true) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY, "Insufficient memory");
      chunk = nullptr;
      LOG_ERROR("create sort chunk Insufficient memory.");
      return KStatus::FAIL;
    }
  }
  size_t buffer_size = 0;
  for (auto& b : chunks_) {
    buffer_size += b->RowSize() * b->Count();
    total_rows_ += b->Count();
  }
  k_bool is_mem_sort = true;
  if (buffer_size > BaseOperator::DEFAULT_MAX_MEM_BUFFER_SIZE) {
    is_mem_sort = false;
  }

  if (KStatus::SUCCESS != CreateSortContainer(buffer_size, chunks_,
                                              chunks_.front()->GetColumnInfo(),
                                              chunks_.front()->ColumnNum(),
                                              is_mem_sort)) {
    chunk = nullptr;
    LOG_ERROR("Create sort container failed.");
    return KStatus::FAIL;
  }
  // Sort
  container_->Sort();

  for (size_t i = 0; i < container_->Count(); i++) {
    k_int32 row = container_->NextLine();
    if (row < 0) {
      break;
    }
    chunk->InsertData(ctx, container_.get(), nullptr);
  }
  return KStatus::SUCCESS;
}

KStatus LocalMergeInboundOperator::CreateSortContainer(
    k_uint32 size, std::deque<DataChunkPtr>& buffer, ColumnInfo* col_info,
    k_int32 col_num, k_bool is_mem) {
  // create container
  KStatus ret = SUCCESS;
  if (is_mem) {
    container_ =
        std::make_unique<MemRowContainer>(order_info_, col_info, col_num);
  } else {
    container_ =
        std::make_unique<DiskDataContainer>(order_info_, col_info, col_num);
  }
  ret = container_->Init();
  if (ret != SUCCESS) {
    container_ = nullptr;
    return ret;
  }
  // copy buffer to container
  if (!buffer.empty()) {
    std::queue<DataChunkPtr> buf(std::deque<DataChunkPtr>(std::move(buffer)));
    ret = container_->Append(buf);
    if (ret != SUCCESS) {
      return ret;
    }
    // buffer.clear();
  }


  return ret;
}

}  // namespace kwdbts
