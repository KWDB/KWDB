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
#include <pthread.h>
#include <list>
#include <utility>
#include <vector>
#include <deque>

#include "ee_row_batch.h"
#include "ee_parallel_group.h"
#include "kwdb_type.h"

namespace kwdbts {

// relational batch queue to receive relational data from ME for multiple model processing
class RelBatchQueue {
 private:
  ColumnInfo* output_col_info_{nullptr};
  k_int32 output_col_num_{0};
  std::mutex mutex_;
  std::condition_variable cv;

  std::deque<DataChunkPtr> data_queue_;
  bool no_more_data_chunk{false};
  bool is_init_{false};
  bool is_error_{false};

 public:
  RelBatchQueue();
  ~RelBatchQueue();

  /**
   * @brief Initialize relational batch queue
   * @param[in] output_fields the output fields
   * @return Status
   */
  KStatus Init(std::vector<Field*> &output_fields);

  /**
   * @brief Add a batch of relational data into RelBatchQueue
   * @param[in] ctx       kwdb context
   * @param[in] batchData batch data buffer
   * @param[in] count     number of rows
   */
  KStatus Add(kwdbContext_p ctx, char *batchData, k_uint32 count);

  /**
   * @brief Get next batch of relational data from RelBatchQueue
   * @param[in] ctx     kwdb context
   * @param[out] chunk  data chunk
   * @return Iterator error code
   */
  EEIteratorErrCode Next(kwdbContext_p ctx, DataChunkPtr& chunk);

  /**
   * @brief Indicates all relational data chunks are added
   * @param[in] ctx     kwdb context
   * @return Iterator error code
   */
  EEIteratorErrCode Done(kwdbContext_p ctx);

  void NotifyError() {
    is_error_ = true;
    cv.notify_all();
  }
};

};  // namespace kwdbts
