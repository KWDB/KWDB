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

#ifndef KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_
#define KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_


#include <unordered_map>
#include <utility>
#include <vector>
#include <optional>
#include "cm_kwdb_context.h"
#include "libkwdbts2.h"

namespace kwdbts {

struct OperatorFetcher {
  void Update(k_int32 read_rows, k_int64 stall_time, k_int64 bytes_read, k_int64 max_memory_use,
                  k_int64 max_disk_use, k_int64 output_rows, k_int64 build_time = 0) {
    read_rows_ += read_rows;
    stall_time_ += stall_time;
    bytes_read_ += bytes_read;
    max_memory_use_ += max_memory_use;
    max_disk_use_ += max_disk_use;
    output_rows_ += output_rows;
    // build_time_ is only used for hash_tag_scan_op
    // denoting the hash index contruction time cost
    build_time_ += build_time;
  }

  void Reset() {
    read_rows_ = 0;
    stall_time_ = 0;
    bytes_read_ = 0;
    max_memory_use_ = 0;
    max_disk_use_ = 0;
    output_rows_ = 0;
    build_time_ = 0;
  }

  k_int32 read_rows_{0};
  k_int64 stall_time_{0};
  k_int64 bytes_read_{0};
  k_int64 max_memory_use_{0};
  k_int64 max_disk_use_{0};
  k_int64 output_rows_{0};
  k_int64 build_time_{0};
};

class TsFetcherCollection {
 public:
  ~TsFetcherCollection() {fetchers_map_.clear();}
  void emplace(int32_t processor_id, OperatorFetcher *fetcher) {
    auto it = fetchers_map_.find(processor_id);
    if (it != fetchers_map_.end()) {
      std::vector<OperatorFetcher *> &fetc_vec = it->second;
      fetc_vec.push_back(fetcher);
    } else {
      fetchers_map_.emplace(processor_id, std::vector<OperatorFetcher*>{fetcher});
    }
  }

  kwdbts::KStatus GetAnalyse(kwdbContext_p ctx) {
    auto *fetchers = static_cast<VecTsFetcher *>(ctx->fetcher);
    if (fetchers != nullptr && fetchers->collected) {
      for (int i = 0; i < fetchers->size; ++i) {
        TsFetcher *fetcher = &fetchers->TsFetchers[i];
        auto it = fetchers_map_.find(fetcher->processor_id);
        if (it != fetchers_map_.end()) {
          std::vector<OperatorFetcher *> &data = it->second;
          for (auto &f : data) {
            fetcher->row_num += f->read_rows_;
            fetcher->stall_time += f->stall_time_;
            fetcher->bytes_read += f->bytes_read_;
            fetcher->max_allocated_mem += f->max_memory_use_;
            fetcher->max_allocated_disk += f->max_disk_use_;
            fetcher->output_row_num += f->output_rows_;
            fetcher->build_time += f->build_time_;
          }
        }
      }
    }

    return KStatus::SUCCESS;
  }

  std::unordered_map<int32_t, std::vector<OperatorFetcher *>> fetchers_map_;
};

}  //  namespace kwdbts

#endif  // KWDBTS2_COMMON_INCLUDE_EXPLAIN_ANALYSE_H_
