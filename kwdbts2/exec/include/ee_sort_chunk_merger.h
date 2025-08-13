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

#pragma once
#include <functional>
#include <memory>
#include <queue>
#include <vector>

#include "cm_kwdb_context.h"
#include "ee_merge_cursor.h"
#include "ee_sort_desc.h"

namespace kwdbts {

class DataChunkMerger {
 public:
  explicit DataChunkMerger(kwdbContext_p ctx) {}
  virtual ~DataChunkMerger() = default;

  virtual KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
                       const std::vector<k_uint32>* sort_column_indices,
                       const SortingRules& sort_rules) = 0;
  virtual KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
                       const std::vector<k_uint32>* sort_column_indices,
                       const std::vector<k_bool>* sort_directions,
                       const std::vector<k_bool>* nullsFirstFlags) = 0;

  virtual k_bool IsDataReady() = 0;
  virtual KStatus GetNextMergeChunk(DataChunkPtr* merged_chunk, std::atomic<k_bool>* is_end_eos,
                               k_bool* should_exit) = 0;
};

// TODO(murphy) refactor it with CascadingCursorMerger
// Merge sorted chunks in cascade style
class CascadeDataChunkMerger : public DataChunkMerger {
 public:
  explicit CascadeDataChunkMerger(kwdbContext_p ctx);
  ~CascadeDataChunkMerger() = default;

  KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
               const std::vector<k_uint32>* sort_column_indices,
               const SortingRules& sort_rules) override;
  KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
               const std::vector<k_uint32>* sort_column_indices,
               const std::vector<k_bool>* sort_directions,
               const std::vector<k_bool>* nullsFirstFlags) override;

  k_bool IsDataReady() override;
  KStatus GetNextMergeChunk(DataChunkPtr* merged_chunk, std::atomic<k_bool>* is_end_eos,
                       k_bool* should_exit) override;

 private:
  std::unique_ptr<CascadingCursorMerger> cursor_merger_;
  const std::vector<k_uint32>* order_column_;
  SortingRules sort_rules_;
};

class ConstDataChunkMerger : public DataChunkMerger {
 public:
  explicit ConstDataChunkMerger(kwdbContext_p ctx);
  ~ConstDataChunkMerger() = default;

  KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
               const std::vector<k_uint32>* sort_column_indices,
               const SortingRules& sort_rules) override;
  KStatus Init(const std::vector<DataChunkProvider>& chunk_providers,
               const std::vector<k_uint32>* sort_column_indices,
               const std::vector<k_bool>* sort_directions,
               const std::vector<k_bool>* nullsFirstFlags) override;

  k_bool IsDataReady() override;
  KStatus GetNextMergeChunk(DataChunkPtr* merged_chunk, std::atomic<k_bool>* is_end_eos,
                       k_bool* should_exit) override;

 private:
  std::vector<DataChunkProvider> providers_;
};

}  // namespace kwdbts
