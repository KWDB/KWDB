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

#include "ee_sort_chunk_merger.h"

namespace kwdbts {

CascadeDataChunkMerger::CascadeDataChunkMerger(kwdbContext_p ctx)
    : DataChunkMerger(ctx) {}

KStatus CascadeDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& chunk_providers,
    const std::vector<k_uint32>* sort_column_indices, const SortingRules& sort_rules) {
  std::vector<std::unique_ptr<SortedChunkCursor>> cursors;
  for (const auto& provider : chunk_providers) {
    cursors.push_back(
        std::make_unique<SortedChunkCursor>(provider, sort_column_indices));
  }
  order_column_ = sort_column_indices;
  sort_rules_ = sort_rules;
  cursor_merger_ = std::make_unique<CascadingCursorMerger>();
  return cursor_merger_->Init(sort_rules_, std::move(cursors));
}
KStatus CascadeDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& chunk_providers,
    const std::vector<k_uint32>* sort_column_indices,
    const std::vector<k_bool>* sort_directions,
    const std::vector<k_bool>* nullsFirstFlags) {
  auto descs = SortingRules(*sort_directions, *nullsFirstFlags);
  if (KStatus::SUCCESS != Init(chunk_providers, sort_column_indices, descs)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

k_bool CascadeDataChunkMerger::IsDataReady() { return cursor_merger_->IsDataReady(); }

KStatus CascadeDataChunkMerger::GetNextMergeChunk(DataChunkPtr* output,
                                             std::atomic<k_bool>* eos,
                                             k_bool* should_exit) {
  if (cursor_merger_->IsAtEnd()) {
    *eos = true;
    *should_exit = true;
    return KStatus::SUCCESS;
  }

  DataChunkPtr chunk = cursor_merger_->FetchNextSortedChunk();
  if (!chunk) {
    *eos = cursor_merger_->IsAtEnd();
    *should_exit = true;
    return KStatus::SUCCESS;
  }
  *output = std::move(chunk);

  return KStatus::SUCCESS;
}

ConstDataChunkMerger::ConstDataChunkMerger(kwdbContext_p ctx)
    : DataChunkMerger(ctx) {}

KStatus ConstDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& chunk_providers,
    const std::vector<k_uint32>* sort_column_indices, const SortingRules& sort_rules) {
  providers_ = chunk_providers;
  return KStatus::SUCCESS;
}
KStatus ConstDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& chunk_providers,
    const std::vector<k_uint32>* sort_column_indices,
    const std::vector<k_bool>* sort_directions,
    const std::vector<k_bool>* nullsFirstFlags) {
  providers_ = chunk_providers;
  return KStatus::SUCCESS;
}

k_bool ConstDataChunkMerger::IsDataReady() {
  for (const auto& p : providers_) {
    if (p(nullptr, nullptr)) {
      return true;
    }
  }
  return false;
}

KStatus ConstDataChunkMerger::GetNextMergeChunk(DataChunkPtr* output,
                                           std::atomic<k_bool>* eos,
                                           k_bool* should_exit) {
  k_bool all = true;
  k_bool c = false;
  for (const auto& p : providers_) {
    c = false;
    if (p(output, &c)) {
      *eos = false;
      return KStatus::SUCCESS;
    }
    all &= c;
  }

  *eos = all;
  if (*eos) {
    *should_exit = true;
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
