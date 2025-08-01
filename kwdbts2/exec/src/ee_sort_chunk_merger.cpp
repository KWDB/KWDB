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
    const std::vector<DataChunkProvider>& providers,
    const std::vector<k_uint32>* order_column, const SortDescs& sort_desc) {
  std::vector<std::unique_ptr<SimpleChunkSortCursor>> cursors;
  for (const auto& provider : providers) {
    cursors.push_back(
        std::make_unique<SimpleChunkSortCursor>(provider, order_column));
  }
  order_column_ = order_column;
  sort_desc_ = sort_desc;
  merger_ = std::make_unique<MergeCursorsCascade>();
  return merger_->Init(sort_desc_, std::move(cursors));
}
KStatus CascadeDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& providers,
    const std::vector<k_uint32>* order_column,
    const std::vector<k_bool>* sort_orders,
    const std::vector<k_bool>* null_firsts) {
  auto descs = SortDescs(*sort_orders, *null_firsts);
  if (KStatus::SUCCESS != Init(providers, order_column, descs)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

k_bool CascadeDataChunkMerger::IsDataReady() { return merger_->IsDataReady(); }

KStatus CascadeDataChunkMerger::GetNextChunk(DataChunkPtr* output,
                                             std::atomic<k_bool>* eos,
                                             k_bool* should_exit) {
  if (merger_->IsEos()) {
    *eos = true;
    *should_exit = true;
    return KStatus::SUCCESS;
  }

  DataChunkPtr chunk = merger_->TryGetNextChunk();
  if (!chunk) {
    *eos = merger_->IsEos();
    *should_exit = true;
    return KStatus::SUCCESS;
  }
  *output = std::move(chunk);

  return KStatus::SUCCESS;
}

ConstDataChunkMerger::ConstDataChunkMerger(kwdbContext_p ctx)
    : DataChunkMerger(ctx) {}

KStatus ConstDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& providers,
    const std::vector<k_uint32>* order_column, const SortDescs& sort_desc) {
  providers_ = providers;
  return KStatus::SUCCESS;
}
KStatus ConstDataChunkMerger::Init(
    const std::vector<DataChunkProvider>& providers,
    const std::vector<k_uint32>* order_column,
    const std::vector<k_bool>* sort_orders,
    const std::vector<k_bool>* null_firsts) {
  providers_ = providers;
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

KStatus ConstDataChunkMerger::GetNextChunk(DataChunkPtr* output,
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
