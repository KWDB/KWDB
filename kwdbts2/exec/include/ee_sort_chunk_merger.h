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

  virtual KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
                       const std::vector<k_uint32>* order_column,
                       const SortDescs& _sort_desc) = 0;
  virtual KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
                       const std::vector<k_uint32>* order_column,
                       const std::vector<k_bool>* sort_orders,
                       const std::vector<k_bool>* null_firsts) = 0;

  virtual k_bool IsDataReady() = 0;
  virtual KStatus GetNextChunk(DataChunkPtr* chunk, std::atomic<k_bool>* eos,
                               k_bool* should_exit) = 0;
};

// TODO(murphy) refactor it with MergeCursorsCascade
// Merge sorted chunks in cascade style
class CascadeDataChunkMerger : public DataChunkMerger {
 public:
  explicit CascadeDataChunkMerger(kwdbContext_p ctx);
  ~CascadeDataChunkMerger() = default;

  KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
               const std::vector<k_uint32>* order_column,
               const SortDescs& _sort_desc) override;
  KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
               const std::vector<k_uint32>* order_column,
               const std::vector<k_bool>* sort_orders,
               const std::vector<k_bool>* null_firsts) override;

  k_bool IsDataReady() override;
  KStatus GetNextChunk(DataChunkPtr* chunk, std::atomic<k_bool>* eos,
                       k_bool* should_exit) override;

 private:
  const std::vector<k_uint32>* order_column_;
  SortDescs sort_desc_;
  std::unique_ptr<MergeCursorsCascade> merger_;
};

class ConstDataChunkMerger : public DataChunkMerger {
 public:
  explicit ConstDataChunkMerger(kwdbContext_p ctx);
  ~ConstDataChunkMerger() = default;

  KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
               const std::vector<k_uint32>* order_column,
               const SortDescs& _sort_desc) override;
  KStatus Init(const std::vector<DataChunkProvider>& has_suppliers,
               const std::vector<k_uint32>* order_column,
               const std::vector<k_bool>* sort_orders,
               const std::vector<k_bool>* null_firsts) override;

  k_bool IsDataReady() override;
  KStatus GetNextChunk(DataChunkPtr* chunk, std::atomic<k_bool>* eos,
                       k_bool* should_exit) override;

 private:
  std::vector<DataChunkProvider> providers_;
};

}  // namespace kwdbts
