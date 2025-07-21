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
#include <vector>
#include <string>
#include <list>
#include "libkwdbts2.h"
#include "ts_io.h"
#include "ts_file_vector_index.h"

namespace kwdbts {

const char COUNT_FILE_NAME[] = "entity_count.item";

struct TsEntityCountHeader {
  uint64_t entity_id;
  timestamp64 min_ts;
  timestamp64 max_ts;
  uint64_t valid_count;
  bool is_count_valid;
  bool changed_aft_prepare;
  char reserverd[89];
};
static_assert(sizeof(TsEntityCountHeader) == 128, "wrong size of FileHeader, please check TsEntityCountHeader.");

struct TsEntityFlushInfo {
  TSEntityID entity_id;
  timestamp64 min_ts;
  timestamp64 max_ts;
  uint64_t depulcate_count;
  char reserverd[32];
};
static_assert(sizeof(TsEntityFlushInfo) == 64, "wrong size of FileHeader, please check TsEntityFlushInfo.");


class TsPartitionEntityCountManager {
 private:
  std::string path_;
  TsMMapAllocFile mmap_alloc_;
  // get offset of first index node.
  VectorIndexForFile<uint64_t> index_;
  KRWLatch* rw_lock_{nullptr};

 public:
  explicit TsPartitionEntityCountManager(std::string path);
  ~TsPartitionEntityCountManager();
  KStatus Open();
  KStatus AddFlushEntityAgg(TsEntityFlushInfo& info);
  KStatus SetEntityCountInValid(TSEntityID e_id, const KwTsSpan& del_range);
  KStatus PrepareEntityCountValid(TSEntityID e_id);
  KStatus SetEntityCountValid(TSEntityID e_id, TsEntityFlushInfo* info);
  KStatus GetEntityCount(TSEntityID e_id, bool* valid, uint64_t* count);
  void DropAll();
  KStatus Reset();

 private:
  KStatus updateEntityCount(TsEntityCountHeader* header, TsEntityFlushInfo* info, bool update_ts = true);
};


}  // namespace kwdbts
