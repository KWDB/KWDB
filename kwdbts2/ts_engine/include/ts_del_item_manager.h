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

// LSN range span info
struct KwLSNSpan {
  TS_LSN begin;
  TS_LSN end;
  inline bool Equal(const KwLSNSpan& desc) {
    return begin == desc.begin && end == desc.end;
  }
};

// LSN and TS range struct
struct TSAndLSNRange {
  KwTsSpan ts_span;
  KwLSNSpan lsn_span;
  TSAndLSNRange() {}
  TSAndLSNRange(const KwTsSpan& ts, const KwLSNSpan& lsn) : ts_span(ts), lsn_span(lsn) {}
};

// rename for understanding easily.
typedef TSAndLSNRange STDelRange;
typedef TSAndLSNRange STScanRange;

enum TsEntityDelItemStatus : uint8_t {
  DEL_ITEM_OK = 1,
  DEL_ITEM_ROLLBACK,
  DEL_ITEM_DROPPED,
};

// delete item info for ceratin entity.
struct TsEntityDelItem {
  STDelRange range;
  TSEntityID entity_id;  // entity id in vgroup.
  TsEntityDelItemStatus status;
  TsEntityDelItem(const KwTsSpan& ts, const KwLSNSpan& lsn, TSEntityID e_id) : range(ts, lsn),
    entity_id(e_id), status(DEL_ITEM_OK) {}
};

class LSNRangeUtil {
 public:
  static std::vector<STScanRange> MergeScanAndDelRange(const std::vector<STScanRange>& ranges, const STDelRange& del);
  static void MergeRangeCross(const STScanRange& range, const STDelRange& del, std::vector<STScanRange>* result);

 private:
  static inline bool IsTSRangeNoCross(const KwTsSpan& span1, const KwTsSpan& span2) {
    return (span1.begin > span2.end || span1.end < span2.begin);
  }
  static inline bool IsSpan1IncludeSpan2(const KwTsSpan& span1, const KwTsSpan& span2) {
    return span1.begin <= span2.begin && span1.end >= span2.end;
  }
  static inline bool IsSpan1IncludeSpan2(const KwLSNSpan& span1, const KwLSNSpan& span2) {
    return span1.begin <= span2.begin && span1.end >= span2.end;
  }
};

const char DEL_FILE_NAME[] = "partition_del.item";

class TsDelItemManager {
 private:
  struct IndexNode {
    uint64_t pre_node_offset;
    TsEntityDelItem del_item;
  };
  std::string path_;
  TsMMapAllocFile mmap_alloc_;
  // get offset of first index node.
  VectorIndexForFile<uint64_t> index_;
  KRWLatch* rw_lock_{nullptr};

 public:
  explicit TsDelItemManager(std::string path);
  ~TsDelItemManager();
  KStatus Open();
  KStatus AddDelItem(TSEntityID entity_id, const TsEntityDelItem& del_item);
  KStatus RollBackDelItem(TSEntityID entity_id, const KwLSNSpan& lsn);
  KStatus GetDelItem(TSEntityID entity_id, std::list<TsEntityDelItem>& del_items);
  KStatus GetDelRange(TSEntityID entity_id, std::list<STDelRange>& del_range);
  void DropAll();
  KStatus Reset();
};


}  // namespace kwdbts
