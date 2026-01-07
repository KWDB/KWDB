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

enum TsEntityDelItemStatus : uint8_t {
  DEL_ITEM_OK = 1,
  DEL_ITEM_ROLLBACK,
  DEL_ITEM_DROPPED,
};

enum TsEntityDelItemType : uint8_t {
  DEL_ITEM_TYPE_USER = 1,
  DEL_ITEM_TYPE_OTHER,
};

// delete item info for ceratin entity.
struct TsEntityDelItem {
  STDelRange range;
  TSEntityID entity_id;  // entity id in vgroup.
  TsEntityDelItemStatus status;
  TsEntityDelItemType type;  // is this delete caused by user operation.
  TsEntityDelItem(const KwTsSpan& ts, const KwOSNSpan& lsn, TSEntityID e_id,
    TsEntityDelItemType t = TsEntityDelItemType::DEL_ITEM_TYPE_USER) : range{ts, lsn},
    entity_id(e_id), status(DEL_ITEM_OK), type(t) {}
};

class LSNRangeUtil {
 public:
  static std::vector<STScanRange> MergeScanAndDelRange(const std::vector<STScanRange>& ranges, const STDelRange& del);
  static void MergeRangeCross(const STScanRange& range, const STDelRange& del, std::vector<STScanRange>* result);
  static inline bool IsTSRangeNoCross(const KwTsSpan& span1, const KwTsSpan& span2) {
    return (span1.begin > span2.end || span1.end < span2.begin);
  }
  static inline bool IsSpan1IncludeSpan2(const KwTsSpan& span1, const KwTsSpan& span2) {
    return span1.begin <= span2.begin && span1.end >= span2.end;
  }
  static inline bool IsSpan1IncludeSpan2(const KwOSNSpan& span1, const KwOSNSpan& span2) {
    return span1.begin <= span2.begin && span1.end >= span2.end;
  }
  static inline bool IsSpan1CrossSpan2(const KwOSNSpan& span1, const KwOSNSpan& span2) {
    return  !(span1.begin > span2.end || span1.end < span2.begin);
  }
};

const char DEL_FILE_NAME[] = "partition_del.item";

class TsDelItemManager {
 private:
  struct IndexNode {
    uint64_t pre_node_offset;
    TsEntityDelItem del_item;
  };
  struct DelItemHeader {
    uint64_t max_entity_id;
    uint64_t delitem_num;
    uint64_t dropped_num;
    TS_OSN clear_max_lsn;
    TS_OSN min_lsn;
    TS_OSN max_lsn;
    char reserved[80];
  };
  static_assert(sizeof(DelItemHeader) == 128, "wrong size of DelItemHeader, please check compatibility.");
  std::string path_;
  TsMMapAllocFile mmap_alloc_;
  DelItemHeader* header_{nullptr};
  // get offset of first index node.
  VectorIndexForFile<uint64_t> index_;
  KRWLatch* rw_lock_{nullptr};

 public:
  explicit TsDelItemManager(std::string path);
  ~TsDelItemManager();
  KStatus Open();
  KStatus AddDelItem(TSEntityID entity_id, const TsEntityDelItem& del_item);
  KStatus RollBackDelItem(TSEntityID entity_id, const KwOSNSpan& lsn);
  KStatus GetDelItem(TSEntityID entity_id, std::list<TsEntityDelItem*>& del_items);
  KStatus GetDelRange(TSEntityID entity_id, std::list<STDelRange>& del_range);
  KStatus GetDelRangeByOSN(TSEntityID entity_id, std::vector<KwOSNSpan>& osn_span, std::list<KwTsSpan>& del_range);
  KStatus GetDelRangeWithOSN(TSEntityID entity_id, std::vector<KwOSNSpan>& osn_span,
    std::list<STDelRange>& del_range);
  KStatus HasValidDelItem(const KwOSNSpan& lsn, bool& has_valid);
  KStatus RmDeleteItems(TSEntityID entity_id, const KwOSNSpan &lsn);
  KStatus DropEntity(TSEntityID entity_id);
  uint64_t GetTotalNum() { return header_->delitem_num; }
  uint64_t GetDroppedNum() { return header_->dropped_num; }
  TS_OSN GetMaxLsn() { return header_->max_lsn; }
  TS_OSN GetMinLsn() { return header_->min_lsn; }
  TS_OSN GetClearMaxLsn() { return header_->clear_max_lsn; }
  uint64_t GetMaxEntityId() { return header_->max_entity_id; }
  KStatus GetDelMaxOSN(TSEntityID entity_id, uint64_t& max_osn);
  void DropAll();
  KStatus Reset();
};


}  // namespace kwdbts
