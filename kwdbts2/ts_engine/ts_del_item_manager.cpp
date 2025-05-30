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

#include <assert.h>
#include "ts_del_item_manager.h"

namespace kwdbts {

std::vector<STScanRange> LSNRangeUtil::MergeScanAndDelRange(const std::vector<STScanRange>& ranges, const STDelRange& del) {
  std::vector<STScanRange> result;
  result.reserve(ranges.size());
  for ( auto& range : ranges) {
    MergeRangeCross(range, del, &result);
  }
  return std::move(result);
}

#define  IsMinLSN(lsn) (lsn == 0)
#define  IsMaxLSN(lsn) (lsn == UINT64_MAX)
#define  IsMinTS(ts) (ts == INT64_MIN)
#define  IsMaxTS(ts) (ts == INT64_MAX)

void LSNRangeUtil::MergeRangeCross(const STScanRange& range, const STDelRange& del, std::vector<STScanRange>* result) {
  if (IsTSRangeNoCross(range.ts_span, del.ts_span)) {
    result->push_back(range);
    return;
  }
  STScanRange cross;
  cross.ts_span.begin = std::max(range.ts_span.begin, del.ts_span.begin);
  cross.ts_span.end = std::min(range.ts_span.end, del.ts_span.end);
  assert(cross.ts_span.begin <= cross.ts_span.end);
  if (IsSpan1IncludeSpan2(range.lsn_span, del.lsn_span)) {
    // range.lsn span  1------------------8
    // del.lsn span        3--------5
    // result lsn      1-2            6--8 
    if (range.lsn_span.begin < del.lsn_span.begin) {
      cross.lsn_span.begin = range.lsn_span.begin;
      cross.lsn_span.end = del.lsn_span.begin - (IsMinLSN(del.lsn_span.begin) ? 0 : 1);
      result->push_back(cross);
    }
    if (range.lsn_span.end > del.lsn_span.end) {
      cross.lsn_span.begin = del.lsn_span.end + (IsMaxLSN(del.lsn_span.end) ? 0 : 1);
      cross.lsn_span.end = range.lsn_span.end;
      result->push_back(cross);
    }
  } else {
    // range.lsn span  1------------------8
    // del.lsn span        3---------------------11
    // result lsn      1-2
    if (range.lsn_span.end >= del.lsn_span.begin) {
      cross.lsn_span.begin = range.lsn_span.begin;
      cross.lsn_span.end = del.lsn_span.begin - (IsMinLSN(del.lsn_span.begin) ? 0 : 1);
      if (cross.lsn_span.begin <= cross.lsn_span.end) {
        result->push_back(cross);
      }
    }
    // range.lsn span                4----------8
    // del.lsn span        1--------------6
    // result lsn                           7---8
    if (range.lsn_span.begin >= del.lsn_span.end) {
      cross.lsn_span.begin = del.lsn_span.end + (IsMaxLSN(del.lsn_span.end) ? 0 : 1);
      cross.lsn_span.end = range.lsn_span.end;
      if (cross.lsn_span.begin <= cross.lsn_span.end) {
        result->push_back(cross);
      }
    }
    // range.lsn span                4----------8
    // del.lsn span        1-----------------------11
    // result lsn    no span left.
  }

  // range.ts span                4----------8
  // del.ts   span        1-----------------------11
  // result   ts          1------3
  if (cross.ts_span.begin > range.ts_span.begin) {
    STScanRange front_part;
    front_part.ts_span.begin = range.ts_span.begin;
    front_part.ts_span.end = cross.ts_span.begin - (IsMinTS(cross.ts_span.begin) ? 0 : 1);
    front_part.lsn_span = range.lsn_span;
    result->push_back(front_part);
  }
  // range.ts span                4----------8
  // del.ts   span        1------------6
  // result   ts                         7---8
  if (cross.ts_span.end < range.ts_span.end) {
    STScanRange end_part;
    end_part.ts_span.begin = cross.ts_span.end + (IsMaxTS(cross.ts_span.end) ? 0 : 1);
    end_part.ts_span.end = range.ts_span.end;
    end_part.lsn_span = range.lsn_span;
    result->push_back(end_part);
  }
}

TsDelItemManager::TsDelItemManager(std::string path) : path_(path + "/" + DEL_FILE_NAME), mmap_alloc_(path_),
                index_(&mmap_alloc_, mmap_alloc_.GetStartPos()) {
  rw_lock_ = new KRWLatch(RWLATCH_ID_MMAP_DEL_ITEM_MGR_RWLOCK);
}

TsDelItemManager::~TsDelItemManager(){
  if (rw_lock_) {
    delete rw_lock_;
  }
}

KStatus TsDelItemManager::AddDelItem(TSEntityID entity_id, const TsEntityDelItem& del_item) {
  auto offset = mmap_alloc_.AllocateAssigned(sizeof(IndexNode), INVALID_POSITION);
  auto new_node = reinterpret_cast<IndexNode*>(mmap_alloc_.GetAddrForOffset(offset, sizeof(IndexNode)));
  new_node->del_item = del_item;
  new_node->del_item.status = DEL_ITEM_OK;
  auto node = index_.GetIndexObject(entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu].", entity_id);
    return KStatus::FAIL;
  }
  {
    RW_LATCH_X_LOCK(rw_lock_);
    if (*node != INVALID_POSITION) {
      new_node->pre_node_offset = *node;
    }
    *node = offset;
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::GetDelItem(TSEntityID entity_id, std::list<TsEntityDelItem>& del_items) {
  auto node = index_.GetIndexObject(entity_id, false);
  if (node == nullptr) {
    // this entity has no del item.
    return KStatus::SUCCESS;
  }
  {
    RW_LATCH_S_LOCK(rw_lock_);
    auto cur_node_offset = *node;
    while (cur_node_offset != INVALID_POSITION) {
      auto cur_node = reinterpret_cast<IndexNode*>(mmap_alloc_.GetAddrForOffset(*node, sizeof(IndexNode)));
      if (cur_node == nullptr) {
        LOG_ERROR("GetAddrForOffset failed. offset [%lu]", *node);
        return KStatus::FAIL;
      }
      del_items.push_back(cur_node->del_item);
      cur_node_offset = cur_node->pre_node_offset;
    }
    RW_LATCH_UNLOCK(rw_lock_);
  }
  return KStatus::SUCCESS;
}


}  // namespace kwdbts
