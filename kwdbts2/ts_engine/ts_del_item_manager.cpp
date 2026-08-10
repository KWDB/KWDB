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

#include <memory>
#include <algorithm>
#include <string>
#include <list>
#include <utility>
#include <vector>
#include "data_type.h"
#include "ts_del_item_manager.h"
#include "ts_ts_lsn_span_utils.h"
namespace kwdbts {
std::vector<STScanRange> LSNRangeUtil::MergeScanAndDelRange(const std::vector<STScanRange>& ranges, const STDelRange& del) {
  std::vector<STScanRange> result;
  result.reserve(ranges.size());
  for (auto& range : ranges) {
    MergeRangeCross(range, del, &result);
  }
  return result;
}

#define  IsMinLSN(lsn) ((lsn) == 0)
#define  IsMaxLSN(lsn) ((lsn) == UINT64_MAX)
#define  IsMinTS(ts) ((ts) == INT64_MIN)
#define  IsMaxTS(ts) ((ts) == INT64_MAX)

void LSNRangeUtil::MergeRangeCross(const STScanRange& range, const STDelRange& del, std::vector<STScanRange>* result) {
  if (IsTSRangeNoCross(range.ts_span, del.ts_span)) {
    result->push_back(range);
    return;
  }
  STScanRange cross;
  cross.ts_span.begin = std::max(range.ts_span.begin, del.ts_span.begin);
  cross.ts_span.end = std::min(range.ts_span.end, del.ts_span.end);
  assert(cross.ts_span.begin <= cross.ts_span.end);
  if (IsSpan1IncludeSpan2(range.osn_span, del.osn_span)) {
    // range.lsn span  1------------------8
    // del.lsn span        3--------5
    // result lsn      1-2            6--8
    if (range.osn_span.begin < del.osn_span.begin) {
      cross.osn_span.begin = range.osn_span.begin;
      cross.osn_span.end = del.osn_span.begin - (IsMinLSN(del.osn_span.begin) ? 0 : 1);
      result->push_back(cross);
    }
    if (range.osn_span.end > del.osn_span.end) {
      cross.osn_span.begin = del.osn_span.end + (IsMaxLSN(del.osn_span.end) ? 0 : 1);
      cross.osn_span.end = range.osn_span.end;
      result->push_back(cross);
    }
  } else {
    // range.lsn span  1------------------8
    // del.lsn span        3---------------------11
    // result lsn      1-2
    if (range.osn_span.begin <= del.osn_span.begin && del.osn_span.begin <= range.osn_span.end) {
      cross.osn_span.begin = range.osn_span.begin;
      cross.osn_span.end = del.osn_span.begin - (IsMinLSN(del.osn_span.begin) ? 0 : 1);
      if (cross.osn_span.begin <= cross.osn_span.end) {
        result->push_back(cross);
      }
    }
    // range.lsn span                4----------8
    // del.lsn span        1--------------6
    // result lsn                           7---8
    if (range.osn_span.begin <= del.osn_span.end && del.osn_span.end <= range.osn_span.end) {
      cross.osn_span.begin = del.osn_span.end + (IsMaxLSN(del.osn_span.end) ? 0 : 1);
      cross.osn_span.end = range.osn_span.end;
      if (cross.osn_span.begin <= cross.osn_span.end) {
        result->push_back(cross);
      }
    }
    // range.lsn span                4----------8
    // del.lsn span        1------3                  10----- 13
    // result lsn                    4----------8
    if (del.osn_span.end < range.osn_span.begin || del.osn_span.begin > range.osn_span.end) {
      cross.osn_span = range.osn_span;
      result->push_back(cross);
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
    front_part.osn_span = range.osn_span;
    result->push_back(front_part);
  }
  // range.ts span                4----------8
  // del.ts   span        1------------6
  // result   ts                         7---8
  if (cross.ts_span.end < range.ts_span.end) {
    STScanRange end_part;
    end_part.ts_span.begin = cross.ts_span.end + (IsMaxTS(cross.ts_span.end) ? 0 : 1);
    end_part.ts_span.end = range.ts_span.end;
    end_part.osn_span = range.osn_span;
    result->push_back(end_part);
  }
}

TsDelItemManager::TsDelItemManager(const std::string& path) : path_(path + "/" + DEL_FILE_NAME) {
  rw_lock_ = new KRWLatch(RWLATCH_ID_MMAP_DEL_ITEM_MGR_RWLOCK);
}

TsDelItemManager::~TsDelItemManager() {
  Sync();
  mmap_alloc_->Close();
  if (rw_lock_) {
    delete rw_lock_;
  }
}

KStatus TsDelItemManager::Open() {
  mmap_alloc_ = std::make_shared<TsMMapAllocFile>(path_);
  if (mmap_alloc_ == nullptr) {
    LOG_ERROR("mmap alloc file init failed.");
    return KStatus::FAIL;
  }
  if (mmap_alloc_->Open() == KStatus::SUCCESS) {
    if (mmap_alloc_->GetAllocSize() >= sizeof(DelItemHeader)) {
      header_ = reinterpret_cast<DelItemHeader*>(mmap_alloc_->addr(mmap_alloc_->GetStartPos()));
      if (header_->min_lsn == 0) {
        // actual osn cannot be 0.
        header_->min_lsn = UINT64_MAX;
      }
    } else {
      auto offset = mmap_alloc_->AllocateAssigned(sizeof(DelItemHeader), 0);
      header_ = reinterpret_cast<DelItemHeader*>(mmap_alloc_->addr(offset));
      header_->min_lsn = UINT64_MAX;
    }
    KStatus s = index_.Init(mmap_alloc_.get(), &(mmap_alloc_->getHeader()->index_header_offset));
    if (s == KStatus::SUCCESS) {
      return s;
    }
  }
  LOG_ERROR("deleteitem open failed.");
  return KStatus::FAIL;
}

KStatus TsDelItemManager::HasValidDelItem(const KwOSNSpan& lsn, bool& has_valid) {
  has_valid = false;
  for (size_t i = 1; i <= header_->max_entity_id; i++) {
    std::list<STDelRange> del_range;
    auto s = GetDelRange(i, del_range);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("GetDelRange failed. for entity Id [%lu]", i);
      return s;
    }
    for (STDelRange& cur_del : del_range) {
      if (LSNRangeUtil::IsSpan1CrossSpan2(cur_del.osn_span, lsn)) {
        has_valid = true;
        return KStatus::SUCCESS;
      }
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::RmDeleteItems(TSEntityID entity_id, const KwOSNSpan &lsn) {
  uint64_t total_dropped = 0;
  std::list<TsEntityDelItem*> del_range;
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = InsertPrepare();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginInsert failed. for entity Id [%lu]", entity_id);
    return s;
  }
  s = GetDelItem(entity_id, del_range);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelRange failed. for entity Id [%lu]", entity_id);
    return s;
  }
  for (TsEntityDelItem* cur_del : del_range) {
    if (LSNRangeUtil::IsSpan1IncludeSpan2(lsn, cur_del->range.osn_span)) {
      cur_del->status = TsEntityDelItemStatus::DEL_ITEM_DROPPED;
      total_dropped++;
    }
  }
  {
    header_->dropped_num += total_dropped;
    header_->clear_max_lsn = std::max(header_->clear_max_lsn, lsn.end);
    writing_num_.fetch_add(1);
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::AddDelItem(TSEntityID entity_id, const TsEntityDelItem& del_item) {
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = InsertPrepare();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginInsert failed. for entity Id [%lu]", entity_id);
    return s;
  }
  if (writeDelItem(entity_id, del_item) == nullptr) {
    LOG_ERROR("writeDelItem failed. entity_id [%lu]", entity_id);
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

TsEntityDelItem* TsDelItemManager::writeDelItem(TSEntityID entity_id, const TsEntityDelItem& del_item) {
  auto offset = mmap_alloc_->AllocateAssigned(sizeof(IndexNode), INVALID_POSITION);
  auto new_node = reinterpret_cast<IndexNode*>(mmap_alloc_->GetAddrForOffset(offset, sizeof(IndexNode)));
  if (new_node == nullptr) {
    return nullptr;
  }
  new_node->del_item = del_item;
  new_node->del_item.status = DEL_ITEM_OK;
  auto node = index_.GetIndexObject(entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu].", entity_id);
    return nullptr;
  }
  {
    if (*node != INVALID_POSITION) {
      new_node->pre_node_offset = *node;
    }
    *node = offset;
    header_->max_entity_id = std::max(header_->max_entity_id, entity_id);
    header_->delitem_num += 1;
    header_->min_lsn = std::min(header_->min_lsn, del_item.range.osn_span.begin);
    header_->max_lsn = std::max(header_->max_lsn, del_item.range.osn_span.end);
    writing_num_.fetch_add(1);
  }
  return &(new_node->del_item);
}

// inner function, called position lock.
KStatus TsDelItemManager::GetDelItem(TSEntityID entity_id, std::list<TsEntityDelItem*>& del_items) {
  auto node = index_.GetIndexObject(entity_id, false);
  if (node == nullptr) {
    // this entity has no del item.
    return KStatus::SUCCESS;
  }
  auto cur_node_offset = *node;
  while (cur_node_offset != INVALID_POSITION) {
    auto cur_node = reinterpret_cast<IndexNode*>(mmap_alloc_->GetAddrForOffset(cur_node_offset, sizeof(IndexNode)));
    if (cur_node == nullptr) {
      LOG_ERROR("GetAddrForOffset failed. offset [%lu]", *node);
      return KStatus::FAIL;
    }
    del_items.push_back(&(cur_node->del_item));
    cur_node_offset = cur_node->pre_node_offset;
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::DropEntity(TSEntityID entity_id) {
  // todo(liangbo01) entity deleted, but delete info should be seen.
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::RollBackDelItem(TSEntityID entity_id, const KwOSNSpan& lsn) {
  auto node = index_.GetIndexObject(entity_id, false);
  if (node == nullptr) {
    // this entity has no del item.
    return KStatus::SUCCESS;
  }
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = InsertPrepare();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("BeginInsert failed. for entity Id [%lu]", entity_id);
    return s;
  }
  uint64_t dropped_num = 0;
  {
    auto cur_node_offset = *node;
    while (cur_node_offset != INVALID_POSITION) {
      auto cur_node = reinterpret_cast<IndexNode*>(mmap_alloc_->GetAddrForOffset(cur_node_offset, sizeof(IndexNode)));
      if (cur_node == nullptr) {
        LOG_ERROR("GetAddrForOffset failed. offset [%lu]", *node);
        return KStatus::FAIL;
      }
      if (cur_node->del_item.range.osn_span.begin == lsn.begin &&
          cur_node->del_item.range.osn_span.end == lsn.end) {
        cur_node->del_item.status = DEL_ITEM_ROLLBACK;
        dropped_num++;
      }
      cur_node_offset = cur_node->pre_node_offset;
    }
  }
  {
    header_->dropped_num += dropped_num;
    writing_num_.fetch_add(1);
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::GetDelRange(TSEntityID entity_id, std::list<STDelRange>& del_range) {
  std::list<TsEntityDelItem*> del_items;
  RW_LATCH_S_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = GetDelItem(entity_id, del_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelItem failed. entity_id [%lu]", entity_id);
    return s;
  }
  for (auto& item : del_items) {
    if (item->status == DEL_ITEM_OK) {
      del_range.push_back(item->range);
    }
  }
  return KStatus::SUCCESS;
}

void TsDelItemManager::DropAll() {
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  writing_num_.store(0);
  is_writing_.store(false);
  index_.Reset();
  mmap_alloc_->DropAll();
  std::string cmd = "rm -rf " + path_ + "*";
  System(cmd);
}

KStatus TsDelItemManager::Reset() {
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  index_.Reset();
  Sync();
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::GetDelRangeByOSN(TSEntityID entity_id, std::vector<KwOSNSpan>& osn_span,
  std::list<KwTsSpan>& del_range) {
  std::list<TsEntityDelItem*> del_items;
  RW_LATCH_S_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = GetDelItem(entity_id, del_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelItemByOSN failed. entity_id [%lu]", entity_id);
    return s;
  }
  for (auto& item : del_items) {
    if (item->type == DEL_ITEM_TYPE_USER && IsOsnInSpans(item->range.osn_span.end, osn_span)) {
      del_range.push_back(item->range.ts_span);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::GetDelRangeWithOSN(TSEntityID entity_id, std::vector<KwOSNSpan>& osn_span,
  std::list<STDelRange>& del_range) {
  std::list<TsEntityDelItem*> del_items;
  RW_LATCH_S_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = GetDelItem(entity_id, del_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelItemByOSN failed. entity_id [%lu]", entity_id);
    return s;
  }
  for (auto& item : del_items) {
    if (item->type == DEL_ITEM_TYPE_USER && IsOsnInSpans(item->range.osn_span.end, osn_span)) {
      del_range.push_back(item->range);
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::GetDelMaxOSN(TSEntityID entity_id, TS_OSN& max_osn) {
  max_osn = 0;
  std::list<TsEntityDelItem*> del_items;
  RW_LATCH_S_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = GetDelItem(entity_id, del_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelItem failed. entity_id [%lu]", entity_id);
    return s;
  }
  for (auto& item : del_items) {
    max_osn = std::max(max_osn, item->range.osn_span.end);
  }
  return KStatus::SUCCESS;
}

// Inner function, called position lock xlock.
KStatus TsDelItemManager::InsertPrepare() {
  bool now = is_writing_.load();
  if (!now && is_writing_.compare_exchange_strong(now, true)) {
    auto desc_alloc = std::make_shared<TsMMapAllocFile>(path_ + "_writing");
    auto s = mmap_alloc_->CopyTo(desc_alloc.get());
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("CopyTo failed. path [%s]", path_.c_str());
      is_writing_.store(false);
      return s;
    }
    index_.RelocateFile(desc_alloc.get(), &(desc_alloc->getHeader()->index_header_offset));
    header_ = reinterpret_cast<DelItemHeader*>(desc_alloc->addr(desc_alloc->GetStartPos()));
    auto older_alloc = mmap_alloc_;
    mmap_alloc_ = desc_alloc;
    older_alloc->Close();
  }
  return KStatus::SUCCESS;
}

KStatus TsDelItemManager::Sync() {
  if (!is_writing_.load()) {
    return KStatus::SUCCESS;
  }
  {
    RW_LATCH_X_LOCK(rw_lock_);
    Defer _([this]() {
      RW_LATCH_UNLOCK(rw_lock_);
    });
    if (writing_num_.load() == 0) {
      return KStatus::SUCCESS;
    }
    // store to temp file.
    auto s = mmap_alloc_->SyncToFile(path_ + "_tmp");
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("SyncToFile failed. path [%s]", path_.c_str());
      return s;
    }
    // rename temp file to real file.
    if (rename((path_ + "_tmp").c_str(), path_.c_str()) < 0) {
      LOG_ERROR("rename failed. path [%s]", path_.c_str());
      return KStatus::FAIL;
    }
    writing_num_.store(0);
  }
  // continue using mmap file, just backup data to real file.
  return KStatus::SUCCESS;
}

// del_ranges is ordered by osn in ascending order. for function DeplicateTsSpans
// operating temp _writing file. so if core dump, the real file will ok.
KStatus TsDelItemManager::CoverDelRange(TSEntityID entity_id, const std::list<STDelRange>& del_ranges) {
  RW_LATCH_X_LOCK(rw_lock_);
  Defer _([this]() {
    RW_LATCH_UNLOCK(rw_lock_);
  });
  auto s = InsertPrepare();
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("InsertPrepare failed. entity_id [%lu]", entity_id);
    return s;
  }
  std::list<TsEntityDelItem*> del_items;
  s = GetDelItem(entity_id, del_items);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("GetDelItem failed. entity_id [%lu]", entity_id);
    return s;
  }
  if (del_ranges.empty()) {
    for (auto& item : del_items) {
      item->status = DEL_ITEM_DROPPED;
      item->type = DEL_ITEM_TYPE_OTHER;
    }
    return KStatus::SUCCESS;
  }
  TsEntityDelItem cur_del_item(del_ranges.front().ts_span, del_ranges.front().osn_span, entity_id);
  while (del_ranges.size() > del_items.size()) {
    auto store_item = writeDelItem(entity_id, cur_del_item);
    if (store_item == nullptr) {
      LOG_ERROR("writeDelItem failed. entity_id [%lu]", entity_id);
      return s;
    }
    del_items.push_front(store_item);
  }
  auto store_iter = del_items.begin();
  // write using osn descending order. because del_items is ordered by osn in descending order.
  auto cover_iter = del_ranges.rbegin();
  while (store_iter != del_items.end()) {
    if (cover_iter == del_ranges.rend()) {
      // Store item number greater than cover item number, reset left del item to zero.
      TsEntityDelItem* store_item = *store_iter;
      store_item->status = DEL_ITEM_DROPPED;
      store_item->type = DEL_ITEM_TYPE_OTHER;
      store_iter++;
    } else {
      TsEntityDelItem* store_item = *store_iter;
      store_item->range = *cover_iter;
      store_item->status = DEL_ITEM_OK;
      store_item->type = DEL_ITEM_TYPE_USER;
      store_iter++;
      cover_iter++;
    }
  }
  return KStatus::SUCCESS;
}

}  // namespace kwdbts
