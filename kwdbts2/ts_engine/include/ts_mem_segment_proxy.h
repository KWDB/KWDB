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

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "kwdb_type.h"
#include "spin_mutex.h"
#include "spin_shared_mutex.h"
#include "ts_common.h"
#include "ts_entity_segment.h"
#include "ts_lastsegment.h"
#include "ts_mem_segment_mgr.h"
#include "ts_segment.h"

namespace kwdbts {

class TsEntitySegmentView {
 private:
  std::shared_ptr<TsEntitySegment> segment_;
  uint64_t min_readable_block_id_ = 0;

 public:
  TsEntitySegmentView() = default;
  explicit TsEntitySegmentView(std::shared_ptr<TsEntitySegment> entity_seg, uint64_t min_readable_block_id)
      : segment_(std::move(entity_seg)), min_readable_block_id_(min_readable_block_id) {}
  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                        const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        const std::shared_ptr<MMapMetricsTable>& scan_schema,
                        TsScanStats* ts_scan_stats = nullptr) const {
    if (!Valid()) {
      return SUCCESS;
    }
    return segment_->GetBlockSpans(filter, blocks, tbl_schema_mgr, scan_schema, min_readable_block_id_, ts_scan_stats);
  }

  KStatus GetMaxOSN(TSEntityID entity_id, TS_OSN& max_osn) const {
    if (!Valid()) {
      max_osn = 0;
      return SUCCESS;
    }
    return segment_->GetMaxOSN(entity_id, max_osn, min_readable_block_id_);
  }

  std::string GetFilePath() const {
    if (!Valid()) {
      return "null";
    }
    return segment_->GetPath();
  }

  bool Valid() const { return segment_ != nullptr; }
};

namespace mem_proxy_detail {
struct PartitionedSegment {
  std::shared_ptr<TsLastSegment> last_seg;
  TsEntitySegmentView entity_seg;

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                        const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        const std::shared_ptr<MMapMetricsTable>& scan_schema,
                        TsScanStats* ts_scan_stats = nullptr) const {
    KStatus s = SUCCESS;
    if (last_seg != nullptr) {
      s = last_seg->GetBlockSpans(filter, blocks, tbl_schema_mgr, scan_schema, ts_scan_stats);
      if (s == FAIL) {
        auto path = last_seg->GetFilePath();
        LOG_ERROR("GetBlockSpans failed on lastsegment %s", path.c_str());
        return s;
      }
    }
    if (entity_seg.Valid()) {
      s = entity_seg.GetBlockSpans(filter, blocks, tbl_schema_mgr, scan_schema, ts_scan_stats);
      if (s == FAIL) {
        auto path = entity_seg.GetFilePath();
        LOG_ERROR("GetBlockSpans failed on entitysegment %s", path.c_str());
        return s;
      }
    }
    return s;
  }

  KStatus GetMaxOSN(TSEntityID entity_id, TS_OSN& max_osn) const {
    max_osn = 0;
    if (last_seg != nullptr) {
      TS_OSN last_max_osn = 0;
      auto s = last_seg->GetMaxOSN(entity_id, last_max_osn);
      if (s == FAIL) {
        auto path = last_seg->GetFilePath();
        LOG_ERROR("GetMaxOSN failed on lastsegment %s", path.c_str());
        return s;
      }
      max_osn = std::max(max_osn, last_max_osn);
    }
    if (entity_seg.Valid()) {
      TS_OSN entity_max_osn = 0;
      auto s = entity_seg.GetMaxOSN(entity_id, entity_max_osn);
      if (s == FAIL) {
        auto path = entity_seg.GetFilePath();
        LOG_ERROR("GetMaxOSN failed on entitysegment %s", path.c_str());
        return s;
      }
      max_osn = std::max(max_osn, entity_max_osn);
    }
    return SUCCESS;
  }
};
}  // namespace mem_proxy_detail

class TsDiskSegmentHandle {
  friend class TsMemSegmentProxy;

 private:
  std::map<PartitionIdentifier, mem_proxy_detail::PartitionedSegment> disk_segments_;

 public:
  TsDiskSegmentHandle() = default;
  void AddLastSegment(PartitionIdentifier partition_id, std::shared_ptr<TsLastSegment> last_seg) {
    disk_segments_[partition_id].last_seg = std::move(last_seg);
  }

  void AddEntitySegment(PartitionIdentifier partition_id, std::shared_ptr<TsEntitySegment> entity_seg,
                        uint64_t minimum_block_id) {
    TsEntitySegmentView entity_seg_view(std::move(entity_seg), minimum_block_id);
    disk_segments_[partition_id].entity_seg = std::move(entity_seg_view);
  }
};

class TsMemSegmentProxy {
  enum class State : uint8_t { kMem, kDisk };

 private:
  mutable spin_shared_mutex mtx_;
  std::atomic<State> state_{State::kMem};

  const int64_t mem_segment_id_;
  std::shared_ptr<TsMemSegment> mem_;

  std::map<PartitionIdentifier, mem_proxy_detail::PartitionedSegment> disk_;

  std::shared_ptr<TsMemSegment> GetMemSegment() const {
    if (state_.load(std::memory_order_acquire) == State::kDisk) {
      return nullptr;
    }
    mtx_.lock_shared();
    auto mem = mem_;
    mtx_.unlock_shared();
    return mem;
  }

 public:
  explicit TsMemSegmentProxy(std::shared_ptr<TsMemSegment> memseg)
      : mem_segment_id_{memseg->GetId()}, mem_{std::move(memseg)} {
    assert(mem_ != nullptr);
  }
  TsMemSegmentProxy(const TsMemSegmentProxy& other) = delete;
  TsMemSegmentProxy& operator=(const TsMemSegmentProxy& other) = delete;
  ~TsMemSegmentProxy() = default;

  int64_t GetId() const { return mem_segment_id_; }
  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter, std::list<shared_ptr<TsBlockSpan>>& blocks,
                        const std::shared_ptr<TsTableSchemaManager>& tbl_schema_mgr,
                        const std::shared_ptr<MMapMetricsTable>& scan_schema, TsScanStats* ts_scan_stats = nullptr) {
    // hold mem segment lock to avoid race condition with SwitchToDisk
    auto mem = GetMemSegment();
    if (mem != nullptr) {
      return mem->GetBlockSpans(filter, blocks, tbl_schema_mgr, scan_schema, ts_scan_stats);
    }

    auto s = SUCCESS;
    for (const auto& [p, seg] : disk_) {
      s = seg.GetBlockSpans(filter, blocks, tbl_schema_mgr, scan_schema, ts_scan_stats);
      if (s == FAIL) {
        break;
      }
    }
    return s;
  }

  KStatus GetMaxOSN(uint32_t db_id, TSTableID table_id, TSEntityID entity_id, const PartitionIdentifier& par_id,
                    const KwTsSpan& span, TS_OSN& max_osn) {
    auto mem = GetMemSegment();
    if (mem != nullptr) {
      return mem->GetMaxOSN(db_id, table_id, entity_id, span, max_osn);
    }
    max_osn = 0;
    auto it = disk_.find(par_id);
    if (it == disk_.end()) {
      return SUCCESS;
    }
    return it->second.GetMaxOSN(entity_id, max_osn);
  }

  // move is needed here to avoid copy constructor
  void SwitchToDisk(TsDiskSegmentHandle&& disk_handle) {
    assert(disk_.empty());
    assert(mem_ != nullptr);

    for (auto& [p, seg] : disk_handle.disk_segments_) {
      disk_[p] = std::move(seg);
      assert(disk_.find(p) != disk_.end());
    }

    state_.store(State::kDisk, std::memory_order_release);
    // release the mem segment under write lock
    std::lock_guard lock(mtx_);
    mem_ = nullptr;
  }
};

}  // namespace kwdbts
