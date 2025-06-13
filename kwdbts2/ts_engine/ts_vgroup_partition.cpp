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

#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <memory>
#include <utility>
#include <regex>

#include "ts_entity_segment.h"
#include "ts_vgroup_partition.h"
#include "ts_lastsegment.h"
#include "ts_lastsegment_manager.h"

namespace kwdbts {

TsVGroupPartition::TsVGroupPartition(std::filesystem::path root, int database_id, TsEngineSchemaManager* schema_mgr,
                                     int64_t start, int64_t end)
    : database_id_(database_id),
      schema_mgr_(schema_mgr),
      start_(start),
      end_(end),
      path_(root / GetFileName()),
      del_info_(root.string() + "/" + GetFileName()),
      last_segment_mgr_(path_) {
  partition_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_MMAP_GROUP_PARTITION_RWLOCK);
}

TsVGroupPartition::~TsVGroupPartition() {}

KStatus TsVGroupPartition::Open() {
  std::filesystem::create_directories(path_);
  entity_segment_ = std::make_unique<TsEntitySegment>(path_);

  // reload lastsegments
  std::error_code ec;
  std::filesystem::directory_iterator dir_iter{path_, ec};
  if (ec.value() != 0) {
    LOG_ERROR("TsVGroupPartition::Open fail, reason: %s", ec.message().c_str());
  }
  std::regex re("last.ver-([0-9]{12})");
  for (const auto& it : dir_iter) {
    std::string fname = it.path().filename();
    std::smatch res;
    bool ok = std::regex_match(fname, res, re);
    if (!ok) {
      continue;
    }
    uint64_t file_number = std::stoi(res.str(1));
    std::shared_ptr<TsLastSegment> last;
    last_segment_mgr_.OpenLastSegmentFile(file_number, &last);
  }
  // open del item file.
  if (del_info_.Open() != KStatus::SUCCESS) {
    LOG_ERROR(" del_info_ Open fail.");
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::Compact() {
  // 1. Get all the last segments that need to be compacted.
  std::vector<std::shared_ptr<TsLastSegment>> last_segments;
  while (last_segment_mgr_.GetCompactLastSegments(last_segments) && !last_segments.empty()) {
    // 2. Build the column block.
    TsEntitySegmentBuilder builder(last_segments, this);
    KStatus s = builder.BuildAndFlush();
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("partition[%s] compact failed", path_.c_str());
      return s;
    }
    // 3. Set the compacted version.
    last_segment_mgr_.ClearLastSegments(last_segments.back()->GetVersion());
    last_segments.clear();
  }
  return KStatus::SUCCESS;
}

bool TsVGroupPartition::NeedCompact() {
  return last_segment_mgr_.NeedCompact();
}

KStatus TsVGroupPartition::AppendToBlockSegment(TSTableID table_id, TSEntityID entity_id, uint32_t table_version,
                                                uint32_t col_num, uint32_t row_num, timestamp64 max_ts, timestamp64 min_ts,
                                                TSSlice block_data, TSSlice block_agg) {
  // generating new block item info ,and append to block segment.
  TsEntitySegmentBlockItem blk_item;
  blk_item.entity_id = entity_id;
  blk_item.table_version = table_version;
  blk_item.n_cols = col_num;
  blk_item.n_rows = row_num;
  blk_item.max_ts = max_ts;
  blk_item.min_ts = min_ts;

  KStatus s = entity_segment_->AppendBlockData(blk_item, block_data, block_agg);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("insert into block segment of partition[%s] failed.", path_.c_str());
    return s;
  }

  return KStatus::SUCCESS;
}

KStatus TsVGroupPartition::NewLastSegmentFile(std::unique_ptr<TsFile>* last_segment,
                                              uint32_t* ver) {
  return last_segment_mgr_.NewLastSegmentFile(last_segment, ver);
}

void TsVGroupPartition::PublicLastSegment(uint32_t file_number) {
  std::shared_ptr<TsLastSegment> file;
  last_segment_mgr_.OpenLastSegmentFile(file_number, &file);
}

std::filesystem::path TsVGroupPartition::GetPath() const { return path_; }

std::string TsVGroupPartition::GetFileName() const {
  char buffer[64];
  std::snprintf(buffer, sizeof(buffer), "db%02d-%014ld", database_id_, start_);
  return buffer;
}

KStatus TsVGroupPartition::DeleteData(TSEntityID e_id, const std::vector<KwTsSpan>& ts_spans, const KwLSNSpan& lsn) {
  kwdbts::TsEntityDelItem del_item(ts_spans[0], lsn, e_id);
  for (auto& ts_span : ts_spans) {
    assert(ts_span.begin <= ts_span.end);
    del_item.range.ts_span = ts_span;
    auto s = del_info_.AddDelItem(e_id, del_item);
    if (s != KStatus::SUCCESS) {
      LOG_ERROR("AddDelItem failed. for entity[%lu]", e_id);
      return s;
    }
  }
  return KStatus::SUCCESS;
}
KStatus TsVGroupPartition::GetDelRange(TSEntityID e_id, std::list<STDelRange>& del_items) {
  return del_info_.GetDelRange(e_id, del_items);
}

}  //  namespace kwdbts
