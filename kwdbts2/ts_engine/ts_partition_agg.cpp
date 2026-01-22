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

#include "ts_partition_agg.h"

namespace kwdbts {
constexpr char TS_PARTITION_AGG_ENTITY_ITEM_FILE_NAME[] = "partition_agg.e";
constexpr char TS_PARTITION_AGG_FILE_NAME[] = "partition_agg";


TsPartitionAggEntityItemFile::TsPartitionAggEntityItemFile(const fs::path& partition_path) :
  file_path_(partition_path / TS_PARTITION_AGG_ENTITY_ITEM_FILE_NAME), file_(file_path_) {}

TsPartitionAggEntityItemFile::~TsPartitionAggEntityItemFile() {
  file_.Close();
}

KStatus TsPartitionAggEntityItemFile::Open() {
  if (file_.Open() == SUCCESS) {
    if (file_.GetAllocSize() >= sizeof(TsAggStatsFileHeader)) {
      header_ = reinterpret_cast<TsAggStatsFileHeader*>(file_.addr(file_.GetStartPos()));
    } else {
      auto offset = file_.AllocateAssigned(sizeof(TsAggStatsFileHeader), 0);
      header_ = reinterpret_cast<TsAggStatsFileHeader*>(file_.addr(offset));
    }
    KStatus s = index_.Init(&file_, &(file_.getHeader()->index_header_offset));
    if (s == SUCCESS) {
      return s;
    }
  }
  LOG_ERROR("partition agg entity item file open failed, file_path='%s'", file_path_.c_str());
  return FAIL;
}

KStatus TsPartitionAggEntityItemFile::Close() {
  return file_.Close();
}

KStatus TsPartitionAggEntityItemFile::GetPartitionAggHeader(TsAggStatsFileHeader& header) {
  header.max_entity_id = header_->max_entity_id;
  header.version_num = header_->version_num;
  header.max_osn = header_->max_osn;
  return KStatus::SUCCESS;
}

KStatus TsPartitionAggEntityItemFile::SetEntityAggStats(TsEntityAggStats& agg_stats) {
  auto node = index_.GetIndexObject(agg_stats.entity_id, true);
  if (node == nullptr) {
    LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", agg_stats.entity_id, file_path_.c_str());
    return KStatus::FAIL;
  }
  if (*node == INVALID_POSITION) {
    auto offset = file_.AllocateAssigned(sizeof(TsEntityAggStats), 0);
    if (offset == INVALID_POSITION) {
      LOG_ERROR("get node from index file failed. entity [%lu] path [%s].", agg_stats.entity_id, file_path_.c_str());
      return FAIL;
    }
    *node = offset;
  }
  {
    auto* stats = reinterpret_cast<TsEntityAggStats*>(file_.addr(*node));
    assert(stats != nullptr);
    stats->table_id = agg_stats.table_id;
    stats->entity_id = agg_stats.entity_id;
    stats->table_version = agg_stats.table_version;
    stats->min_ts = agg_stats.min_ts;
    stats->max_ts = agg_stats.max_ts;
    stats->max_osn = agg_stats.max_osn;
    stats->agg_offset = agg_stats.agg_offset;
    stats->agg_len = agg_stats.agg_len;
  }
  return SUCCESS;
}

KStatus TsPartitionAggEntityItemFile::GetEntityAggStats(TsEntityAggStats& stats) {
  auto node = index_.GetIndexObject(stats.entity_id, false);
  if (node == nullptr) {
    // LOG_DEBUG("not found node from index file. entity [%lu] path [%s].", stats.entity_id, file_path_.c_str());
    stats.agg_offset = 0;
    stats.agg_len = 0;
    return KStatus::SUCCESS;
  }
  {
    TsEntityAggStats* header = reinterpret_cast<TsEntityAggStats*>(file_.addr(*node));
    assert(header != nullptr);
    stats.table_id = header->table_id;
    stats.min_ts = header->min_ts;
    stats.max_ts = header->max_ts;
    stats.table_version = header->table_version;
    stats.agg_offset = header->agg_offset;
    stats.agg_len = header->agg_len;
  }
  return KStatus::SUCCESS;
}

KStatus TsPartitionAggFile::Open() {
  auto s = io_env_->NewRandomReadFile(file_path_, &r_file_);
  if (s != SUCCESS) {
    LOG_ERROR("TsPartitionAggFile NewRandomReadFile failed, file_path='%s'", file_path_.c_str())
  }
  return s;
}

KStatus TsPartitionAggFile::ReadAggData(uint64_t offset, TsSliceGuard* data, size_t len) {
  r_file_->Read(offset, len, data);
  if (data->size() != len) {
    LOG_ERROR("TsPartitionAggFile read agg block failed, offset=%lu, len=%zu", offset, len)
    return FAIL;
  }
  return SUCCESS;
}

TsPartitionAggFileBuilder::TsPartitionAggFileBuilder(TsIOEnv* env, const fs::path& partition_path)
    : io_env_(env), file_path_(partition_path / TS_PARTITION_AGG_FILE_NAME) {
}

KStatus TsPartitionAggFileBuilder::Open() {
  auto s = io_env_->NewAppendOnlyFile(file_path_, &w_file_, false);
  if (s != SUCCESS) {
    LOG_ERROR("TsPartitionAggFileBuilder NewAppendOnlyFile failed, file_path='%s'", file_path_.c_str())
  }
  return s;
}

KStatus TsPartitionAggFileBuilder::Close() {
  w_file_->Sync();
  return w_file_->Close();
}

KStatus TsPartitionAggFileBuilder::AppendAggBlock(const TSSlice& agg, uint64_t* offset) {
  *offset = w_file_->GetFileSize();
  auto s = w_file_->Append(agg);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg data failed");
    return s;
  }
  return SUCCESS;
}

TsPartitionAggCalculator::TsPartitionAggCalculator(TsIOEnv* io_env, const fs::path& path) :
    io_env_(io_env), partition_path_(path) {
  entity_item_file_ = std::make_unique<TsPartitionAggEntityItemFile>(partition_path_);
  agg_builder_ = std::make_unique<TsPartitionAggFileBuilder>(io_env, partition_path_);
}

TsPartitionAggCalculator::~TsPartitionAggCalculator() {
}

KStatus TsPartitionAggCalculator::Open() {
  auto s = entity_item_file_->Open();
  if (s != SUCCESS) {
    LOG_ERROR("Open partition agg entity item file failed");
    return s;
  }
  s = agg_builder_->Open();
  if (s != SUCCESS) {
    LOG_ERROR("Open partition agg data file failed");
    return s;
  }
  return SUCCESS;
}

KStatus TsPartitionAggCalculator::Close() {
  auto s = entity_item_file_->Close();
  if (s != SUCCESS) {
    LOG_ERROR("Close partition agg entity item file '%s' failed",
               (partition_path_ / TS_PARTITION_AGG_ENTITY_ITEM_FILE_NAME).c_str());
    return s;
  }
  s = agg_builder_->Close();
  if (s != SUCCESS) {
    LOG_ERROR("Close partition agg data file '%s' failed", (partition_path_ / TS_PARTITION_AGG_FILE_NAME).c_str());
    return s;
  }
  return SUCCESS;
}

KStatus TsPartitionAggCalculator::GetPartitionAggHeader(TsAggStatsFileHeader& header) {
  return entity_item_file_->GetPartitionAggHeader(header);
}

KStatus TsPartitionAggCalculator::AppendEntityAgg(const TSSlice& agg, TsEntityAggStats& stats) {
  uint64_t offset;
  auto s = agg_builder_->AppendAggBlock(agg, &offset);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg data failed");
    return s;
  }
  stats.agg_offset = offset;
  stats.agg_len = agg.len;
  s = entity_item_file_->SetEntityAggStats(stats);
  if (s != SUCCESS) {
    LOG_ERROR("Set entity agg item failed, entity_id=%lu", stats.entity_id);
  }
  return SUCCESS;
}

KStatus TsPartitionAggCalculator::GetEntityAggStats(TsEntityAggStats& stats) {
  return entity_item_file_->GetEntityAggStats(stats);
}

TsPartitionAggReader::TsPartitionAggReader(TsIOEnv* io_env, const fs::path& path) : io_env_(io_env), partition_path_(path) {
  entity_item_file_ = std::make_unique<TsPartitionAggEntityItemFile>(partition_path_);
  agg_file_ = std::make_unique<TsPartitionAggFile>(io_env, partition_path_ / TS_PARTITION_AGG_FILE_NAME);
}

TsPartitionAggReader::~TsPartitionAggReader() {
  entity_item_file_->Close();
}

KStatus TsPartitionAggReader::Open() {
  auto s = entity_item_file_->Open();
  if (s != SUCCESS) {
    LOG_ERROR("Open partition agg entity item file failed");
    return s;
  }
  s = agg_file_->Open();
  if (s != SUCCESS) {
    LOG_ERROR("Open partition agg data file failed");
    return s;
  }
  return SUCCESS;
}

KStatus TsPartitionAggReader::GetPartitionAggHeader(TsAggStatsFileHeader& header) {
  return entity_item_file_->GetPartitionAggHeader(header);
}

KStatus TsPartitionAggReader::GetPartitionAggStats(TsEntityAggStats& stats) {
  auto s = entity_item_file_->GetEntityAggStats(stats);
  if (s != SUCCESS) {
    LOG_ERROR("Get entity agg stats failed");
  }
  return s;
}

KStatus TsPartitionAggReader::GetPartitionAgg(TSEntityID entity_id, TsSliceGuard& agg) {
  TsEntityAggStats stats;
  stats.entity_id = entity_id;
  stats.agg_offset = 0;
  stats.agg_len = 0;
  auto s = entity_item_file_->GetEntityAggStats(stats);
  if (s != SUCCESS) {
    LOG_ERROR("Get entity agg stats failed");
    return s;
  }
  s = agg_file_->ReadAggData(stats.agg_offset, &agg, stats.agg_len);
  if (s != SUCCESS) {
    LOG_ERROR("Read partition agg data failed");
    return s;
  }
  return SUCCESS;
}
}  // namespace kwdbts
