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

KStatus TsPartitionAggEntityItemFile::GetPartitionAggHeader(TsAggStatsFileHeader& header) {
  header.max_entity_id = header_->max_entity_id;
  header.version_num = header_->version_num;
  header.max_osn = header_->max_osn;
  return KStatus::SUCCESS;
}

KStatus TsPartitionAggEntityItemFile::AddEntityAggStats(TsEntityAggStats& stats) {
  return SUCCESS;
}

KStatus TsPartitionAggEntityItemFile::SetEntityAggStats(TsEntityAggStats& stats) {
  return SUCCESS;
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

KStatus TsPartitionAggFileBuilder::AppendAggBlock(const TSSlice& agg, uint64_t* offset) {
  *offset = w_file_->GetFileSize();
  w_file_->Append(agg);
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

KStatus TsPartitionAggCalculator::GetPartitionAggHeader(TsAggStatsFileHeader& header) {
  return entity_item_file_->GetPartitionAggHeader(header);
}

KStatus TsPartitionAggCalculator::CalcPartitionAgg() {
  return SUCCESS;
}
}  // namespace kwdbts
