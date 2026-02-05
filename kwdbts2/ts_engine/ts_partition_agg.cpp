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
constexpr char TS_PARTITION_AGG_FILE_NAME[] = "partition_agg";


TsPartitionAggBuilder::TsPartitionAggBuilder(TsIOEnv* io_env, const fs::path& path, uint32_t max_entity_id):
    io_env_(io_env), file_path_(path), max_entity_id_(max_entity_id) {
  entity_index_buffer_.assign(max_entity_id_, TsEntityPartitionAggIndex{});
}

TsPartitionAggBuilder::~TsPartitionAggBuilder() { }

KStatus TsPartitionAggBuilder::Open() {
  auto s = io_env_->NewAppendOnlyFile(file_path_, &w_file_, false);
  if (s != SUCCESS) {
    LOG_ERROR("TsPartitionAggFileBuilder NewAppendOnlyFile failed, file_path='%s'", file_path_.c_str())
  }
  return s;
}

KStatus TsPartitionAggBuilder::Close() {
  w_file_->Sync();
  return w_file_->Close();
}

KStatus TsPartitionAggBuilder::AppendEntityAgg(const TSSlice& agg, TsEntityPartitionAggIndex& agg_index) {
  uint64_t offset = w_file_->GetFileSize();
  auto s = w_file_->Append(agg);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg data failed");
    return s;
  }
  agg_index.agg_offset = offset;
  agg_index.agg_len = agg.len;
  entity_index_buffer_[agg_index.entity_id - 1] = agg_index;
  return SUCCESS;
}

KStatus TsPartitionAggBuilder::Finalize() {
  TsPartitionAggFooter footer{0, 0, 0, 0};
  footer.entity_agg_stats_idx_offset = w_file_->GetFileSize();
  footer.max_entity_id = entity_index_buffer_.size();
  TSSlice agg_index_slice = {reinterpret_cast<char*>(entity_index_buffer_.data()),
                   entity_index_buffer_.size() * sizeof(TsEntityPartitionAggIndex)};
  w_file_->Append(agg_index_slice);
  TSSlice footer_slice = {reinterpret_cast<char*>(&footer), sizeof(TsPartitionAggFooter)};
  w_file_->Append(footer_slice);
  // w_file_->Sync();

  return SUCCESS;
}

KStatus TsPartitionAggBuilder::GetEntityAggIndex(TsEntityPartitionAggIndex& agg_index) {
  return SUCCESS;
}

TsPartitionAggReader::TsPartitionAggReader(TsIOEnv* io_env, fs::path  path) : io_env_(io_env),
  file_path_(std::move(path)) {}

TsPartitionAggReader::~TsPartitionAggReader() {
  if (delete_after_free_) {
    r_file_->MarkDelete();
  }
}

KStatus TsPartitionAggReader::Open() {
  auto s = io_env_->NewRandomReadFile(file_path_, &r_file_);
  if (s != SUCCESS) {
    LOG_WARN("TsPartitionAggFile NewRandomReadFile failed, file_path='%s'", file_path_.c_str());
    return SUCCESS;
  }
  auto size = r_file_->GetFileSize();
  if (size < sizeof(TsPartitionAggFooter)) {
    LOG_ERROR("partition agg file %s corrupted", this->file_path_.c_str());
    return FAIL;
  }
  size_t offset = size - sizeof(TsPartitionAggFooter);
  s = r_file_->Read(offset, sizeof(TsPartitionAggFooter), &footer_guard_);
  if (s == FAIL || footer_guard_.size() != sizeof(TsPartitionAggFooter)) {
    LOG_ERROR("partition agg file[%s] GetFooter failed.", file_path_.c_str());
    return s;
  }
  memcpy(&footer_, footer_guard_.data(), footer_guard_.size());
  agg_index_buffer_.resize(footer_.max_entity_id);
  s = r_file_->Read(footer_.entity_agg_stats_idx_offset, agg_index_buffer_.size() * sizeof(TsEntityPartitionAggIndex),
                       &entity_index_data_);
  if (s == FAIL || entity_index_data_.size() != agg_index_buffer_.size() * sizeof(TsEntityPartitionAggIndex)) {
    LOG_ERROR("cannot read data from file");
    return s;
  }
  memcpy(agg_index_buffer_.data(), entity_index_data_.data(), entity_index_data_.size());
  ready_ = true;
  return SUCCESS;
}

KStatus TsPartitionAggReader::Reload() {
  if (!ready_) {
    r_file_.release();
    return Open();
  }
  return SUCCESS;
}

KStatus TsPartitionAggReader::GetPartitionAggIndex(TsEntityPartitionAggIndex& agg_index) {
  if (agg_index.entity_id > footer_.max_entity_id) {
    LOG_ERROR("entity id %lu out of range", agg_index.entity_id);
    return FAIL;
  }
  agg_index = agg_index_buffer_[agg_index.entity_id - 1];
  return SUCCESS;
}

KStatus TsPartitionAggReader::GetPartitionAgg(TSEntityID entity_id, TsSliceGuard& agg) {
  TsEntityPartitionAggIndex agg_index;
  agg_index.entity_id = entity_id;
  agg_index.agg_offset = 0;
  agg_index.agg_len = 0;

  auto s = GetPartitionAggIndex(agg_index);
  if (s != SUCCESS) {
    LOG_ERROR("GetPartitionAggIndex failed.");
    return s;
  }
  r_file_->Read(agg_index.agg_offset, agg_index.agg_len, &agg);
  if (agg.size() != agg_index.agg_len) {
    LOG_ERROR("TsPartitionAggFile read agg block failed, offset=%lu, len=%zu", agg_index.agg_offset, agg_index.agg_len)
    return FAIL;
  }
  return SUCCESS;
}
}  // namespace kwdbts
