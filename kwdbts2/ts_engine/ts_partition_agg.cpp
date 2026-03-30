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

#include <cstring>
#include <cstddef>

namespace kwdbts {
constexpr char TS_PARTITION_AGG_FILE_NAME[] = "partition_agg";

namespace {

char* GetAggIndexSlot(char* data, size_t index) {
  return data + index * sizeof(TsEntityPartitionAggIndex);
}

void SerializeAggIndex(char* dst, const TsEntityPartitionAggIndex& src) {
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, table_id), &src.table_id, sizeof(src.table_id));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, entity_id), &src.entity_id, sizeof(src.entity_id));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, table_version), &src.table_version, sizeof(src.table_version));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, min_ts), &src.min_ts, sizeof(src.min_ts));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, max_ts), &src.max_ts, sizeof(src.max_ts));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, max_osn), &src.max_osn, sizeof(src.max_osn));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, agg_offset), &src.agg_offset, sizeof(src.agg_offset));
  std::memcpy(dst + offsetof(TsEntityPartitionAggIndex, agg_len), &src.agg_len, sizeof(src.agg_len));
}

}  // namespace


TsPartitionAggBuilder::TsPartitionAggBuilder(TsIOEnv* io_env, const fs::path& path, uint32_t max_entity_id):
    io_env_(io_env), file_path_(path), max_entity_id_(max_entity_id) {
  entity_index_buffer_.resize(max_entity_id_ * sizeof(TsEntityPartitionAggIndex));
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
  auto s = w_file_->Sync();
  if (s != SUCCESS) {
    LOG_ERROR("Sync partition agg file failed, file_path='%s'", file_path_.c_str());
    return s;
  }
  return w_file_->Close();
}

KStatus TsPartitionAggBuilder::AppendEntityAgg(const TSSlice& agg, TsEntityPartitionAggIndex& agg_index) {
  assert(w_file_ != nullptr);
  assert(agg.len != 0);
  assert(agg_index.entity_id > 0 && agg_index.entity_id <= max_entity_id_);
  uint64_t offset = w_file_->GetFileSize();
  auto s = w_file_->Append(agg);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg data failed");
    return s;
  }
  agg_index.agg_offset = offset;
  agg_index.agg_len = agg.len;
  SerializeAggIndex(GetAggIndexSlot(entity_index_buffer_.data(), agg_index.entity_id - 1), agg_index);
  return SUCCESS;
}

KStatus TsPartitionAggBuilder::Finalize() {
  TsPartitionAggFooter footer{0, 0, 0, 0};
  footer.entity_agg_stats_idx_offset = w_file_->GetFileSize();
  footer.max_entity_id = max_entity_id_;
  TSSlice agg_index_slice = {entity_index_buffer_.data(), entity_index_buffer_.size()};
  auto s = w_file_->Append(agg_index_slice);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg index failed, file_path='%s'", file_path_.c_str());
    return s;
  }
  TSSlice footer_slice = {reinterpret_cast<char*>(&footer), sizeof(TsPartitionAggFooter)};
  s = w_file_->Append(footer_slice);
  if (s != SUCCESS) {
    LOG_ERROR("Append partition agg footer failed, file_path='%s'", file_path_.c_str());
    return s;
  }
  // w_file_->Sync();

  return SUCCESS;
}

TsPartitionAggReader::TsPartitionAggReader(TsIOEnv* io_env, fs::path  path) : io_env_(io_env),
  file_path_(std::move(path)) {}

TsPartitionAggReader::~TsPartitionAggReader() {
  if (delete_after_free_ && r_file_ != nullptr) {
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
  TsSliceGuard footer_guard;
  s = r_file_->Read(offset, sizeof(TsPartitionAggFooter), &footer_guard);
  if (s == FAIL || footer_guard.size() != sizeof(TsPartitionAggFooter)) {
    LOG_ERROR("partition agg file[%s] GetFooter failed.", file_path_.c_str());
    return s;
  }
  memcpy(&footer_, footer_guard.data(), footer_guard.size());
  if (footer_.entity_agg_stats_idx_offset > offset) {
    LOG_ERROR("partition agg file[%s] index offset invalid.", file_path_.c_str());
    return FAIL;
  }
  size_t index_bytes = footer_.max_entity_id * sizeof(TsEntityPartitionAggIndex);
  if (index_bytes > offset - footer_.entity_agg_stats_idx_offset) {
    LOG_ERROR("partition agg file[%s] index region invalid.", file_path_.c_str());
    return FAIL;
  }
  return SUCCESS;
}

KStatus TsPartitionAggReader::GetPartitionAggIndex(TsEntityPartitionAggIndex& agg_index) {
  if (agg_index.entity_id == 0 || agg_index.entity_id > footer_.max_entity_id) {
    LOG_DEBUG("entity id %lu out of range", agg_index.entity_id);
    return FAIL;
  }
  TsSliceGuard entity_index_data;
  uint64_t index_offset = footer_.entity_agg_stats_idx_offset +
                          (agg_index.entity_id - 1) * sizeof(TsEntityPartitionAggIndex);
  auto s = r_file_->Read(index_offset, sizeof(TsEntityPartitionAggIndex), &entity_index_data);
  if (s == FAIL || entity_index_data.size() != sizeof(TsEntityPartitionAggIndex)) {
    LOG_ERROR("partition agg file[%s] read index failed, entity_id=%lu.", file_path_.c_str(), agg_index.entity_id);
    return FAIL;
  }
  memcpy(&agg_index, entity_index_data.data(), entity_index_data.size());
  return SUCCESS;
}

KStatus TsPartitionAggReader::GetPartitionAgg(uint64_t agg_offset, uint64_t agg_len, TsSliceGuard& agg) {
  auto s = r_file_->Read(agg_offset, agg_len, &agg);
  if (s != SUCCESS) {
    LOG_ERROR("TsPartitionAggFile read agg block failed, file_path='%s', offset=%lu, len=%zu",
              file_path_.c_str(), agg_offset, agg_len);
    return s;
  }
  if (agg.size() != agg_len) {
    LOG_ERROR("TsPartitionAggFile read agg block failed, file_path='%s', offset=%lu, len=%zu",
              file_path_.c_str(), agg_offset, agg_len)
    return FAIL;
  }
  return SUCCESS;
}
}  // namespace kwdbts
