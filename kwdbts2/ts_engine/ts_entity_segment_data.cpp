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

#include "ts_entity_segment_data.h"
#include "ts_entity_segment_handle.h"
#include "ts_filename.h"

namespace kwdbts {

TsEntitySegmentBlockFile::TsEntitySegmentBlockFile(const string& root, EntitySegmentMetaInfo info)
    : root_path_(root), info_(std::move(info)) {
  memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
}

TsEntitySegmentBlockFile::~TsEntitySegmentBlockFile() {}

KStatus TsEntitySegmentBlockFile::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  std::string file_path_ = root_path_ / DataBlockFileName(info_.datablock_info.file_number);
  if (env->NewRandomReadFile(file_path_, &r_file_, info_.datablock_info.length) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockFile NewRandomReadFile failed, file_path=%s", file_path_.c_str())
    assert(false);
  }

  if (r_file_->GetFileSize() < sizeof(TsAggAndBlockFileHeader)) {
    LOG_ERROR("TsEntitySegmentBlockFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  TSSlice result;
  KStatus s = r_file_->Read(0, sizeof(TsAggAndBlockFileHeader), &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = *reinterpret_cast<TsAggAndBlockFileHeader*>(result.data);
  if (header_.status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentBlockFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}

KStatus TsEntitySegmentBlockFile::ReadData(uint64_t offset, char** buff, size_t len) {
  TSSlice result;
  KStatus s = r_file_->Read(offset, len, &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentBlockFile read block failed, offset=%lu, len=%zu", offset, len)
    return KStatus::FAIL;
  }
  *buff = result.data;
  return KStatus::SUCCESS;
}

TsEntitySegmentAggFile::TsEntitySegmentAggFile(const string& root, EntitySegmentMetaInfo info)
    : root_(root), info_(std::move(info)) {
  memset(&header_, 0, sizeof(TsAggAndBlockFileHeader));
}

KStatus TsEntitySegmentAggFile::Open() {
  TsIOEnv* env = &TsMMapIOEnv::GetInstance();
  std::string file_path_ = root_ / EntityAggFileName(info_.agg_info.file_number);
  if (env->NewRandomReadFile(file_path_, &r_file_, info_.agg_info.length) != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentAggFile NewRandomReadFile failed, file_path=%s", file_path_.c_str())
    assert(false);
  }

  if (r_file_->GetFileSize() < sizeof(TsAggAndBlockFileHeader)) {
    LOG_ERROR("TsEntitySegmentAggFile open failed, file_path=%s", file_path_.c_str())
    return KStatus::FAIL;
  }
  TSSlice result;
  KStatus s = r_file_->Read(0, sizeof(TsAggAndBlockFileHeader), &result, nullptr);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("TsEntitySegmentAggFile read failed, file_path=%s", file_path_.c_str())
    return s;
  }
  header_ = *reinterpret_cast<TsAggAndBlockFileHeader*>(result.data);
  if (header_.status != TsFileStatus::READY) {
    LOG_ERROR("TsEntitySegmentAggFile not ready, file_path=%s", file_path_.c_str())
  }
  return s;
}

KStatus TsEntitySegmentAggFile::ReadAggData(uint64_t offset, char** buff, size_t len) {
  TSSlice result;
  r_file_->Read(offset, len, &result, nullptr);
  if (result.len != len) {
    LOG_ERROR("TsEntitySegmentAggFile read agg block failed, offset=%lu, len=%zu", offset, len)
    return FAIL;
  }
  *buff = result.data;
  return SUCCESS;
}

}  //  namespace kwdbts

