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

#include "ts_block_segment_data.h"

namespace kwdbts {

TsBlockSegmentBlockFile::TsBlockSegmentBlockFile(const string& file_path) : file_path_(file_path) {
  file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
  file_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_BLOCK_FILE_RWLOCK);
  memset(&header_, 0, sizeof(TsBlockFileHeader));
}

TsBlockSegmentBlockFile::~TsBlockSegmentBlockFile() {}

KStatus TsBlockSegmentBlockFile::Open() {
  TSSlice result;
  KStatus s = file_->Read(0, sizeof(TsBlockFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.status = TsFileStatus::READY;
    header_.magic = TS_BLOCK_SEGMENT_BLOCK_FILE_MAGIC;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsBlockFileHeader)});
  }
  return s == KStatus::SUCCESS ? KStatus::SUCCESS : KStatus::FAIL;
}

KStatus TsBlockSegmentBlockFile::AppendBlock(const TSSlice& block, uint64_t* offset) {
  RW_LATCH_X_LOCK(file_mtx_);
  *offset = file_->GetFileSize();
  file_->Append(block);
  RW_LATCH_UNLOCK(file_mtx_);
  return KStatus::SUCCESS;
}

KStatus TsBlockSegmentBlockFile::ReadData(uint64_t offset, char* buff, size_t len) {
  RW_LATCH_S_LOCK(file_mtx_);
  TSSlice result;
  file_->Read(offset, len, &result, buff);
  if (result.len != len) {
    LOG_ERROR("TsBlockSegmentBlockFile read block failed, offset=%lu, len=%zu", offset, len)
    return KStatus::FAIL;
  }
  RW_LATCH_UNLOCK(file_mtx_);
  return KStatus::SUCCESS;
}

TsBlockSegmentAggFile::TsBlockSegmentAggFile(const string& file_path)
        : file_path_(file_path) {
  file_ = std::make_unique<TsMMapFile>(file_path, false /*read_only*/);
  agg_file_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_BLOCK_AGG_RWLOCK);
  memset(&header_, 0, sizeof(TsAggFileHeader));
}

KStatus TsBlockSegmentAggFile::Open() {
  TSSlice result;
  auto s = file_->Read(0, sizeof(TsAggFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.magic = TS_BLOCK_SEGMENT_BLOCK_FILE_MAGIC;
    header_.status = TsFileStatus::READY;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsAggFileHeader)});
  }
  return s;
}

KStatus TsBlockSegmentAggFile::AppendAggBlock(const TSSlice& agg, uint64_t* offset) {
  RW_LATCH_X_LOCK(agg_file_mtx_);
  *offset = file_->GetFileSize();
  file_->Append(agg);
  RW_LATCH_UNLOCK(agg_file_mtx_);
  return SUCCESS;
}

KStatus TsBlockSegmentAggFile::ReadAggBlock(uint64_t offset, char* buff, size_t len) {
  RW_LATCH_S_LOCK(agg_file_mtx_);
  TSSlice result;
  file_->Read(offset, len, &result, buff);
  if (result.len != len) {
    LOG_ERROR("TsBlockSegmentAggFile read agg block failed, offset=%lu, len=%zu", offset, len)
    return FAIL;
  }
  RW_LATCH_UNLOCK(agg_file_mtx_);
  return SUCCESS;
}

}  //  namespace kwdbts

