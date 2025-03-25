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

TsBlockFile::TsBlockFile(const string& file_path) : file_path_(file_path) {
  file_mtx_ = std::make_unique<KRWLatch>(RWLATCH_ID_BLOCK_FILE_RWLOCK);
}

TsBlockFile::~TsBlockFile() {
  if (file_ != nullptr) {
    fclose(file_);
    file_ = nullptr;
  }
}

KStatus TsBlockFile::Open() {
  file_ = std::fopen(file_path_.c_str(), "wb+");
  if (file_ == NULL) {
    LOG_ERROR("open file [%s] failed. errno: %d.", file_path_.c_str(), errno);
    return KStatus::FAIL;
  }
  std::fseek(file_, 0, SEEK_END);
  file_len_ = ftell(file_);
  LOG_DEBUG("open file [%s] success. file length: %lu", file_path_.c_str(), file_len_);
  return KStatus::SUCCESS;
}

KStatus TsBlockFile::Append(const TSSlice& block, uint64_t* offset) {
  RW_LATCH_X_LOCK(file_mtx_);
  std::fseek(file_, 0, SEEK_END);
  assert(file_len_ == ftell(file_));
  *offset = file_len_;
  fwrite(block.data, 1, block.len, file_);
  file_len_ += block.len;
  RW_LATCH_UNLOCK(file_mtx_);
  return KStatus::SUCCESS;
}

KStatus TsBlockFile::ReadBlock(uint64_t offset, char* buff) {
  return KStatus::SUCCESS;
}


}  //  namespace kwdbts

