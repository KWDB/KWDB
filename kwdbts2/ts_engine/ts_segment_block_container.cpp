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

#include "ts_segment_block_container.h"

namespace kwdbts {

TsSegmentBlockContainer::TsSegmentBlockContainer(const std::shared_ptr<TsSegmentFile>& segment_file)
          : entity_blocks_rw_latch_(RWLATCH_ID_ENTITY_BLOCKS_RWLOCK),
            segment_file_rw_latch_(RWLATCH_ID_SEGMENT_FILE_RWLOCK) {
  segment_file_ = segment_file;
}

TsSegmentBlockContainer::TsSegmentBlockContainer(uint32_t max_blocks)
          : entity_blocks_rw_latch_(RWLATCH_ID_ENTITY_BLOCKS_RWLOCK),
            segment_file_rw_latch_(RWLATCH_ID_SEGMENT_FILE_RWLOCK) {
  entity_blocks_.resize(max_blocks);
}

TsSegmentBlockContainer::~TsSegmentBlockContainer() {
}

void TsSegmentBlockContainer::UpgradeSegmentFile(const std::shared_ptr<TsSegmentFile>& segment_file) {
  RW_LATCH_X_LOCK(&segment_file_rw_latch_);
  segment_file_ = segment_file;
  RW_LATCH_UNLOCK(&segment_file_rw_latch_);
}

const EntitySegmentMetaInfo& TsSegmentBlockContainer::GetHandleInfo() {
  RW_LATCH_S_LOCK(&segment_file_rw_latch_);
  EntitySegmentMetaInfo info = segment_file_->GetHandleInfo();
  RW_LATCH_UNLOCK(&segment_file_rw_latch_);
  return info;
}

std::string TsSegmentBlockContainer::GetPath() {
  RW_LATCH_S_LOCK(&segment_file_rw_latch_);
  std::string path = segment_file_->GetPath();
  RW_LATCH_UNLOCK(&segment_file_rw_latch_);
  return path;
}

std::string TsSegmentBlockContainer::GetHandleInfoStr() {
  RW_LATCH_S_LOCK(&segment_file_rw_latch_);
  std::string info_str =  segment_file_->GetHandleInfoStr();
  RW_LATCH_UNLOCK(&segment_file_rw_latch_);
  return info_str;
}
}  //  namespace kwdbts
