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

#include "ts_block_segment.h"

namespace kwdbts {

const char entity_item_meta_file_name[] = "header.e";
const char block_item_meta_file_name[] = "header.b";
const char block_data_file_name[] = "block";

KStatus TsSegmentEntityMetaFile::Open() {
  TSSlice result;
  TsStatus s = file_->Read(0, sizeof(TsEntityMetaFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (result.len == 0) {
    // lots of changes are needed here to fix memory issue,
    // here is just a temporary solution.
    memset(&header_, 0, sizeof(TsEntityMetaFileHeader));
  }
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.magic = TS_SEGMENT_ENTITY_META_MAGIC;
    header_.status = TsFileStatus::READY;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsEntityMetaFileHeader)});
  }
  return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
}

KStatus TsSegmentEntityMetaFile::UpdateEntityItem(uint64_t entity_id, const TsBlockItemInfo& block_item_info) {
  TsEntityItem entity_item{};
  TSSlice result;
  entity_hash_latch_.WrLock(entity_id);
  TsStatus s = file_->Read(sizeof(TsEntityMetaFileHeader) + (entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                           &result, reinterpret_cast<char *>(&entity_item));
  if (!entity_item.is_initialized) {
    entity_item.entity_id = entity_id;
  }
  entity_item.cur_block_id = block_item_info.block_id;
  if (entity_item.max_ts < block_item_info.max_ts_in_block) {
    entity_item.max_ts = block_item_info.max_ts_in_block;
  }
  if (entity_item.min_ts > block_item_info.min_ts_in_block) {
    entity_item.min_ts = block_item_info.min_ts_in_block;
  }
  s = file_->Write(sizeof(TsEntityMetaFileHeader) + (entity_id - 1) * sizeof(TsEntityItem),
                   TSSlice{reinterpret_cast<char *>(&entity_item), sizeof(entity_item)});
  entity_hash_latch_.Unlock(entity_id);
  return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
}

KStatus TsSegmentEntityMetaFile::GetEntityCurBlockId(uint64_t entity_id, uint64_t& cur_block_id) {
  TsEntityItem entity_item{};
  TSSlice result;

  entity_hash_latch_.RdLock(entity_id);
  TsStatus s = file_->Read(sizeof(TsEntityMetaFileHeader) + (entity_id - 1) * sizeof(TsEntityItem), sizeof(TsEntityItem),
                           &result, reinterpret_cast<char *>(&entity_item));
  entity_hash_latch_.Unlock(entity_id);

  cur_block_id = entity_item.cur_block_id;
  return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
}

KStatus TsSegmentBlockMetaFile::TsSegmentBlockMetaFile::Open() {
  TSSlice result;
  TsStatus s = file_->Read(0, sizeof(TsBlockFileMetaFileHeader), &result, reinterpret_cast<char *>(&header_));
  if (result.len == 0) {
    // lots of changes are needed here to fix memory issue,
    // here is just a temporary solution.
    memset(&header_, 0, sizeof(TsBlockFileMetaFileHeader));
  }
  if (header_.status != TsFileStatus::READY) {
    file_->Reset();
    header_.status = TsFileStatus::READY;
    header_.magic = TS_SEGMENT_BLOCK_META_MAGIC;
    s = file_->Append(TSSlice{reinterpret_cast<char *>(&header_), sizeof(TsBlockFileMetaFileHeader)});
  }
  return s == TsStatus::OK() ? KStatus::SUCCESS : KStatus::FAIL;
}

KStatus TsSegmentBlockMetaFile::AllocateBlockItem(uint64_t entity_id, TsBlockItemInfo& block_item_info) {
  RW_LATCH_X_LOCK(block_item_mtx_);
  // file header
  header_.block_num += 1;
  KStatus s = writeFileMeta(header_);
  // block item info
  block_item_info.block_id = header_.block_num;
  size_t offset = sizeof(TsBlockFileMetaFileHeader) + (block_item_info.block_id - 1) * sizeof(TsBlockItemInfo);
  file_->Write(offset, TSSlice{reinterpret_cast<char *>(&block_item_info), sizeof(TsBlockItemInfo)});
  RW_LATCH_UNLOCK(block_item_mtx_);
  return s;
}

KStatus TsSegmentBlockMetaFile::GetBlockItem(uint64_t entity_id, uint64_t blk_id, std::shared_ptr<TsBlockItem>& blk_item) {
  RW_LATCH_S_LOCK(block_item_mtx_);
  TSSlice result;
  file_->Read(sizeof(TsBlockFileMetaFileHeader) + (blk_id - 1) * sizeof(TsBlockItemInfo), sizeof(TsBlockItemInfo), &result,
              reinterpret_cast<char *>(&(blk_item->Info())));
  RW_LATCH_UNLOCK(block_item_mtx_);
  if (result.len != sizeof(TsBlockItemInfo)) {
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

TsBlkSegmentMetaManager::TsBlkSegmentMetaManager(const string& path) :
  path_(path), entity_meta_(path + "/" + entity_item_meta_file_name),
  block_meta_(path + "/" + block_item_meta_file_name) {
}

KStatus TsBlkSegmentMetaManager::Open() {
  // Attempt to access the directory
  if (access(path_.c_str(), 0)) {
    LOG_ERROR("cannot open directory [%s].", path_.c_str());
    return KStatus::FAIL;
  }
  KStatus s = entity_meta_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_meta_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlkSegmentMetaManager::AppendBlockItem(TsBlockItem* blk_item) {
  uint64_t last_blk_id;
  uint64_t entity_id = blk_item->Info().entity_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_meta_.AllocateBlockItem(entity_id, blk_item->Info());
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = entity_meta_.UpdateEntityItem(entity_id, blk_item->Info());
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlkSegmentMetaManager::GetAllBlockItems(TSEntityID entity_id,
                                                  std::vector<std::shared_ptr<TsBlockItem>>* blk_items) {
  uint64_t last_blk_id;
  KStatus s = entity_meta_.GetEntityCurBlockId(entity_id, last_blk_id);
  if (s != KStatus::SUCCESS) {
    return s;
  }

  std::shared_ptr<TsBlockItem> cur_blk_item = std::make_shared<TsBlockItem>();
  while (last_blk_id > 0) {
    s = block_meta_.GetBlockItem(entity_id, last_blk_id, cur_blk_item);
    if (s != KStatus::SUCCESS) {
      return s;
    }
    blk_items->push_back(cur_blk_item);
    last_blk_id = cur_blk_item->Info().prev_block_id;
  }
  return KStatus::SUCCESS;
}

TsBlockSegment::TsBlockSegment(const std::filesystem::path& root)
    : dir_path_(root), meta_mgr_(root), block_file_(root / block_data_file_name) {}

KStatus TsBlockSegment::Open() {
  KStatus s = meta_mgr_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = block_file_.Open();
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::AppendBlockData(TsBlockItem* blk_item, const TSSlice& data, const TSSlice& agg) {
  uint64_t blk_offset = 0;
  KStatus s = block_file_.Append(data, &blk_offset);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to block file failed. data len: %lu.", data.len);
    return s;
  }
  blk_item->Info().block_offset = blk_offset;
  s = meta_mgr_.AppendBlockItem(blk_item);
  if (s != KStatus::SUCCESS) {
    LOG_ERROR("append to meta file failed. data len: %lu.", data.len);
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsBlockSegment::GetAllBlockItems(TSEntityID entity_id, std::vector<std::shared_ptr<TsBlockItem>>* blk_items) {
  return meta_mgr_.GetAllBlockItems(entity_id, blk_items);
}

KStatus TsBlockSegment::GetBlockData(TsBlockItem* blk_item, char* buff) {
  return block_file_.ReadBlock(blk_item->Info().block_offset, buff);
}

}  //  namespace kwdbts

