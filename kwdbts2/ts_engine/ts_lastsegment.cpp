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

#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_coding.h"
#include "ts_compressor.h"
#include "ts_io.h"
#include "ts_lastsegment_manager.h"
#include "ts_status.h"
namespace kwdbts {

TsStatus TsLastSegment::Append(const TSSlice& data) { return file_->Append(data); }

TsStatus TsLastSegment::Flush() { return file_->Flush(); }

size_t TsLastSegment::GetFileSize() const { return file_->GetFileSize(); }

TsFile* TsLastSegment::GetFilePtr() { return file_.get(); }

uint32_t TsLastSegment::GetVersion() const { return ver_; }

KStatus TsLastSegment::GetFooter(TsLastSegmentFooter* footer) {
  TSSlice result;
  size_t offset = file_->GetFileSize() - sizeof(TsLastSegmentFooter);
  file_->Read(offset, sizeof(TsLastSegmentFooter), &result, reinterpret_cast<char*>(footer));
  if (result.len != sizeof(TsLastSegmentFooter) || footer->magic_number != FOOTER_MAGIC) {
    LOG_ERROR("last segment[%s] GetFooter failed.", file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(TsLastSegmentFooter& footer,
                                        std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TSSlice result;
  uint64_t nblock = footer.n_data_block;
  block_indexes->resize(nblock);
  for (uint64_t i = 0; i < nblock; ++i) {
    file_->Read(footer.block_info_idx_offset + i * sizeof(TsLastSegmentBlockIndex),
                sizeof(TsLastSegmentBlockIndex), &result,
                reinterpret_cast<char*>(&(*block_indexes)[i]));
    if (result.len != sizeof(TsLastSegmentBlockIndex)) {
      LOG_ERROR("last segment[%s] GetAllBlockIndex failed.", file_->GetFilePath().c_str());
      return KStatus::FAIL;
    }
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes) {
  TsLastSegmentFooter last_segment_footer;
  KStatus s = GetFooter(&last_segment_footer);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  s = GetAllBlockIndex(last_segment_footer, block_indexes);
  if (s != KStatus::SUCCESS) {
    return s;
  }
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlockInfo(TsLastSegmentBlockIndex& block_index,
                                    TsLastSegmentBlockInfo* block_info) {
  TSSlice result;
  // block info header
  auto buf = std::make_unique<char[]>(block_index.length);
  file_->Read(block_index.offset, block_index.length, &result, buf.get());
  if (result.len != block_index.length) {
    LOG_ERROR(
        "last segment[%s] GetBlockInfo failed, read header failed. "
        "table id: %lu, table version: %u, block info offset: %lu.",
        file_->GetFilePath().c_str(), block_index.table_id, block_index.table_version,
        block_index.offset);
    return KStatus::FAIL;
  }
  const char* ptr = buf.get();
  // TODO(zzr): endian problems
  memcpy(block_info, ptr, LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE);
  ptr += LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE;

  // block info column offset
  block_info->col_infos.resize(block_info->ncol);
  for (size_t col_idx = 0; col_idx < block_info->ncol; ++col_idx) {
    block_info->col_infos[col_idx].offset = DecodeFixed32(ptr);
    ptr += 4;
    block_info->col_infos[col_idx].bitmap_len = DecodeFixed16(ptr);
    ptr += 2;
    block_info->col_infos[col_idx].data_len = DecodeFixed32(ptr);
    ptr += 4;
  }
  assert(ptr == result.data + result.len);
  return KStatus::SUCCESS;
}

KStatus TsLastSegment::GetBlock(TsLastSegmentBlockInfo& block_info, TsLastSegmentBlock* block) {
  block->column_blocks.resize(block_info.ncol);
  // column block
  TSSlice result;
  size_t offset = block_info.block_offset;
  for (uint32_t i = 0; i < block_info.ncol; ++i) {
    size_t col_block_len = block_info.col_infos[i].bitmap_len + block_info.col_infos[i].data_len;
    // read col block data
    auto col_block_buf = std::make_unique<char[]>(col_block_len);
    file_->Read(offset, col_block_len, &result, col_block_buf.get());
    if (result.len != col_block_len) {
      LOG_ERROR("last segment[%s] GetBlock failed, read column block[%u] failed.",
                file_->GetFilePath().c_str(), i);
      return KStatus::FAIL;
    }

    // Decompress:

    // parse TsBitmap;
    const TsBitmap* p_bitmap = nullptr;
    TsBitmap tmp;
    bool has_bitmap = block_info.col_infos[i].bitmap_len != 0;
    if (has_bitmap) {
      // TODO(zzr) decompress bitmap first, compression for bitmap is not implemented yet.
      BitmapCompAlg comp_type = static_cast<BitmapCompAlg>(*col_block_buf.get());
      if (comp_type == BitmapCompAlg::kPlain) {
        size_t raw_bitmap_len = block_info.col_infos[i].bitmap_len - 1;
        TSSlice raw_bitmap{col_block_buf.get() + 1, raw_bitmap_len};
        tmp.Map(raw_bitmap, block_info.nrow);
        p_bitmap = &tmp;
      } else {
        assert(false);  // bitmap compression not implemented
      }
    }

    // parse Data
    char* ptr = col_block_buf.get() + block_info.col_infos[i].bitmap_len;
    TsCompAlg first = static_cast<TsCompAlg>(*ptr);
    ptr++;
    GenCompAlg second = static_cast<GenCompAlg>(*ptr);
    ptr++;
    assert(first < TsCompAlg::TS_COMP_ALG_LAST && second < GenCompAlg::GEN_COMP_ALG_LAST);
    auto compressor = CompressorManager::GetInstance().GetCompressor(first, second);

    std::string_view plain_sv{ptr, block_info.col_infos[i].data_len - 2};
    std::string plain;
    if (compressor.IsPlain()) {
    } else {
      bool ok = compressor.Decompress({ptr, plain_sv.size()}, p_bitmap, block_info.nrow, &plain);
      if (!ok) {
        LOG_ERROR("last segment[%s] GetBlock failed, decode column block[%u] failed.",
                  file_->GetFilePath().c_str(), i);
        return KStatus::FAIL;
      }
      plain_sv = plain;
    }

    // save decompressed col block data
    block->column_blocks[i].buffer.assign(plain_sv);
    if (has_bitmap) {
      block->column_blocks[i].bitmap = *p_bitmap;  // copy
    }
    offset += col_block_len;
  }
  // read var data
  char* var_buf = new char[block_info.var_len];
  file_->Read(offset, block_info.var_len, &result, var_buf);
  if (result.len != block_info.var_len) {
    delete[] var_buf;
    LOG_ERROR("last segment[%s] GetBlock failed, read var data failed.",
              file_->GetFilePath().c_str());
    return KStatus::FAIL;
  }
  // save var data
  block->var_buffer.assign(var_buf, block_info.var_len);
  delete[] var_buf;

  return KStatus::SUCCESS;
}

KStatus TsLastSegmentManager::NewLastSegment(std::unique_ptr<TsLastSegment>* last_segment) {
  char buffer[64];
  ver_.fetch_add(1, std::memory_order_relaxed);
  std::snprintf(buffer, sizeof(buffer), "last.ver-%04u", ver_.load(std::memory_order_relaxed));
  auto filename = dir_path_ / buffer;
  *last_segment =
      std::make_unique<TsLastSegment>(ver_, new TsMMapFile(filename, false /*read_only*/));
  return KStatus::SUCCESS;
}

void TsLastSegmentManager::TakeLastSegmentOwnership(std::unique_ptr<TsLastSegment>&& last_segment) {
  wrLock();
  last_segments_.emplace_back(std::move(last_segment));
  n_lastsegment_.fetch_add(1, std::memory_order_relaxed);
  unLock();
}

// TODO(zzr) get lastsegments from VersionManager, this method must be atomic
std::vector<std::shared_ptr<TsLastSegment>> TsLastSegmentManager::GetCompactLastSegments() {
  std::vector<std::shared_ptr<TsLastSegment>> result;
  rdLock();
  if (last_segments_.empty()) {
    unLock();
    return result;
  }
  size_t offset = compacted_ver_ - last_segments_[0]->GetVersion() + 1;
  assert(offset < last_segments_.size());
  if (ver_ - compacted_ver_ >= MAX_COMPACT_NUM) {
    result.assign(last_segments_.begin() + offset,
                  last_segments_.begin() + offset + MAX_COMPACT_NUM);
  }
  unLock();
  return result;
}

bool TsLastSegmentManager::NeedCompact() {
  assert(ver_ > compacted_ver_);
  return n_lastsegment_.load(std::memory_order_relaxed) > MAX_COMPACT_NUM;
}

void TsLastSegmentManager::ClearLastSegments(uint32_t ver) {
  wrLock();
  compacted_ver_ = ver;
  for (auto it = last_segments_.begin(); it != last_segments_.end();) {
    if ((*it)->GetVersion() <= ver) {
      (*it)->GetFilePtr()->MarkDelete();
      it = last_segments_.erase(it);
      n_lastsegment_.fetch_sub(1, std::memory_order_relaxed);
    } else {
      ++it;
    }
  }
  unLock();
}

}  // namespace kwdbts
