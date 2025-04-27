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

#pragma once
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_arena.h"
#include "ts_bitmap.h"
#include "ts_io.h"
#include "ts_segment.h"

namespace kwdbts {

class MetaBlockBase {
 public:
  virtual char* GetName() const = 0;
  virtual void Serialize(std::string* dst) = 0;
};

// first 8 byte of `md5 -s kwdbts::TsLastSegment`
// TODO(zzr) fix endian
static constexpr uint64_t FOOTER_MAGIC = 0xcb2ffe9321847271;

struct TsLastSegmentFooter {
  uint64_t block_info_idx_offset, n_data_block;
  uint64_t meta_block_idx_offset, n_meta_block;
  uint8_t padding[16] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
  uint64_t file_version;
  const uint64_t magic_number = FOOTER_MAGIC;
};
static_assert(sizeof(TsLastSegmentFooter) == 64);

struct TsLastSegmentBlockIndex {
  uint64_t offset, length;
  uint64_t table_id;
  uint32_t table_version, n_entity;
  int64_t min_ts, max_ts;
  uint64_t min_entity_id, max_entity_id;
};

struct TsLastSegmentBlockInfo {
  uint64_t block_offset;
  uint32_t nrow;
  uint32_t ncol;
  uint32_t var_offset;
  uint32_t var_len;
  struct ColInfo {
    uint32_t offset;
    uint16_t bitmap_len;
    uint32_t data_len;
  };
  std::vector<ColInfo> col_infos;
};
const size_t LAST_SEGMENT_BLOCK_INFO_HEADER_SIZE = sizeof(uint64_t) + 4 * sizeof(uint32_t);
const size_t LAST_SEGMENT_BLOCK_INFO_COL_INFO_SIZE =
    sizeof(uint32_t) + sizeof(uint16_t) + sizeof(uint32_t);

struct TsLastSegmentColumnBlock {
  TsBitmap bitmap;
  std::string buffer;
};

class TsLastSegment : public TsSegmentBase, public std::enable_shared_from_this<TsLastSegment> {
 public:
  static int kNRowPerBlock;
  friend class TsLastBlock;

 private:
  uint32_t file_number_;
  std::unique_ptr<TsFile> file_;

  explicit TsLastSegment(uint32_t file_number, const std::string& path)
      : file_number_(file_number), file_(std::make_unique<TsMMapFile>(path, true)) {}

 public:
  template <class... Args>
  static std::shared_ptr<TsLastSegment> Create(Args&&... args) {
    return std::shared_ptr<TsLastSegment>(new TsLastSegment(std::forward<Args>(args)...));
  }

  ~TsLastSegment() = default;

  KStatus Open() {
    // just check the magic number;
    auto sz = file_->GetFileSize();
    if (sz < sizeof(TsLastSegmentFooter)) {
      LOG_ERROR("lastsegment file corrupted");
      return FAIL;
    }
    uint64_t magic;
    TSSlice result;
    file_->Read(sz - 8, sz, &result, reinterpret_cast<char*>(&magic));
    if (magic != FOOTER_MAGIC) {
      LOG_ERROR("magic mismatch, expect: %lx, found: %lx", FOOTER_MAGIC, magic);
      return FAIL;
    }
    return SUCCESS;
  }

  uint32_t GetVersion() const { return file_number_; }

  std::string GetFilePath() const { return file_->GetFilePath(); }

  void MarkDelete() { file_->MarkDelete(); }

  KStatus GetFooter(TsLastSegmentFooter* footer) const;

  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes);

  KStatus GetBlockSpans(std::list<TsBlockSpan>* spans);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter,
                        std::list<TsBlockSpan>* spans) override;

 private:
  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>*) const;
};

}  // namespace kwdbts
