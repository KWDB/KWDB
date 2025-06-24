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
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kwdb_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_arena.h"
#include "ts_bitmap.h"
#include "ts_bloomfilter.h"
#include "ts_coding.h"
#include "ts_io.h"
#include "ts_segment.h"
#include "ts_engine_schema_manager.h"

namespace kwdbts {

class LastSegmentMetaBlockBase {
 protected:
  virtual void SerializeImpl(std::string* dst) = 0;

 public:
  virtual std::string GetName() const = 0;
  void Serialize(std::string* dst) {
    dst->clear();
    auto name = this->GetName();
    assert(name.size() < 0xFF);
    dst->push_back(static_cast<uint8_t>(name.size()));
    dst->append(name);
    std::string serialized;
    this->SerializeImpl(&serialized);
    dst->append(serialized);
  }
};

class LastSegmentBloomFilter : public LastSegmentMetaBlockBase {
 private:
  std::unique_ptr<TsBloomFilterBuiler> bloomfilter_;

 public:
  explicit LastSegmentBloomFilter(double p = 0.001)
      : bloomfilter_(std::make_unique<TsBloomFilterBuiler>(p)) {}
  static std::string Name() { return "LastSegmentBloomFilter"; }
  std::string GetName() const override { return Name(); }
  void Add(TSEntityID entity_id) { bloomfilter_->Add(entity_id); }
  void SerializeImpl(std::string* dst) override {
    auto filter = bloomfilter_->Finalize();
    filter.Serialize(dst);
  }
};

// first 8 byte of `md5 -s kwdbts::TsLastSegment`
// TODO(zzr) fix endian
static constexpr uint64_t FOOTER_MAGIC = 0xcb2ffe9321847271;

struct TsLastSegmentFooter {
  uint64_t block_info_idx_offset, n_data_block;
  uint64_t meta_block_idx_offset, n_meta_block;
  uint8_t padding[16];
  uint64_t file_version;
  uint64_t magic_number;
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
  std::unique_ptr<TsRandomReadFile> file_;
  std::unique_ptr<TsBloomFilter> bloom_filter_;

  explicit TsLastSegment(uint32_t file_number, std::unique_ptr<TsRandomReadFile>&& file)
      : file_number_(file_number), file_(std::move(file)) {}

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
    TsLastSegmentFooter footer;
    auto s = GetFooter(&footer);
    if (s == FAIL) {
      return s;
    }
    if (footer.magic_number != FOOTER_MAGIC) {
      LOG_ERROR("magic mismatch");
      return FAIL;
    }

    // load necessary meta block to memory.
    // NOTICE: maybe we will support lazy loading later. For now, just load all meta blocks in
    // Open()
    Arena arena;
    int nmeta = footer.n_meta_block;
    TSSlice result;
    char* buf = arena.Allocate(nmeta * 16);
    s = file_->Read(footer.meta_block_idx_offset, nmeta * 16, &result, buf);
    if (s == FAIL) {
      return s;
    }
    std::vector<size_t> meta_offset(nmeta);
    std::vector<size_t> meta_len(nmeta);
    for (int i = 0; i < nmeta; ++i) {
      GetFixed64(&result, &meta_offset[i]);
      GetFixed64(&result, &meta_len[i]);
    }

    for (int i = 0; i < nmeta; ++i) {
      char* buf2 = arena.Allocate(meta_len[i]);
      s = file_->Read(meta_offset[i], meta_len[i], &result, buf2);
      if (s == FAIL) {
        return FAIL;
      }
      uint8_t len = static_cast<uint8_t>(result.data[0]);
      std::string_view sv{result.data + 1, len};
      result.data += len + 1;
      result.len -= len + 1;
      if (sv == LastSegmentBloomFilter::Name()) {
        s = TsBloomFilter::FromData(result, &bloom_filter_);
      } else {
        assert(false);
      }
      if (s == FAIL) {
        return FAIL;
      }
    }
    return SUCCESS;
  }

  uint32_t GetFileNumber() const { return file_number_; }

  std::string GetFilePath() const { return file_->GetFilePath(); }

  void MarkDelete() { file_->MarkDelete(); }

  KStatus GetFooter(TsLastSegmentFooter* footer) const;

  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>* block_indexes);

  KStatus GetBlockSpans(std::list<shared_ptr<TsBlockSpan>>& block_spans, TsEngineSchemaManager* schema_mgr);

  KStatus GetBlockSpans(const TsBlockItemFilterParams& filter,
                        std::list<shared_ptr<TsBlockSpan>>& block_spans,
                        std::shared_ptr<TsTableSchemaManager> tbl_schema_mgr,
                        uint32_t scan_version) override;

  bool MayExistEntity(TSEntityID entity_id) const override {
    return bloom_filter_ ? bloom_filter_->MayExist(entity_id)
                         : true;  // always return true when bloom filter doesn't exist.
  }

 private:
  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>*) const;
};

}  // namespace kwdbts
