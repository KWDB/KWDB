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
#include <shared_mutex>
#include <string>
#include <type_traits>
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
#include "ts_lastsegment_endec.h"

namespace kwdbts {

class LastSegmentMetaBlockBase {
 protected:
  virtual void SerializeImpl(std::string* dst) = 0;

 public:
  virtual ~LastSegmentMetaBlockBase() {}
  virtual std::string GetName() const = 0;
  void Serialize(std::string* dst) {
    dst->clear();
    std::string serialized;
    this->SerializeImpl(&serialized);
    if (serialized.empty()) {
      return;
    }
    auto name = this->GetName();
    assert(name.size() < 0xFF);
    dst->push_back(static_cast<uint8_t>(name.size()));
    dst->append(name);
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
    if (!bloomfilter_->IsEmpty()) {
      auto filter = bloomfilter_->Finalize();
      filter.Serialize(dst);
    }
  }
};

class TsLastSegment : public TsSegmentBase {
 public:
  static int kNRowPerBlock;
  friend class TsLastBlock;

 private:
  uint32_t file_number_;
  std::unique_ptr<TsRandomReadFile> file_;
  std::unique_ptr<TsBloomFilter> bloom_filter_;

  class TsLastSegBlockCache;
  std::unique_ptr<TsLastSegBlockCache> block_cache_;

  TsLastSegmentFooter footer_;

  explicit TsLastSegment(uint32_t file_number, std::unique_ptr<TsRandomReadFile>&& file)
      : file_number_(file_number), file_(std::move(file)) {}

 public:
  template <class... Args>
  static std::shared_ptr<TsLastSegment> Create(Args&&... args) {
    return std::shared_ptr<TsLastSegment>(new TsLastSegment(std::forward<Args>(args)...));
  }

  ~TsLastSegment() = default;

  KStatus Open();

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

class TsLastSegment::TsLastSegBlockCache {
 private:
  TsLastSegment* segment_;

  class BlockIndexCache;
  std::unique_ptr<BlockIndexCache> block_index_cache_;

  class BlockInfoCache;
  std::unique_ptr<BlockInfoCache> block_info_cache_;

  class BlockCache;
  std::unique_ptr<BlockCache> block_cache_;

 public:
  explicit TsLastSegBlockCache(TsLastSegment* last, int nblock);

  KStatus GetAllBlockIndex(std::vector<TsLastSegmentBlockIndex>** block_indexes) const;
  KStatus GetBlockIndex(int block_id, TsLastSegmentBlockIndex** index) const;
  KStatus GetBlockInfo(int block_id, TsLastSegmentBlockInfo2** info) const;
  KStatus GetBlock(int block_id, std::shared_ptr<TsBlock>* block) const;
};

class TsLastSegment::TsLastSegBlockCache::BlockIndexCache {
 private:
  TsLastSegment* lastseg_;
  bool cached_ = false;
  std::vector<TsLastSegmentBlockIndex> block_indices_;
  std::shared_mutex mu_;

 public:
  explicit BlockIndexCache(TsLastSegment* lastseg) : lastseg_(lastseg) {}
  KStatus GetBlockIndices(std::vector<TsLastSegmentBlockIndex>** block_indices);
};

class TsLastSegment::TsLastSegBlockCache::BlockInfoCache {
 private:
  TsLastSegBlockCache* lastseg_cache_;
  std::vector<uint8_t> cache_flag_;
  std::vector<TsLastSegmentBlockInfo2> block_infos_;
  std::shared_mutex mu_;

 public:
  explicit BlockInfoCache(TsLastSegBlockCache* lastseg_cache, int nblocks)
      : lastseg_cache_(lastseg_cache), cache_flag_(nblocks, 0), block_infos_(nblocks) {}
  KStatus GetBlockInfo(int block_id, TsLastSegmentBlockInfo2** info);
};

class TsLastSegment::TsLastSegBlockCache::BlockCache {
 private:
  TsLastSegBlockCache* lastseg_cache_;
  std::vector<uint8_t> cache_flag_;
  std::vector<std::shared_ptr<TsBlock>> block_infos_;
  std::shared_mutex mu_;

 public:
  explicit BlockCache(TsLastSegBlockCache* cache, int nblocks)
      : lastseg_cache_(cache), cache_flag_(nblocks, 0), block_infos_(nblocks) {}
  KStatus GetBlock(int block_id, std::shared_ptr<TsBlock>* block);
};

}  // namespace kwdbts
