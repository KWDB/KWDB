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
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include <tuple>
#include <limits>

#include "data_type.h"
#include "libkwdbts2.h"
#include "ts_block.h"
#include "ts_common.h"
#include "ts_engine_schema_manager.h"
#include "ts_io.h"
#include "ts_lastsegment.h"
#include "ts_metric_block.h"
#include "ts_payload.h"
#include "ts_table_schema_manager.h"
#include "ts_version.h"
#include "ts_lastsegment_endec.h"

namespace kwdbts {

class TsLastSegmentBuilder {
 private:
  TsEngineSchemaManager* engine_schema_manager_;
  std::unique_ptr<TsAppendOnlyFile> last_segment_file_;

  class BlockIndexCollector;
  std::unique_ptr<BlockIndexCollector> block_index_collector_;
  std::unique_ptr<TsMetricBlockBuilder> metric_block_builder_;

  std::vector<TSEntityID> entity_id_buffer_;
  std::vector<TsLastSegmentBlockInfo> block_info_buffer_;
  std::vector<TsLastSegmentBlockIndex> block_index_buffer_;

  using TableVersionInfo = std::tuple<TSTableID, uint32_t>;
  TableVersionInfo table_version_ = {0, 0};  // initialized to invalid value
  uint64_t file_number_ = 0;

  // bloom filter
  std::unique_ptr<LastSegmentBloomFilter> bloom_filter_;

  std::vector<LastSegmentMetaBlockBase*> meta_blocks_;

 public:
  TsLastSegmentBuilder(TsEngineSchemaManager* schema_mgr, std::unique_ptr<TsAppendOnlyFile>&& last_segment,
                        uint64_t file_number)
      : engine_schema_manager_(schema_mgr),
        last_segment_file_(std::move(last_segment)),
        file_number_(file_number),
        bloom_filter_(std::make_unique<LastSegmentBloomFilter>()) {
    meta_blocks_.push_back(bloom_filter_.get());
  }
  KStatus PutBlockSpan(std::shared_ptr<TsBlockSpan> span);
  KStatus Finalize();
  uint64_t GetFileNumber() const { return file_number_; }
  uint64_t GetMaxOSN() const;

 private:
  KStatus RecordAndWriteBlockToFile();
};

class TsLastSegmentBuilder::BlockIndexCollector {
 private:
  TSTableID table_id_;
  uint32_t version_;

  TSEntityID prev_entity_id_ = -1;
  uint32_t n_entity_ = 0;

  TSEntityID max_entity_id_ = 0;
  TSEntityID min_entity_id_ = std::numeric_limits<TSEntityID>::max();
  uint64_t max_osn_ = 0;
  uint64_t min_osn_ = std::numeric_limits<uint64_t>::max();
  uint64_t first_osn_ = std::numeric_limits<uint64_t>::max();
  uint64_t last_osn_ = std::numeric_limits<uint64_t>::max();
  timestamp64 max_ts_ = std::numeric_limits<timestamp64>::min();
  timestamp64 min_ts_ = std::numeric_limits<timestamp64>::max();
  timestamp64 first_ts_ = std::numeric_limits<timestamp64>::max();
  timestamp64 last_ts_ = std::numeric_limits<timestamp64>::max();

 public:
  BlockIndexCollector(TSTableID table_id, uint32_t version) : table_id_(table_id), version_(version) {}
  void Collect(TsBlockSpan* span);
  TsLastSegmentBlockIndex GetIndex() const;

  void Reset() { new (this) BlockIndexCollector(table_id_, version_); }
};
}  // namespace kwdbts
