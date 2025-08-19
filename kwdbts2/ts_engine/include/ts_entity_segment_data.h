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
#include <map>
#include <memory>
#include <string>
#include <cstdio>
#include "ts_common.h"
#include "ts_entity_segment_handle.h"
#include "ts_io.h"

namespace kwdbts {
static constexpr uint64_t TS_ENTITY_SEGMENT_BLOCK_FILE_MAGIC = 0xcb2ffe9321847274;
static constexpr uint64_t TS_ENTITY_SEGMENT_AGG_FILE_MAGIC = 0xcb2ffe9321847275;

struct TsAggAndBlockFileHeader {
  uint64_t magic;
  int32_t encoding;
  int32_t status;
};

class TsEntitySegmentBlockFile {
 private:
  fs::path root_path_;
  const EntitySegmentHandleInfo info_;

  std::unique_ptr<TsRandomReadFile> r_file_ = nullptr;
  TsAggAndBlockFileHeader header_;

 public:
  explicit TsEntitySegmentBlockFile(const string& root, EntitySegmentHandleInfo info);

  ~TsEntitySegmentBlockFile();

  KStatus Open();
  KStatus ReadData(uint64_t offset, char** buff, size_t len);
  void MarkDelete() { r_file_->MarkDelete(); }
};

/*
 * Aggregation block structure
 * +-------------+-------------+-------------+---------------+---------------+---------------+
 * | col1 offset | col2 offset | col3 offset | col1 agg      | col2 agg      | col3 agg      |
 * +-------------+-------------+-------------+---------------+---------------+---------------+
 *
 * Bytes:
 * offset: uint32_t
 * col agg: Which is related to the size of the data type
 */

/*
 * Fixed-length column aggregation structure
 * +-------+-----+-----+
 * | count | max | min |
 * +-------+-----+-----+
 *
 * Bytes:
 * - count: uint16_t
 * - max: column type size, e.g. double type is 8 bytes
 * - min: column type size, e.g. double type is 8 bytes
 */

/*
 * Variable-length column aggregation structure:
 * +-------+---------+---------+---------+---------+
 * | count | max_len | min_len | max_str | min_str |
 * +-------+---------+---------+---------+---------+

 * Bytes:
 * - count: uint16_t
 * - max_len: uint32_t
 * - min_len: uint32_t
 * - max_str: Actual string length
 * - min_str: Actual string length
 */
class TsEntitySegmentAggFile {
 private:
  fs::path root_;
  EntitySegmentHandleInfo info_;
  std::unique_ptr<TsRandomReadFile> r_file_ = nullptr;
  TsAggAndBlockFileHeader header_;

 public:
  explicit TsEntitySegmentAggFile(const string& root, EntitySegmentHandleInfo info);

  ~TsEntitySegmentAggFile() {}

  KStatus Open();
  KStatus ReadAggData(uint64_t offset, char** buff, size_t len);
  void MarkDelete() { r_file_->MarkDelete(); }
};

}  // namespace kwdbts
