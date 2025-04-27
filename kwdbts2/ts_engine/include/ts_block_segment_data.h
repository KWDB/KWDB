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

#include <map>
#include <memory>
#include <string>
#include <cstdio>
#include "ts_common.h"
#include "ts_io.h"

namespace kwdbts {
static constexpr uint64_t TS_BLOCK_SEGMENT_BLOCK_FILE_MAGIC = 0xcb2ffe9321847274;

class TsBlockSegmentBlockFile {
 private:
  string file_path_;
  std::unique_ptr<TsFile> file_ = nullptr;
  std::unique_ptr<KRWLatch> file_mtx_;

  struct TsBlockFileHeader {
    uint64_t magic;
    int32_t encoding;
    int32_t status;
  };

  TsBlockFileHeader header_;

 public:
  explicit TsBlockSegmentBlockFile(const string& file_path);

  ~TsBlockSegmentBlockFile();

  KStatus Open();
  KStatus AppendBlock(const TSSlice& block, uint64_t* offset);
  KStatus ReadData(uint64_t offset, char* buff, size_t len);
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
class TsBlockSegmentAggFile {
  string file_path_;
  std::unique_ptr<TsFile> file_;
  std::unique_ptr<KRWLatch> agg_file_mtx_;

  struct TsAggFileHeader {
    uint64_t magic;               // Magic number for block.e file.
    int32_t encoding;             // Encoding scheme.
    int32_t status;               // status flag.
  };

  TsAggFileHeader header_;

 public:
  explicit TsBlockSegmentAggFile(const string& file_path);

  ~TsBlockSegmentAggFile() {}

  KStatus Open();
  KStatus AppendAggBlock(const TSSlice& agg, uint64_t* offset);
  KStatus ReadAggBlock(uint64_t offset, char* buff, size_t len);
};

}  // namespace kwdbts
