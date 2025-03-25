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

#include <vector>
#include "ts_common.h"

namespace kwdbts {

/**
 * Block Struct that compressed to store into disk.
 * col_data contains bitmap info.
+------------+------------+-----+----------------+------------+-------+-------+-----+-------------+
| Col_1_len  | Col_2_len  | ... | Col_{N_1}_len  | Col_1_data | Col_2_data | ... | Col_{N_1}_data | 
+------------+-------+-------+-----+-------------+------------+-------+-------+-----+-------------+
|  4 bytes   |  4 bytes   | ... |   4 bytes      |  n bytes   |  n bytes   | ... |    n bytes     |
+------------+-------+-------+-----+-------------+------------+-------+-------+-----+-------------+
 */

/**
 * col data struct that compressed to store into disk.
+---------------+------------+--------------------------+
| compressor_id |  compressed col data and bitmap data  |
+---------------+------------+--------------------------+
 */

// used for caching decompressed column data
struct TsBlockColData {
  TSSlice data;
  TSSlice bitmap;
  bool need_free;
  ~TsBlockColData() {
    if (need_free) {
      if (data.data != nullptr) {
        free(data.data);
      }
      if (bitmap.data != nullptr) {
        free(bitmap.data);
      }
    }
  }
};

class TsMetricBlockBuilder {
 private:
  uint32_t row_num_;
  const std::vector<AttributeInfo>& schema_;

 public:
  TsMetricBlockBuilder(const std::vector<AttributeInfo>& schema, uint32_t num) : row_num_(num), schema_(schema) {}

  TSSlice Build(const std::vector<TsBlockColData>& col_datas);
};

class TsMetricBlockParser {
 private:
  uint32_t row_num_;
  const std::vector<AttributeInfo>& schema_;
  std::vector<TSSlice> col_compressed_data_;

 public:
  TsMetricBlockParser(const std::vector<AttributeInfo>& schema, uint32_t num) : row_num_(num), schema_(schema) {}

  bool Parse(TSSlice block_addr);
  // first column idx is 0.
  bool GetColData(uint32_t col_idx, TsBlockColData* data);
};

}  // namespace kwdbts
