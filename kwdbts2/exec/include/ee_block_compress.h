// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once

#include <lz4.h>
#include <snappy-sinksource.h>
#include <snappy.h>
// #include <snappy/snappy-sinksource.h>
// #include <snappy/snappy.h>

#include <limits>
#include <string>
#include <vector>

#include "br_internal_service.pb.h"
#include "ee_fast_string.h"
#include "ee_new_slice.h"
#include "kwdb_type.h"

namespace kwdbts {
class BlockCompressor {
 public:
  explicit BlockCompressor(CompressionTypePB type) : compression_type_(type) {
  }

  virtual ~BlockCompressor() = default;

  // Calculate the maximum compressed length of the input data.
  virtual size_t CalculateMaxCompressedLength(size_t len) const = 0;
  // Compress the input data.
  virtual KStatus CompressBlock(const KSlice& input, KSlice* output, k_bool use_compressed_buff = false,
                                size_t uncompressed_size = -1, QuickString* compressed_fast = nullptr,
                                std::string* compressed_std = nullptr) const = 0;
  virtual k_bool CheckIfExceedsMaxInputSize(size_t len) const {
    return false;
  }
  // no use now
  virtual LZ4_stream_t* GetLz4StreamObject() const {
    return nullptr;
  }
  // no use now
  virtual BlockCompressor* GetBlockCompressor() const {
    return nullptr;
  }

  // Decompress the input data.
  virtual KStatus DecompressBlock(const KSlice& input, KSlice* output) const = 0;

  // get max size
  virtual size_t GetMaxInputSize() const {
    return std::numeric_limits<size_t>::max();
  }
  // Compress the input data.
  virtual KStatus CompressBlock(const std::vector<KSlice>& input, KSlice* output, k_bool use_compressed_buff = false,
                                size_t uncompressed_size = -1, QuickString* compressed_fast = nullptr,
                                std::string* compressed_std = nullptr) const;

  virtual size_t GetOutputSize() const {
    return output_size_;
  }

  // get compression type
  CompressionTypePB GetCompressionType() const {
    return compression_type_;
  }

  virtual k_int32 GetCompressionLevel() const {
    return compression_level_;
  }

 protected:
  CompressionTypePB compression_type_;

 private:
  size_t output_size_ = 0;
  k_int32 compression_level_ = -1;
};

// Return SUCCESS.
KStatus GetBlockCompressor(CompressionTypePB type, const BlockCompressor** codec,
                                 k_int32 compression_level = -1);

k_bool UseCompressionPool(CompressionTypePB type);
}  // namespace kwdbts
