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

#include <limits>
#include <string>
#include <vector>

#include "br_internal_service.pb.h"
#include "ee_fast_string.h"
#include "ee_new_slice.h"
#include "kwdb_type.h"

namespace kwdbts {
class BlockCompressionCodec {
 public:
  explicit BlockCompressionCodec(CompressionTypePB type) : type_(type) {
  }

  virtual ~BlockCompressionCodec() = default;
  virtual KStatus Compress(const KSlice& input, KSlice* output, k_bool use_compression_buffer = false,
                           size_t uncompressed_size = -1, faststring* compressed_body1 = nullptr,
                           std::string* compressed_body2 = nullptr) const = 0;
  virtual KStatus Compress(const std::vector<KSlice>& input, KSlice* output, k_bool use_compression_buffer = false,
                           size_t uncompressed_size = -1, faststring* compressed_body1 = nullptr,
                           std::string* compressed_body2 = nullptr) const;
  virtual KStatus Decompress(const KSlice& input, KSlice* output) const = 0;
  virtual size_t MaxCompressedLen(size_t len) const = 0;
  virtual k_bool ExceedMaxInputSize(size_t len) const {
    return false;
  }

  virtual size_t MaxInputSize() const {
    return std::numeric_limits<size_t>::max();
  }

  virtual LZ4_stream_t* GetLz4() const {
    return nullptr;
  }
  CompressionTypePB Type() const {
    return type_;
  }

 protected:
  CompressionTypePB type_;
};

// Return SUCCESS.
KStatus GetBlockCompressionCodec(CompressionTypePB type, const BlockCompressionCodec** codec,
                                 k_int32 compression_level = -1);

k_bool UseCompressionPool(CompressionTypePB type);
}  // namespace kwdbts
