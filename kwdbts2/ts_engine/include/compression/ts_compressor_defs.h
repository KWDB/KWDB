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
#include "compression/ts_compressor_base.h"
#include "snappy-sinksource.h"
#include "snappy.h"
namespace kwdbts {

class SnappyString : public CompressorImpl {
 private:
  SnappyString() = default;
  class BufferSink : public snappy::Sink {
    TsBufferBuilder *out_;

   public:
    explicit BufferSink(TsBufferBuilder *out) : out_(out) {}
    void Append(const char *bytes, size_t n) override { out_->append({bytes, n}); }
  };

 public:
  static constexpr int stride = -1;
  static SnappyString &GetInstance() {
    static SnappyString inst;
    return inst;
  }
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &config) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override;
};

class LZ4String : public CompressorImpl {
 private:
  LZ4String() = default;

 public:
  static constexpr int stride = -1;
  static LZ4String &GetInstance() {
    static LZ4String inst;
    return inst;
  }

  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &config) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override;
};

class ZSTDString : public CompressorImpl {
 private:
  ZSTDString() = default;

 public:
  static constexpr int stride = -1;
  static ZSTDString &GetInstance() {
    static ZSTDString inst;
    return inst;
  }

  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &config) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override;
};

class ZLIBString : public CompressorImpl {
 private:
  ZLIBString() = default;

 public:
  static constexpr int stride = -1;
  static ZLIBString &GetInstance() {
    static ZLIBString inst;
    return inst;
  }

  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &config) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override;
};

}  // namespace kwdbts
