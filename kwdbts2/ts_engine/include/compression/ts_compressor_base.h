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

#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_compressor_manager.h"
#include "ts_sliceguard.h"

namespace kwdbts {

int GetLevelIdx(roachpb::ColumnCompressLevel level);

class TsCompressorBase {
 public:
  virtual ~TsCompressorBase() = default;
  virtual bool Compress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count, TsBufferBuilder *out,
                        const TsCompressionConfig &cfg) const = 0;
  virtual bool Decompress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count, TsSliceGuard *out) const = 0;
};

class GenCompressorBase {
 public:
  virtual ~GenCompressorBase() = default;
  virtual bool Compress(TSSlice raw, TsBufferBuilder *out, const TsCompressionConfig &cfg) const = 0;
  virtual bool Decompress(TSSlice raw, TsSliceGuard *out) const = 0;
};

class CompressorImpl {
 protected:
  CompressorImpl() = default;

 public:
  virtual ~CompressorImpl() = default;
  CompressorImpl(const CompressorImpl &) = delete;
  void operator=(const CompressorImpl &) = delete;
  virtual bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const = 0;
  virtual bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const = 0;
  virtual size_t GetUncompressedSize(TSSlice data, uint64_t count) const = 0;
};

template <class Compressor>
class ConcreateTsCompressor : public TsCompressorBase {
 private:
  ConcreateTsCompressor() = default;

 public:
  static TsCompressorBase &GetInstance() {
    static ConcreateTsCompressor inst;
    return inst;
  }
  bool Compress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count, TsBufferBuilder *out,
                const TsCompressionConfig &cfg) const override {
    assert(bitmap == nullptr || bitmap->GetCount() == count);
    int stride = Compressor::stride;
    if (stride < 0 || bitmap == nullptr || bitmap->IsAllValid()) {
      return Compressor::GetInstance().Compress(raw, count, out, cfg);
    }
    assert(raw.len == count * stride);

    TsBufferBuilder valid_data;
    const char *data = raw.data;
    valid_data.reserve(bitmap->GetValidCount() * stride);
    for (int i = 0; i < count; ++i) {
      if ((*bitmap)[i] != kValid) continue;
      valid_data.append(data + i * stride, stride);
    }
    auto buffer = valid_data.GetBuffer();

    return Compressor::GetInstance().Compress(buffer.AsSlice(), bitmap->GetValidCount(), out, cfg);
  }

  bool Decompress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count, TsSliceGuard *out) const override {
    int stride = Compressor::stride;
    if (stride < 0 || bitmap == nullptr || bitmap->IsAllValid()) {
      return Compressor::GetInstance().Decompress(raw, count, out);
    }
    TsSliceGuard buf;
    bool ok = Compressor::GetInstance().Decompress(raw, bitmap->GetValidCount(), &buf);
    if (!ok) {
      return false;
    }

    assert(buf.size() == bitmap->GetValidCount() * stride);
    const char *ptr = buf.data();
    TsBufferBuilder builder;
    builder.resize(static_cast<size_t>(count) * stride);
    for (int i = 0; i < count; ++i) {
      if ((*bitmap)[i] == kValid) {
        std::memcpy(builder.data() + static_cast<size_t>(i) * stride, ptr, stride);
        ptr += stride;
      }
    }
    *out = builder.GetBuffer();
    return true;
  }
};

template <class Compressor>
class ConcreateGenCompressor : public GenCompressorBase {
 private:
  ConcreateGenCompressor() = default;

 public:
  static GenCompressorBase &GetInstance() {
    static ConcreateGenCompressor inst;
    return inst;
  }
  bool Compress(TSSlice raw, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override {
    const CompressorImpl &comp = Compressor::GetInstance();
    return comp.Compress(raw, 0, out, cfg);
  }
  bool Decompress(TSSlice raw, TsSliceGuard *out) const override {
    const CompressorImpl &comp = Compressor::GetInstance();
    return comp.Decompress(raw, 0, out);
  }
};

}  // namespace kwdbts
