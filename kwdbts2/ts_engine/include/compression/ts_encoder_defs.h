
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

#include <type_traits>

#include "ts_compressor_base.h"

namespace kwdbts {

class GorillaInt : public CompressorImpl {
 private:
  GorillaInt() = default;

 public:
  static GorillaInt &GetInstance() {
    static GorillaInt inst;
    return inst;
  }
  static constexpr int stride = 8;
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class GorillaIntV2 : public CompressorImpl {
 private:
  GorillaIntV2() = default;

 public:
  static GorillaIntV2 &GetInstance() {
    static GorillaIntV2 inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class Chimp : public CompressorImpl {
  static_assert(std::is_floating_point_v<T>);

 private:
  Chimp() = default;

 public:
  static Chimp &GetInstance() {
    static Chimp inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class Simple8BInt : public CompressorImpl {
 private:
  Simple8BInt() = default;

 public:
  static Simple8BInt &GetInstance() {
    static Simple8BInt inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class Simple8BIntV2 : public CompressorImpl {
 private:
  Simple8BIntV2() = default;

 public:
  static Simple8BIntV2 &GetInstance() {
    static Simple8BIntV2 inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

class BitPacking : public CompressorImpl {
 private:
  BitPacking() = default;

 public:
  static BitPacking &GetInstance() {
    static BitPacking inst;
    return inst;
  }
  static constexpr int stride = 1;
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class ELF : public CompressorImpl {
  static_assert(std::is_floating_point_v<T>);

 private:
  ELF() = default;

 public:
  static ELF &GetInstance() {
    static ELF inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class ALP : public CompressorImpl {
  static_assert(std::is_floating_point_v<T>);

 private:
  ALP() = default;

 public:
  static ALP &GetInstance() {
    static ALP inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class BSS : public CompressorImpl {
  static_assert(std::is_floating_point_v<T>);

 private:
  BSS() = default;

 public:
  static BSS &GetInstance() {
    static BSS inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

template <class T>
class FpTrunc : public CompressorImpl {
  static_assert(std::is_floating_point_v<T>);

 private:
  FpTrunc() = default;

 public:
  static FpTrunc &GetInstance() {
    static FpTrunc inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

// encoding bool and bitmap
class TsRangeCodecBool : public CompressorImpl {
 private:
  TsRangeCodecBool() = default;

 public:
  static TsRangeCodecBool &GetInstance() {
    static TsRangeCodecBool inst;
    return inst;
  }
  static constexpr int stride = 1;
  bool Compress(TSSlice data, uint64_t count, TsBufferBuilder *out, const TsCompressionConfig &cfg) const override;
  bool Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const override;
  size_t GetUncompressedSize(TSSlice data, uint64_t count) const override { return stride * count; }
};

}  // namespace kwdbts
