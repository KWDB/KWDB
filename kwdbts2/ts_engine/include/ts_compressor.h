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
#include <string>
#include <tuple>
#include <unordered_map>

#include "data_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"

namespace kwdbts {

enum class TsCompAlg : uint16_t {
  kPlain = 0,
  kGorilla_32 = 1,
  kGorilla_64 = 2,
  kSimple8B_s8 = 3,
  kSimple8B_s16 = 4,
  kSimple8B_s32 = 5,
  kSimple8B_s64 = 6,
  kSimple8B_u8 = 7,
  kSimple8B_u16 = 8,
  kSimple8B_u32 = 9,
  kSimple8B_u64 = 10,
  kChimp_32 = 11,
  kChimp_64 = 12,
  // kALP,
  // kELF,
  TS_COMP_ALG_LAST
};

// compression algorithms for general purpose.
enum class GenCompAlg : uint16_t {
  kPlain = 0,
  kSnappy = 1,
  // kGzip = 2,
  // kLzo = 3,
  // kLz4 = 4,
  // kXz = 5,
  // kZstd = 6,
  // kLzma = 7,
  GEN_COMP_ALG_LAST
};

enum class BitmapCompAlg : uint8_t {
  kPlain = 0,
  kCompressed = 1,
};

class TsSliceGuard {
 private:
  TSSlice slice;
  std::string str;

 public:
  TsSliceGuard() : slice{nullptr, 0} {}
  explicit TsSliceGuard(TSSlice s) : slice{s} {}
  explicit TsSliceGuard(std::string&& str_) : str(std::move(str_)) {
    slice.data = str.data();
    slice.len = str.size();
  }

  TsSliceGuard(const TsSliceGuard&) = delete;
  TsSliceGuard& operator=(const TsSliceGuard&) = delete;

  TsSliceGuard(TsSliceGuard&& other) noexcept { *this = std::move(other); }
  TsSliceGuard& operator=(TsSliceGuard&& other) noexcept {
    if (&other == this) {
      return *this;
    }
    if (!other.str.empty()) {
      this->str = std::move(other.str);
      this->slice = TSSlice{this->str.data(), this->str.size()};
    } else {
      this->slice = other.slice;
    }
    return *this;
  }

  TSSlice AsSlice() const { return slice; }
  std::string_view AsStringView() const { return {slice.data, slice.len}; }
  size_t size() const { return slice.len; }
  const char* data() const { return slice.data; }
  char* data() { return slice.data; }
  bool empty() const { return slice.len == 0; }

  const std::string& AsString() const { return str; }
};

class TsCompressorBase;
class GenCompressorBase;
class CompressorManager {
 private:
  class TwoLevelCompressor {
   private:
    const TsCompressorBase* first_;
    const GenCompressorBase* second_;

    TsCompAlg first_algo_;
    GenCompAlg second_algo_;

   public:
    TwoLevelCompressor(const TsCompressorBase* first, const GenCompressorBase* second,
                       TsCompAlg first_algo, GenCompAlg second_algo)
        : first_(first), second_(second) {
      first_algo_ = first == nullptr ? TsCompAlg::kPlain : first_algo;
      second_algo_ = second == nullptr ? GenCompAlg::kPlain : second_algo;
    }
    bool Compress(const TSSlice& raw, const TsBitmap* bitmap, uint32_t count, std::string* out) const;

    bool Decompress(const TSSlice& raw, const TsBitmap* bitmap, uint32_t count, std::string* out) const;
    bool IsPlain() const { return (first_ == nullptr && second_ == nullptr); }

    std::tuple<TsCompAlg, GenCompAlg> GetAlgorithms() const;
  };

  std::unordered_map<TsCompAlg, TsCompressorBase*> ts_comp_;
  std::unordered_map<GenCompAlg, GenCompressorBase*> general_compressor_;
  std::unordered_map<DATATYPE, std::tuple<TsCompAlg, GenCompAlg>> default_algs_;

  CompressorManager();

 public:
  static CompressorManager& GetInstance() {
    static CompressorManager mgr;
    return mgr;
  }
  CompressorManager(const CompressorManager&) = delete;
  void operator=(const CompressorManager&) = delete;

  TwoLevelCompressor GetCompressor(TsCompAlg first, GenCompAlg second) const;
  std::tuple<TsCompAlg, GenCompAlg> GetDefaultAlgorithm(DATATYPE dtype) const;
  TwoLevelCompressor GetDefaultCompressor(DATATYPE dtype) const;

  bool CompressData(TSSlice input, const TsBitmap* bitmap, uint64_t count, std::string* output,
                    TsCompAlg fisrt, GenCompAlg second) const;
  bool CompressVarchar(TSSlice input, std::string* output, GenCompAlg alg) const;
  bool DecompressData(TSSlice input, const TsBitmap* bitmap, uint64_t count, TsSliceGuard* out) const;
  bool DecompressVarchar(TSSlice input, TsSliceGuard* out) const;
};

}  //  namespace kwdbts
