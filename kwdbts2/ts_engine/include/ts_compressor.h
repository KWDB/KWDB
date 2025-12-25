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
#include <tuple>
#include <unordered_map>
#include <utility>

#include "data_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_sliceguard.h"

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

  kSimple8B_V2_s8 = 13,
  kSimple8B_V2_s16 = 14,
  kSimple8B_V2_s32 = 15,
  kSimple8B_V2_s64 = 16,
  kSimple8B_V2_u8 = 17,
  kSimple8B_V2_u16 = 18,
  kSimple8B_V2_u32 = 19,
  kSimple8B_V2_u64 = 20,

  kBitPacking = 21,

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

enum class BitmapType : uint8_t {
  kRaw = 0,
  kAllValid = 1,
  kAllNone = 2,
  kAllNull = 3,

  BITMAP_COMP_ALG_LAST
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
    bool Compress(TSSlice raw, const TsBitmapBase* bitmap, uint32_t count, TsBufferBuilder* out) const;

    bool Decompress(TSSlice raw, const TsBitmapBase* bitmap, uint32_t count, TsSliceGuard* out) const;
    bool IsPlain() const { return (first_ == nullptr && second_ == nullptr); }

    std::tuple<TsCompAlg, GenCompAlg> GetAlgorithms() const;
  };

  std::unordered_map<TsCompAlg, TsCompressorBase*> ts_comp_;
  std::unordered_map<GenCompAlg, GenCompressorBase*> general_compressor_;
  std::unordered_map<DATATYPE, std::tuple<TsCompAlg, GenCompAlg>> default_algs_;

  CompressorManager();

  bool DoDecompressData(uint32_t alg, TsSliceGuard&& input, const TsBitmapBase* bitmap, uint64_t count,
                        TsSliceGuard* out) const;
  bool DoDecompressVarchar(GenCompAlg alg, TsSliceGuard&& input, TsSliceGuard* out) const;

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

  bool CompressData(TSSlice input, const TsBitmapBase* bitmap, uint64_t count, TsBufferBuilder* output,
                    TsCompAlg first, GenCompAlg second) const;
  bool CompressVarchar(TSSlice input, TsBufferBuilder* output, GenCompAlg alg) const;
  bool DecompressData(TsSliceGuard&& input, const TsBitmapBase* bitmap, uint64_t count, TsSliceGuard* out) const {
    if (input.size() < 4) {
      LOG_ERROR("Invalid input length, too short");
      return false;
    }
    uint32_t v;
    GetFixed32(&input, &v);
    if (v == 0) {
      *out = std::move(input);
      return true;
    }

    return DoDecompressData(v, std::move(input), bitmap, count, out);
  }
  bool DecompressVarchar(TsSliceGuard&& input, TsSliceGuard* out) const {
    if (input.size() < 2) {
      LOG_ERROR("Invalid input length, too short");
      return false;
    }
    uint16_t v;
    GetFixed16(&input, &v);
    auto alg = static_cast<GenCompAlg>(v);
    if (alg == GenCompAlg::kPlain) {
      *out = std::move(input);
      return true;
    }
    return DoDecompressVarchar(alg, std::move(input), out);
  }

  bool CompressBitmap(TsBitmapBase* bitmap, TsBufferBuilder* output) const;
  bool DecompressBitmap(TSSlice input, std::unique_ptr<TsBitmapBase>* bitmap, uint64_t count,
                        uint64_t* bytes_consumed) const;
};

}  //  namespace kwdbts
