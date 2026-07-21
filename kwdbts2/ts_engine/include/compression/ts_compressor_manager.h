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
#include <limits>
#include <memory>
#include <optional>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "compression/ts_codec_utils.h"
#include "data_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_sliceguard.h"

namespace kwdbts {

enum class BitmapType : uint8_t {
  kRaw = 0,
  kAllValid = 1,
  kAllNone = 2,
  kAllNull = 3,

  BITMAP_COMP_ALG_LAST
};

struct TsExtraCompConfig {
  double rel_err, abs_err;
  std::shared_ptr<ALPCodec::ALPState> alp_state;
  TsExtraCompConfig()
      : rel_err(std::numeric_limits<double>::infinity()), abs_err(std::numeric_limits<double>::infinity()) {}
};

struct TsCompressionConfig {
  EncodeAlgo encoder;
  CompressAlgo compressor;
  roachpb::ColumnCompressLevel level;
  std::optional<TsExtraCompConfig> extra_cfg = std::nullopt;

  TsCompressionConfig()
      : encoder(EncodeAlgo::kPlain), compressor(CompressAlgo::kPlain), level(roachpb::COMPRESS_LEVEL_UNSPECIFIED) {}
  TsCompressionConfig(EncodeAlgo encoder, CompressAlgo compressor, roachpb::ColumnCompressLevel level)
      : encoder(encoder), compressor(compressor), level(level) {}
  TsCompressionConfig(EncodeAlgo encoder, CompressAlgo compressor, int level = roachpb::COMPRESS_LEVEL_UNSPECIFIED)
      : TsCompressionConfig{encoder, compressor, static_cast<roachpb::ColumnCompressLevel>(level)} {}
};

class TsCompressorBase;
class GenCompressorBase;
class CompressorManager {
 private:
  class TwoLevelCompressor {
   private:
    const TsCompressorBase* first_;
    const GenCompressorBase* second_;

    EncodeAlgo first_algo_;
    CompressAlgo second_algo_;

    void EncodeAlgorithm(TsBufferBuilder* out, EncodeAlgo first, CompressAlgo second) const {
      PutFixed16(out, static_cast<uint16_t>(first));
      PutFixed16(out, static_cast<uint16_t>(second));
    }

   public:
    TwoLevelCompressor(const TsCompressorBase* first, const GenCompressorBase* second, EncodeAlgo first_algo,
                       CompressAlgo second_algo)
        : first_(first), second_(second) {
      first_algo_ = first == nullptr ? EncodeAlgo::kPlain : first_algo;
      second_algo_ = second == nullptr ? CompressAlgo::kPlain : second_algo;
    }
    bool Compress(TSSlice raw, const TsBitmapBase* bitmap, uint32_t count, TsBufferBuilder* out,
                  const TsCompressionConfig& cfg) const;

    bool Decompress(TSSlice raw, const TsBitmapBase* bitmap, uint32_t count, TsSliceGuard* out) const;
    bool IsPlain() const { return (first_ == nullptr && second_ == nullptr); }

    std::tuple<EncodeAlgo, CompressAlgo> GetAlgorithms() const;
  };

  std::unordered_map<EncodeAlgo, TsCompressorBase*> ts_encoders_;
  std::unordered_map<CompressAlgo, GenCompressorBase*> ts_compressors_;
  // default_encode_algs_ stores the type-based default encoding algorithm. The default general compression algorithm for
  // COMPRESS_ALGO_UNSPECIFIED is resolved dynamically from the current EngineOptions when needed.
  std::unordered_map<DATATYPE, EncodeAlgo> default_encode_algs_;

  CompressorManager();

  bool DoDecompressData(TsSliceGuard&& input, const TsBitmapBase* bitmap, uint64_t count,
                        TsSliceGuard* out) const;
  bool DoDecompressVarchar(CompressAlgo alg, TsSliceGuard&& input, TsSliceGuard* out) const;

 public:
  static CompressorManager& GetInstance() {
    static CompressorManager mgr;
    return mgr;
  }
  CompressorManager(const CompressorManager&) = delete;
  void operator=(const CompressorManager&) = delete;

  TwoLevelCompressor GetCompressor(const TsCompressionConfig &cfg) const;
  TsCompressionConfig GetDefaultCompConfig(TSTableID table_id, const AttributeInfo &attr_info) const;
  TsCompressionConfig GetDefaultCompConfigByType(DATATYPE dtype) const;
  TsCompressionConfig GetCompConfig(TSTableID table_id, const AttributeInfo &attr_info) const;

  bool CompressData(TSSlice input, const TsBitmapBase* bitmap, uint64_t count, TsBufferBuilder* output,
                    const TsCompressionConfig& cfg) const;
  bool CompressVarchar(TSSlice input, TsBufferBuilder* output, const TsCompressionConfig& cfg) const;
  bool DecompressData(TsSliceGuard&& input, const TsBitmapBase* bitmap, uint64_t count, TsSliceGuard* out) const {
    if (input.size() < 4) {
      LOG_ERROR("Invalid input length %lu, too short", input.size());
      return false;
    }
    auto v = DecodeFixed32(input.data());
    if (v == 0) {
      input.RemovePrefix(sizeof(uint32_t));
      *out = std::move(input);
      return true;
    }

    return DoDecompressData(std::move(input), bitmap, count, out);
  }
  bool DecompressVarchar(TsSliceGuard&& input, TsSliceGuard* out) const {
    if (input.size() < 2) {
      LOG_ERROR("Invalid input length %lu, too short", input.size());
      return false;
    }
    uint16_t v;
    GetFixed16(&input, &v);
    auto alg = static_cast<CompressAlgo>(v);
    if (alg == CompressAlgo::kPlain) {
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
