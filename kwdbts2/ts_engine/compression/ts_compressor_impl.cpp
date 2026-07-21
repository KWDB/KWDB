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

#include <endian.h>

#include <algorithm>
#include <array>
#include <cassert>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <utility>
#include <vector>

#include "compression/ts_compressor_base.h"
#include "compression/ts_compressor_defs.h"
#include "compression/ts_encoder_defs.h"
#include "compression/ts_floatrep_helper.h"
#include "data_type.h"
#include "lg_api.h"
#include "libkwdbts2.h"
#include "settings.h"
#include "ts_bitmap.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_common.h"
#include "ts_sliceguard.h"

namespace kwdbts {

int GetLevelIdx(roachpb::ColumnCompressLevel level) {
  constexpr int kDefaultCompressionLevelIndex = 0;
  constexpr int kLowCompressionLevelIndex = 1;
  constexpr int kMediumCompressionLevelIndex = 2;
  constexpr int kHighCompressionLevelIndex = 3;
  switch (level) {
    case roachpb::COMPRESS_LEVEL_UNSPECIFIED:
      switch (EngineOptions::compress_level) {
        case CompressLevel::LOW:
          return kLowCompressionLevelIndex;
        case CompressLevel::MEDIUM:
          return kMediumCompressionLevelIndex;
        case CompressLevel::HIGH:
          return kHighCompressionLevelIndex;
        default:
          LOG_ERROR("Invalid cluster compress level: %d, fallback to default level.",
                    static_cast<int>(EngineOptions::compress_level));
          return kDefaultCompressionLevelIndex;
      }
    case roachpb::COMPRESS_LEVEL_LOW:
    case roachpb::COMPRESS_LEVEL_MEDIUM:
    case roachpb::COMPRESS_LEVEL_HIGH:
      return level;
    default: {
      if (level < 0 || level >= 4) {
        LOG_ERROR("Invalid compress level index: %d, fallback to default level.", level);
        return kDefaultCompressionLevelIndex;
      }
    }
  }
  return kDefaultCompressionLevelIndex;
}

CompressAlgo GetDefaultCompressAlgo(DATATYPE dtype) {
  switch (dtype) {
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      return CompressAlgo::kPlain;
    default:
      break;
  }
  return (EngineOptions::compress_stage & kCompressEnableMask) ? EngineOptions::compression_algorithm
                                                               : CompressAlgo::kPlain;
}

bool CompressorManager::TwoLevelCompressor::Compress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count,
                                                     TsBufferBuilder *out, const TsCompressionConfig &cfg) const {
  auto first = first_algo_;
  auto second = second_algo_;
  if (IsPlain()) {
    EncodeAlgorithm(out, first, second);
    out->append(raw);
    return true;
  }

  TsBufferBuilder first_out;
  TsBufferBuilder second_out;

  TSSlice data;
  bool ok = true;
  if (first_ == nullptr) {
    data = raw;
  } else {
    first_out.clear();
    ok = first_->Compress(raw, bitmap, count, &first_out, cfg);
    data = first_out.AsSlice();
  }
  if (!ok || data.len > raw.len) {
    first = EncodeAlgo::kPlain;
    data = raw;
  }
  if (second_ == nullptr) {
    EncodeAlgorithm(out, first, second);
    out->append(data);
    return true;
  }
  second_out.clear();
  ok = second_->Compress(data, &second_out, cfg);
  if (!ok || second_out.size() > data.len) {
    second = CompressAlgo::kPlain;
  } else {
    data = second_out.AsSlice();
  }
  EncodeAlgorithm(out, first, second);
  out->append(data);
  return true;
}
bool CompressorManager::TwoLevelCompressor::Decompress(TSSlice raw, const TsBitmapBase *bitmap, uint32_t count,
                                                       TsSliceGuard *out) const {
  if (IsPlain()) return false;  // control should not reach here.
  uint16_t first_algo, second_algo;
  GetFixed16(&raw, &first_algo);
  GetFixed16(&raw, &second_algo);

  TsSliceGuard buf;
  TSSlice data;
  bool ok = true;
  if (second_ == nullptr) {
    data = raw;
  } else {
    ok = second_->Decompress(raw, &buf);
    data = buf.AsSlice();
  }
  if (!ok) {
    return false;
  }
  if (first_ == nullptr) {
    std::swap(*out, buf);
    return true;
  }
  return first_->Decompress(data, bitmap, count, out);
}

std::tuple<EncodeAlgo, CompressAlgo> CompressorManager::TwoLevelCompressor::GetAlgorithms() const {
  return {first_algo_, second_algo_};
}

CompressorManager::CompressorManager() {
  // 1. construct default algorithms.
  const std::vector<DATATYPE> timestamp_type{DATATYPE::TIMESTAMP64, DATATYPE::TIMESTAMP64_MICRO,
                                             DATATYPE::TIMESTAMP64_NANO};
  for (auto i : timestamp_type) {
    default_encode_algs_[i] = EncodeAlgo::kSimple8B_V2_s64;
  }

  default_encode_algs_[DATATYPE::INT16] = EncodeAlgo::kSimple8B_V2_s16;
  default_encode_algs_[DATATYPE::INT32] = EncodeAlgo::kSimple8B_V2_s32;
  default_encode_algs_[DATATYPE::INT64] = EncodeAlgo::kSimple8B_V2_s64;
  default_encode_algs_[DATATYPE::FLOAT] = EncodeAlgo::kChimp_32;
  default_encode_algs_[DATATYPE::DOUBLE] = EncodeAlgo::kChimp_64;
  // char string
  default_encode_algs_[DATATYPE::BYTE] = EncodeAlgo::kPlain;
  default_encode_algs_[DATATYPE::CHAR] = EncodeAlgo::kPlain;
  default_encode_algs_[DATATYPE::BINARY] = EncodeAlgo::kPlain;

  default_encode_algs_[DATATYPE::BOOL] = EncodeAlgo::kBitPacking;

  // 2. construct encoding algorithms.
  ts_encoders_[EncodeAlgo::kGorilla_32] = &ConcreateTsCompressor<GorillaIntV2<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kGorilla_64] = &ConcreateTsCompressor<GorillaIntV2<int64_t>>::GetInstance();

  ts_encoders_[EncodeAlgo::kSimple8B_s8] = &ConcreateTsCompressor<Simple8BInt<int8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s16] = &ConcreateTsCompressor<Simple8BInt<int16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s32] = &ConcreateTsCompressor<Simple8BInt<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_s64] = &ConcreateTsCompressor<Simple8BInt<int64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u8] = &ConcreateTsCompressor<Simple8BInt<uint8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u16] = &ConcreateTsCompressor<Simple8BInt<uint16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u32] = &ConcreateTsCompressor<Simple8BInt<uint32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_u64] = &ConcreateTsCompressor<Simple8BInt<uint64_t>>::GetInstance();

  ts_encoders_[EncodeAlgo::kSimple8B_V2_s8] = &ConcreateTsCompressor<Simple8BIntV2<int8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s16] = &ConcreateTsCompressor<Simple8BIntV2<int16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s32] = &ConcreateTsCompressor<Simple8BIntV2<int32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_s64] = &ConcreateTsCompressor<Simple8BIntV2<int64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u8] = &ConcreateTsCompressor<Simple8BIntV2<uint8_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u16] = &ConcreateTsCompressor<Simple8BIntV2<uint16_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u32] = &ConcreateTsCompressor<Simple8BIntV2<uint32_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kSimple8B_V2_u64] = &ConcreateTsCompressor<Simple8BIntV2<uint64_t>>::GetInstance();
  ts_encoders_[EncodeAlgo::kBitPacking] = &ConcreateTsCompressor<BitPacking>::GetInstance();
  // Float
  ts_encoders_[EncodeAlgo::kChimp_32] = &ConcreateTsCompressor<Chimp<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kChimp_64] = &ConcreateTsCompressor<Chimp<double>>::GetInstance();

  ts_encoders_[EncodeAlgo::kELF_32] = &ConcreateTsCompressor<ELF<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kELF_64] = &ConcreateTsCompressor<ELF<double>>::GetInstance();

  ts_encoders_[EncodeAlgo::kALP_32] = &ConcreateTsCompressor<ALP<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kALP_64] = &ConcreateTsCompressor<ALP<double>>::GetInstance();

  ts_encoders_[EncodeAlgo::kFptrunc_32] = &ConcreateTsCompressor<FpTrunc<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kFptrunc_64] = &ConcreateTsCompressor<FpTrunc<double>>::GetInstance();

  ts_encoders_[EncodeAlgo::kBSS_32] = &ConcreateTsCompressor<BSS<float>>::GetInstance();
  ts_encoders_[EncodeAlgo::kBSS_64] = &ConcreateTsCompressor<BSS<double>>::GetInstance();

  ts_encoders_[EncodeAlgo::kRC] = &ConcreateTsCompressor<TsRangeCodecBool>::GetInstance();

  // kDeltaD (delta-of-delta) uses the same compressor as Gorilla
  ts_encoders_[EncodeAlgo::kDeltaD] = &ConcreateTsCompressor<GorillaIntV2<int64_t>>::GetInstance();

  // construct general compression algorithms
  ts_compressors_[CompressAlgo::kSnappy] = &ConcreateGenCompressor<SnappyString>::GetInstance();
  ts_compressors_[CompressAlgo::kLz4] = &ConcreateGenCompressor<LZ4String>::GetInstance();
  ts_compressors_[CompressAlgo::kZstd] = &ConcreateGenCompressor<ZSTDString>::GetInstance();
  ts_compressors_[CompressAlgo::kZlib] = &ConcreateGenCompressor<ZLIBString>::GetInstance();
}
auto CompressorManager::GetCompressor(const TsCompressionConfig &cfg) const -> TwoLevelCompressor {
  const TsCompressorBase *first_comp = nullptr;
  const GenCompressorBase *second_comp = nullptr;
  {
    auto it = ts_encoders_.find(cfg.encoder);
    if (it != ts_encoders_.end()) first_comp = it->second;
  }
  {
    auto it = ts_compressors_.find(cfg.compressor);
    if (it != ts_compressors_.end()) second_comp = it->second;
  }
  return TwoLevelCompressor{first_comp, second_comp, cfg.encoder, cfg.compressor};
}

TsCompressionConfig CompressorManager::GetDefaultCompConfigByType(DATATYPE dtype) const {
  assert(dtype != FLOAT && dtype != DOUBLE);
  TsCompressionConfig cfg;
  auto it = default_encode_algs_.find(dtype);
  cfg.encoder = it == default_encode_algs_.end() ? EncodeAlgo::kPlain : it->second;
  cfg.compressor = GetDefaultCompressAlgo(dtype);
  return cfg;
}

TsCompressionConfig CompressorManager::GetDefaultCompConfig(TSTableID table_id, const AttributeInfo &attr) const {
  auto dtype = static_cast<DATATYPE>(attr.type);
  TsCompressionConfig cfg;
  auto it = default_encode_algs_.find(dtype);
  cfg.encoder = it == default_encode_algs_.end() ? EncodeAlgo::kPlain : it->second;
  cfg.compressor = GetDefaultCompressAlgo(dtype);
  return cfg;
}

static auto ParseSimple8B(DATATYPE dtype) {
  switch (dtype) {
    case DATATYPE::INT16:
      return EncodeAlgo::kSimple8B_V2_s16;
    case DATATYPE::INT32:
      return EncodeAlgo::kSimple8B_V2_s32;
    case DATATYPE::INT64:
    case DATATYPE::TIMESTAMP64:
    case DATATYPE::TIMESTAMP64_MICRO:
    case DATATYPE::TIMESTAMP64_NANO:
      return EncodeAlgo::kSimple8B_V2_s64;
    default:
      LOG_ERROR("The data type %d does not match simple8b algorithm.", dtype);
  }
  return EncodeAlgo::kPlain;
}

static auto ParseFloatEncoder(DATATYPE dtype, roachpb::ColumnEncodeAlgo pb_enc_type) {
  using EncType_32_64 = std::array<EncodeAlgo, 2>;
  static const std::map<roachpb::ColumnEncodeAlgo, EncType_32_64> float_enc_map{
      {roachpb::ENCODE_ALGO_CHIMP, {EncodeAlgo::kChimp_32, EncodeAlgo::kChimp_64}},
      {roachpb::ENCODE_ALGO_ALP, {EncodeAlgo::kALP_32, EncodeAlgo::kALP_64}},
      {roachpb::ENCODE_ALGO_ELF, {EncodeAlgo::kELF_32, EncodeAlgo::kELF_64}},
      {roachpb::ENCODE_ALGO_BSS, {EncodeAlgo::kBSS_32, EncodeAlgo::kBSS_64}},
      {roachpb::ENCODE_ALGO_FPTRUNC, {EncodeAlgo::kFptrunc_32, EncodeAlgo::kFptrunc_64}},
  };

  auto it = float_enc_map.find(pb_enc_type);
  int idx = 0;
  switch (dtype) {
    case DATATYPE::FLOAT:
      idx = 0;
      break;
    case DATATYPE::DOUBLE:
      idx = 1;
      break;
    default:
      LOG_WARN("The data type %d does not match float encoding algorithm.", dtype);
      idx = 2;
  }
  return it != float_enc_map.end() && idx < 2 ? it->second[idx] : EncodeAlgo::kPlain;
}

void FloatEncoderPostProcess(TsCompressionConfig &cfg, TSTableID table_id, const AttributeInfo &attr) {
  if (attr.encode_algo != roachpb::ENCODE_ALGO_ALP && attr.encode_algo != roachpb::ENCODE_ALGO_FPTRUNC) return;
  cfg.extra_cfg = TsExtraCompConfig{};
  if (attr.encode_algo == roachpb::ENCODE_ALGO_ALP) {
    uint64_t hash_key = table_id << 32 | attr.id;
    auto &alp_states = ALPStateManager::GetInstance();
    cfg.extra_cfg->alp_state = alp_states.Get(hash_key);
  }
  if (attr.encode_algo == roachpb::ENCODE_ALGO_FPTRUNC) {
    // rel_err/abs_err are stored as FP16 (FP16_REL: 8 exp + 8 mantissa bits,
    // FP16_ABS: 11 exp + 5 mantissa bits).  The round-trip double→FP16→double
    // incurs up to ~3–6% relative quantization error on the error bound itself,
    // which is negligible for compression control.
    FP16_ABS abs_err{attr.abs_err};
    FP16_REL rel_err{attr.rel_err};
    cfg.extra_cfg->abs_err = abs_err.ToDouble();
    cfg.extra_cfg->rel_err = rel_err.ToDouble();
  }
}

TsCompressionConfig CompressorManager::GetCompConfig(TSTableID table_id, const AttributeInfo &attr) const {
  auto dtype = static_cast<DATATYPE>(attr.type);
  TsCompressionConfig cfg = GetDefaultCompConfig(table_id, attr);
  if (attr.encode_algo != roachpb::ENCODE_ALGO_UNSPECIFIED) {
    switch (attr.encode_algo) {
      case roachpb::ENCODE_ALGO_SIMPLE8B:
        cfg.encoder = ParseSimple8B(dtype);
        break;
      case roachpb::ENCODE_ALGO_CHIMP:
      case roachpb::ENCODE_ALGO_ALP:
      case roachpb::ENCODE_ALGO_ELF:
      case roachpb::ENCODE_ALGO_BSS:
      case roachpb::ENCODE_ALGO_FPTRUNC:
        cfg.encoder = ParseFloatEncoder(dtype, static_cast<roachpb::ColumnEncodeAlgo>(attr.encode_algo));
        FloatEncoderPostProcess(cfg, table_id, attr);
        break;

      case roachpb::ENCODE_ALGO_BIT_PACKING:
        cfg.encoder = EncodeAlgo::kBitPacking;
        break;
      case roachpb::ENCODE_ALGO_RC:
        cfg.encoder = EncodeAlgo::kRC;
        break;
      case roachpb::ENCODE_ALGO_DELTA_D:
        cfg.encoder = EncodeAlgo::kDeltaD;
        break;
      case roachpb::ENCODE_ALGO_DISABLED:
        cfg.encoder = EncodeAlgo::kPlain;
        break;
      default:
        LOG_ERROR("Unsupported encode algo: %d, fallback to plain.", attr.encode_algo);
        break;
    }
  }
  if (attr.compress_algo != roachpb::COMPRESS_ALGO_UNSPECIFIED) {
    static std::unordered_map<roachpb::ColumnCompressAlgo, CompressAlgo> compressor_map{
        {roachpb::COMPRESS_ALGO_SNAPPY, CompressAlgo::kSnappy},
        {roachpb::COMPRESS_ALGO_LZ4, CompressAlgo::kLz4},
        {roachpb::COMPRESS_ALGO_ZLIB, CompressAlgo::kZlib},
        {roachpb::COMPRESS_ALGO_ZSTD, CompressAlgo::kZstd}};
    auto it = compressor_map.find(static_cast<roachpb::ColumnCompressAlgo>(attr.compress_algo));
    cfg.compressor = it == compressor_map.end() ? CompressAlgo::kPlain : it->second;
    cfg.level = static_cast<roachpb::ColumnCompressLevel>(attr.compress_level);
  }

  if (!(EngineOptions::compress_stage & kEncodeEnableMask)) {
    cfg.encoder = EncodeAlgo::kPlain;
  }
  if (!(EngineOptions::compress_stage & kCompressEnableMask)) {
    cfg.compressor = CompressAlgo::kPlain;
  }

  return cfg;
}

bool CompressorManager::CompressData(TSSlice input, const TsBitmapBase *bitmap, uint64_t count, TsBufferBuilder *output,
                                     const TsCompressionConfig &cfg) const {
  auto effective_cfg = cfg;
  if (!(EngineOptions::compress_stage & kEncodeEnableMask)) {
    effective_cfg.encoder = EncodeAlgo::kPlain;
  }
  if (!(EngineOptions::compress_stage & kCompressEnableMask)) {
    effective_cfg.compressor = CompressAlgo::kPlain;
  }

  auto compressor = GetCompressor(effective_cfg);
  return compressor.Compress(input, bitmap, count, output, effective_cfg);
}

bool CompressorManager::CompressVarchar(TSSlice input, TsBufferBuilder *output, const TsCompressionConfig &cfg) const {
  auto alg = cfg.compressor;
  if (!(EngineOptions::compress_stage & kCompressEnableMask)) {
    alg = CompressAlgo::kPlain;
  }
  static_assert(sizeof(alg) == sizeof(uint16_t));
  if (alg == CompressAlgo::kPlain) {
    PutFixed16(output, static_cast<uint16_t>(alg));
    output->append(input.data, input.len);
    return true;
  }
  TsBufferBuilder tmp;
  PutFixed16(&tmp, static_cast<uint16_t>(alg));
  auto it = ts_compressors_.find(alg);
  if (it == ts_compressors_.end()) {
    LOG_ERROR("Invalid general compression algorithm: %d", static_cast<int>(alg));
    return false;
  }
  bool ok = it->second->Compress(input, &tmp, cfg);
  if (!ok) {
    return false;
  }
  if (tmp.size() >= input.len) {
    PutFixed16(output, static_cast<uint16_t>(CompressAlgo::kPlain));
    output->append(input.data, input.len);
    return true;
  }
  output->append(tmp.AsSlice());
  return true;
}

bool CompressorManager::DoDecompressData(TsSliceGuard &&input, const TsBitmapBase *bitmap, uint64_t count,
                                         TsSliceGuard *out) const {
  auto algo = input.SubSlice(0, sizeof(EncodeAlgo) + sizeof(CompressAlgo));
  uint16_t v;
  GetFixed16(&algo, &v);
  EncodeAlgo encoder = static_cast<EncodeAlgo>(v);
  GetFixed16(&algo, &v);
  CompressAlgo compressor = static_cast<CompressAlgo>(v);
  if (encoder >= EncodeAlgo::TS_COMP_ALG_LAST || compressor >= CompressAlgo::GEN_COMP_ALG_LAST) {
    LOG_ERROR("Invalid algorithm id: first: %d, second: %d", static_cast<int>(encoder), static_cast<int>(compressor));
    return false;
  }
  auto two_levl_compressor = GetCompressor(TsCompressionConfig{encoder, compressor});
  return two_levl_compressor.Decompress(input.AsSlice(), bitmap, count, out);
}

bool CompressorManager::DoDecompressVarchar(CompressAlgo alg, TsSliceGuard &&input, TsSliceGuard *out) const {
  if (alg >= CompressAlgo::GEN_COMP_ALG_LAST) {
    return false;
  }
  auto it = ts_compressors_.find(alg);
  if (it == ts_compressors_.end()) {
    return false;
  }
  return it->second->Decompress(input.AsSlice(), out);
}

bool CompressorManager::CompressBitmap(TsBitmapBase *bitmap, TsBufferBuilder *output) const {
  if (bitmap->IsAllValid()) {
    output->push_back(static_cast<char>(BitmapType::kAllValid));
    return true;
  }

  if (bitmap->IsAllNull()) {
    output->push_back(static_cast<char>(BitmapType::kAllNull));
    return true;
  }

  if (bitmap->IsAllNone()) {
    output->push_back(static_cast<char>(BitmapType::kAllNone));
    return true;
  }

  output->push_back(static_cast<char>(BitmapType::kRaw));
  output->append(bitmap->GetStr());
  return true;
}

bool CompressorManager::DecompressBitmap(TSSlice input, std::unique_ptr<TsBitmapBase> *bitmap, uint64_t count,
                                         uint64_t *bytes_consumed) const {
  if (input.len < 1) {
    LOG_ERROR("Invalid input length = 0, too short");
    return false;
  }
  BitmapType alg = static_cast<BitmapType>(input.data[0]);
  RemovePrefix(&input, 1);
  *bytes_consumed = 1;
  switch (alg) {
    case BitmapType::kRaw: {
      auto n_bytes = TsBitmap::GetBitmapLen(count);
      if (input.len < n_bytes) {
        LOG_ERROR("Invalid bitmap length, too short. expected: %lu, actual: %lu", n_bytes, input.len);
        return false;
      }
      *bitmap = std::make_unique<TsBitmap>(TSSlice{input.data, n_bytes}, count);
      *bytes_consumed += n_bytes;
      break;
    }

    case BitmapType::kAllValid: {
      *bitmap = std::make_unique<TsUniformBitmap<kValid>>(count);
      break;
    }

    case BitmapType::kAllNull: {
      *bitmap = std::make_unique<TsUniformBitmap<kNull>>(count);
      break;
    }

    case BitmapType::kAllNone: {
      *bitmap = std::make_unique<TsUniformBitmap<kNone>>(count);
      break;
    }

    default: {
      LOG_ERROR("Invalid bitmap type: %d", static_cast<int>(alg));
      return false;
    }
  }
  return true;
}

}  // namespace kwdbts
