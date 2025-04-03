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
#include <string>
#include <tuple>
#include <unordered_map>

#include "data_type.h"
#include "libkwdbts2.h"
#include "lt_rw_latch.h"
#include "ts_bitmap.h"

#define COMP_OVERFLOW_BYTES 2
#define BITS_PER_BYTE 8
// Masks
#define INT64MASK(_x) ((((uint64_t)1) << _x) - 1)
#define INT32MASK(_x) (((uint32_t)1 << _x) - 1)
#define INT8MASK(_x) (((uint8_t)1 << _x) - 1)
// Compression algorithm
#define NO_COMPRESSION 0
#define ONE_STAGE_COMP 1
#define TWO_STAGE_COMP 2

#define BUILDIN_CLZL(val) __builtin_clzl(val)
#define BUILDIN_CTZL(val) __builtin_ctzl(val)
#define BUILDIN_CLZ(val) __builtin_clz(val)
#define BUILDIN_CTZ(val) __builtin_ctz(val)

#define CHAR_BYTES sizeof(char)
#define SHORT_BYTES sizeof(int16_t)
#define INT_BYTES sizeof(int32_t)
#define LONG_BYTES sizeof(int64_t)
#define FLOAT_BYTES sizeof(float)
#define DOUBLE_BYTES sizeof(double)
#define POINTER_BYTES sizeof(void*)  // 8 by default  assert(sizeof(ptrdiff_t) == sizseof(void*)

namespace kwdbts {

/**
 * compressed data struct
+---------------+------------+-------------------------+
| compressor_id |  compressed col data and bitmap data  |
+---------------+------------+-------------------------+
 */

class ColumnBlockCompressor {
 protected:
  // unique id for different compressor. defined by REGISTER_DECOMPRESSOR.
  uint32_t compressor_id_;

 public:
  /**
   * @brief encode column data
   *
   * @param plain          column data
   * @param bitmap         bitmap for column data
   * @param count          number of rows of data
   * @return compressed data. if failed, data is empty.
   */
  virtual TSSlice Compress(TSSlice plain, TSSlice bitmap, uint32_t count) = 0;
  /**
   * @brief decode column data and bitmap.
   *
   * @param data            compressed column data
   * @param count           column data row number
   * @param[out] bitmap     bitmap for column data
   * @param[out] plain      original column data
   * @return bool
   */
  virtual bool DeCompress(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) = 0;

  // set compressor id, this function is only used by REGISTER_DECOMPRESSOR
  void SetID(uint32_t id) { compressor_id_ = id; }

 protected:
  // malloc memory, and set id at the start position of memory.
  // we can fill column data at return position.
  char* mallocWithID(size_t size, TSSlice* col_compressed);
};

class ColumnCompressorPlain : public ColumnBlockCompressor {
 public:
  TSSlice Compress(TSSlice plain, TSSlice bitmap, uint32_t count) override;
  bool DeCompress(TSSlice dasta, uint32_t count, TSSlice* plain, TSSlice* bitmap) override;
};

class ColumnCompressorTS : public ColumnBlockCompressor {
 public:
  TSSlice Compress(TSSlice plain, TSSlice bitmap, uint32_t count) override;
  bool DeCompress(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) override;
};

class ColumnCompressorInt32 : public ColumnBlockCompressor {
 public:
  TSSlice Compress(TSSlice plain, TSSlice bitmap, uint32_t count) override;
  bool DeCompress(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) override;
};

class ColumnCompressorDouble : public ColumnBlockCompressor {
 public:
  TSSlice Compress(TSSlice plain, TSSlice bitmap, uint32_t count) override;
  bool DeCompress(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) override;
};

/**
 * ColumnCompressorPolicy is used for user setting compressor degree.
 * Set policy using REGISTER_POLICY_COMPRESSOR funtion
 */
enum ColumnCompressorPolicy {
  PLAIN,
  LIGHT_COMRESS,
  HEAVY_COMRESS,
};

/**
 * ColumnCompressorMgr used for encoding and decoding column data of all kind column type.
 *
 *  we can dynamicly modify compress policy, whick store at compress_policy_ and compressor_.
 */
class ColumnCompressorMgr {
 private:
  KRWLatch* compressor_mutex_;
  ColumnCompressorPolicy compress_policy_{ColumnCompressorPolicy::PLAIN};
  // compressor for column datatype, used for column compressing.
  std::unordered_map<DATATYPE, ColumnBlockCompressor*> compressor_;
  // decompress for column data, key is compressor_id.
  std::unordered_map<uint32_t, std::unique_ptr<ColumnBlockCompressor>> decompressor_;

 public:
  ColumnCompressorMgr();

  ~ColumnCompressorMgr();
  // initialize all paramters.
  bool Init();
  // initialize compressor_ and compress_policy_
  bool ResetPolicy(ColumnCompressorPolicy policy);

  // check if we need call encode function.
  bool NeedCompress() { return compress_policy_ != ColumnCompressorPolicy::PLAIN; }
  /**
   * @brief encode column data
   *
   * @param plain          column data
   * @param bitmap         bitmap for column data
   * @param count          number of rows of data
   * @param type           column type
   * @return compressed data. if failed, data is empty.
   */
  TSSlice Encode(TSSlice plain, TSSlice bitmap, uint32_t count, DATATYPE type);
  /**
   * @brief decode column data and bitmap.
   *
   * @param data            compressed column data
   * @param count           column data row number
   * @param[out] bitmap     bitmap for column data
   * @param[out] plain      original column data
   * @return bool
   */
  bool Decode(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap);

 private:
  // initialize decompress_
  void initDeCompressor();
};

// Compression algorithm for timeseries
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
    bool Compress(const TSSlice& raw, const TsBitmap* bitmap, uint32_t count, std::string* out);

    bool Decompress(const TSSlice& raw, const TsBitmap* bitmap, uint32_t count, std::string* out);
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
};

}  //  namespace kwdbts
