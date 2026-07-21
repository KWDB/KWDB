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
#include <array>
#include <atomic>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "lg_api.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"
#include "ts_floatrep_helper.h"

namespace kwdbts {

// Integer bit-width: bit_width(v) = floor(log2(v)) + 1.
// Replaces std::ceil(std::log2(v+1)) to avoid double-precision loss for large v
// (e.g. v=2^56 → v+1 rounds to 2^56 in double → log2=56.0 instead of 56.000...).
template <typename T>
inline int GetValidBits(T v) {
  static_assert(std::is_unsigned_v<T>);
  if (v == 0) return 1;
  return 64 - __builtin_clzll(static_cast<uint64_t>(v));
}

template <class T>
static inline void TypedPutVarint(TsBufferBuilder *dst, T v) {
  static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>);
  if constexpr (std::is_same_v<T, uint64_t>) {
    return PutVarint64(dst, v);
  } else {
    return PutVarint32(dst, v);
  }
}

template <class T>
static inline const char *TypedDecodeVarint(const char *ptr, const char *limit, T *v) {
  static_assert(std::is_same_v<T, uint64_t> || std::is_same_v<T, uint32_t>);
  if constexpr (std::is_same_v<T, uint64_t>) {
    return DecodeVarint64(ptr, limit, v);
  } else {
    return DecodeVarint32(ptr, limit, v);
  }
}

template <class T>
static inline bool CheckedSub(const T a, const T b, T *out) {
  static_assert(std::is_same_v<T, int64_t> || std::is_same_v<T, int32_t>);
  if constexpr (std::is_same_v<T, int64_t>) {
    return __builtin_ssubl_overflow(a, b, out);
  } else {
    return __builtin_ssub_overflow(a, b, out);
  }
  return false;
}

template <class T>
static inline void TypedPutFixed(TsBufferBuilder *dst, T v) {
  if constexpr (sizeof(T) == 8) {
    return PutFixed64(dst, v);
  } else {
    return PutFixed32(dst, v);
  }
}

constexpr size_t kGeneralCompressionHeaderSize = sizeof(uint64_t);
inline bool HasGeneralCompressionHeader(TSSlice data, const char *algorithm_name) {
  if (data.len < kGeneralCompressionHeaderSize) {
    LOG_ERROR("%s input too short: %lu", algorithm_name, data.len);
    return false;
  }
  return true;
}

// Encoder utils
// ALP:

namespace ALPCodec {
namespace details {

template <class T>
struct ALPConst {};

template <>
struct ALPConst<float> {
  constexpr static size_t MAX_E = 9;
  constexpr static std::array<float, MAX_E + 1> Exp10{1.0f,      10.0f,      100.0f,      1000.0f,      10000.0f,
                                                      100000.0f, 1000000.0f, 10000000.0f, 100000000.0f, 1000000000.0f};
  constexpr static std::array<float, MAX_E + 1> iExp10{1.0f,     0.1f,      0.01f,      0.001f,      0.0001f,
                                                       0.00001f, 0.000001f, 0.0000001f, 0.00000001f, 0.000000001f};
};

template <>
struct ALPConst<double> {
  constexpr static size_t MAX_E = 18;
  constexpr static std::array<double, MAX_E + 1> Exp10{
      1.0,
      10.0,
      100.0,
      1000.0,
      10000.0,
      100000.0,
      1000000.0,
      10000000.0,
      100000000.0,
      1000000000.0,
      10000000000.0,
      100000000000.0,
      1000000000000.0,
      10000000000000.0,
      100000000000000.0,
      1000000000000000.0,
      10000000000000000.0,
      100000000000000000.0,
      1000000000000000000.0,
  };
  constexpr static std::array<double, MAX_E + 1> iExp10{
      1.0,
      0.1,
      0.01,
      0.001,
      0.0001,
      0.00001,
      0.000001,
      0.0000001,
      0.00000001,
      0.000000001,
      0.0000000001,
      0.00000000001,
      0.000000000001,
      0.0000000000001,
      0.00000000000001,
      0.000000000000001,
      0.0000000000000001,
      0.00000000000000001,
      0.000000000000000001,
  };
};

// (e,f) factor/exponent pair for the ALP scale factor: 10^e / 10^f.
// e is the power-of-10 multiplier, f is the divisor power.
// Valid range: 0 ≤ f ≤ e ≤ MAX_E (18 for double, 9 for float).
struct EF {
  uint32_t e = 0, f = 0;
  bool operator==(const EF &x) const { return x.e == e && x.f == f; }
};

template <class T>
class Span {
 private:
  const T *data = nullptr;
  size_t size_ = 0;

 public:
  size_t size() const { return size_; }
  bool empty() const { return size_ == 0; }

  Span() = default;
  explicit Span(const T *data, size_t size) : data(data), size_(size) {}
  explicit Span(const std::vector<T> &v) : Span(v.data(), v.size()) {}

  const T &operator[](size_t i) const { return data[i]; }
};

}  // namespace details

// Adaptive (e,f) search state. Remembers which pairs compressed well in recent
// batches and tries them first, avoiding exhaustive enumeration every time.
class ALPState {
 private:
  struct BlockMeta {
    std::vector<int> bpv;          // bits-per-value scores for each (e,f) tried
    std::vector<details::EF> efs;  // corresponding (e,f) pairs
  };
  constexpr static size_t kWindowSize = 16;
  std::array<BlockMeta, kWindowSize> circular_buffer_;
  std::atomic<uint64_t> window_idx_ = 0;
  mutable std::shared_mutex mu_;

  // Histogram: bit-width bucket → (e,f) pairs that achieved it.
  std::array<std::vector<details::EF>, 80> Stats(const std::array<BlockMeta, kWindowSize> &buffer) const;

  // Candidate (e,f) pairs. Cold start: exhaustive. Warm: top-N from history.
  std::vector<details::EF> GetCandidates(uint32_t max_e, int ncandidates = -1) const;

  // Estimated bits-per-value: inliers pay bit_width(diff), exceptions pay
  // sizeof(T)*8 + 16 (raw storage + position overhead).
  template <class T>
  int Estimate(const details::Span<T> &raw, const details::Span<FloatSRep_t<T>> &encoded,
               const details::Span<T> &decoded);

 public:
  // Find best (e,f). Uses stratified sampling for batches >512 values.
  template <class T>
  details::EF FindBestEF(const T *data, size_t size);
};
}  // namespace ALPCodec

// Global manager for per-column ALPState instances.
// Isolated by hash_key (e.g. (table_id << 32) | column_id).
// Thread-safe: shared_lock for reads, unique_lock for writes.
// TTL-based eviction: entries idle for > kTTLSec are reclaimed on creation.
class ALPStateManager {
 public:
  static ALPStateManager& GetInstance() {
    static ALPStateManager mgr;
    return mgr;
  }

  std::shared_ptr<ALPCodec::ALPState> Get(uint64_t hash_key) {
    uint64_t now = GetNowSec();

    // Phase 1: fast path — shared_lock
    {
      std::shared_lock lk(mu_);
      auto it = cache_.find(hash_key);
      if (it != cache_.end()) {
        uint64_t last = it->second.last_access_s.load(std::memory_order_relaxed);
        if (now - last < kTTLSec) {
          it->second.last_access_s.store(now, std::memory_order_relaxed);
          return it->second.state;
        }
      }
    }

    // Phase 2: slow path — unique_lock + double-check
    {
      std::unique_lock lk(mu_);

      auto it = cache_.find(hash_key);
      if (it != cache_.end()) {
        uint64_t last = it->second.last_access_s.load(std::memory_order_relaxed);
        if (now - last < kTTLSec) {
          return it->second.state;
        }
        cache_.erase(it);
      }

      auto sp = std::make_shared<ALPCodec::ALPState>();
      auto [iter, ok] = cache_.try_emplace(hash_key, sp, GetNowSec());
      assert(ok);

      SweepLocked();
      return sp;
    }
  }

 private:
  ALPStateManager() = default;

  // Monotonic seconds for coarse-grained TTL tracking. Precision is deliberately
  // low (1 s) — concurrent store races may overwrite with a value that differs by
  // < 1 s, which is negligible against a 1-day TTL.
  static uint64_t GetNowSec() {
    return std::chrono::duration_cast<std::chrono::seconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
  }

  struct Entry {
    std::shared_ptr<ALPCodec::ALPState> state;
    std::atomic<uint64_t> last_access_s;
    Entry(std::shared_ptr<ALPCodec::ALPState> s, uint64_t t)
        : state(std::move(s)), last_access_s(t) {}
  };

  static constexpr uint64_t kTTLSec = 24 * 3600;  // 1 day

  void SweepLocked() {
    uint64_t now = GetNowSec();
    for (auto it = cache_.begin(); it != cache_.end(); ) {
      uint64_t last = it->second.last_access_s.load(std::memory_order_relaxed);
      if (now - last > kTTLSec) {
        it = cache_.erase(it);
      } else {
        ++it;
      }
    }
  }

  mutable std::shared_mutex mu_;
  std::unordered_map<uint64_t, Entry> cache_;
};

}  // namespace kwdbts
