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

#include <cassert>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <map>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "compression/ts_encoder_defs.h"
#include "ts_bufferbuilder.h"
#include "ts_coding.h"

namespace kwdbts {

namespace RangeCodec {

// -- FreqTable<T>: quantized CDF frequency table -------------------------------
//
//  Construction: sort symbols by descending frequency -> compute pdf ->
//  pick shift so total = 2^shift and the rarest symbol gets at least 1 unit
//  (min_p * total >= 1) -> partition [0, total) proportionally.
//
//  T must be trivially copyable (Serialize uses raw memcpy).

namespace detail {

// -- FreqTableParams<T>: snapshot of FreqTable state for serialization ---------
//
//  Captures the essential data needed to reconstruct a FreqTable:
//    shift   — quantization shift, total = 1 << shift, must be in [2, 23]
//    symbols — symbols in CDF order (descending frequency)
//    lows    — cumulative lower bounds, size == symbols.size(), low[0] == 0
//
//  The upper bound for symbol i is lows[i+1] (or total for the last symbol).

template <class T>
struct FreqTableParams {
  uint64_t shift = 0;
  std::vector<T> symbols;
  std::vector<uint64_t> lows;

  bool IsValid() const {
    if (shift < 2 || shift > 23 || symbols.empty() || symbols.size() != lows.size() ||
        lows.back() >= (1ULL << shift))
      return false;
    for (size_t i = 0; i + 1 < lows.size(); ++i) {
      if (lows[i] >= lows[i + 1]) return false;
    }
    return true;
  }
};

template <class T>
class FreqTable {
  static_assert(std::is_trivially_copyable_v<T>, "T must be trivially copyable for serialization");

 private:
  enum Status {
    kOk = 0,
    kFrequencyTooSmall,
    kParseError,
  };

  Status status_ = kOk;

  struct SymbolCDFRange {
    T sym;
    uint64_t low = 0;
    uint64_t high = 0;
  };

  std::vector<SymbolCDFRange> symbols_;          // sorted by descending frequency
  std::unordered_map<T, uint32_t> sym_idx_;      // symbol -> index in symbols_
  std::map<uint64_t, uint32_t> inverse_cdf_;     // lower bound -> index, O(log N) upper_bound

  uint64_t total_ = 0;   // always a power of two (1 << shift)
  uint64_t shift_ = 0;

 public:
  // shift is in [2, 23]; max total = 2^23 ~ 8.3 million
  explicit FreqTable(const std::unordered_map<T, uint64_t> &freq_map) {
    auto sz = freq_map.size();

    // sort by descending frequency (multimap + greater).
    // Zero-frequency entries are filtered out here: they contribute nothing to
    // the CDF and would cause min_p=0 → kFrequencyTooSmall.
    std::multimap<uint64_t, T, std::greater<uint64_t>> sorted_freq_map;
    for (auto &pair : freq_map) {
      if (pair.second == 0) continue;
      sorted_freq_map.insert(std::make_pair(pair.second, pair.first));
      total_ += pair.second;
    }
    sz = sorted_freq_map.size();
    assert(sz > 0);

    // compute pdf in descending-frequency order
    std::vector<double> pdf;
    std::vector<T> symbols;
    symbols.reserve(sz);
    pdf.reserve(sz);
    for (const auto &[freq, symbol] : sorted_freq_map) {
      pdf.push_back(static_cast<double>(freq) / total_);
      symbols.push_back(symbol);
    }

    // pick quantization shift:
    //   a) min_p * (1<<shift) >= 1  — rarest symbol gets at least 1 unit
    //   b) max_p truncation error < 5% — avoid flattening skewed distributions
    //      (e.g. p=0.25/0.75 with shift=2 would get 2:2 instead of 3:1)
    uint64_t shift = 2;
    uint64_t total = 1 << shift;
    double min_p = pdf.back();
    double max_p = pdf.front();
    while (shift < 24) {
      if (min_p * total >= 1) {
        double quant = max_p * total;
        double err = quant - std::floor(quant);   // fractional part lost
        if (err / quant < 0.05) break;             // < 5% relative error
      }
      shift++;
      total = 1 << shift;
    }
    if (shift == 24) {
      status_ = kFrequencyTooSmall;
      return;
    }

    total_ = total;
    shift_ = shift;

    // allocate CDF intervals; last symbol's high = total_ absorbs float rounding error
    uint64_t acc_cum = 0;
    symbols_.resize(sz);
    for (size_t i = 0; i < sz; ++i) {
      uint64_t curr_cum = std::max<uint64_t>(1, static_cast<uint64_t>(pdf[i] * total_));
      T symbol = symbols[i];
      symbols_[i] = {symbol, acc_cum, i + 1 == sz ? total_ : acc_cum + curr_cum};
      inverse_cdf_[acc_cum] = i;
      sym_idx_[symbol] = i;
      acc_cum += curr_cum;
    }
  }

  // Construct from a snapshot previously obtained via GetParams().
  // The params must be valid (shift in [2,23], non-empty, symbols.size() == lows.size()).
  // Invalid params produce a table with Ok() == false and kParseError.
  explicit FreqTable(const FreqTableParams<T> &params) {
    if (!params.IsValid()) {
      status_ = kParseError;
      return;
    }
    shift_ = params.shift;
    total_ = 1ULL << shift_;

    size_t n = params.symbols.size();
    symbols_.reserve(n);
    for (size_t i = 0; i < n; ++i) {
      uint64_t low = params.lows[i];
      uint64_t high = (i + 1 < n) ? params.lows[i + 1] : total_;
      assert(high > low);
      symbols_.push_back({params.symbols[i], low, high});
      sym_idx_[params.symbols[i]] = i;
      inverse_cdf_[low] = i;
    }
    status_ = kOk;
  }

  bool Ok() const { return status_ == kOk; }

  const char *ErrorMsg() const {
    switch (status_) {
      case kOk:                return nullptr;
      case kFrequencyTooSmall: return "frequency too small";
      case kParseError:        return "parse error";
    }
    return "unknown";
  }
  uint64_t Total() const { return total_; }
  uint64_t Shift() const { return shift_; }
  uint64_t GetValidBits() const { return (shift_ / 8 + 1) * 8; }

  auto GetRange(T sym) const {
    auto it = sym_idx_.find(sym);
    assert(it != sym_idx_.end());
    auto r = symbols_[it->second];
    std::pair<uint64_t, uint64_t> range = {r.low, r.high};
    return range;
  }

  // Reverse lookup: val in [0, total) -> interval via upper_bound + step back
  auto Find(uint64_t val) const {
    assert(val < total_);
    auto it = inverse_cdf_.upper_bound(val);
    assert(it != inverse_cdf_.begin());
    --it;
    return symbols_[it->second];
  }

  // Snapshot of all data needed to reconstruct this table via the params constructor.
  FreqTableParams<T> GetParams() const {
    FreqTableParams<T> p;
    p.shift = shift_;
    p.symbols.reserve(symbols_.size());
    p.lows.reserve(symbols_.size());
    for (const auto &[low, idx] : inverse_cdf_) {
      p.symbols.push_back(symbols_[idx].sym);
      p.lows.push_back(low);
    }
    return p;
  }
};

// -- Symbol codec (type-specific; specialize for T=bool etc.) ------------------
//
//  Wire format: [shift:1B] [nsymbols:varint64] [symbols payload]
//  Default payload is nsymbols * sizeof(T) raw bytes.
//
//  The bool specialization packs symbol identity / ordering into bit 7 of the
//  shift byte (shift ∈ [2,23] uses only 5 bits), saving the symbol payload.

template <class T>
struct FreqTableSymbolCodec {
  static void Write(const FreqTableParams<T> &params, TsBufferBuilder *out) {
    out->push_back(static_cast<char>(params.shift));
    PutVarint64(out, params.symbols.size());
    for (size_t i = 0, n = params.symbols.size(); i < n; ++i) {
      out->append(reinterpret_cast<const char *>(&params.symbols[i]), sizeof(T));
    }
  }

  // Reads [shift] [nsymbols] [symbols] from [ptr, limit) into params.
  // Returns advanced ptr on success, nullptr on parse error.
  // On error params may be left with partial data — caller discards the table.
  static const char *Read(const char *ptr, const char *limit, FreqTableParams<T> &params) {
    if (ptr == limit) return nullptr;

    uint64_t shift = static_cast<uint8_t>(*ptr++);
    if (shift < 2 || shift > 23) return nullptr;
    params.shift = shift;

    uint64_t nsymbols = 0;
    ptr = DecodeVarint64(ptr, limit, &nsymbols);
    if (ptr == nullptr || nsymbols == 0) return nullptr;
    if (ptr + nsymbols * sizeof(T) > limit) return nullptr;

    params.symbols.reserve(nsymbols);
    for (uint64_t i = 0; i < nsymbols; ++i) {
      T sym;
      memcpy(&sym, ptr, sizeof(T));
      ptr += sizeof(T);
      params.symbols.push_back(sym);
    }
    return ptr;
  }
};

// -- bool specialization: single-byte header, no nsymbols varint ---------------
//
//  shift ∈ [2, 23] fits in 6 bits (max 63).  The remaining 2 bits encode
//  the complete symbol information so nsymbols and payload are both omitted:
//
//    bit 7 (0x80) — symbols[0]  (0 = false, 1 = true)
//    bit 6 (0x40) — has_second  (1 = 2 symbols, 0 = 1 symbol)
//    bits 0–5     — shift (validated ∈ [2, 23])
//
//  Total header: exactly 1 byte.  The delta section follows as usual.

template <>
struct FreqTableSymbolCodec<bool> {
  static void Write(const FreqTableParams<bool> &params, TsBufferBuilder *out) {
    assert(params.shift >= 2 && params.shift <= 23);
    uint8_t packed = static_cast<uint8_t>(params.shift);  // bits 0-5
    auto n = params.symbols.size();
    assert(n == 1 || n == 2);
    packed |= (params.symbols[0] ? 0x80 : 0);
    packed |= n == 2 ? 0x40 : 0;  // has_second
    out->push_back(static_cast<char>(packed));
  }

  static const char *Read(const char *ptr, const char *limit,
                          FreqTableParams<bool> &params) {
    if (ptr == limit) return nullptr;

    uint8_t packed = static_cast<uint8_t>(*ptr++);
    uint64_t shift = packed & 0x3F;   // bits 0-5
    if (shift < 2 || shift > 23) return nullptr;
    params.shift = shift;

    bool flag       = (packed & 0x80) != 0;
    bool has_second = (packed & 0x40) != 0;

    if (has_second) {
      params.symbols = {flag, !flag};
    } else {
      params.symbols = {flag};
    }
    return ptr;
  }
};

// -- CDF delta encoding (common to all T — not part of FreqTableSymbolCodec) ---
//
//  Wire format for the CDF section: n-1 delta-encoded varint64s representing
//  the gaps between consecutive strictly-increasing lower bounds.
//  low[0] is always 0 and is not stored.

template <class T>
void FreqTableWriteDeltas(const FreqTableParams<T> &params, TsBufferBuilder *out) {
  size_t n = params.symbols.size();
  for (size_t i = 0; i + 1 < n; ++i) {
    assert(params.lows[i] < params.lows[i + 1]);
    PutVarint64(out, params.lows[i + 1] - params.lows[i]);
  }
}

// Decode deltas into params.lows.  Returns advanced ptr on success, nullptr on
// parse error (params may have partial lows — caller must discard the table).
template <class T>
const char *FreqTableReadDeltas(const char *ptr, const char *limit, uint64_t nsymbols,
                                FreqTableParams<T> &params) {
  params.lows.reserve(nsymbols);
  params.lows.push_back(0);
  uint64_t current = 0;
  for (uint64_t i = 0; i + 1 < nsymbols; ++i) {
    uint64_t delta = 0;
    ptr = DecodeVarint64(ptr, limit, &delta);
    if (ptr == nullptr) return nullptr;        // truncated delta
    current += delta;
    params.lows.push_back(current);
  }
  return ptr;
}

// -- Top-level Serialize / Parse -----------------------------------------------

template <class T>
void FreqTableSerialize(const FreqTable<T> &ft, TsBufferBuilder *out) {
  auto params = ft.GetParams();
  FreqTableSymbolCodec<T>::Write(params, out);
  FreqTableWriteDeltas(params, out);
}

template <class T>
FreqTable<T> FreqTableParse(TsSliceGuard &in) {
  FreqTableParams<T> params;
  const char *const start = in.data();
  const char *ptr = start;
  const char *limit = ptr + in.size();

  ptr = FreqTableSymbolCodec<T>::Read(ptr, limit, params);
  if (ptr == nullptr) {
    in.RemovePrefix(limit - start);
    return FreqTable<T>(params);  // invalid params → kParseError
  }

  ptr = FreqTableReadDeltas<T>(ptr, limit, params.symbols.size(), params);
  if (ptr == nullptr) {
    in.RemovePrefix(limit - start);
    return FreqTable<T>(params);  // incomplete lows, invalid
  }

  in.RemovePrefix(ptr - start);
  return FreqTable<T>(params);
}

// -- Encoder<T>: Range Encoder -----------------------------------------------
//
//  Encodes a message as a value within a high-precision integer interval
//  [low, high). Each symbol scales the current interval down to its CDF
//  sub-interval; the range shrinks and high bytes become fixed and are emitted.
//
//  Carry handling (BytesPlusFollow):
//    When an output byte may be affected by a future carry, it is held in
//    buffered_byte while follow bytes (all 0xFF) accumulate. Once the carry
//    is resolved: buffered_byte+1 on carry (follow bytes become 0x00),
//    or output as-is without carry.
//
//  kValidBits = 56 leaves 8 bits of headroom in the 64-bit register so that
//  a carry only ever affects the upper byte, never already-emitted bytes.

template <class T>
class Encoder {
 private:
  const FreqTable<T> *freq_table_;
  TsBufferBuilder *out_;

  uint64_t low_;
  uint64_t range_;
  uint64_t shift;

  uint64_t bytes_to_follow = 0;
  uint8_t buffered_byte;

  static constexpr int kValidBits = 56;
  static constexpr uint64_t kValidMask = (1ULL << kValidBits) - 1;

  void DoBytesPlusFollow() {
    if (bytes_to_follow == 0) {
      buffered_byte = static_cast<uint8_t>(low_ >> (kValidBits - 8));
      // buffered_byte == 0xFF is unreachable because by the time low_'s upper
      // byte reaches 0xFF, a prior Renormalize pass must already have entered
      // case (b) and captured a different buffered_byte (see Renormalize comment).
      // 0xFF would make a future carry overflow the byte boundary.
      assert(buffered_byte != 0xFF);
    }
    ++bytes_to_follow;
  }

  // buffered_byte == 0xFF with is_carry == true is forbidden: 0xFF+1 overflows
  void FinishBytesPlusFollow(bool is_carry) {
    if (bytes_to_follow == 0) return;
    assert(!(is_carry && buffered_byte == 0xFF));
    out_->push_back(static_cast<char>(buffered_byte + is_carry));
    uint8_t c = 0xFF + is_carry;  // 0 -> 0xFF, 1 -> 0x00
    for (uint64_t i = 1; i < bytes_to_follow; ++i) {
      out_->push_back(static_cast<char>(c));
    }
    bytes_to_follow = 0;
  }

  // -- Renormalize: keep the encoding interval well-sized -------------------
  //
  //  The interval [low, low+range) shrinks with each encoded symbol. When
  //  range drops below 2^40 the interval is too narrow to carry enough
  //  precision for subsequent symbols. We expand it by left-shifting both
  //  low and range by 8 bits (effectively multiplying by 256), while
  //  emitting any high bytes that have already become fixed.
  //
  //  The key insight: when the top byte of "low" and "closed_high"
  //  (low+range-1, the inclusive upper bound) are equal, that byte can
  //  never change regardless of how the interval narrows further, so it is
  //  safe to emit. When they differ, the interval straddles a 256-boundary
  //  and the top byte is not yet final -- a future carry could flip it.
  //
  //  Two cases per shift:
  //    (a) first_byte == 0: top bytes match. Emit the fixed byte, possibly
  //        after flushing any deferred follow bytes with the pending carry.
  //    (b) first_byte != 0: top bytes differ. Defer output by buffering the
  //        current top byte (buffered_byte) and accumulating a count of
  //        follow bytes (all 0xFF). When a carry is later resolved, either
  //        buffered_byte+1 and follow bytes become 0x00, or the buffered
  //        byte is output as-is and follow bytes stay 0xFF.
  //
  //  The 2^40 threshold balances output frequency against precision:
  //  a lower threshold means fewer output bytes but risks the interval
  //  becoming too small to distinguish symbols with the given shift.
  void Renormalize() {
    if (range_ >= (1ULL << 40)) return;

    // inclusive upper bound of the interval
    auto closed_high = low_ + range_ - 1;
    // XOR extracts the top byte; if low and closed_high share the same
    // top byte, XOR is 0 in that byte, so first_byte == 0.
    auto first_byte = (low_ ^ closed_high) >> (kValidBits - 8);

    // case (a): top byte is fixed -- flush any deferred bytes first
    if (first_byte == 0) {
      // carry bit = bit 56 of low (the bit just above the valid mask)
      FinishBytesPlusFollow((low_ >> kValidBits) & 0b1);
    }

    // emit all consecutive fixed top bytes while range is still too narrow
    while (first_byte == 0 && range_ < (1ULL << 40)) {
      // extract and emit the determined top byte (bits 48-55)
      out_->push_back(static_cast<char>((low_ >> (kValidBits - 8)) & 0xFF));
      low_ <<= 8;
      low_ &= kValidMask;    // keep low within 56 bits
      range_ <<= 8;          // expand interval
      closed_high = low_ + range_ - 1;
      first_byte = (low_ ^ closed_high) >> (kValidBits - 8);
    }

    // case (b): top byte not yet final -- defer via follow bytes
    // each iteration buffers one byte and shifts to grow the interval
    while (range_ < (1ULL << 40)) {
      DoBytesPlusFollow();
      low_ <<= 8;
      range_ <<= 8;
    }
    low_ &= kValidMask;
  }

  // Scale interval to symbol's CDF sub-range:
  //   new_low   = low  + CDF[s].low  * factor        where factor = range >> shift
  //   new_range = (CDF[s].high - CDF[s].low) * factor
  void Put(T sym) {
    Renormalize();
    auto [low, high] = freq_table_->GetRange(sym);
    auto factor = range_ >> shift;
    low_ += low * factor;
    range_ = (high - low) * factor;
  }

  uint64_t FindShortest(uint64_t low, uint64_t high) {
    uint64_t candidate = 0;
    for (int shift = kValidBits - 8; shift >= 0; shift -= 8) {
      auto hb = (high >> shift) & 0xFF;
      candidate += hb << shift;
      if (candidate >= low) break;
    }
    // assert(candidate <= high && candidate >= low);
    return candidate;
  }

  // Flush remaining low bits so the decoder can uniquely identify the value
  void Finish() {
    auto high = low_ + range_ - 1;
    if ((((low_ ^ high) >> kValidBits) & 0b1) != 0) {
      if (bytes_to_follow != 0) {
        out_->push_back(static_cast<char>(buffered_byte + 1));
      }
      return;
    }

    FinishBytesPlusFollow((low_ >> kValidBits) & 0b1);

    auto v = FindShortest(low_ & kValidMask, high & kValidMask);
    auto valid_bits = kValidBits;
    while ((v & 0xFF) == 0 && valid_bits > 0) {
      v >>= 8;
      valid_bits -= 8;
    }
    for (int shift = valid_bits - 8; shift >= 0; shift -= 8) {
      out_->push_back(static_cast<char>((v >> shift) & 0xFF));
    }
  }

 public:
  Encoder(const FreqTable<T> &freq_table, TsBufferBuilder *out)
      : freq_table_(&freq_table), out_(out), low_(0), range_(1ULL << kValidBits) {
    assert(freq_table_->Ok());
    shift = freq_table_->Shift();
  }

  // Output: [encoded bytes] [Finish tail bytes]  (n is caller's responsibility)
  // Returns false if the FreqTable is invalid (e.g. shift overflow).
  bool Encode(size_t n, const T *symbols) {
    if (!freq_table_->Ok()) return false;
    if (n == 0) return true;
    for (size_t i = 0; i < n; ++i) {
      Put(symbols[i]);
    }
    Finish();
    return true;
  }

  // Encode symbols from an arbitrary input range.
  // InputIter must be at least an input iterator; its value_type must be
  // implicitly convertible to T.
  // Returns false if the FreqTable is invalid.
  template <class InputIter>
  bool Encode(InputIter first, InputIter last) {
    if (!freq_table_->Ok()) return false;
    while (first != last) {
      Put(static_cast<T>(*first++));
    }
    Finish();
    return true;
  }
};

// -- Decoder<T>: Range Decoder -----------------------------------------------
//
//  Inverse of encoding. Reads a 56-bit cur_v window -> each iteration uses
//  cur_v / factor to locate a symbol in the CDF -> subtracts the CDF lower
//  bound contribution from cur_v, scales range -> renormalizes by reading
//  new bytes -> loops until n symbols are decoded.
//
//  Renormalization is symmetric with the Encoder: read input bytes first,
//  pad with zeros when exhausted.

template <class T>
class Decoder {
 private:
  const FreqTable<T> *freq_table_;
  uint64_t shift = 0;

  static constexpr int kValidBits = 56;
  static constexpr uint64_t kValidMask = (1ULL << kValidBits) - 1;

  // Output iterator that appends each T value to a TsBufferBuilder.
  // Enables the pointer-based Decode to forward to the iterator overload.
  class TsBufferBuilderOutputIter {
   public:
    explicit TsBufferBuilderOutputIter(TsBufferBuilder *out) : out_(out) {}
    TsBufferBuilderOutputIter& operator*() { return *this; }
    TsBufferBuilderOutputIter& operator++() { return *this; }
    TsBufferBuilderOutputIter operator++(int) { return *this; }
    void operator=(T val) {
      out_->append(reinterpret_cast<const char *>(&val), sizeof(T));
    }
   private:
    TsBufferBuilder *out_;
  };

 public:
  using value_type = T;

  explicit Decoder(const FreqTable<T> &freq_table) : freq_table_(&freq_table), shift(freq_table_->Shift()) {
    assert(freq_table_->Ok());
  }

  // Decode n symbols from the encoded byte slice.
  // Writes decoded symbols as raw bytes (n * sizeof(T)) into out.
  // Returns true on success, false on parse error.
  bool Decode(const TsSliceGuard &in, size_t n, TsBufferBuilder *out) {
    return Decode(in, n, TsBufferBuilderOutputIter{out});
  }

  // Decode n symbols from the encoded byte slice, writing to an output iterator.
  // OutputIter must support *out++ = sym where sym is of type T.
  // This overload writes one symbol at a time — no intermediate buffer —
  // and is suitable for bitpacked or custom storage backends.
  template <class OutputIter>
  bool Decode(const TsSliceGuard &in, size_t n, OutputIter out) {
    if (n == 0) return true;
    auto ptr = in.data();
    auto limit = ptr + in.size();

    uint64_t total = freq_table_->Total();

    // cur_v is the decoder's 56-bit window over the encoded bytes,
    // analogous to the Encoder's low_. range mirrors the Encoder's
    // interval width, starting at 1 and growing to 1<<56.
    uint64_t cur_v = 0, range = 1;
    size_t i = 0;

    // -- Phase 1: fill the initial 56-bit cur_v window --------------------
    // shift in bytes from the encoded input. If the input has fewer than
    // 7 bytes (very short message), pad with zeros. This is correct
    // because the Encoder's Finish() emits only enough tail bytes to
    // uniquely identify a value within the final interval; padding with
    // zeros picks the smallest such value, which is always valid.
    while (ptr < limit && range < (1ULL << kValidBits)) {
      cur_v = (cur_v << 8) + static_cast<uint8_t>(*ptr++);
      range <<= 8;
      assert(cur_v < range);
    }
    while (range < (1ULL << kValidBits)) {
      cur_v <<= 8;   // pad with zero bits when input is exhausted
      range <<= 8;
    }

    // -- Phase 2: decode symbols one by one --------------------------------
    while (true) {
      // renormalize: keep range >= 2^40, symmetric with Encoder::Renormalize.
      // feed in input bytes when available; pad with zeros when exhausted
      // (the Encoder's trailing zeros from Finish).
      while (range < (1ULL << 40) && ptr < limit) {
        auto c = static_cast<uint8_t>(*ptr++);
        cur_v = (cur_v << 8) + c;
        range <<= 8;
        assert(cur_v < range);
      }
      while (range < (1ULL << 40)) {
        cur_v <<= 8;   // pad with zero when input exhausted
        range <<= 8;
      }

      // map cur_v back to CDF coordinates:
      //   factor = range >> shift   is the unit width of one CDF step
      //   v = cur_v / factor        is the position within [0, total)
      auto factor = range >> shift;
      auto v = cur_v / factor;
      auto [sym, low, high] = freq_table_->Find(v);

      // remove the decoded symbol's contribution (inverse of Encoder::Put):
      //   cur_v -= CDF[sym].low * factor         strip off preceding CDF mass
      //   range  = (CDF[sym].high - CDF[sym].low) * factor  narrow to symbol
      cur_v -= low * factor;
      range = (high - low) * factor;

      *out++ = sym;
      if (++i == n) break;
    }
    return true;
  }
};

}  // namespace detail

// -- Convenience: one-shot Encode / Decode ------------------------------------
//
//  Encode: scan data to build a frequency table, serialize it, then
//  range-encode.  Wire format: [FreqTable] [encoded bytes] [tail]
//
//  Decode: parse FreqTable from the stream, then range-decode the remainder.
//  Writes decoded symbols as raw bytes (n * sizeof(T)) into out.

template <class T>
bool Encode(const T *data, size_t size, TsBufferBuilder *out) {
  assert(size > 0);
  std::unordered_map<T, uint64_t> freq;
  for (size_t i = 0; i < size; ++i) ++freq[data[i]];
  detail::FreqTable<T> ft(freq);
  if (!ft.Ok()) return false;
  detail::FreqTableSerialize(ft, out);
  return detail::Encoder<T>(ft, out).Encode(size, data);
}

// Encode from an arbitrary input range.  Requires at least a forward iterator
// (the range is scanned twice: once for frequency counting, once for encoding).
// Returns false if the FreqTable is invalid (e.g. shift overflow).
template <class ForwardIter>
bool Encode(ForwardIter first, ForwardIter last, TsBufferBuilder *out) {
  using T = typename std::iterator_traits<ForwardIter>::value_type;
  size_t size = 0;
  std::unordered_map<T, uint64_t> freq;
  for (auto it = first; it != last; ++it) {
    ++freq[*it];
    ++size;
  }
  assert(size > 0);
  detail::FreqTable<T> ft(freq);
  if (!ft.Ok()) return false;
  detail::FreqTableSerialize(ft, out);
  return detail::Encoder<T>(ft, out).Encode(first, last);
}

template <class T>
bool Decode(TsSliceGuard &in, size_t n, TsBufferBuilder *out) {
  auto ft = detail::FreqTableParse<T>(in);  // advances in past FreqTable
  if (!ft.Ok()) return false;
  detail::Decoder<T> decoder(ft);
  return decoder.Decode(in, n, out);
}

// Decode n symbols into an arbitrary output iterator.
// OutputIter must support *out++ = sym where sym is of type T.
template <class T, class OutputIter>
bool Decode(TsSliceGuard &in, size_t n, OutputIter out) {
  auto ft = detail::FreqTableParse<T>(in);  // advances in past FreqTable
  if (!ft.Ok()) return false;
  detail::Decoder<T> decoder(ft);
  return decoder.Decode(in, n, out);
}

// -- Bool input iterator: normalizes raw bytes to bool (non-zero → true) -------
// Column data may contain arbitrary byte values for "true"; this adapter ensures
// the frequency table always has at most two symbols.

struct BoolIter {
  using value_type = bool;
  using difference_type = std::ptrdiff_t;
  using iterator_category = std::input_iterator_tag;
  using pointer = void;
  using reference = bool;
  const char *ptr;
  explicit BoolIter(const char *p) : ptr(p) {}
  bool operator*() const { return *ptr != 0; }
  BoolIter &operator++() { ++ptr; return *this; }
  BoolIter operator++(int) { auto t = *this; ++ptr; return t; }
  bool operator==(const BoolIter &o) const { return ptr == o.ptr; }
  bool operator!=(const BoolIter &o) const { return ptr != o.ptr; }
};

}  // namespace RangeCodec

bool TsRangeCodecBool::Compress(TSSlice data, uint64_t count, TsBufferBuilder *out,
                                const TsCompressionConfig &cfg) const {
  if (count == 0) return true;
  return RangeCodec::Encode(RangeCodec::BoolIter(data.data),
                            RangeCodec::BoolIter(data.data + count), out);
}
bool TsRangeCodecBool::Decompress(TSSlice data, uint64_t count, TsSliceGuard *out) const {
  if (count == 0) return true;
  TsBufferBuilder tmp;
  TsSliceGuard data_guard(data);  // advances in past FreqTable
  bool ok = RangeCodec::Decode<bool>(data_guard, count, &tmp);
  *out = tmp.GetBuffer();
  return ok;
}

}  // namespace kwdbts
