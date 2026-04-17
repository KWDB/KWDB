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
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "libkwdbts2.h"
#include "ts_bufferbuilder.h"

namespace kwdbts {
enum DataFlags : uint8_t { kValid = 0b00, kNull = 0b01, kNone = 0b10 };

class TsBitmap;

template <DataFlags kFlag>
class TsUniformBitmap;

inline constexpr uint8_t GetBitmapBytePattern(DataFlags flag) {
  switch (flag) {
    case kValid:
      return 0x00;
    case kNull:
      return 0x55;
    case kNone:
      return 0xAA;
  }
  return 0x00;
}

inline uint8_t GetBitmapTailMask(size_t nrows) {
  const size_t tail_rows = nrows % 4;
  if (tail_rows == 0) {
    return 0xFF;
  }
  return static_cast<uint8_t>((1U << (tail_rows * 2)) - 1);
}

class TsBitmapBase {
  friend class TsBitmap;

 public:
  virtual ~TsBitmapBase() {}
  DataFlags At(size_t idx) const { return this->operator[](idx); }
  virtual DataFlags operator[](size_t idx) const = 0;
  virtual size_t GetCount() const = 0;
  virtual size_t Count(DataFlags flag) const = 0;
  virtual size_t GetValidCount() const = 0;

  virtual bool IsAllValid() const { return GetValidCount() == GetCount(); }
  virtual bool IsAllNull() const { return Count(kNull) == GetCount(); }
  virtual bool IsAllNone() const { return Count(kNone) == GetCount(); }

  virtual std::unique_ptr<TsBitmapBase> Slice(int start, int count) const = 0;
  virtual std::unique_ptr<TsBitmapBase> AsView() const = 0;

  virtual std::string GetStr() const = 0;
  virtual bool AppendToAligned(TsBitmap* dst) const { return false; }
};

// TsBitmapView is a read-only view of a TsBitmap.
class TsBitmapView : public TsBitmapBase {
  friend class TsBitmap;

 private:
  constexpr static int nbit_per_row = 2;
  std::string_view sv_;
  size_t nrows_;

  int start_row_;

  mutable int valid_count_ = -1;

  static constexpr size_t kRowsPerByte = 4;

  TsBitmapView(std::string_view sv, size_t nrows, int start_row)
      : sv_(sv), nrows_(nrows), start_row_(start_row) {}

 public:
  TsBitmapView(TSSlice data, int nrows) : TsBitmapView(std::string_view(data.data, data.len), nrows, 0) {}
  TsBitmapView(const TsBitmapView &) = default;
  TsBitmapView(TsBitmapView &&) = default;
  TsBitmapView &operator=(const TsBitmapView &) = default;
  TsBitmapView &operator=(TsBitmapView &&) = default;

  DataFlags operator[](size_t idx) const override {
    assert(idx < nrows_);
    idx += start_row_;
    size_t bitidx = nbit_per_row * idx;
    uint32_t charidx = (bitidx >> 3);
    uint8_t charoff = (bitidx & 0b111);
    return static_cast<DataFlags>((sv_[charidx] >> charoff) & 0b11);
  }

  size_t GetCount() const override { return nrows_; }
  size_t Count(DataFlags flag) const override {
    size_t sum = 0;
    size_t bit_idx = 0;
    for (; bit_idx < nrows_ && (bit_idx + start_row_) % 4 != 0; ++bit_idx) {
      sum += ((*this)[bit_idx] == flag);
    }

    const uint8_t flag_int = static_cast<uint8_t>(flag);
    size_t nleft = nrows_ - bit_idx;
    for (; nleft >= 4; bit_idx += 4, nleft -= 4) {
      const size_t char_idx = (bit_idx + start_row_) / kRowsPerByte;
      const uint8_t c = static_cast<uint8_t>(sv_[char_idx]);
      sum += ((c & 0b11) == flag_int) + ((c & 0b1100) == (flag_int << 2)) + ((c & 0b110000) == (flag_int << 4)) +
             ((c & 0b11000000) == (flag_int << 6));
    }
    for (; bit_idx < nrows_; ++bit_idx) {
      sum += ((*this)[bit_idx] == flag);
    }
    return sum;
  }
  size_t GetValidCount() const override {
    if (valid_count_ != -1) {
      return valid_count_;
    }
    valid_count_ = Count(kValid);
    return valid_count_;
  }

  std::string GetStr() const override;

  std::unique_ptr<TsBitmapBase> Slice(int start, int count) const override {
    assert(start + count <= nrows_);
    return std::unique_ptr<TsBitmapView>(new TsBitmapView(sv_, count, start + start_row_));
  }

  std::unique_ptr<TsBitmapBase> AsView() const override { return std::make_unique<TsBitmapView>(*this); }
  bool AppendToAligned(TsBitmap* dst) const override;
};

class TsBitmap : public TsBitmapBase {
  friend class TsBitmapView;
  template <DataFlags kFlag>
  friend class TsUniformBitmap;

 public:
  struct Proxy {
    TsBitmap *p;
    uint32_t charidx;
    uint8_t charoff;
    explicit Proxy(TsBitmap *bitmap, size_t offset) : p{bitmap}, charidx(offset / 8), charoff(offset % 8) {}
    void operator=(DataFlags flag) {
      p->rep_[charidx] &= ~(0b11 << charoff);  // unset the exist flag;
      p->rep_[charidx] |= (flag << charoff);   // set as the given flag;
    }

    void operator=(const Proxy &flag) { *this = static_cast<DataFlags>(flag); }
    operator DataFlags() const { return static_cast<DataFlags>((p->rep_[charidx] >> charoff) & 0b11); }

    bool operator==(const Proxy &flag) const { return static_cast<DataFlags>(*this) == static_cast<DataFlags>(flag); }
  };

 private:
  constexpr static int nbit_per_row = 2;
  constexpr static size_t kRowsPerByte = 4;
  size_t nrows_;
  TsBufferBuilder rep_;

  void ClearUnusedBitsInLastByte() {
    if (rep_.empty()) {
      return;
    }
    rep_.back() = static_cast<char>(static_cast<uint8_t>(rep_.back()) & GetBitmapTailMask(nrows_));
  }

  void AppendAlignedBytes(std::string_view bytes, size_t count) {
    rep_.append(bytes.substr(0, GetBitmapLen(count)));
    nrows_ += count;
    ClearUnusedBitsInLastByte();
  }

  void AppendAlignedUniform(DataFlags flag, size_t count) {
    const size_t old_size = rep_.size();
    nrows_ += count;
    rep_.resize(GetBitmapLen(nrows_));
    std::fill(rep_.data() + old_size, rep_.data() + rep_.size(), static_cast<char>(GetBitmapBytePattern(flag)));
    ClearUnusedBitsInLastByte();
  }

 public:
  TsBitmap() : nrows_(0) {}
  explicit TsBitmap(int nrows) { Reset(nrows); }
  explicit TsBitmap(TSSlice rep, int nrows) {
    nrows_ = nrows;
    rep_.assign(rep.data, rep.len);
  }

  TsBitmap(const TsBitmap &) = delete;
  TsBitmap(TsBitmap &&) = default;

  TsBitmap &operator=(const TsBitmap &) = delete;
  TsBitmap &operator=(TsBitmap &&) = default;

  void Reset(int nrows) {
    nrows_ = nrows;
    rep_.resize((nbit_per_row * nrows + 7) / 8);
    SetAllValid();
  }

  Proxy operator[](size_t idx) {
    assert(idx < nrows_);
    size_t bitidx = nbit_per_row * idx;
    return Proxy{this, bitidx};
  }

  DataFlags operator[](size_t idx) const override {
    assert(idx < nrows_);
    size_t bitidx = nbit_per_row * idx;
    uint32_t charidx = (bitidx / 8);
    uint8_t charoff = (bitidx % 8);
    return static_cast<DataFlags>((rep_[charidx] >> charoff) & 0b11);
  }

  void SetAll(DataFlags f) {
    std::fill(rep_.begin(), rep_.end(), static_cast<char>(GetBitmapBytePattern(f)));
    ClearUnusedBitsInLastByte();
  }

  void SetAllValid() { std::fill(rep_.begin(), rep_.end(), 0); }

  void SetData(TSSlice rep) { rep_.assign(rep.data, rep.len); }

  void SetCount(size_t count) {
    nrows_ = count;
    Reset(nrows_);
  }

  std::unique_ptr<TsBitmapBase> Slice(int start, int count) const override {
    assert(start + count <= nrows_);
    return std::unique_ptr<TsBitmapView>(new TsBitmapView(rep_.AsStringView(), count, start));
  }
  std::unique_ptr<TsBitmapBase> AsView() const override { return Slice(0, nrows_); }

  void Truncate(size_t count) {
    nrows_ = count;
    rep_.resize(GetBitmapLen(nrows_));
  }

  TSSlice GetData() { return rep_.AsSlice(); }
  std::string GetStr() const override { return std::string{rep_.AsStringView()}; }
  TsBufferBuilder GetUnderlyingBuffer() { return std::move(rep_); }

  size_t GetCount() const override { return nrows_; }

  static size_t GetBitmapLen(size_t nrows) { return (nbit_per_row * nrows + 7) / 8; }
  size_t Count(DataFlags flag) const override {
    if (rep_.empty()) {
      return 0;
    }
    const uint8_t flag_int = static_cast<uint8_t>(flag);
    size_t sum = 0;
    const size_t full_bytes = nrows_ / kRowsPerByte;
    for (size_t i = 0; i < full_bytes; ++i) {
      const uint8_t c = static_cast<uint8_t>(rep_[i]);
      sum += ((c & 0b11) == flag_int) + ((c & 0b1100) == (flag_int << 2)) + ((c & 0b110000) == (flag_int << 4)) +
             ((c & 0b11000000) == (flag_int << 6));
    }
    const size_t tail_rows = nrows_ % kRowsPerByte;
    if (tail_rows == 0) {
      return sum;
    }
    const uint8_t c = static_cast<uint8_t>(rep_.back());
    for (size_t i = 0; i < tail_rows; ++i) {
      sum += ((c >> (2 * i)) & 0b11) == flag_int;
    }
    return sum;
  }
  size_t GetValidCount() const override { return Count(kValid); }

  bool IsAllValid() const override {
    return std::all_of(rep_.begin(), rep_.end(), [](char c) { return c == 0; });
  }

  bool AppendToAligned(TsBitmap* dst) const override {
    dst->AppendAlignedBytes(rep_.AsStringView(), nrows_);
    return true;
  }

  void Append(const TsBitmapBase *rhs);

  void push_back(DataFlags flag) {
    rep_.resize(GetBitmapLen(nrows_ + 1));
    nrows_++;
    (*this)[nrows_ - 1] = flag;
  }

  void push_back(Proxy flag) { this->push_back(static_cast<DataFlags>(flag)); }
};

inline std::string TsBitmapView::GetStr() const {
  if (nrows_ == 0) {
    return {};
  }
  if (start_row_ % kRowsPerByte == 0) {
    const size_t start_byte = start_row_ / kRowsPerByte;
    std::string result{sv_.substr(start_byte, TsBitmap::GetBitmapLen(nrows_))};
    result.back() = static_cast<char>(static_cast<uint8_t>(result.back()) & GetBitmapTailMask(nrows_));
    return result;
  }
  TsBitmap builder(this->GetCount());
  for (size_t i = 0; i < this->GetCount(); ++i) {
    builder[i] = this->At(i);
  }
  return builder.GetStr();
}

template <DataFlags kFlag>
class TsUniformBitmap : public TsBitmapBase {
  int nrows_;

 public:
  explicit TsUniformBitmap(int nrows) : nrows_(nrows) {}
  DataFlags operator[](size_t idx) const override { return kFlag; }
  size_t GetCount() const override { return nrows_; }
  size_t GetValidCount() const override { return kFlag == kValid ? nrows_ : 0; }
  size_t Count(DataFlags flag) const override { return kFlag == flag? nrows_ : 0; }

  std::unique_ptr<TsBitmapBase> Slice(int start, int count) const override {
    assert(start + count <= nrows_);
    return std::make_unique<TsUniformBitmap<kFlag>>(count);
  }
  std::unique_ptr<TsBitmapBase> AsView() const override { return std::make_unique<TsUniformBitmap<kFlag>>(nrows_); }

  std::string GetStr() const override {
    std::string result(TsBitmap::GetBitmapLen(nrows_), static_cast<char>(GetBitmapBytePattern(kFlag)));
    if (!result.empty()) {
      result.back() = static_cast<char>(static_cast<uint8_t>(result.back()) & GetBitmapTailMask(nrows_));
    }
    return result;
  }

  bool AppendToAligned(TsBitmap* dst) const override {
    dst->AppendAlignedUniform(kFlag, nrows_);
    return true;
  }
};

inline bool TsBitmapView::AppendToAligned(TsBitmap* dst) const {
  if (start_row_ % kRowsPerByte != 0) {
    return false;
  }
  const size_t start_byte = start_row_ / kRowsPerByte;
  dst->AppendAlignedBytes(sv_.substr(start_byte, TsBitmap::GetBitmapLen(nrows_)), nrows_);
  return true;
}

inline void TsBitmap::Append(const TsBitmapBase *rhs) {
  const size_t rhs_count = rhs->GetCount();
  if (rhs_count == 0) {
    return;
  }

  const size_t old_count = nrows_;
  const size_t new_count = old_count + rhs_count;
  if (old_count % kRowsPerByte == 0 && rhs->AppendToAligned(this)) {
    return;
  }

  nrows_ = new_count;
  rep_.resize(GetBitmapLen(new_count));
  for (size_t i = 0; i < rhs_count; ++i) {
    (*this)[i + old_count] = rhs->At(i);
  }
  ClearUnusedBitsInLastByte();
}

}  // namespace kwdbts
