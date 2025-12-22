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
#include <cstring>
#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <utility>

#include "libkwdbts2.h"

namespace kwdbts {
enum DataFlags : uint8_t { kValid = 0b00, kNull = 0b01, kNone = 0b10 };

class TsBitmapBase {
 public:
  virtual ~TsBitmapBase() {}
  DataFlags At(size_t idx) const { return this->operator[](idx); }
  virtual DataFlags operator[](size_t idx) const = 0;
  virtual size_t GetCount() const = 0;
  virtual size_t Count(DataFlags flag) const = 0;
  virtual size_t GetValidCount() const = 0;

  bool IsAllValid() const { return GetValidCount() == GetCount(); }
  bool IsAllNull() const { return Count(kNull) == GetCount(); }
  bool IsAllNone() const { return Count(kNone) == GetCount(); }

  virtual std::unique_ptr<TsBitmapBase> Slice(int start, int count) const = 0;
  virtual std::unique_ptr<TsBitmapBase> AsView() const = 0;

  virtual std::string GetStr() const = 0;
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

  TsBitmapView(std::string_view sv, size_t nrows, int start_row) : sv_(sv), nrows_(nrows), start_row_(start_row) {}

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
    int sum = 0;
    int bit_idx = 0;
    for (; bit_idx < nrows_ && (bit_idx + start_row_) % 4 != 0; ++bit_idx) {
      sum += ((*this)[bit_idx] == flag);
    }

    uint8_t flag_int = static_cast<uint8_t>(flag);
    int nleft = nrows_ - bit_idx;
    for (; nleft >= 4; bit_idx += 4, nleft -= 4) {
      int char_idx = (bit_idx + start_row_) / 4;
      uint8_t c = sv_[char_idx];
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
};

class TsBitmap : public TsBitmapBase {
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
  size_t nrows_;
  std::string rep_;

 public:
  TsBitmap() : nrows_(0) {}
  explicit TsBitmap(int nrows) { Reset(nrows); }
  explicit TsBitmap(std::string rep, int nrows) : nrows_(nrows), rep_(std::move(rep)) {}
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
    for (int i = 0; i < nrows_; ++i) {
      (*this)[i] = f;
    }
  }

  void SetAllValid() { std::fill(rep_.begin(), rep_.end(), 0); }

  void SetData(TSSlice rep) { rep_.assign(rep.data, rep.len); }

  void SetCount(size_t count) {
    nrows_ = count;
    Reset(nrows_);
  }

  std::unique_ptr<TsBitmapBase> Slice(int start, int count) const override {
    assert(start + count <= nrows_);
    return std::unique_ptr<TsBitmapView>(new TsBitmapView(rep_, count, start));
  }
  std::unique_ptr<TsBitmapBase> AsView() const override { return Slice(0, nrows_); }

  void Truncate(size_t count) {
    nrows_ = count;
    rep_.resize(GetBitmapLen(nrows_));
  }

  TSSlice GetData() { return {rep_.data(), rep_.size()}; }
  std::string GetStr() const override { return rep_; }

  size_t GetCount() const override { return nrows_; }

  static size_t GetBitmapLen(size_t nrows) { return (nbit_per_row * nrows + 7) / 8; }
  size_t Count(DataFlags flag) const override {
    if (rep_.empty()) {
      return 0;
    }
    uint8_t flag_int = static_cast<uint8_t>(flag);
    int sum = 0;
    int end = rep_.size() - (nrows_ % 4 != 0);
    for (int i = 0; i < end; ++i) {
      uint8_t c = rep_[i];
      sum += ((c & 0b11) == flag_int) + ((c & 0b1100) == (flag_int << 2)) + ((c & 0b110000) == (flag_int << 4)) +
             ((c & 0b11000000) == (flag_int << 6));
    }
    // last byte
    uint8_t c = rep_.back();
    for (int i = 0; i < nrows_ % 4; ++i) {
      sum += ((c >> (2 * i)) & 0b11) == flag_int;
    }
    return sum;
  }
  size_t GetValidCount() const override { return Count(kValid); }

  bool IsAllValid() const {
    return std::all_of(rep_.begin(), rep_.end(), [](char c) { return c == 0; });
  }

  void Append(const TsBitmapBase *rhs) {
    size_t old_count = this->GetCount();
    size_t new_count = old_count + rhs->GetCount();
    nrows_ = new_count;
    rep_.resize(GetBitmapLen(new_count));
    for (int i = 0; i < rhs->GetCount(); ++i) {
      (*this)[i + old_count] = rhs->At(i);
    }
  }

  void push_back(DataFlags flag) {
    rep_.resize(GetBitmapLen(nrows_ + 1));
    nrows_++;
    (*this)[nrows_ - 1] = flag;
  }

  void push_back(Proxy flag) { this->push_back(static_cast<DataFlags>(flag)); }
};

inline std::string TsBitmapView::GetStr() const {
  TsBitmap builder(this->GetCount());
  for (int i = 0; i < this->GetCount(); ++i) {
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
    TsBitmap bitmap(nrows_);
    bitmap.SetAll(kFlag);
    return bitmap.GetStr();
  }
};

}  // namespace kwdbts
