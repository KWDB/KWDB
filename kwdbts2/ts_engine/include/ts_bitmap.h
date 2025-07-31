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
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

#include "libkwdbts2.h"

namespace kwdbts {
enum DataFlags : uint8_t { kValid = 0b00, kNull = 0b01, kNone = 0b10 };

// TsBitmapView is a read-only view of a TsBitmap.
class TsBitmapView {
  friend class TsBitmap;

 private:
  constexpr static int nbit_per_row = 2;
  std::string_view sv_;
  size_t nrows_;

  int start_row_;

  mutable int valid_count_ = -1;

  TsBitmapView(std::string_view sv, size_t nrows, int start_row) : sv_(sv), nrows_(nrows), start_row_(start_row) {}
 public:
  TsBitmapView(const TsBitmapView &) = default;
  TsBitmapView(TsBitmapView &&) = default;
  TsBitmapView &operator=(const TsBitmapView &) = default;
  TsBitmapView &operator=(TsBitmapView &&) = default;

  DataFlags operator[](size_t idx) const {
    assert(idx < nrows_);
    idx += start_row_;
    size_t bitidx = nbit_per_row * idx;
    uint32_t charidx = (bitidx >> 3);
    uint8_t charoff = (bitidx & 0b111);
    return static_cast<DataFlags>((sv_[charidx] >> charoff) & 0b11);
  }

  size_t GetCount() const { return nrows_; }
  size_t GetValidCount() const {
    if (valid_count_ != -1) {
      return valid_count_;
    }
    int sum = 0;
    int bit_idx = 0;
    for (; bit_idx < nrows_ && (bit_idx + start_row_) % 4 != 0; ++bit_idx) {
      sum += ((*this)[bit_idx] == kValid);
    }

    int nleft = nrows_ - bit_idx;
    for (; nleft >= 4; bit_idx += 4, nleft -= 4) {
      int char_idx = (bit_idx + start_row_) / 4;
      uint8_t c = sv_[char_idx];
      sum += ((c & 0b11) == 0) + ((c & 0b1100) == 0) + ((c & 0b110000) == 0) + ((c & 0b11000000) == 0);
    }
    for (; bit_idx < nrows_; ++bit_idx) {
      sum += ((*this)[bit_idx] == kValid);
    }
    valid_count_ = sum;
    return sum;
  }

  TsBitmapView Slice(int start, int count) {
    assert(start + count < nrows_);
    return TsBitmapView(sv_, count, start + start_row_);
  }
};

class TsBitmap {
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
  explicit TsBitmap(TSSlice rep, int nrows) {
    nrows_ = nrows;
    rep_.assign(rep.data, rep.len);
  }

  TsBitmap(const TsBitmap &) = default;
  TsBitmap(TsBitmap &&) = default;

  TsBitmap &operator=(const TsBitmap &) = default;
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

  DataFlags operator[](size_t idx) const {
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

  void SetAllValid() {
    std::fill(rep_.begin(), rep_.end(), 0);
  }

  void SetData(TSSlice rep) { rep_.assign(rep.data, rep.len); }

  void SetCount(size_t count) {
    nrows_ = count;
    Reset(nrows_);
  }

  TsBitmapView Slice(int start, int count) {
    assert(start + count < nrows_);
    return TsBitmapView(rep_, count, start);
  }

  void Truncate(size_t count) {
    nrows_ = count;
    rep_.resize(GetBitmapLen(nrows_));
  }

  TSSlice GetData() { return {rep_.data(), rep_.size()}; }
  const std::string &GetStr() const { return rep_; }

  size_t GetCount() const { return nrows_; }

  static size_t GetBitmapLen(size_t nrows) { return (nbit_per_row * nrows + 7) / 8; }
  size_t GetValidCount() const {
    if (rep_.empty()) {
      return 0;
    }
    int sum = 0;
    int end = rep_.size() - (nrows_ % 4 != 0);
    for (int i = 0; i < end; ++i) {
      uint8_t c = rep_[i];
      sum += ((c & 0b11) == 0) + ((c & 0b1100) == 0) + ((c & 0b110000) == 0) + ((c & 0b11000000) == 0);
    }
    // last byte
    uint8_t c = rep_.back();
    for (int i = 0; i < nrows_ % 4; ++i) {
      sum += ((c >> (2 * i)) & 0b11) == 0b00;
    }
    return sum;
  }
  bool IsAllValid() const {
    return std::all_of(rep_.begin(), rep_.end(), [](char c) { return c == 0; });
  }

  TsBitmap &operator+=(const TsBitmap &rhs) {
    size_t old_count = this->GetCount();
    size_t new_count = old_count + rhs.GetCount();
    nrows_ = new_count;
    rep_.resize(GetBitmapLen(new_count));
    for (int i = 0; i < rhs.GetCount(); ++i) {
      (*this)[i + old_count] = rhs[i];
    }
    return *this;
  }

  void push_back(DataFlags flag) {
    rep_.resize(GetBitmapLen(nrows_ + 1));
    nrows_++;
    (*this)[nrows_ - 1] = flag;
  }

  void push_back(Proxy flag) { this->push_back(static_cast<DataFlags>(flag)); }
};

}  // namespace kwdbts
