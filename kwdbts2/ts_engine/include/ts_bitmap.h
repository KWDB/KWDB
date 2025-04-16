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
#include <string>

#include "libkwdbts2.h"

namespace kwdbts {
enum DataFlags : uint8_t { kValid = 0b00, kNull = 0b01, kNone = 0b10 };

class TsBitmap {
 private:
  constexpr static int nbit_per_row = 2;
  size_t nrows_, nvalid_;
  std::string rep_;

  struct Proxy {
    TsBitmap *p;
    uint32_t charidx;
    uint8_t charoff;
    explicit Proxy(TsBitmap *bitmap, size_t offset)
        : p{bitmap}, charidx(offset / 8), charoff(offset % 8) {}
    void operator=(DataFlags flag) {
      DataFlags old_flag = *this;
      p->rep_[charidx] &= ~(0b11 << charoff);  // unset the exist flag;
      p->rep_[charidx] |= (flag << charoff);   // set as the given flag;
      p->nvalid_ += (flag == kValid) - (old_flag == kValid);
    }
    operator DataFlags() const {
      return static_cast<DataFlags>((p->rep_[charidx] >> charoff) & 0b11);
    }
  };

 public:
  TsBitmap() : nrows_(0), nvalid_(0) {}
  explicit TsBitmap(int nrows) { Reset(nrows); }
  explicit TsBitmap(TSSlice rep, int nrows) { Map(rep, nrows); }

  TsBitmap(const TsBitmap &) = default;
  TsBitmap(TsBitmap &&) = default;

  TsBitmap &operator=(const TsBitmap &) = default;
  TsBitmap &operator=(TsBitmap &&) = default;

  void Map(TSSlice rep, int nrows) {
    nrows_ = nrows;
    rep_.assign(rep.data, rep.len);
    nvalid_ = 0;
    for (int i = 0; i < nrows_; ++i) {
      nvalid_ += ((*this)[i] == kValid);
    }
  }

  void Reset(int nrows) {
    nrows_ = nrows;
    nvalid_ = nrows;
    rep_.clear();
    rep_.resize((nbit_per_row * nrows + 7) / 8);
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

  void SetData(TSSlice rep) { rep_.assign(rep.data, rep.len); }

  void SetCount(size_t count) {
    nrows_ = count;
    Reset(nrows_);
    SetAll(DataFlags::kValid);
  }

  
#define	CLEAR_BIT(x, bit)	(x &= ~(1 << bit))
  // todo(liangbo01) check if function correct.
  bool SetRowBitmap(size_t idx, DataFlags flag) {
    assert(idx < nrows_);
    size_t bitidx = nbit_per_row * idx;
    uint32_t charidx = (bitidx / 8);
    uint8_t charoff = (bitidx % 8);
    // first reset bitmap to 00
    for (size_t i = 0; i < nbit_per_row; i++) {
      CLEAR_BIT(rep_[charidx], (charoff + i));
    }
    // second add flag.
    rep_[charidx] |= (flag << charoff);
    return true;
  }

  TSSlice GetData() { return {rep_.data(), rep_.size()}; }

  size_t GetCount() const { return nrows_; }

  static size_t GetBitmapLen(size_t nrows) { return (nbit_per_row * nrows + 7) / 8; }
  size_t GetValidCount() const { return nvalid_; }
  bool IsAllValid() const {
    return std::all_of(rep_.begin(), rep_.end(), [](char c) { return c == 0; });
  }
};
}  // namespace kwdbts
