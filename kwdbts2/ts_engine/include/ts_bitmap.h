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
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <string>

#include "libkwdbts2.h"

namespace kwdbts {
enum DataFlags : uint8_t { kValid = 0b00, kNull = 0b01, kNone = 0b10 };

class TsBitmap {
 private:
  constexpr static int nbit_per_row = 2;
  size_t nrow_;
  std::string rep_;

  struct Proxy {
    TsBitmap *p;
    uint32_t charidx;
    uint8_t charoff;
    explicit Proxy(TsBitmap *bitmap, size_t offset)
        : p{bitmap}, charidx(offset / 8), charoff(offset % 8) {}
    void operator=(DataFlags flag) {
      uint8_t mask = 0b11000000;
      p->rep_[charidx] &= ~(mask >> charoff);  // unset the exist flag;
      uint8_t f = flag << (8 - nbit_per_row);
      p->rep_[charidx] |= (f >> charoff);  // set as the given flag;
    }
    operator DataFlags() const {
      uint8_t mask = 0b11000000;
      return static_cast<DataFlags>((p->rep_[charidx] & (mask >> charoff)) >>
                                    (8 - nbit_per_row - charoff));
    }
  };

 public:
  TsBitmap() : nrow_(0) {}
  explicit TsBitmap(int nrows) { Reset(nrows); }
  explicit TsBitmap(TSSlice rep, int nrows) : nrow_(nrows) {
    assert(rep.len >= rep_.size());
    rep_.assign(rep.data, rep.len);
  }

  void Reset(int nrows) {
    nrow_ = nrows;
    rep_.clear();
    rep_.resize((nbit_per_row * nrows + 7) / 8);
  }

  Proxy operator[](size_t idx) {
    assert(idx < nrow_);
    size_t bitidx = nbit_per_row * idx;
    return Proxy{this, bitidx};
  }

  void SetAll(DataFlags f) {
    for (int i = 0; i < nrow_; ++i) {
      (*this)[i] = f;
    }
  }

  void SetData(TSSlice rep) {
    rep_.assign(rep.data, rep.len);
  }

  TSSlice GetData() {
    return {rep_.data(), rep_.size()};
  }

  size_t GetNRow() const {
    return nrow_;
  }

  static size_t GetBitmapLen(size_t nrows) {
    return (nbit_per_row * nrows + 7) / 8;
  }
};
}  // namespace kwdbts
