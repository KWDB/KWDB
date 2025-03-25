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
#include <cstring>
#include <string>
#include <string_view>
#include <algorithm>

namespace kwdbts {

namespace coding::details {
template <int N>
struct Type {};

#define declear_type(n)        \
  template <>                  \
  struct Type<n> {             \
    using dtype = uint##n##_t; \
  }
declear_type(8);
declear_type(16);
declear_type(32);
declear_type(64);
#undef declear_type

template <int N>
using DType = typename Type<N>::dtype;

}  // namespace coding::details

template <int N>
void EncodeFixed(char *dst, coding::details::DType<N> v) {
  // TODO(zzr) fix endian
  memcpy(dst, &v, sizeof(v));
}

template <int N>
void PutFixed(std::string *dst, coding::details::DType<N> v) {
  dst->append(const_cast<const char *>(reinterpret_cast<char *>(&v)), sizeof(v));
}

template <class T>
inline void PutType(std::string *dst, const T &v) {
  dst->append(reinterpret_cast<const char *>(&v), sizeof(v));
}

template <class T>
inline char *PutType(char *dst, const T &v) {
  memcpy(dst, &v, sizeof(v));
  dst += sizeof(v);
  return dst;
}

template <int N>
coding::details::DType<N> DecodeFixed(const char *ptr) {
  coding::details::DType<N> result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}

template <class T>
T DecodeType(const char *ptr) {
  T result;
  memcpy(&result, ptr, sizeof(result));
  return result;
}

class TsBitWriter {
 private:
  std::string *rep_;
  size_t pos_;

 public:
  explicit TsBitWriter(std::string *rep) : rep_(rep), pos_(0) { rep_->clear(); }
  const std::string &Str() const { return *rep_; }
  bool WriteBits(int nbits, uint64_t v) {
    assert(nbits > 0 && nbits <= 64);
    assert(nbits == 64 || v <= ((1ULL << nbits) - 1));
    while (nbits > 0) {
      int nspace = rep_->size() * 8 - pos_;
      if (nspace == 0) {
        rep_->push_back(0);
      } else if (nspace >= nbits) {
        rep_->back() |= v << (nspace - nbits);
        pos_ += nbits;
        nbits = 0;
      } else {
        uint64_t mask = ((1ULL << nspace) - 1) << (nbits - nspace);
        rep_->back() |= (v & mask) >> (nbits - nspace);

        // v &= (1ULL << (nbits - nspace)) - 1;
        v &= ~mask;
        nbits -= nspace;
        pos_ += nspace;
      }
    }
    return true;
  }

  bool WriteBit(bool bit) {
    if (rep_->size() * 8 - pos_ == 0) {
      rep_->push_back(0);
    }
    rep_->back() |= bit << (7 - pos_ % 8);
    pos_++;
    return true;
  }

  bool WriteByte(char b) { return WriteBits(8, static_cast<uint8_t>(b)); }
};

class TsBitReader {
 private:
  std::string_view rep_;
  size_t pos_;

 public:
  // TsBitReader(const std::string &rep, size_t pos = 0) : rep_(rep), pos_(pos) {}
  explicit TsBitReader(const std::string_view &rep, size_t pos = 0) : rep_(rep), pos_(pos) {}
  bool ReadByte(uint8_t *v) {
    int idx = pos_ / 8;
    if (idx >= rep_.size()) {
      return false;
    }
    *v = 0;
    int ntail = 8 - pos_ % 8;
    (*v) += rep_[idx] & ((1 << ntail) - 1);
    (*v) <<= (8 - ntail);

    int nhead = 8 - ntail;
    if (nhead != 0 && idx + 1 == rep_.size()) {
      return false;
    }

    uint8_t tmp = rep_[idx + 1] & (((1 << nhead) - 1) << ntail);
    tmp >>= ntail;
    *v += tmp;
    pos_ += 8;
    return true;
  }
  bool ReadBit(bool *bit) {
    size_t idx = pos_ / 8;
    if (idx >= rep_.size()) {
      return false;
    }
    size_t off = pos_ % 8;
    *bit = rep_[idx] & (0x80 >> off);
    ++pos_;
    return true;
  }
  bool ReadBits(uint32_t nbits, uint64_t *v) {
    if (nbits > 64) {
      return false;
    }
    size_t pos = pos_;

    *v = 0;
    while (nbits >= 8) {
      uint8_t byte;
      bool ok = ReadByte(&byte);
      if (!ok) {
        pos_ = pos;
        return false;
      }
      *v = ((*v) << 8) + byte;
      nbits -= 8;
    }
    if (nbits == 0) {
      return true;
    }
    *v <<= nbits;
    size_t idx = pos_ / 8;
    size_t off = pos_ % 8;
    if (idx >= rep_.size()) {
      pos_ = pos;
      return false;
    }

    // read bits from current byte;
    int nleft = 8 - off;
    size_t nread = std::min<int>(nleft, nbits);
    uint64_t tmp1 = (rep_[idx] >> (nleft - nread)) & ((1 << nread) - 1);
    nbits -= nread;
    tmp1 <<= nbits;
    pos_ += nread;

    if (nbits != 0 && idx + 1 >= rep_.size()) {
      pos_ = pos;
      return false;
    }
    // read bits from next byte;
    uint64_t tmp2 = (rep_[idx + 1] >> (8 - nbits)) & ((1 << nbits) - 1);
    *v += (tmp1 + tmp2);
    pos_ += nbits;
    return true;
  }
};

}  // namespace kwdbts
