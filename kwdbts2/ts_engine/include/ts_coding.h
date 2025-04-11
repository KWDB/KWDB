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

#include <endian.h>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>
#include <string_view>
#include <algorithm>
#include <type_traits>
#include "libkwdbts2.h"

namespace kwdbts {

char *EncodeFixed16(char *buf, uint16_t v);
char *EncodeFixed32(char *buf, uint32_t v);
char *EncodeFixed64(char *buf, uint64_t v);

// LEB128 encoding (unsigned)
char *EncodeVarint32(char *buf, uint32_t v);
char *EncodeVarint64(char *buf, uint64_t v);

void PutFixed16(std::string *dst, uint16_t v);
void PutFixed32(std::string *dst, uint32_t v);
void PutFixed64(std::string *dst, uint64_t v);

uint16_t DecodeFixed16(const char *ptr);
uint32_t DecodeFixed32(const char *ptr);
uint64_t DecodeFixed64(const char *ptr);

const char *DecodeVarint32(const char *ptr, const char *limit, uint32_t *v);
const char *DecodeVarint64(const char *ptr, const char *limit, uint64_t *v);

inline char *EncodeFixed16(char *buf, uint16_t v) {
  v = htole16(v);
  std::memcpy(buf, &v, sizeof(v));
  return buf + sizeof(v);
}

inline char *EncodeFixed32(char *buf, uint32_t v) {
  v = htole32(v);
  std::memcpy(buf, &v, sizeof(v));
  return buf + sizeof(v);
}

inline char *EncodeFixed64(char *buf, uint64_t v) {
  v = htole64(v);
  std::memcpy(buf, &v, sizeof(v));
  return buf + sizeof(v);
}

inline char *EncodeVarint32(char *dst, uint32_t v) {
  unsigned char* ptr = reinterpret_cast<unsigned char*>(dst);
  static const int B = 128;
  if (v < (1 << 7)) {
    *(ptr++) = v;
  } else if (v < (1 << 14)) {
    *(ptr++) = v | B;
    *(ptr++) = v >> 7;
  } else if (v < (1 << 21)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = v >> 14;
  } else if (v < (1 << 28)) {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = v >> 21;
  } else {
    *(ptr++) = v | B;
    *(ptr++) = (v >> 7) | B;
    *(ptr++) = (v >> 14) | B;
    *(ptr++) = (v >> 21) | B;
    *(ptr++) = v >> 28;
  }
  return reinterpret_cast<char*>(ptr);
}

inline char *EncodeVarint64(char *dst, uint64_t v) {
  static const unsigned int B = 128;
  unsigned char *ptr = reinterpret_cast<unsigned char *>(dst);
  while (v >= B) {
    *(ptr++) = (v & (B - 1)) | B;
    v >>= 7;
  }
  *(ptr++) = static_cast<unsigned char>(v);
  return reinterpret_cast<char *>(ptr);
}

inline uint16_t DecodeFixed16(const char *ptr) {
  uint16_t result;
  memcpy(&result, ptr, sizeof(result));
  return le16toh(result);
}

inline uint32_t DecodeFixed32(const char *ptr) {
  uint32_t result;
  memcpy(&result, ptr, sizeof(result));
  return le32toh(result);
}

inline uint64_t DecodeFixed64(const char *ptr) {
  uint64_t result;
  memcpy(&result, ptr, sizeof(result));
  return le64toh(result);
}

inline void PutFixed16(std::string *dst, uint16_t v) {
  v = htole16(v);
  dst->append(reinterpret_cast<char *>(&v), sizeof(v));
}

inline void PutFixed32(std::string *dst, uint32_t v) {
  v = htole32(v);
  dst->append(reinterpret_cast<char *>(&v), sizeof(v));
}

inline void PutFixed64(std::string *dst, uint64_t v) {
  v = htole64(v);
  dst->append(reinterpret_cast<char *>(&v), sizeof(v));
}

inline void GetChar(TSSlice *slice, char v){

}

inline void GetFixed16(TSSlice *slice, uint16_t *v) {
  assert(slice->len >= 2);
  *v = DecodeFixed16(slice->data);
  slice->data += 2;
  slice->len -= 2;
}

inline void GetFixed32(TSSlice *slice, uint32_t *v) {
  assert(slice->len >= 4);
  *v = DecodeFixed32(slice->data);
  slice->data += 4;
  slice->len -= 4;
}

inline void GetFixed64(TSSlice *slice, uint64_t *v) {
  assert(slice->len >= 8);
  *v = DecodeFixed64(slice->data);
  slice->data += 8;
  slice->len -= 8;
}

inline void RemovePrefix(TSSlice *slice, int n) {
  slice->data += n;
  slice->len -= n;
}

inline void PutVarint32(std::string *dst, uint32_t v) {
  char buf[5];
  char *ptr = EncodeVarint32(buf, v);
  dst->append(buf, ptr - buf);
}

inline void PutVarint64(std::string *dst, uint64_t v) {
  char buf[10];
  char *ptr = EncodeVarint64(buf, v);
  dst->append(buf, ptr - buf);
}

inline const char *DecodeVarint32(const char *ptr, const char *limit, uint32_t *v) {
  *v = 0;
  int shift = 0;
  for (int shift = 0; ptr < limit && shift <= 28; shift += 7) {
    uint32_t b = static_cast<uint8_t>(*ptr);
    ++ptr;
    *v += (b & 0x7F) << shift;
    if ((b & 0x80) == 0) return ptr;
  }
  return nullptr;
}
inline const char *DecodeVarint64(const char *ptr, const char *limit, uint64_t *v) {
  *v = 0;
  int shift = 0;
  for (int shift = 0; ptr < limit && shift <= 63; shift += 7) {
    uint64_t b = static_cast<uint8_t>(*ptr);
    ++ptr;
    *v += (b & 0x7F) << shift;
    if ((b & 0x80) == 0) return ptr;
  }
  return nullptr;
}

template <class T>
std::make_unsigned_t<T> EncodeZigZag(T v) {
  static_assert(std::is_signed_v<T>);
  return (v << 1) ^ (v >> (sizeof(T) * 8 - 1));
}
template <class T>
std::make_signed_t<T> DecodeZigZag(T v) {
  static_assert(std::is_unsigned_v<T>);
  return (v >> 1) ^ -(v & 1);
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
    int nhead = pos_ % 8;
    if (nhead == 0) {
      *v = static_cast<uint8_t>(rep_[idx]);
      pos_ += 8;
      return true;
    }

    int ntail = 8 - nhead;
    *v += rep_[idx] & ((1U << ntail) - 1);
    *v <<= nhead;

    if (idx + 1 == rep_.size()) {
      return false;
    }

    assert(idx + 1 < rep_.size());
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
    *v += tmp1;
    pos_ += nread;

    if (nbits == 0) {
      return true;
    }
    if (idx + 1 > rep_.size()) {
      pos_ = pos;
      return false;
    }
    // read bits from next byte;
    assert(idx + 1 < rep_.size());
    uint64_t tmp2 = (rep_[idx + 1] >> (8 - nbits)) & ((1 << nbits) - 1);
    *v += tmp2;
    pos_ += nbits;
    return true;
  }
};

}  // namespace kwdbts
