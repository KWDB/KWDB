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

#include "ts_utils.h"
#include <cmath>
#include "ee_encoding.h"

namespace kwdbts {

/*
 * @Description : Check the maximum leading zero
 * @IN x : origin data
 * @Return : maximum leading zero
 */
template<typename T>
constexpr int custom_countl_zero(T x) {
  if (x == 0)
      return sizeof(T) * 8;

  int count = 0;
  // Check that each bit is zero starting from the highest bit
  for (int i = sizeof(T) * 8 - 1; i >= 0; i--) {
    // Check whether bit i is 1
    if (x & (T(1) << i))
        break;
    count++;
  }
  return count;
}

std::vector<byte> marshalBinary_Byte(const std::vector<byte>& list) {
    // 4 bytes for the size of the list, and a byte for each element in the
    // list.
    std::vector<byte> data;
    data.reserve(4 + list.size());

    // Append the size of the list to a vector as a 32-bit big-endian integer
    // Length of the list. We only need 32 bits because the size of the set
    // couldn't exceed that on 32 bit architectures.
    auto size = static_cast<uint32_t>(list.size());
    data.push_back(static_cast<byte>((size >> 24) & 0xFF));
    data.push_back(static_cast<byte>((size >> 16) & 0xFF));
    data.push_back(static_cast<byte>((size >> 8) & 0xFF));
    data.push_back(static_cast<byte>(size & 0xFF));

    // Marshal each element in the list.
    data.insert(data.end(), list.begin(), list.end());

    return data;
}

std::vector<byte> marshalBinary_Set(const std::unordered_set<uint32_t>& s) {
    std::vector<byte> data;

    // Reserve 4 bytes for byte array
    size_t sl = s.size();
    data.reserve(4 + 4 * sl);

    // Add element
    data.push_back(static_cast<byte>(sl >> 24));
    data.push_back(static_cast<byte>(sl >> 16));
    data.push_back(static_cast<byte>(sl >> 8));
    data.push_back(static_cast<byte>(sl));

    // Add element from set
    for (uint32_t k : s) {
        data.push_back(static_cast<byte>(k >> 24));
        data.push_back(static_cast<byte>(k >> 16));
        data.push_back(static_cast<byte>(k >> 8));
        data.push_back(static_cast<byte>(k));
    }

    return data;
}

uint64_t bextr(uint64_t x, byte start, byte len) {
    return (x >> start) & ((1ULL << len) - 1);
}

uint32_t encodeHash(uint64_t x, byte p, byte pp) {
    // pp bits are extracted from the high part of x
    uint32_t idx = static_cast<uint32_t>(bextr(x, 64 - pp, pp));
    if (bextr(x, 64 - pp, pp - p) == 0) {
        uint64_t temp = (bextr(x, 0, 64 - pp) << pp) | (1ULL << (pp - 1));
        uint32_t zeros = static_cast<uint32_t>(custom_countl_zero(temp)) + 1;
        return (idx << 7) | (zeros << 1) | 1;
    }
    return idx << 1;
}

double alpha(double m) {
    if (m == 16.0) {
        return 0.673;
    } else if (m == 32.0) {
        return 0.697;
    } else if (m == 64.0) {
        return 0.709;
    }
    return 0.7213 / (1 + 1.079 / m);
}

bool PutUint64LittleEndian(std::vector<byte>* buf, uint64_t val) {
    // Make sure buf has at least 8 bytes of capacity
    if (buf->capacity() < 8) {
        buf->resize(8);
    } else {
        (*buf) = std::vector<byte>(buf->begin(), buf->begin() + 8);
    }

    // Put val into buf in little endian order
    // eg: a uint16 val，hex: 0x1234
    // big endian：
    //
    // address N： 0x12
    // address N+1： 0x34
    // little endian：
    //
    // address N： 0x34
    // address N+1： 0x12
    (*buf)[0] = static_cast<byte>(val);
    (*buf)[1] = static_cast<byte>(val >> 8);
    (*buf)[2] = static_cast<byte>(val >> 16);
    (*buf)[3] = static_cast<byte>(val >> 24);
    (*buf)[4] = static_cast<byte>(val >> 32);
    (*buf)[5] = static_cast<byte>(val >> 40);
    (*buf)[6] = static_cast<byte>(val >> 48);
    (*buf)[7] = static_cast<byte>(val >> 56);

    return true;
}

uint64_t rotate_right(uint64_t v, unsigned int k) {
    return (v >> k) | (v << (64 - k));
}

uint64_t uint64_from_bytes(const std::vector<byte>& buffer, size_t offset) {
    uint64_t value;
    // Buffer.data () returns a pointer to the first element of the buffer,
    // and offset is used to specify where to start reading data.
    std::memcpy(&value, buffer.data() + offset, sizeof(uint64_t));
    return value;
}

uint32_t uint32_from_bytes(const std::vector<byte>& buffer, size_t offset) {
    uint32_t value;
    std::memcpy(&value, buffer.data() + offset, sizeof(uint32_t));
    return value;
}

uint16_t uint16_from_bytes(const std::vector<byte>& buffer, size_t offset) {
    uint16_t value;
    std::memcpy(&value, buffer.data() + offset, sizeof(uint16_t));
    return value;
}

uint64_t Hash64(const std::vector<byte>& buffer, uint64_t seed) {
    // fixed constant
    const uint64_t k0 = 0xD6D018F5;
    const uint64_t k1 = 0xA2AA033B;
    const uint64_t k2 = 0x62992FC1;
    const uint64_t k3 = 0x30BC5B29;

    // why?
    // Initialize hash
    uint64_t hash = (seed + k2) * k0;
    size_t i = 0;

    // Process 32 bytes of data
    while (i + 32 <= buffer.size()) {
        uint64_t v0 = uint64_from_bytes(buffer, i) * k0;
        v0 = rotate_right(v0, 29) + hash;
        uint64_t v1 = uint64_from_bytes(buffer, i + 8) * k1;
        v1 = rotate_right(v1, 29) + hash;
        uint64_t v2 = uint64_from_bytes(buffer, i + 16) * k2;
        v2 = rotate_right(v2, 29) + hash;
        uint64_t v3 = uint64_from_bytes(buffer, i + 24) * k3;
        v3 = rotate_right(v3, 29) + hash;

        v0 ^= rotate_right(v0 * k0 + v3, 37) * k1;
        v1 ^= rotate_right(v1 * k1 + v2, 37) * k0;
        v2 ^= rotate_right(v2 * k0 + v1, 37) * k1;
        v3 ^= rotate_right(v3 * k1 + v0, 37) * k0;

        hash += v0 ^ v1;
        i += 32;
    }

    // Process remaining data
    if (i + 16 <= buffer.size()) {
        uint64_t v0 = hash + uint64_from_bytes(buffer, i) * k2;
        v0 = rotate_right(v0, 29) * k3;
        uint64_t v1 = hash + uint64_from_bytes(buffer, i + 8) * k2;
        v1 = rotate_right(v1, 29) * k3;
        v0 ^= rotate_right(v0 * k0, 21) + v1;
        v1 ^= rotate_right(v1 * k3, 21) + v0;
        hash += v1;
        i += 16;
    }

    if (i + 8 <= buffer.size()) {
        hash += uint64_from_bytes(buffer, i) * k3;
        hash ^= rotate_right(hash, 55) * k1;
        i += 8;
    }

    if (i + 4 <= buffer.size()) {
        hash += uint32_from_bytes(buffer, i) * k3;
        hash ^= rotate_right(hash, 26) * k1;
        i += 4;
    }

    if (i + 2 <= buffer.size()) {
        hash += uint16_from_bytes(buffer, i) * k3;
        hash ^= rotate_right(hash, 48) * k1;
        i += 2;
    }

    if (i < buffer.size()) {
        hash += buffer[i] * k3;
        hash ^= rotate_right(hash, 37) * k1;
    }

    // Final mixing operation
    hash ^= rotate_right(hash, 28);
    hash *= k0;
    hash ^= rotate_right(hash, 29);

    return hash;
}

uint32_t bextr32(uint32_t v, uint8_t start, uint8_t length) {
  return (v >> start) & ((1 << length) - 1);
}

uint32_t getIndex(uint32_t k, uint8_t p, uint8_t pp) {
  if ((k & 1) == 1) {
    return bextr32(k, 32 - p, p);
  }
  return bextr32(k, pp - p + 1, p);
}

std::pair<uint32_t, uint8_t> decodeHash(uint32_t k, uint8_t p, uint8_t pp) {
  uint8_t r;
  if ((k & 1) == 1) {
    r = static_cast<uint8_t>(bextr32(k, 1, 6)) + pp - p;
  } else {
    r = static_cast<uint8_t>(custom_countl_zero(static_cast<uint64_t>(k << (32 - pp + p - 1))) - 31);
  }
  return {getIndex(k, p, pp), r};
}

std::pair<uint64_t, uint8_t> getPosVal(uint64_t x, uint8_t p) {
  uint64_t i = bextr(x, 64-p, p);  // Extract the high p-bit of x
  uint64_t w = (x << p) | (1ULL << (p - 1));
  uint8_t rho = static_cast<uint8_t>(custom_countl_zero(w)) + 1;
  return {i, rho};
}

std::vector<byte> EncodeVarintAscending(std::vector<uint8_t>& b, int64_t v) {
  if (v < 0) {
    // Encoding negative values
    if (v >= -0xFF) {
      b.push_back(IntMin + 7);
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFF) {
      b.push_back(IntMin + 6);
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFFFF) {
      b.push_back(IntMin + 5);
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFFFFFF) {
      b.push_back(IntMin + 4);
      b.push_back(static_cast<byte>(v >> 24));
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFFFFFFFF) {
      b.push_back(IntMin + 3);
      b.push_back(static_cast<byte>(v >> 32));
      b.push_back(static_cast<byte>(v >> 24));
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFFFFFFFFFF) {
      b.push_back(IntMin + 2);
      b.push_back(static_cast<byte>(v >> 40));
      b.push_back(static_cast<byte>(v >> 32));
      b.push_back(static_cast<byte>(v >> 24));
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else if (v >= -0xFFFFFFFFFFFFFF) {
      b.push_back(IntMin + 1);
      b.push_back(static_cast<byte>(v >> 48));
      b.push_back(static_cast<byte>(v >> 40));
      b.push_back(static_cast<byte>(v >> 32));
      b.push_back(static_cast<byte>(v >> 24));
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    } else {
      b.push_back(IntMin);
      b.push_back(static_cast<byte>(v >> 56));
      b.push_back(static_cast<byte>(v >> 48));
      b.push_back(static_cast<byte>(v >> 40));
      b.push_back(static_cast<byte>(v >> 32));
      b.push_back(static_cast<byte>(v >> 24));
      b.push_back(static_cast<byte>(v >> 16));
      b.push_back(static_cast<byte>(v >> 8));
      b.push_back(static_cast<byte>(v));
    }
  } else {
    // Positive values delegate to EncodeUvarintAscending
    return EncodeUvarintAscending(b, static_cast<uint64_t>(v));
  }
  return b;
}

std::vector<byte> EncodeUvarintAscending(std::vector<uint8_t>& b, uint64_t v) {
  if (v <= IntSmall) {
    b.push_back(IntZero + static_cast<byte>(v));
  } else if (v <= 0xFF) {
    b.push_back(IntMax - 7);
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFF) {
    b.push_back(IntMax - 6);
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFFFF) {
    b.push_back(IntMax - 5);
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFFFFFF) {
    b.push_back(IntMax - 4);
    b.push_back(static_cast<byte>(v >> 24));
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFFFFFFFF) {
    b.push_back(IntMax - 3);
    b.push_back(static_cast<byte>(v >> 32));
    b.push_back(static_cast<byte>(v >> 24));
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFFFFFFFFFF) {
    b.push_back(IntMax - 2);
    b.push_back(static_cast<byte>(v >> 40));
    b.push_back(static_cast<byte>(v >> 32));
    b.push_back(static_cast<byte>(v >> 24));
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else if (v <= 0xFFFFFFFFFFFFFF) {
    b.push_back(IntMax - 1);
    b.push_back(static_cast<byte>(v >> 48));
    b.push_back(static_cast<byte>(v >> 40));
    b.push_back(static_cast<byte>(v >> 32));
    b.push_back(static_cast<byte>(v >> 24));
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  } else {
    b.push_back(IntMax);
    b.push_back(static_cast<byte>(v >> 56));
    b.push_back(static_cast<byte>(v >> 48));
    b.push_back(static_cast<byte>(v >> 40));
    b.push_back(static_cast<byte>(v >> 32));
    b.push_back(static_cast<byte>(v >> 24));
    b.push_back(static_cast<byte>(v >> 16));
    b.push_back(static_cast<byte>(v >> 8));
    b.push_back(static_cast<byte>(v));
  }
  return b;
}

std::vector<byte> EncodeFloatAscending(std::vector<uint8_t>& b, k_float64 val) {
  if (std::isnan(val)) {
    // Nan
    b.push_back(ValuesEncoding::encodedNotNull + 1);
  } else if (val == 0.0) {
    // Zero
    b.push_back(ValuesEncoding::floatZero);
  } else {
    uint64_t u;
    std::memcpy(&u, &val, sizeof(val));
    if (val < 0) {
      u = ~u;
      // Prefix negative numbers
      b.push_back(ValuesEncoding::floatNeg);
    } else {
      // Prefix positive numbers
      b.push_back(ValuesEncoding::floatPos);
    }

    // Append u to buf in big-endian format
    for (int i = 7; i >= 0; --i) {
      b.push_back((u >> (i * 8)) & 0xFF);
    }
  }
  return b;
}

std::vector<uint8_t> EncodeStringAscending(std::vector<uint8_t>& b, const std::string& val) {
  // Add prefix
  b.push_back(ValuesEncoding::bytesPrefix);

  for (char c : val) {
    if (c == '\x00') {
      // Escaping '\x00'
      b.push_back(escape);
      b.push_back(escaped00);
    } else {
      // The ASCII value of a character
      b.push_back(static_cast<uint8_t>(c));
    }
  }

  // Add termination sequence "\x00\x01"
  b.push_back(escape);
  b.push_back(escapedTerm);

  return b;
}

}  // namespace kwdbts
