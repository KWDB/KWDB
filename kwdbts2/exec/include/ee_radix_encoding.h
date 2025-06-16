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

#include <sys/time.h>

#include <cfloat>
#include <climits>
#include <cmath>
#include <ctime>
#include <string>
#include <limits>

#include "ee_bswap.h"
#include "ee_common.h"
#include "kwdb_type.h"

namespace kwdbts {
/**
 * @brief A utility class that provides radix encoding and decoding functions for various data types.
 * 
 * The `Radix` struct contains a set of static methods for encoding and decoding different data types,
 * such as integers, floating-point numbers, booleans, and variable-length strings. It supports both
 * normal and inverted encoding, which can be useful for sorting data in ascending or descending order.
 * Additionally, it provides functions to handle endianness, flip sign bits, and manage null values.
 */
struct Radix {
 public:
  static inline bool IsLittleEndian() {
    int n = 1;
    if (*reinterpret_cast<char*>(n) == 1) {
      return true;
    } else {
      return false;
    }
  }
  template <class T>
  static void EncodeData(k_uint8* dataptr, k_uint8* valueptr) {
    LOG_ERROR("Cannot encode data from this type");
  }

  template <class T>
  static void EncodeDataInvert(k_uint8* dataptr, k_uint8* valueptr) {
    LOG_ERROR("Cannot encode data from this type");
  }
  template <class T>
  static void EncodeData(k_uint8* dataptr, T* valueptr) {
    LOG_ERROR("Cannot encode data from this type");
  }

  template <class T>
  static void EncodeDataInvert(k_uint8* dataptr, T* valueptr) {
    LOG_ERROR("Cannot encode data from this type");
  }

  template <class T>
  static inline void Store(T* dataptr, k_uint8* valueptr, k_uint32 len) {
    memcpy(dataptr, valueptr, len);  // NOLINT
  }

  template <class T>
  static inline void StoreInvert(k_uint8* dataptr, T* valueptr, k_uint32 len) {
    const auto val_ptr = reinterpret_cast<k_uint8*>(valueptr);
    for (size_t i = 0; i < len; ++i) {
      *(dataptr + i) = ~*(val_ptr + i);
    }
  }

  static inline void EncodeVarData(k_uint8* dataptr, k_uint8* valueptr,
                                   k_uint32 length) {
    k_uint16* var_length = reinterpret_cast<k_uint16*>(valueptr);
    memcpy(dataptr, valueptr + STRING_WIDE, *var_length);
    memset(dataptr + *var_length, 0, length - *var_length - STRING_WIDE);
    k_uint16 encoded_len = BSwap(*var_length);
    memcpy(dataptr + length - STRING_WIDE, &encoded_len, STRING_WIDE);
  }

  static inline void EncodeVarDataInvert(k_uint8* dataptr, k_uint8* valueptr,
                                         k_uint32 length) {
    k_uint16* var_length = reinterpret_cast<k_uint16*>(valueptr);
    StoreInvert(dataptr, valueptr + STRING_WIDE, *var_length);
    memset(dataptr + *var_length, UINT8_MAX, length - *var_length - STRING_WIDE);
    k_uint16 encoded_len = BSwap(*var_length);
    StoreInvert(dataptr + length - STRING_WIDE,
                &encoded_len, STRING_WIDE);
  }

  static inline void DecodeVarData(k_uint8* dataptr, k_uint8* valueptr,
                                   k_uint32 length) {
    k_uint16* var_length = reinterpret_cast<k_uint16*>(valueptr + length - STRING_WIDE);
    k_uint16 decoded_len = BSwap(*var_length);
    memmove(dataptr + STRING_WIDE, valueptr, decoded_len);
    memcpy(dataptr, &decoded_len, STRING_WIDE);
  }

  static inline void DecodeVarDataInvert(k_uint8* dataptr, k_uint8* valueptr,
                                         k_uint32 length) {
    k_uint16 var_length = 0;
    StoreInvert(reinterpret_cast<k_uint8*>(&var_length),
                valueptr + length - STRING_WIDE, STRING_WIDE);
    var_length = BSwap(var_length);
    // memmove(dataptr + STRING_WIDE, valueptr, var_length);
    for (k_uint16 i = var_length; i > 0; --i) {
      dataptr[STRING_WIDE + i - 1] = ~valueptr[i - 1];
    }
    memcpy(dataptr, &var_length, STRING_WIDE);
  }

  template <class T>
  static void DecodeData(k_uint8* dataptr, k_uint8* valueptr) {
    LOG_ERROR("Cannot decode data from this type");
  }
  template <class T>
  static void DecodeDataInvert(k_uint8* dataptr, k_uint8* valueptr) {
    LOG_ERROR("Cannot decode data from this type");
  }
  template <class T>
  static void DecodeData(k_uint8* dataptr, T* valueptr) {
    LOG_ERROR("Cannot decode data from this type");
  }

  template <class T>
  static void DecodeDataInvert(k_uint8* dataptr, T* valueptr) {
    LOG_ERROR("Cannot decode data from this type");
  }
  [[nodiscard]] static inline k_uint8 FlipSign(k_uint8 key_byte) {
    return key_byte ^ 128;
  }

 private:
  template <class T>
  static void EncodeSigned(k_uint8* dataptr, k_uint8* valueptr);

  template <class T>
  static void EncodeSignedInvert(k_uint8* dataptr, k_uint8* valueptr);

  template <class T>
  static void DecodeSigned(k_uint8* dataptr, k_uint8* valueptr);

  template <class T>
  static void DecodeSignedInvert(k_uint8* dataptr, k_uint8* valueptr);

  /**
   * @brief Encodes a single-precision floating-point number into a 32-bit unsigned integer.
   * 
   * This function converts a single-precision floating-point number pointed to by `valueptr`
   * into a 32-bit unsigned integer using a radix encoding scheme. The encoding is designed
   * to preserve the order of the floating-point numbers when compared as integers, which is
   * useful for sorting purposes.
   * 
   * @param valueptr Pointer to the single-precision floating-point number to be encoded.
   * @return k_uint32 The 32-bit unsigned integer representation of the encoded floating-point number.
   */
  static inline k_uint32 EncodeFloat(k_uint8* valueptr) {
    float x = *reinterpret_cast<float*>(valueptr);
    k_uint32 buff;
    if (x == 0) {
      buff = 0;
      buff |= (1u << 31);
      return buff;
    }
    if (std::isnan(x)) {
      return UINT_MAX;
    }
    //! infinity
    if (x > FLT_MAX) {
      return UINT_MAX - 1;
    }
    //! -infinity
    if (x < -FLT_MAX) {
      return 0;
    }

    memcpy(&buff, valueptr, sizeof(k_uint32));
    // Check if the number is positive (including positive zero)
    if ((buff & (1U << 31)) == 0) {
      // Set the sign bit to 1 to ensure positive numbers are ordered correctly
      buff |= (1U << 31);
    } else {
      // Invert all bits of the buffer to ensure negative numbers are ordered correctly
      buff = ~buff;
    }

    return buff;
  }

  static inline float DecodeFloat(k_uint32 input) {
    // nan
    if (input == UINT_MAX) {
      return std::numeric_limits<float>::quiet_NaN();
    }
    if (input == UINT_MAX - 1) {
      return std::numeric_limits<float>::infinity();
    }
    if (input == 0) {
      return -std::numeric_limits<float>::infinity();
    }
    float result;
    if (input & (1U << 31)) {
      // positive numbers - flip sign bit
      input = input ^ (1U << 31);
    } else {
      // negative numbers - invert
      input = ~input;
    }
    memcpy(&result, &input, sizeof(float));
    return result;
  }
    /**
   * @brief Encodes a double-precision floating-point number into a 64-bit unsigned integer.
   * 
   * This function converts a double-precision floating-point number pointed to by `valueptr`
   * into a 64-bit unsigned integer using a radix encoding scheme. The encoding is designed
   * to preserve the order of the floating-point numbers when compared as integers, which is
   * useful for sorting purposes.
   * 
   * @param valueptr Pointer to the double-precision floating-point number to be encoded.
   * @return uint64_t The 64-bit unsigned integer representation of the encoded floating-point number.
   */
  static inline uint64_t EncodeDouble(k_uint8* valueptr) {
    // Convert the pointer to a double-precision floating-point number
    k_double64 x = *reinterpret_cast<k_double64*>(valueptr);

    uint64_t buff;
    //! zero
    if (x == 0) {
      buff = 0;
      buff += (1ULL << 63);
      return buff;
    }
    // nan
    if (std::isnan(x)) {
      return ULLONG_MAX;
    }
    //! infinity
    if (x > DBL_MAX) {
      return ULLONG_MAX - 1;
    }
    //! -infinity
    if (x < -DBL_MAX) {
      return 0;
    }

    // Copy the binary representation of the double-precision number into the buffer
    memcpy(&buff, valueptr, sizeof(uint64_t));
    if (buff < (1ULL << 63)) {
      // Set the sign bit to 1 to ensure positive numbers are ordered correctly
      buff += (1ULL << 63);
    } else {
      // Invert all bits of the buffer to ensure negative numbers are ordered correctly
      buff = ~buff;
    }
    return buff;
  }

  static inline double DecodeDouble(k_uint32 input) {
    // nan
    if (input == ULLONG_MAX) {
      return std::numeric_limits<double>::quiet_NaN();
    }
    if (input == ULLONG_MAX - 1) {
      return std::numeric_limits<double>::infinity();
    }
    if (input == 0) {
      return -std::numeric_limits<double>::infinity();
    }
    double result;
    if (input & (1ULL << 63)) {
      // positive numbers - flip sign bit
      input = input ^ (1ULL << 63);
    } else {
      // negative numbers - invert
      input = ~input;
    }
    memcpy(&result, &input, sizeof(double));
    return result;
  }
};

template <>
inline void Radix::EncodeData<bool>(k_uint8* dataptr, k_uint8* valueptr) {
  memcpy(dataptr, valueptr, sizeof(k_uint8));
}

template <>
inline void Radix::EncodeDataInvert<bool>(k_uint8* dataptr, k_uint8* valueptr) {
  StoreInvert(dataptr, valueptr, sizeof(k_uint8));
}

template <>
inline void Radix::DecodeData<bool>(k_uint8* dataptr, k_uint8* valueptr) {
  memcpy(dataptr, valueptr, sizeof(k_uint8));
}

template <>
inline void Radix::DecodeDataInvert<bool>(k_uint8* dataptr, k_uint8* valueptr) {
  StoreInvert(dataptr, valueptr, sizeof(k_uint8));
}

template <class T>
void Radix::EncodeSigned(k_uint8* dataptr, k_uint8* valueptr) {
  using UNSIGNED = typename std::make_unsigned<T>::type;
  UNSIGNED bytes;
  memcpy(reinterpret_cast<k_uint8*>(&bytes), valueptr, sizeof(T));
  bytes = BSwap(bytes);
  memcpy(dataptr, &bytes, sizeof(UNSIGNED));
  dataptr[0] = FlipSign(dataptr[0]);
}

template <class T>
void Radix::EncodeSignedInvert(k_uint8* dataptr, k_uint8* valueptr) {
  using UNSIGNED = typename std::make_unsigned<T>::type;
  UNSIGNED bytes;
  memcpy(reinterpret_cast<k_uint8*>(&bytes), valueptr, sizeof(T));
  bytes = BSwap(bytes);
  StoreInvert(dataptr, &bytes, sizeof(UNSIGNED));
  dataptr[0] = FlipSign(dataptr[0]);
}

template <class T>
void Radix::DecodeSigned(k_uint8* dataptr, k_uint8* valueptr) {
  valueptr[0] = FlipSign(valueptr[0]);
  using UNSIGNED = typename std::make_unsigned<T>::type;
  UNSIGNED bytes;
  memcpy(reinterpret_cast<k_uint8*>(&bytes), valueptr, sizeof(T));
  bytes = BSwap(bytes);
  memcpy(dataptr, &bytes, sizeof(UNSIGNED));
}

template <class T>
void Radix::DecodeSignedInvert(k_uint8* dataptr, k_uint8* valueptr) {
  valueptr[0] = FlipSign(valueptr[0]);
  using UNSIGNED = typename std::make_unsigned<T>::type;
  UNSIGNED bytes;
  StoreInvert(reinterpret_cast<k_uint8*>(&bytes), valueptr, sizeof(T));
  bytes = BSwap(bytes);
  memcpy(dataptr, &bytes, sizeof(UNSIGNED));
}

template <>
inline void Radix::EncodeData<k_int8>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSigned<int8_t>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeDataInvert<k_int8>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSignedInvert<k_int8>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeData<k_int8>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSigned<k_int8>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeDataInvert<k_int8>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSignedInvert<k_int8>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeData<k_int16>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSigned<k_int16>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeDataInvert<k_int16>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSignedInvert<k_int16>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeData<k_int16>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSigned<k_int16>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeDataInvert<k_int16>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSignedInvert<k_int16>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeData<k_int32>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSigned<int32_t>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeDataInvert<k_int32>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSignedInvert<int32_t>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeData<k_int32>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSigned<k_int32>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeDataInvert<k_int32>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSignedInvert<k_int32>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeData<k_int64>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSigned<k_int64>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeDataInvert<k_int64>(k_uint8* dataptr, k_uint8* valueptr) {
  EncodeSignedInvert<k_int64>(dataptr, valueptr);
}


template <>
inline void Radix::DecodeData<k_int64>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSigned<k_int64>(dataptr, valueptr);
}

template <>
inline void Radix::DecodeDataInvert<k_int64>(k_uint8* dataptr, k_uint8* valueptr) {
  DecodeSignedInvert<k_int64>(dataptr, valueptr);
}

template <>
inline void Radix::EncodeData<k_uint32>(k_uint8* dataptr, k_uint32* valueptr) {
  k_uint32 converted_value = BSwap(*valueptr);
  memcpy(dataptr, &converted_value, sizeof(k_uint32));
}

template <>
inline void Radix::EncodeDataInvert<k_uint32>(k_uint8* dataptr, k_uint32* valueptr) {
  k_uint32 converted_value = BSwap(*valueptr);
  StoreInvert(dataptr, &converted_value, sizeof(k_uint32));
}

template <>
inline void Radix::DecodeData<k_uint32>(k_uint8* dataptr, k_uint32* valueptr) {
  k_uint32 converted_value = BSwap(*valueptr);
  memcpy(dataptr, &converted_value, sizeof(k_uint32));
}

template <>
inline void Radix::DecodeDataInvert<k_uint32>(k_uint8* dataptr, k_uint32* valueptr) {
  k_uint32 converted_value = BSwap(*valueptr);
  StoreInvert(dataptr, &converted_value, sizeof(k_uint32));
}

template <>
inline void Radix::EncodeData<k_uint64>(k_uint8* dataptr, k_uint64* valueptr) {
  k_uint64 converted_value = BSwap(*valueptr);
  memcpy(dataptr, &converted_value, sizeof(k_uint64));
}

template <>
inline void Radix::EncodeDataInvert<k_uint64>(k_uint8* dataptr,
                                              k_uint64* valueptr) {
  k_uint64 converted_value = BSwap(*valueptr);
  StoreInvert(dataptr, reinterpret_cast<k_uint8*>(&converted_value),
              sizeof(k_uint64));
}

template <>
inline void Radix::DecodeData<k_uint64>(k_uint8* dataptr, k_uint64* valueptr) {
  uint64_t converted_value = BSwap(*valueptr);
  memcpy(dataptr, &converted_value, sizeof(k_uint64));
}

template <>
inline void Radix::DecodeDataInvert<k_uint64>(k_uint8* dataptr,
                                              k_uint64* valueptr) {
  k_uint64 converted_value = BSwap(*valueptr);
  StoreInvert(dataptr, reinterpret_cast<k_uint8*>(&converted_value),
              sizeof(k_uint64));
}
template <>
inline void Radix::EncodeData<k_float32>(k_uint8* dataptr, k_uint8* valueptr) {
  k_uint32 converted_value = EncodeFloat(valueptr);
  converted_value = BSwap(converted_value);
  memcpy(dataptr, &converted_value, sizeof(k_uint32));
}

template <>
inline void Radix::EncodeDataInvert<k_float32>(k_uint8* dataptr, k_uint8* valueptr) {
  k_uint32 converted_value = EncodeFloat(valueptr);
  converted_value = BSwap(converted_value);
  StoreInvert(dataptr, &converted_value, sizeof(k_uint32));
}
template <>
inline void Radix::DecodeData<k_float32>(k_uint8* dataptr, k_uint8* valueptr) {
  float converted_value =
      DecodeFloat(BSwap(*reinterpret_cast<k_uint32*>(valueptr)));
  memcpy(dataptr, &converted_value, sizeof(float));
}

template <>
inline void Radix::DecodeDataInvert<k_float32>(k_uint8* dataptr, k_uint8* valueptr) {
  k_uint32 temp_value;
  StoreInvert(reinterpret_cast<k_uint8*>(&temp_value), valueptr, sizeof(k_uint32));
  float converted_value =
      DecodeFloat(BSwap(temp_value));
  memcpy(dataptr, &converted_value, sizeof(float));
}

template <>
inline void Radix::EncodeData<k_float64>(k_uint8* dataptr, k_uint8* valueptr) {
  uint64_t converted_value = EncodeDouble(valueptr);
  converted_value = BSwap(converted_value);
  memcpy(dataptr, &converted_value, sizeof(uint64_t));
}

template <>
inline void Radix::EncodeDataInvert<k_float64>(k_uint8* dataptr, k_uint8* valueptr) {
  uint64_t converted_value = EncodeDouble(valueptr);
  converted_value = BSwap(converted_value);
  StoreInvert(dataptr, &converted_value, sizeof(uint64_t));
}

template <>
inline void Radix::DecodeData<k_float64>(k_uint8* dataptr, k_uint8* valueptr) {
  float converted_value =
      DecodeDouble(BSwap(*reinterpret_cast<uint64_t*>(valueptr)));
  memcpy(dataptr, &converted_value, sizeof(double));
}

template <>
inline void Radix::DecodeDataInvert<k_float64>(k_uint8* dataptr, k_uint8* valueptr) {
  k_uint64 temp_value;
  StoreInvert(reinterpret_cast<k_uint8*>(&temp_value), valueptr, sizeof(k_uint32));
  float converted_value =
      DecodeDouble(BSwap(temp_value));
  memcpy(dataptr, &converted_value, sizeof(double));
}

};  // namespace kwdbts
