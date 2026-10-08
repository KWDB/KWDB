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

#ifndef KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_UTILS_H_
#define KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_UTILS_H_

#include <cstdint>
#include <vector>
#include <cstring>
#include <unordered_set>
#include <string>
#include <utility>
#include <variant>
#include "kwdb_type.h"
#define byte uint8_t

namespace kwdbts {

using DataVariant = std::variant<k_int64, k_double64, std::string>;

enum ValuesEncoding {
  encodedNull = 0x00,
  // A marker greater than NULL but lower than any other value.
  // This value is not actually ever present in a stored key, but
  // it's used in keys used as span boundaries for index scans.
  encodedNotNull = 0x01,

  floatNaN = encodedNotNull + 1,
  floatNeg = floatNaN + 1,
  floatZero = floatNeg + 1,
  floatPos = floatZero + 1,
  floatNaNDesc = floatPos + 1,  // NaN encoded descendingly

  bytesPrefix = 0x12,
};

const uint8_t escape = 0x00;
const uint8_t escapedTerm = 0x01;
const uint8_t escaped00 = 0xFF;

/*
 * @Description : Stores a 64-bit unsigned integer (uint64_t) into a vector of bytes in little-endian order.
 *                Little-endian order means the least significant byte is stored at the smallest address (beginning of the vector).
 *                This function ensures that the byte vector has exactly 8 bytes allocated to store the 64-bit integer.
 * @IN buf : Pointer to a vector of bytes where the 64-bit integer will be stored.
 * @IN val : The 64-bit unsigned integer value to be stored in the vector.
 * @OUT buf : The vector is modified to contain the bytes of the 64-bit integer in little-endian order.
 * @Return : Returns true to indicate successful storage of the value.
 * @Example : For a uint64_t value with hexadecimal representation 0x123456789ABCDEF0,
 *            the resulting byte vector will be [0xF0, 0xDE, 0xBC, 0x9A, 0x78, 0x56, 0x34, 0x12].
 */
bool PutUint64LittleEndian(std::vector<byte>* buf, uint64_t val);

/*
 * @Description : Computes a 64-bit hash from a buffer of bytes using custom hash constants and a seed value. This function uses
 *                a complex mixing of inputs with bit manipulation techniques such as rotation to ensure a well-distributed hash value.
 *                The hashing process handles blocks of data in sizes down to a single byte to ensure all data contributes to the final hash,
 *                making it suitable for applications requiring low collision rates such as hash tables or data checksums.
 * @IN buffer : The vector of bytes that contains the data to be hashed.
 * @IN seed : An initial seed value used to start the hash computation, providing the ability to alter the hash outcome with the same input data.
 * @OUT : None. The function operates purely with returns and does not modify external state.
 * @Return : Returns a 64-bit unsigned integer that represents the hash value of the input data.
 * @Detail :
 *    - The hash function initializes with a hash seed altered by constants to prevent patterns when the seed is zero.
 *    - It processes the data in chunks of 32 bytes, 16 bytes, 8 bytes, 4 bytes, 2 bytes, and finally 1 byte, ensuring each part of the data
 *      influences the final output.
 *    - Each step uses different constants (k0 to k3) to mix the bits of the data blocks. These constants help achieve avalanche effect where a
 *      small change in input produces a significant change in output.
 *    - Rotations and xor operations are used heavily to ensure that all bits of the data can affect the output.
 *    - After processing all blocks, a final mixing operation alters the hash further to finalize the hash value, enhancing its randomness.
 */
uint64_t Hash64(const std::vector<uint8_t>& buffer, uint64_t seed);


/*
 * @Description : Calculates and returns the bias correction factor 'alpha' used in algorithms like HyperLogLog.
 *                The function provides values of alpha for different sizes of 'm', which is the number of registers
 *                or substreams used in the estimation algorithm. The alpha values are critical for reducing bias in
 *                the logarithmic counting approach and are derived from empirical analysis specific to certain
 *                register sizes. For register sizes not directly specified, an interpolated formula is used.
 * @IN m : The number of registers (or substreams) used in the HyperLogLog algorithm or similar counting methods.
 *         It's a measure of the granularity of the counting structure, influencing both the accuracy and memory usage.
 * @OUT : None. This is a pure function that does not modify any state.
 * @Return : Returns the bias correction factor 'alpha' as a double. The value depends on the parameter 'm' and
 *           is chosen to minimize the standard error in probabilistic counting algorithms.
 * @Detail :
 *    - For small, predefined sizes of m (16, 32, and 64), specific alpha values are returned:
 *        m = 16.0 -> alpha = 0.673
 *        m = 32.0 -> alpha = 0.697
 *        m = 64.0 -> alpha = 0.709
 *    - For other values of m, alpha is calculated using the formula:
 *        alpha = 0.7213 / (1 + 1.079 / m)
 *      This formula is derived empirically to approximate the optimal alpha across a range of m values not directly specified.
 */
double alpha(double m);

// Encode a hash to be used in the sparse representation.
uint32_t encodeHash(uint64_t x, uint8_t p, uint8_t pp);

/*
 * @Description : Serializes a set of unsigned 32-bit integers into a byte array in big-endian format.
 *                This function is designed to convert a set into a binary representation, which can be used for storage or transmission.
 *                It first encodes the size of the set followed by each integer in the set, ensuring that the data can be accurately reconstructed later.
 * @IN s : The unordered set of uint32_t integers to be serialized.
 * @OUT data : A vector of bytes that represents the serialized form of the set.
 * @Return : A vector of bytes containing the serialized set. The format starts with 4 bytes representing the number of elements in the set
 *           followed by 4 bytes for each integer element.
 * @Detail :
 *    - The function begins by reserving enough space in the vector to hold all the data to prevent multiple reallocations.
 *      The required space is calculated as 4 bytes for the size of the set plus 4 bytes for each element.
 *    - It then encodes the size of the set as a 32-bit integer in big-endian order to ensure compatibility across different systems which might read the data.
 *    - Following the size, each element of the set is encoded in big-endian order. This order is chosen because it is commonly used in network protocols
 *      and ensures that the most significant byte of the number is stored first, which can be beneficial for systems that read data byte by byte.
 */
std::vector<uint8_t> marshalBinary_Set(const std::unordered_set<uint32_t>& s);

/*
 * @Description : Serializes a vector of bytes into a byte array with a prefix indicating the size of the vector.
 *                The function starts by encoding the length of the vector as a 32-bit big-endian integer, followed by the vector's content.
 *                This format is particularly useful for binary serialization where the size of the data needs to be known before processing,
 *                ensuring the entire list can be accurately reconstructed later.
 * @IN list : The vector of bytes to be serialized. This can be any binary data or previously encoded information.
 * @OUT data : A vector of bytes that begins with the size of the input vector followed by its contents.
 * @Return : A vector of bytes containing the serialized form of the input vector. The first four bytes represent the size of the vector,
 *           allowing receivers of this data to know how many bytes to read following the size indicator.
 * @Detail :
 *    - The function first reserves enough space in the resulting vector to hold the size indicator plus the content of the input vector.
 *      This is done to optimize memory allocation and improve performance by avoiding multiple reallocations.
 *    - It encodes the size of the list as a 32-bit integer in big-endian order, which is a standard practice in network communications and
 *      binary file formats because it ensures compatibility across different systems which might read the data.
 *    - After the size, the entire content of the input vector is appended directly to the output vector. This part of the process
 *      is straightforward as it involves a simple copy operation.
 */
std::vector<uint8_t> marshalBinary_Byte(const std::vector<uint8_t>& list);

/*
 * @Description : Extracts a specific bit field from a 64-bit integer. This function shifts the number right by the start position
 *              and then masks it with a bit length specified by 'len' to isolate the desired bits.
 *
 * @IN x : The 64-bit integer from which to extract the bit field.
 * @IN start : The zero-based position of the first bit in the bit field to extract.
 * @IN len : The number of bits in the bit field to extract.
 * @Return : a specific bit field from a 64-bit integer
 */
uint64_t bextr(uint64_t x, byte start, byte len);

// Used to shift the binary representation of a value to the right by a specified number of bits,
// while relocating the overflowed bits on the right to the left of the value.
// This operation is very common in many cryptographic algorithms and hash functions.
uint64_t rotate_right(uint64_t v, unsigned int k);

// Used to extract a value of type uint64_t from a byte vector of type uint8_t.
uint64_t uint64_from_bytes(const std::vector<byte>& buffer, size_t offset);

// Used to extract a value of type uint32_t from a byte vector of type byte.
uint32_t uint32_from_bytes(const std::vector<byte>& buffer, size_t offset);

// Used to extract a value of type uint16_t from a byte vector of type uint8_t.
uint16_t uint16_from_bytes(const std::vector<byte>& buffer, size_t offset);

uint32_t bextr32(uint32_t v, uint8_t start, uint8_t length);

// Gets the number of the bucket accord to the hash value
uint32_t getIndex(uint32_t k, uint8_t p, uint8_t pp);

// Decode a specially encoded hash value
std::pair<uint32_t, uint8_t> decodeHash(uint32_t k, uint8_t p, uint8_t pp);

// Gets the bucket index and maximum leading zero(key point)
std::pair<uint64_t, uint8_t> getPosVal(uint64_t x, uint8_t p);

// EncodeVarintAscending encodes the int64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte. If the value to be encoded is negative the length is encoded
// as 8-numBytes. If the value is positive it is encoded as
// 8+numBytes. The encoded bytes are appended to the supplied buffer
// and the final buffer is returned.
std::vector<byte> EncodeVarintAscending(std::vector<uint8_t>& b, int64_t v);

// EncodeUvarintAscending encodes the uint64 value using a variable length
// (length-prefixed) representation. The length is encoded as a single
// byte indicating the number of encoded bytes (-8) to follow. See
// EncodeVarintAscending for rationale. The encoded bytes are appended to the
// supplied buffer and the final buffer is returned.
std::vector<byte> EncodeUvarintAscending(std::vector<uint8_t>& b, uint64_t v);

// EncodeFloatAscending returns the resulting byte slice with the encoded float64 appended to buf
std::vector<byte> EncodeFloatAscending(std::vector<uint8_t>& b, k_float64 f);

// EncodeStringAscending encodes the string value using an escape-based encoding.
std::vector<uint8_t> EncodeStringAscending(std::vector<uint8_t>& b, const std::string& s);
}  // namespace kwdbts

#endif  // KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_TS_UTILS_H_
