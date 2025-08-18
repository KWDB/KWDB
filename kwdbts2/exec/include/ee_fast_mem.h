// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#pragma once
#ifdef __SSE2__
#include <emmintrin.h>
#include <immintrin.h>
#endif

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <algorithm>

namespace kwdbts {
constexpr std::size_t kAvx2VectorSize = 32;
#define ALWAYS_INLINE __attribute__((always_inline))

ALWAYS_INLINE inline int memcmp_inlined(const void* __restrict _lhs, const void* __restrict _rhs, size_t size) {
  const auto lhs = static_cast<const uint8_t*>(_lhs);
  const auto rhs = static_cast<const uint8_t*>(_rhs);

  if (size == 0) return 0;  // Empty memory blocks are considered equal

[[maybe_unused]] tail:
  if (size <= 16) {
    // Small memory blocks: compare in granular chunks using built-in functions
    if (size >= 8) {
      // Compare upper 8 bytes first, then lower 8 bytes (covers 8-16 bytes)
      int cmp = __builtin_memcmp(lhs + size - 8, rhs + size - 8, 8);
      if (cmp != 0) return cmp;
      return __builtin_memcmp(lhs, rhs, 8);
    } else if (size >= 4) {
      // 4-7 bytes: upper 4 bytes + lower 4 bytes
      int cmp = __builtin_memcmp(lhs + size - 4, rhs + size - 4, 4);
      if (cmp != 0) return cmp;
      return __builtin_memcmp(lhs, rhs, 4);
    } else if (size >= 2) {
      // 2-3 bytes: upper 2 bytes + lower 2 bytes
      int cmp = __builtin_memcmp(lhs + size - 2, rhs + size - 2, 2);
      if (cmp != 0) return cmp;
      return __builtin_memcmp(lhs, rhs, 2);
    } else {
      // 1 byte: direct comparison
      return lhs[0] - rhs[0];
    }
  } else {
#ifdef __AVX2__
    if (size <= 256) {
      // Medium size (17-256 bytes): use AVX2 32-byte vector comparison
      if (size <= 32) {
        // Within 32 bytes: compare 8 bytes first, remaining via tail logic
        int cmp = __builtin_memcmp(lhs, rhs, 8);
        if (cmp != 0) return cmp;
        size -= 8;
        lhs += 8;
        rhs += 8;
        goto tail;
      }

      // Larger than 32 bytes: loop through 32-byte blocks
      while (size > 32) {
        // Compare 32-byte vectors
        __m256i vec_lhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs));
        __m256i vec_rhs = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs));
        __m256i eq_mask = _mm256_cmpeq_epi8(vec_lhs, vec_rhs);  // Mask with 0xFF for equal bytes

        // Check if all bytes are equal (mask is all 0xFF)
        if (_mm256_movemask_epi8(eq_mask) != 0xFFFFFFFF) {
          // Mismatch found: find first differing byte
          for (int i = 0; i < 32; ++i) {
            if (lhs[i] != rhs[i]) {
              return lhs[i] - rhs[i];
            }
          }
        }

        lhs += 32;
        rhs += 32;
        size -= 32;
      }

      // Process remaining <=32 bytes
      goto tail;
    } else {
      // Large blocks (>256 bytes): optimize alignment and batch comparison
      static constexpr size_t KB = 1024;
      if (size >= 512 * KB && size <= 2048 * KB) {
        // Use rep cmpsb for large blocks (x86 enhanced repeat comparison)
        int result;
        asm volatile(
            "rep cmpsb\n"        // Compare bytes repeatedly
            "setne %%al\n"       // Set al=1 if not equal
            "sub $1, %%al\n"     // Convert to -1 (unequal) or 0 (equal)
            "movsbl %%al, %0\n"  // Extend to int
            : "=r"(result), "+D"(lhs), "+S"(rhs), "+c"(size)
            :
            : "memory", "al");
        return result;
      } else {
        // Alignment handling: ensure subsequent comparisons are 32-byte aligned
        size_t padding = (32 - (reinterpret_cast<size_t>(lhs) & 31)) & 31;
        if (padding > 0 && padding <= size) {
          // Compare pre-alignment segment first
          int cmp = __builtin_memcmp(lhs, rhs, padding);
          if (cmp != 0) return cmp;
          lhs += padding;
          rhs += padding;
          size -= padding;
        }

        // Batch compare 256-byte blocks (8x 32-byte vectors)
        while (size >= 256) {
          __m256i vec0 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs)));
          __m256i vec1 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 32)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 32)));
          __m256i vec2 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 64)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 64)));
          __m256i vec3 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 96)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 96)));
          __m256i vec4 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 128)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 128)));
          __m256i vec5 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 160)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 160)));
          __m256i vec6 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 192)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 192)));
          __m256i vec7 = _mm256_cmpeq_epi8(_mm256_loadu_si256(reinterpret_cast<const __m256i*>(lhs + 224)),
                                           _mm256_loadu_si256(reinterpret_cast<const __m256i*>(rhs + 224)));

          // Check if all vectors are completely equal
          if (_mm256_movemask_epi8(vec0) != 0xFFFFFFFF || _mm256_movemask_epi8(vec1) != 0xFFFFFFFF ||
              _mm256_movemask_epi8(vec2) != 0xFFFFFFFF || _mm256_movemask_epi8(vec3) != 0xFFFFFFFF ||
              _mm256_movemask_epi8(vec4) != 0xFFFFFFFF || _mm256_movemask_epi8(vec5) != 0xFFFFFFFF ||
              _mm256_movemask_epi8(vec6) != 0xFFFFFFFF || _mm256_movemask_epi8(vec7) != 0xFFFFFFFF) {
            // Mismatch found: find first differing byte
            for (int i = 0; i < 256; ++i) {
              if (lhs[i] != rhs[i]) {
                return lhs[i] - rhs[i];
              }
            }
          }

          lhs += 256;
          rhs += 256;
          size -= 256;
        }

        // Process remaining bytes
        goto tail;
      }
    }
#else
    // No AVX2 support: fall back to standard library implementation
    return std::memcmp(lhs, rhs, size);
#endif
  }
}

ALWAYS_INLINE inline void memcpy_inlined(void* __restrict _dst, const void* __restrict _src, size_t size) {
  auto dst = static_cast<uint8_t*>(_dst);
  auto src = static_cast<const uint8_t*>(_src);

[[maybe_unused]] tail:
  if (size <= 16) {
    if (size >= 8) {
      __builtin_memcpy(dst + size - 8, src + size - 8, 8);
      __builtin_memcpy(dst, src, 8);
    } else if (size >= 4) {
      __builtin_memcpy(dst + size - 4, src + size - 4, 4);
      __builtin_memcpy(dst, src, 4);
    } else if (size >= 2) {
      __builtin_memcpy(dst + size - 2, src + size - 2, 2);
      __builtin_memcpy(dst, src, 2);
    } else if (size >= 1) {
      *dst = *src;
    }
  } else {
#ifdef __AVX2__
    if (size <= 256) {
      if (size <= 32) {
        __builtin_memcpy(dst, src, 8);
        __builtin_memcpy(dst + 8, src + 8, 8);
        size -= 16;
        dst += 16;
        src += 16;
        goto tail;
      }

      while (size > 32) {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)));
        dst += 32;
        src += 32;
        size -= 32;
      }

      _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst + size - 32),
                          _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + size - 32)));
    } else {
      static constexpr size_t KB = 1024;
      if (size >= 512 * KB && size <= 2048 * KB) {
        // erms(enhanced repeat movsv/stosb) version works well in this region.
        asm volatile("rep movsb" : "=D"(dst), "=S"(src), "=c"(size) : "0"(dst), "1"(src), "2"(size) : "memory");
      } else {
        size_t padding = (32 - (reinterpret_cast<size_t>(dst) & 31)) & 31;

        if (padding > 0) {
          __m256i head = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
          _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst), head);
          dst += padding;
          src += padding;
          size -= padding;
        }

        while (size >= 256) {
          __m256i c0 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src));
          __m256i c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize));
          __m256i c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 2));
          __m256i c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 3));
          __m256i c4 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 4));
          __m256i c5 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 5));
          __m256i c6 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 6));
          __m256i c7 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + kAvx2VectorSize * 7));
          src += 256;

          _mm256_store_si256((reinterpret_cast<__m256i*>(dst)), c0);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize)), c1);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 2)), c2);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 3)), c3);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 4)), c4);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 5)), c5);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 6)), c6);
          _mm256_store_si256((reinterpret_cast<__m256i*>(dst + kAvx2VectorSize * 7)), c7);
          dst += 256;

          size -= 256;
        }

        goto tail;
      }
    }
#else
    std::memcpy(dst, src, size);
#endif
  }
}

// Fill a memory block with a repeated byte value
ALWAYS_INLINE inline void memset_inlined(void* __restrict dst, uint8_t value, size_t size) {
  auto* dest = static_cast<uint8_t*>(dst);

  if (size == 0) return;  // Nothing to fill

[[maybe_unused]] tail:
  if (size <= 16) {
    // Small blocks: unroll byte-wise operations
    for (size_t i = 0; i < size; ++i) {
      dest[i] = value;
    }
  } else {
#ifdef __AVX2__
    // Create vector with repeated byte value
    const __m256i fill_vec = _mm256_set1_epi8(static_cast<int8_t>(value));

    if (size <= 32) {
      // Handle 17-32 bytes with single vector store
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest), fill_vec);
    } else {
      // Process 32-byte chunks
      size_t bulk_size = size & ~31;  // Round down to multiple of 32
      for (size_t i = 0; i < bulk_size; i += 32) {
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(dest + i), fill_vec);
      }

      // Handle remaining bytes
      size %= 32;
      if (size > 0) {
        dest += bulk_size;
        goto tail;
      }
    }
#else
    // Fallback to standard implementation for non-AVX2
    std::memset(dest, value, size);
#endif
  }
}

// Swap two non-overlapping memory blocks of equal size
ALWAYS_INLINE inline void memswap_inlined(void* __restrict a, void* __restrict b, size_t size) {
  auto* ptr_a = static_cast<uint8_t*>(a);
  auto* ptr_b = static_cast<uint8_t*>(b);

  if (size == 0 || a == b) return;  // Nothing to swap or same memory

[[maybe_unused]] tail:
  if (size <= 16) {
    // Small blocks: swap bytes directly
    for (size_t i = 0; i < size; ++i) {
      const uint8_t temp = ptr_a[i];
      ptr_a[i] = ptr_b[i];
      ptr_b[i] = temp;
    }
  } else {
#ifdef __AVX2__
    if (size <= 32) {
      // Swap 17-32 bytes using vector registers
      const __m256i vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr_a));
      const __m256i vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr_b));
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(ptr_a), vec_b);
      _mm256_storeu_si256(reinterpret_cast<__m256i*>(ptr_b), vec_a);
    } else {
      // Process 32-byte chunks
      while (size > 32) {
        const __m256i vec_a = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr_a));
        const __m256i vec_b = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr_b));
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(ptr_a), vec_b);
        _mm256_storeu_si256(reinterpret_cast<__m256i*>(ptr_b), vec_a);

        ptr_a += 32;
        ptr_b += 32;
        size -= 32;
      }

      // Handle remaining bytes
      goto tail;
    }
#else
    // Fallback using temporary buffer for non-AVX2
    uint8_t temp[16];
    while (size > 0) {
      const size_t chunk = std::min(size, sizeof(temp));
      std::memcpy(temp, ptr_a, chunk);
      std::memcpy(ptr_a, ptr_b, chunk);
      std::memcpy(ptr_b, temp, chunk);
      ptr_a += chunk;
      ptr_b += chunk;
      size -= chunk;
    }
#endif
  }
}

// Find first occurrence of a byte in memory block
ALWAYS_INLINE inline const uint8_t* memchr_inlined(const void* __restrict src, uint8_t value, size_t size) {
  const auto* ptr = static_cast<const uint8_t*>(src);

  if (size == 0) return nullptr;  // Empty block can't contain value

[[maybe_unused]] tail:
  if (size <= 16) {
    // Small blocks: linear search with early termination
    for (size_t i = 0; i < size; ++i) {
      if (ptr[i] == value) {
        return &ptr[i];
      }
    }
    return nullptr;
  } else {
#ifdef __AVX2__
    // Create vector with target value (broadcast to all 32 bytes)
    const __m256i target = _mm256_set1_epi8(static_cast<int8_t>(value));

    if (size <= 32) {
      // Check 17-32 bytes with vector comparison
      const __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
      const __m256i eq_mask = _mm256_cmpeq_epi8(vec, target);
      const unsigned int mask = static_cast<unsigned int>(_mm256_movemask_epi8(eq_mask));

      if (mask != 0) {
        // Find the position of the first set bit using compiler intrinsics
#ifdef _MSC_VER
        uint32_t first_match;  // Stores the position of the first set bit
// Use appropriate bit scan function based on mask size
#if defined(_M_X64) && sizeof(mask) == 8
        _BitScanForward64(reinterpret_cast<uint32_t*>(&first_match), mask);
#else
        _BitScanForward(reinterpret_cast<uint32_t*>(&first_match), mask);
#endif
#else
        // Select corresponding built-in function based on mask width
        uint32_t first_match;
        if constexpr (sizeof(mask) == 8) {
          first_match = static_cast<uint32_t>(__builtin_ctzll(mask));
        } else {
          first_match = static_cast<uint32_t>(__builtin_ctz(mask));
        }
#endif

        // Safety check: ensure we don't exceed the original size boundary
        if (first_match < size) {
          return &ptr[first_match];
        }
      }
      return nullptr;
    } else {
      // Search 32-byte chunks
      while (size > 32) {
        const __m256i vec = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(ptr));
        const __m256i eq_mask = _mm256_cmpeq_epi8(vec, target);
        const uint32_t mask = static_cast<uint32_t>(_mm256_movemask_epi8(eq_mask));

        if (mask != 0) {
          // Find first set bit using compiler intrinsic
#ifdef _MSC_VER
          std::uint32_t bit_pos;
          _BitScanForward(reinterpret_cast<uint32_t*>(&bit_pos), mask);
          const std::uint32_t first_match = bit_pos;
#else
          const uint32_t first_match = __builtin_ctz(mask);
#endif
          return &ptr[first_match];
        }

        ptr += 32;
        size -= 32;
      }

      // Check remaining bytes (<=32)
      goto tail;
    }
#else
    // Fallback to standard implementation for non-AVX2 platforms
    return static_cast<const uint8_t*>(std::memchr(ptr, value, size));
#endif
  }
}

}  // namespace kwdbts
