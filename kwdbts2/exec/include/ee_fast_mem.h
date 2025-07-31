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

namespace kwdbts {

  #define ALWAYS_INLINE __attribute__((always_inline))

  ALWAYS_INLINE inline void memcpy_inlined(void* __restrict _dst, const void* __restrict _src, size_t size) {
    auto dst = static_cast<uint8_t*>(_dst);
    auto src = static_cast<const uint8_t*>(_src);

    [[maybe_unused]] tail : if (size <= 16) {
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
                _mm256_storeu_si256(reinterpret_cast<__m256i*>(dst),
                                    _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src)));
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
                    __m256i c1 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 32));
                    __m256i c2 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 64));
                    __m256i c3 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 96));
                    __m256i c4 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 128));
                    __m256i c5 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 160));
                    __m256i c6 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 192));
                    __m256i c7 = _mm256_loadu_si256(reinterpret_cast<const __m256i*>(src + 224));
                    src += 256;

                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst)), c0);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 32)), c1);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 64)), c2);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 96)), c3);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 128)), c4);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 160)), c5);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 192)), c6);
                    _mm256_store_si256((reinterpret_cast<__m256i*>(dst + 224)), c7);
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
}  // namespace kwdbts
