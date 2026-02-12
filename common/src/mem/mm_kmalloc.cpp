// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#include "mm_kmalloc.h"

#include <sys/shm.h>
#include <cmath>
#include <malloc.h>

std::atomic<std::size_t> g_malloc_memory_size = 0; //  The requested memory size

void *k_malloc(std::size_t size) noexcept {
  void *p = malloc(size);
  g_malloc_memory_size.fetch_add(malloc_usable_size(p));
  return p;
}

void *k_realloc(void *ptr, std::size_t size) noexcept {
  g_malloc_memory_size.fetch_sub(malloc_usable_size(ptr));
  void *p = realloc(ptr, size);
  g_malloc_memory_size.fetch_add(malloc_usable_size(p));
  return p;
}

void k_free(void *ptr) noexcept {
  g_malloc_memory_size.fetch_sub(malloc_usable_size(ptr));
  free(ptr);
}

std::size_t malloc_used_bytes() noexcept {
    return g_malloc_memory_size;
}
