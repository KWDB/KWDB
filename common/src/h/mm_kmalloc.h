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

#ifndef COMMON_SRC_H_MM_KMALLOC_H_
#define COMMON_SRC_H_MM_KMALLOC_H_

#include <sys/types.h>  // for pid_t
#include <atomic>
#include "kwdb_type.h"

#ifdef __cplusplus
extern "C" {
#endif

kwdbts::k_size_t malloc_used_bytes() noexcept;

void *k_malloc(kwdbts::k_size_t size) noexcept;
void *k_realloc(void *ptr, kwdbts::k_size_t size) noexcept;
void k_free(void *ptr) noexcept;

#ifdef __cplusplus
}
#endif

extern std::atomic<kwdbts::k_size_t> g_malloc_memory_size;

#endif  // COMMON_SRC_H_MM_KMALLOC_H_
