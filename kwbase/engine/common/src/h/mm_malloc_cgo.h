// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.

#ifndef COMMON_SRC_H_MM_MALLOC_CGO_H_
#define COMMON_SRC_H_MM_MALLOC_CGO_H_

#ifdef OS_LINUX
#include <sys/types.h>
#endif

#ifdef OS_WIN32
#include <stddef.h>
#endif

size_t malloc_used_bytes();

#endif  // COMMON_SRC_H_MM_MALLOC_CGO_H_
