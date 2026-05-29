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

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
// Prevent Windows 'byte' typedef from conflicting with C++17 std::byte
#ifndef _CRT_NO_BYTES
#define _CRT_NO_BYTES
#endif
#ifndef _NO_BYTES
#define _NO_BYTES
#endif
#ifndef byte
#define byte rpc_byte
#endif
#include <windows.h>
#undef byte
#include <io.h>
#include <direct.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <time.h>
#include <cstdint>

// Windows compatibility definitions
#ifndef PROT_READ
#define PROT_READ  0x01
#endif
#ifndef PROT_WRITE
#define PROT_WRITE 0x02
#endif
#ifndef MAP_SHARED
#define MAP_SHARED 0x01
#endif
#ifndef MAP_PRIVATE
#define MAP_PRIVATE 0x02
#endif
#ifndef MAP_ANONYMOUS
#define MAP_ANONYMOUS 0x20
#endif
#ifndef MAP_FAILED
#define MAP_FAILED ((void*)-1)
#endif
#ifndef MREMAP_MAYMOVE
#define MREMAP_MAYMOVE 1
#endif
#ifndef MS_SYNC
#define MS_SYNC 0x0004
#endif
#ifndef MS_ASYNC
#define MS_ASYNC 0x0001
#endif

// madvise constants
#define MADV_NORMAL 0
#define MADV_RANDOM 1
#define MADV_SEQUENTIAL 2
#define MADV_WILLNEED 3
#define MADV_DONTNEED 4

#define FALLOC_FL_KEEP_SIZE 0x01

// posix_fadvise flags (not available on Windows, define for compatibility)
#define POSIX_FADV_NORMAL     0
#define POSIX_FADV_RANDOM     1
#define POSIX_FADV_SEQUENTIAL 2
#define POSIX_FADV_WILLNEED   3
#define POSIX_FADV_DONTNEED   4
#define POSIX_FADV_NOREUSE    5

// Windows file open flags
#define O_RDONLY    _O_RDONLY
#define O_WRONLY    _O_WRONLY
#define O_RDWR      _O_RDWR
#define O_CREAT     _O_CREAT
#define O_EXCL      _O_EXCL
#define O_TRUNC     _O_TRUNC
#define O_APPEND    _O_APPEND
#define O_BINARY    _O_BINARY

// flock constants
#define LOCK_SH 1
#define LOCK_EX 2
#define LOCK_NB 4
#define LOCK_UN 8

// Windows file permissions (not used directly)
#ifndef S_IRUSR
#define S_IRUSR _S_IREAD
#endif
#ifndef S_IWUSR
#define S_IWUSR _S_IWRITE
#endif
#ifndef S_IXUSR
#define S_IXUSR 0
#endif
#ifndef S_IRGRP
#define S_IRGRP 0
#endif
#ifndef S_IWGRP
#define S_IWGRP 0
#endif
#ifndef S_IXGRP
#define S_IXGRP 0
#endif
#ifndef S_IROTH
#define S_IROTH 0
#endif
#ifndef S_IWOTH
#define S_IWOTH 0
#endif
#ifndef S_IXOTH
#define S_IXOTH 0
#endif

// Thread-safe pread/pwrite using OVERLAPPED (no seek, no race condition)
static inline ssize_t pread(int fd, void* buf, size_t count, off_t offset) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    OVERLAPPED ov = {};
    ov.Offset = (DWORD)((uint64_t)offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD)((uint64_t)offset >> 32);
    DWORD bytes_read = 0;
    if (!ReadFile(h, buf, (DWORD)count, &bytes_read, &ov)) {
        if (GetLastError() == ERROR_HANDLE_EOF) return 0;
        errno = EIO;
        return -1;
    }
    return (ssize_t)bytes_read;
}

static inline ssize_t pwrite(int fd, const void* buf, size_t count, off_t offset) {
    HANDLE h = (HANDLE)_get_osfhandle(fd);
    if (h == INVALID_HANDLE_VALUE) { errno = EBADF; return -1; }
    OVERLAPPED ov = {};
    ov.Offset = (DWORD)((uint64_t)offset & 0xFFFFFFFF);
    ov.OffsetHigh = (DWORD)((uint64_t)offset >> 32);
    DWORD bytes_written = 0;
    if (!WriteFile(h, buf, (DWORD)count, &bytes_written, &ov)) {
        errno = EIO;
        return -1;
    }
    return (ssize_t)bytes_written;
}

static inline int madvise(void* addr, size_t length, int advice) {
    (void)addr; (void)length; (void)advice;
    return 0;
}

static inline int posix_fadvise(int fd, off_t offset, off_t len, int advice) {
    (void)fd; (void)offset; (void)len; (void)advice;
    return 0;
}

// Windows compatibility functions (implemented in platform_win.cpp)
int fallocate(int fd, int mode, off_t offset, off_t len);
int mkstemp(char *template_name);
void *mmap(void *addr, size_t length, int prot, int flags, int fd, off_t offset);
int munmap(void *addr, size_t length);
void *mremap(void *old_address, size_t old_size, size_t new_size, int flags);
int msync(void *addr, size_t length, int flags);
int fsync(int fd);
int flock(int fd, int operation);
int symlink(const char* target, const char* linkpath);
int kwdb_rename(const char* oldpath, const char* newpath);

static inline int fdatasync(int fd) {
    return fsync(fd);
}

static inline int getpagesize() {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwPageSize;
}

static inline size_t get_allocation_granularity() {
    SYSTEM_INFO si;
    GetSystemInfo(&si);
    return si.dwAllocationGranularity;
}

// Windows tm doesn't have tm_gmtoff, provide helper to get GMT offset in seconds
static inline long kwdbts_get_gmtoff_sec() {
    TIME_ZONE_INFORMATION tzi;
    DWORD result = GetTimeZoneInformation(&tzi);
    long bias = tzi.Bias + tzi.StandardBias;
    if (result == TIME_ZONE_ID_DAYLIGHT) {
        bias += tzi.DaylightBias - tzi.StandardBias;
    }
    return -bias * 60;
}

// Windows doesn't have timegm(), use _mkgmtime() instead
static inline time_t kwdbts_timegm(struct tm *tm) {
    return _mkgmtime(tm);
}

// Windows doesn't have strptime(), provide a minimal implementation
static inline char *kwdbts_strptime(const char *buf, const char *fmt, struct tm *tm) {
    while (*fmt) {
        if (*fmt == '%') {
            fmt++;
            switch (*fmt) {
                case 'Y': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_year = val - 1900;
                    buf = end;
                    break;
                }
                case 'm': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_mon = val - 1;
                    buf = end;
                    break;
                }
                case 'd': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_mday = val;
                    buf = end;
                    break;
                }
                case 'H': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_hour = val;
                    buf = end;
                    break;
                }
                case 'M': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_min = val;
                    buf = end;
                    break;
                }
                case 'S': {
                    char *end;
                    long val = strtol(buf, &end, 10);
                    if (end == buf) return nullptr;
                    tm->tm_sec = val;
                    buf = end;
                    break;
                }
                default:
                    return nullptr;
            }
        } else {
            if (*buf != *fmt) return nullptr;
            buf++;
        }
        fmt++;
    }
    return const_cast<char *>(buf);
}

#else
// Linux includes
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <stdio.h>
#include <time.h>

static inline size_t get_allocation_granularity() {
    return getpagesize();  // On Linux, page size == allocation granularity
}

// On Linux, rename() atomically replaces the target, so just delegate
static inline int kwdb_rename(const char* oldpath, const char* newpath) {
    return rename(oldpath, newpath);
}

// Linux has timegm() and strptime() natively
static inline time_t kwdbts_timegm(struct tm *tm) {
    return timegm(tm);
}

static inline char *kwdbts_strptime(const char *buf, const char *fmt, struct tm *tm) {
    return strptime(buf, fmt, tm);
}
#endif

// packed struct macros
#if defined(_WIN32) && !defined(__GNUC__)
// MSVC
#define KWDB_PACKED_STRUCT(name) __pragma(pack(push, 1)) struct name
#define KWDB_PACKED_STRUCT_END   __pragma(pack(pop))
#else
// GCC / MinGW / Clang
#define KWDB_PACKED_STRUCT(name) struct __attribute__((packed)) name
#define KWDB_PACKED_STRUCT_END
#endif

// Common definitions
#ifdef _WIN32
#define PATH_SEPARATOR '\\'
#define PATH_SEPARATOR_STR "\\"
#else
#define PATH_SEPARATOR '/'
#define PATH_SEPARATOR_STR "/"
#endif

// Thread ID
#ifdef _WIN32
#include <windows.h>
#ifndef gettid
#define gettid() ((pid_t)GetCurrentThreadId())
#endif
#else
#include <sys/types.h>
#include <unistd.h>
#include <sys/syscall.h>
#define gettid() ((pid_t)syscall(SYS_gettid))
#endif
