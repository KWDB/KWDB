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
// Created by jerry on 2022/5/13.
//

#ifndef COMMON_SRC_INCLUDE_KWDB_COMPATIBLE_H_
#define COMMON_SRC_INCLUDE_KWDB_COMPATIBLE_H_

#ifdef OS_LINUX
#include <sys/syscall.h>

#if __GLIBC__ == 2 && __GLIBC_MINOR__ < 30
#define gettid() syscall(SYS_gettid)
#endif

#endif  // OS_LINUX

#ifdef OS_WIN32
// Prevent Windows 'byte' typedef from conflicting with C++17 std::byte
#ifndef _CRT_NO_BYTES
#define _CRT_NO_BYTES
#endif
#ifndef _NO_BYTES
#define _NO_BYTES
#endif
// Temporarily rename Windows 'byte' typedef to avoid conflict with std::byte
#define byte rpc_byte
#include <windows.h>
#include <process.h>
#include <sys/types.h>
#include <sys/time.h>
#include <time.h>
#include <string.h>
#include <stdio.h>
#undef byte
#define getpid() _getpid()
#ifndef gettid
#define gettid() ((pid_t)GetCurrentThreadId())
#endif

// Check if timeval is already defined (e.g., by winsock)
#ifndef _TIMEVAL_DEFINED
struct timeval {
    long tv_sec;   // seconds
    long tv_usec;  // microseconds
};
#define _TIMEVAL_DEFINED
#endif

// Windows implementation of gettimeofday
// Only define if not already provided by MinGW
#ifndef HAVE_GETTIMEOFDAY
// MinGW provides gettimeofday in sys/time.h, so we don't define it
// If you need a custom implementation, uncomment below
/*
static inline int gettimeofday(struct timeval *tv, void *tz) {
    (void)tz; // timezone not used
    FILETIME ft;
    ULARGE_INTEGER uli;
    GetSystemTimeAsFileTime(&ft);
    uli.LowPart = ft.dwLowDateTime;
    uli.HighPart = ft.dwHighDateTime;
    // Convert from 100-nanosecond intervals since 1601-01-01 UTC
    // to seconds since 1970-01-01 UTC
    uli.QuadPart -= 116444736000000000ULL; // difference between 1601 and 1970
    tv->tv_sec = (long)(uli.QuadPart / 10000000ULL);
    tv->tv_usec = (long)((uli.QuadPart % 10000000ULL) / 10ULL);
    return 0;
}
*/
#endif

// Windows doesn't have strsignal, provide a simple implementation
static inline const char* strsignal(int sig) {
    static char buf[64];
    snprintf(buf, sizeof(buf), "Signal %d", sig);
    return buf;
}

// Windows doesn't have setenv, provide compatible implementation
#if !defined(setenv)
static inline int setenv(const char* name, const char* value, int overwrite) {
    if (!overwrite && getenv(name)) {
        return 0;
    }
    char buf[1024];
    snprintf(buf, sizeof(buf), "%s=%s", name, value);
    return _putenv(buf) == 0 ? 0 : -1;
}
#endif

// Windows doesn't have usleep, but MinGW provides it
// Using MinGW's implementation which uses Sleep internally
// No need to redefine

// Windows uses MinGW's sleep function (returns unsigned int)
// No need to redefine

// Windows doesn't have gmtime_r/localtime_r, use gmtime_s/localtime_s
#define gmtime_r(tp, result) gmtime_s(result, tp)
#define localtime_r(tp, result) localtime_s(result, tp)

// Windows socket compatibility
#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

// Windows already has close() from unistd.h (provided by MinGW)

// Windows doesn't have strptime, provide a simplified implementation
#ifndef HAVE_STRPTIME
static inline char* strptime(const char* buf, const char* fmt, struct tm* tm) {
    // This is a very simplified implementation that only handles common formats
    // For production use, a full implementation should be used
    // IMPORTANT: Returns pointer to the FIRST character NOT parsed, so callers
    // can detect trailing content like sub-second fractions (.123).
    memset(tm, 0, sizeof(struct tm));

    // Try to parse common formats
    if (strcmp(fmt, "%Y-%m-%d %H:%M:%S") == 0) {
        int y, m, d, H, M, S;
        int consumed = 0;
        if (sscanf(buf, "%d-%d-%d %d:%d:%d%n",
                   &y, &m, &d, &H, &M, &S, &consumed) >= 6) {
            tm->tm_year = y - 1900;
            tm->tm_mon = m - 1;
            tm->tm_mday = d;
            tm->tm_hour = H;
            tm->tm_min = M;
            tm->tm_sec = S;
            return (char*)(buf + consumed);
        }
    } else if (strcmp(fmt, "%Y-%m-%dT%H:%M:%S") == 0) {
        int y, m, d, H, M, S;
        int consumed = 0;
        if (sscanf(buf, "%d-%d-%dT%d:%d:%d%n",
                   &y, &m, &d, &H, &M, &S, &consumed) >= 6) {
            tm->tm_year = y - 1900;
            tm->tm_mon = m - 1;
            tm->tm_mday = d;
            tm->tm_hour = H;
            tm->tm_min = M;
            tm->tm_sec = S;
            return (char*)(buf + consumed);
        }
    } else if (strcmp(fmt, "%Y-%m-%d") == 0) {
        int y, m, d;
        int consumed = 0;
        if (sscanf(buf, "%d-%d-%d%n",
                   &y, &m, &d, &consumed) >= 3) {
            tm->tm_year = y - 1900;
            tm->tm_mon = m - 1;
            tm->tm_mday = d;
            return (char*)(buf + consumed);
        }
    } else if (strcmp(fmt, "%H:%M:%S") == 0) {
        int H, M, S;
        int consumed = 0;
        if (sscanf(buf, "%d:%d:%d%n",
                   &H, &M, &S, &consumed) >= 3) {
            tm->tm_hour = H;
            tm->tm_min = M;
            tm->tm_sec = S;
            return (char*)(buf + consumed);
        }
    } else if (strcmp(fmt, "%Y/%m/%d-%H:%M:%S") == 0) {
        int y, m, d, H, M, S;
        int consumed = 0;
        if (sscanf(buf, "%d/%d/%d-%d:%d:%d%n",
                   &y, &m, &d, &H, &M, &S, &consumed) >= 6) {
            tm->tm_year = y - 1900;
            tm->tm_mon = m - 1;
            tm->tm_mday = d;
            tm->tm_hour = H;
            tm->tm_min = M;
            tm->tm_sec = S;
            return (char*)(buf + consumed);
        }
    }

    // For other formats, return NULL to indicate parsing failure
    return NULL;
}
#endif

// Windows doesn't have timegm, use _mkgmtime
#ifndef HAVE_TIMEGM
static inline time_t timegm(struct tm* tm) {
    // _mkgmtime is the Windows equivalent of timegm
    return _mkgmtime(tm);
}
#endif

// Windows doesn't have endian.h, provide byte order conversion functions
#ifndef be32toh
#ifdef _WIN32
// Use compiler builtins or Windows functions
#include <stdlib.h>  // for _byteswap_ulong
#define be32toh(x) _byteswap_ulong(x)
#define htobe32(x) _byteswap_ulong(x)
#define le32toh(x) (x)
#define htole32(x) (x)
#define be16toh(x) _byteswap_ushort(x)
#define htobe16(x) _byteswap_ushort(x)
#define le16toh(x) (x)
#define htole16(x) (x)
#define be64toh(x) _byteswap_uint64(x)
#define htobe64(x) _byteswap_uint64(x)
#define le64toh(x) (x)
#define htole64(x) (x)
#else
// Use system endian.h
#include <endian.h>
#endif
#endif

#endif  // OS_WIN32

#endif  // COMMON_SRC_INCLUDE_KWDB_COMPATIBLE_H_
