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

#pragma once

#include <inttypes.h>
#include <vector>
#include <string>
#include <sstream>

using namespace std;

/******************************************************************************
 * Possible combination of data types:
 *
 * OS           | IDTYPE    | HASH  | PTRTYPE   | NS/BT/BO limit
 * -------------+-----------+-------+-----------+---------------------
 * x64 (BO64)   | 64        | 64    | 64        | 16EB / 16EB / 16EB
 * (unused)     | 32        | 64    | 64        | 4GB / 16EB / 16EB
 * (default)    | 32        | 32    | 64        | 4GB / 4GB / 16EB
 * (BO32)       | 32        | 32    | 32        | 4GB / 4GB / 4GB
 * -------------+-----------+-------+-----------+---------------------
 * x32          | 32        | 32    | 32        | 4GB / 4GB / 4GB
 *****************************************************************************/

#ifdef BO64
#define ID64
#define PTR64
#define HASH64
#endif

#ifdef BO32
#define ID32
#define HASH32
#define PTR32
#endif

typedef uint64_t ID64TYPE;

//#define ID64

#ifndef ID64
typedef uint32_t IDTYPE;
#else
typedef uint64_t IDTYPE;
#endif

#define PTR64
#ifndef PTR64
typedef uint32_t PTRTYPE;
#else
typedef intptr_t PTRTYPE;
#endif

#ifndef HASH64
#define HASH32
typedef uint32_t HASHTYPE;
#else
#undef HASH32
typedef uint64_t HASHTYPE;
#endif

/* Test for GCC < 4.6. However, gcc 4.4 or above is required */
#if __GNUC__ < 4 || (__GNUC__ == 4 && __GNUC_MINOR__ < 6)
#define nullptr		NULL
#endif

#if defined(ANDROID)
#if !defined(__ANDROID__)
#define __ANDROID__
#endif
#endif

//              fixup   INT64   DOUBLE  FLOAT
//---------------------------------------------
// Android      X       X       X       O
// raspberry    O       O       X       X
// arm-64       ?       ?       ?       O
// <= 4 bytes unaligned integers are assumed to be supported by hardware

#if defined(__x86_64__) || defined(_M_X64) || (__aarch64__)
#define __UNALIGNED_ACCESS__
#endif

// non-android (ubuntu) on 64-bit arm (fixup)
#if defined(__arm__)
#undef __UNALIGNED_ACCESS__
#endif

// No aligned access support(use memcpy)
//#define ALIGNED
#if defined(ALIGNED)
#undef __UNALIGNED_ACCESS__
#endif

#define DICTIONARY              0x00000000
#define DEFAULT_STRING_MAX_LEN      63
#define DEFAULT_CHAR_MAX_LEN        32
#define DEFAULT_VARSTRING_MAX_LEN   255
#define STRING_MAX_LEN              1023
#define CHAR_MAX_LEN                786432      // 768K
#define VARSTRING_MAX_LEN           786432      // 768K

#define MAX_ATTR_LEN                30
#define MAX_COLUMNATTR_LEN          63     // 30 + . + 30
#define MAX_DATABASE_NAME_LEN       63
#define MAX_USER_NAME_LEN           32
#define MAX_PASSWORD_LEN            63

#define MAX_INT8_STR_LEN        4
#define MAX_INT16_STR_LEN       6
#define MAX_INT32_STR_LEN       12
#define MAX_INT64_STR_LEN       21
#define MAX_TIME_STR_LEN        13
#define MAX_TIME64_STR_LEN      29  //time64 is time and millisecond, 2023-06-13 10:00:00.123456789

#define MAX_DOUBLE_STR_LEN      22
#define MAX_PRECISION           15

#define L_CHAR                  0
#define R_CHAR                  1

typedef unsigned char uchar;

/**
 * time type to be used in data type or time_unit.
 * @see DataType class
 */
enum TIMEUNITTYPE {
  DAILY, WEEKLY, BIWEEKLY, SEMIMONTHLY, MONTHLY, BIMONTHLY, QUARTERLY,
  SEMIANNUAL, ANNUAL
};


//////////////////////
// TsObject Data type
enum DATATYPE {
  NO_TYPE = -2,             // for create tree
  INVALID = -1,
  TIMESTAMP64 = 0,
  INT16 = 1,
  INT32 = 2,
  INT64 = 3,
  FLOAT = 4,
  DOUBLE = 5,
  BYTE = 6,
  CHAR = 7,
  BINARY = 8,
  VARSTRING = 10,
  VARBINARY = 11,
  STRING = 13,
  INT32_ARRAY = 14,
  INT64_ARRAY = 15,
  FLOAT_ARRAY = 16,
  DOUBLE_ARRAY = 17,
  DATE32 = 19,
  DATETIMEDOS = 20,         // DATE, TIME MS DOS format
  DATETIME64 = 21,
  BOOL = 24,
  TIMESTAMP = 26,           // 4 bytes time stamp up to 2038/01/19
  INT16_ARRAY = 27,
  TIME = 28,                // 4 bytes, curtime(), timediff()
  INT8 = 29,
  BYTE_ARRAY = 30,
  INT8_ARRAY = 34,
  DATETIME32 = 50,          // 32-bit date time.
  TIME64 = 58,              // 8 bytes integer time with microsecond
  TIMESTAMP64_MICRO = 59,
  TIMESTAMP64_NANO = 60,
  NULL_TYPE = 999,
  ROWID = 1000,			    /// ROWID item
  STRING_CONST = 20002,
};

struct LifeTime {
  int64_t ts;  // second
  int32_t precision;  // TIMESTAMP64, TIMESTAMP64_MICRO, TIMESTAMP64_NANO and LSN type
};

typedef uint32_t    timestamp;
typedef int64_t    timestamp64;
typedef uint64_t    TS_LSN;

inline bool isBinaryType(int type) { return (type == BINARY || type == VARBINARY); }

inline bool isVarLenType(int type) { return ((type == VARSTRING) ||  (type == VARBINARY)); }

#define AINFO_INTERNAL              0x00000001  // internal column
#define AINFO_HAS_DEFAULT           0x00000002  // has default value set
#define AINFO_NOT_NULL              0x00000004  // NOT NULL
#define AINFO_DROPPED               0x00000008  // column has been dropped
#define AINFO_EXTERNAL              0x00000010  // external VARSTRING,...,etc.
#define AINFO_GEOHASH               0x00000100
#define AINFO_BITMAP_INDEX          0x00001000
#define AINFO_GRAPHID               0x00010000  // Node id in graph.
#define AINFO_OOO_VARSTRING         0x00100000  // out of order VARSTRING
#define AINFO_KEY                   0x10000000
#define AINFO_INDEX_ORDER           0x01000000


enum ColumnFlag {
    COL_INVALID=-1,
    COL_TS_DATA,
    COL_GENERAL_TAG,
    COL_PRIMARY_TAG,
};


#define TsColumnTailSize      40 // (sizeof(AttributeInfo) - 2 * sizeof(string) - sizeof(uint32_t))
#define COLUMNATTR_LEN      64  // MAX_COLUMNATTR_LEN + 2
struct AttributeInfo {
  uint32_t id;           /// < column id.
  char name[COLUMNATTR_LEN] = {0};       ///< column name.
  int32_t type;           ///< column data type.
  int32_t offset;         ///< Offset.
  int32_t size;           ///< Size.
  int32_t length;         ///< Length.
  int32_t encoding;
  int32_t flag;           ///< internal use.
  int32_t max_len;        ///< max length for string; Geohash precision;
  uint32_t version;       /// table column version
  ColumnFlag col_flag;
  char reserved[4] = {0};

  AttributeInfo();

  bool isValid() { return !(type == INVALID); }

  bool isEqual(const AttributeInfo& other) const { return (id == other.id) && (flag == other.flag); }

  inline void setFlag(int32_t f) { flag |= f; }
  inline void unsetFlag(int32_t f) { flag &= ~f; }
  inline bool isFlag(int32_t f) const { return (flag & f) != 0; }

  inline bool isAttrType(int expAttrType) const
	{ return (col_flag == (ColumnFlag)expAttrType); }

  bool operator==(AttributeInfo& rhs) const;
};

inline bool isSameType(const AttributeInfo &a, const AttributeInfo &b) {
  return (a.type == b.type && a.size == b.size);
}

inline void * offsetAddr(const void *addr, size_t offset)
{ return (void *)((unsigned char *)addr + offset); }

// macro for LD(length data): VarBinary | Geometry
// LD = [len][data]
// data length = sizeof(int32_t) + length of data
#define LD_LEN(x) (*(int32_t *)x)
#define LD_DATALEN(x) ((*(int32_t *)x) - sizeof(int32_t))
#define LD_GETDATA(x) ((unsigned char *)x + sizeof(int32_t))

// calculate the total length of LD
#define CALC_LD_LEN(n) (n + sizeof(int32_t))

// assign total length to LD
inline void __attribute__ ((always_inline)) LD_setLen(void *ld, int n)
{ *((int32_t *)ld) = n; }

// assign data length to LD
inline void __attribute__ ((always_inline)) LD_setDataLen(void *ld, int n)
{ *((int32_t *)ld) = n + sizeof(int32_t); }

struct InputString {
  bool is_ok;           // the string is set by user or not.
  string str;
};

struct TimeStamp64LSN {
  timestamp64 ts64;
  TS_LSN lsn;

  TimeStamp64LSN(timestamp64 ts, TS_LSN lsn) : ts64(ts), lsn(lsn){}

  TimeStamp64LSN(void * buf) {
    ts64 = (*(static_cast<timestamp64 *>(buf)));
    lsn = (*(static_cast<TS_LSN *>((void *)((intptr_t)buf + 8))));
  }

  static timestamp64 * getTimestampAddr(void * buf) {
    return static_cast<timestamp64*>(buf);
  }

  static TS_LSN * getLsnAddr(void * buf) {
    return static_cast<TS_LSN*>((void *)((intptr_t)buf+8));
  }

  std::string to_string() {
    std::stringstream ss;
    ss << ts64 << "," << lsn;
    return ss.str();
  }
};

std::string getDataTypeName(int32_t data_type);

#define ST_VTREE                0x00000001
#define ST_NS				    0x00000010
#define ST_NS_EXT			    0x00000040			/// external name service
#define ST_DATA				    0x00000100
#define ST_COLUMN_TABLE         0x00200000
#define ST_TRANSIENT            0x20000000

#define isTransient(x)			((x & ST_TRANSIENT) != 0)

