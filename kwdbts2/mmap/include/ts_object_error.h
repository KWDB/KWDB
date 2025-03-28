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

#include <pthread.h>
#include <iostream>
#include "kwdb_consts.h"

using namespace std;

#define KWEPERM         -1  // Operation not permitted
#define KWECORR         -2	// TSObject corrupted.
#define	KWEMMAP         -3	// MMap failed.
#define KWENOMEM        -4  // Out of memory
#define KWEEXIST        -5	// TSObject exists.
#define KWENOSPC        -6  // No space left on device.
#define KWENFILE	      -7  // Cannot open more files.
#define KWEINVALPATH    -8  // Invalid path.
#define KWEROFS         -9 // Operation not permitted on read only FS.
#define KWENOOBJ        -10	// No such object.
#define KWENOATTR       -11	// Unknown attribute.
#define KWEINVALIDNAME	-12	// Invalid name
#define KWENAMESERVICE  -13	// Name service Full
#define KWERLOCK        -14 // Cannot lock(r) object.
#define KWEWLOCK        -15 // Cannot lock(w) object.
#define KWEOTHER        -16 // Other errors.
#define KWEVALUE        -17 // Invalid value
#define KWERANGE        -18 // Out of range data
#define KWEDATEVALUE    -19 // Incorrect date value
#define KWEDATA         -20 // Incorrect data value
#define KWELENLIMIT     -21 // Out of len limit
#define KWEDATATYPEMISMATCH  -22 //datatype mismatch
#define KWENOSEGMENT      -23  // need create new segment directory
#define KWEDUPREJECT     -24  // reject duplicate data
#define KWERSRCBUSY      -25  // resource is busy
#define KWEDROPPEDOBJ    -26  // object is dropped
#define KWEFREESPCLIMIT  -27  // free space alert
#define KWEUNINIT        -28  // object is uninitialized

#define ERROR_LEN_LIMIT 128

class ErrorInfo {
protected:
  pthread_mutex_t mutex_;
  bool is_mutex_needed_;

  void lock();
  void unlock();

public:
  int errcode;
  string errmsg;

  ErrorInfo(bool is_mtx_needed = true);

  virtual ~ErrorInfo();

  void clear();

  static const char * errorCodeString(int cerr_ode);

  int setError(const ErrorInfo &rhs);

  int setError(int err_code, const string &str = kwdbts::s_emptyString);

  string toString() {return "";};

  inline bool isOK() { return errcode >= 0;};
};

class TSObject;

ErrorInfo & getDummyErrorInfo();

int errnumToErrorCode(int errnum);
int errnoToErrorCode();
