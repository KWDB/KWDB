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

#include <sstream>
#include "ts_object_error.h"
#include "utils/big_table_utils.h"

ErrorInfo dummy_error_info;

const char * TsErrmsg[] = {
  "",                                   //                   0
  "Operation not permitted: ",          // KWEPERM	        -1
  "TSObject corrupted: ",               // KWECORR	        -2
  "MMAP failed: ",                      // KWEMMAP	        -3
  "No memory is available: ",           // KWENOMEM	        -4
  "TSObject already exists: ",    	    // KWEEXIST	        -5
  "No space left on device.",           // KWENOSPC         -6
  "Cannot open more files.",		        // KWENFILE	        -7
  "Invalid path: ",                     // KWEINVALPATH     -8
  "Operation not permitted on read only file system for ",  // KWEROFS -9
  "TSObject doesn't exist: ",           // KWENOOBJ        -10
  "Unknown attribute: ",                // KWENOATTR       -11
  "Invalid name: ",                     // KWEINVALIDNAME  -12
  "NameService full",                   // KWENAMESERVICE  -13
  "Cannot lock(r) object: ",            // KWERLOCK        -14
  "Cannot lock(w) object: ",            // KWEWLOCK        -15
  "Other error: ",                      // KWEOTHER	       -16
  "Invalid value: ",                    // KWEVALUE        -17
  "Out of range data: ",                // KWERANGE        -18
  "Incorrect date value: ",             // KWEINVDATE      -19
  "Incorrect data value: ",             // KWEDATA         -20
  "Out of len limit: " ,                // KWELENLIMIT     -21
  "Datatype mismatch: " ,               // BOEDATATYPEMISMATCH   -22
  "Need create new segment directory: ",  // KWENOEGMENT         -23
  "Reject duplicate data: " ,           // KWEDUPREJECT     -24
  "Resource is busy: ",                 // KWERSRCBUSY      -25
  "Object is dropped",                  // KWEDROPPEDOBJ    -26
  "Disk free space reaches the alert threshold."   // KWEFREESPCLIMIT  -27
};

#define TsError(x)  (-(x))


ErrorInfo::ErrorInfo(bool is_mtx_needed): is_mutex_needed_(is_mtx_needed) {
  if (is_mutex_needed_)
    pthread_mutex_init(&mutex_, NULL);
  clear();
}

ErrorInfo::~ErrorInfo() {
  if (is_mutex_needed_)
    pthread_mutex_destroy(&mutex_);
};

void ErrorInfo::clear() {
  lock();
  errcode = 0;
  errmsg.clear();
  unlock();
}

void ErrorInfo::lock() {
  if (is_mutex_needed_)
    pthread_mutex_lock(&mutex_);
}

void ErrorInfo::unlock() {
  if (is_mutex_needed_)
    pthread_mutex_unlock(&mutex_);
}

const char * ErrorInfo::errorCodeString(int err_code) {
  err_code = TsError(err_code);
  if (err_code >= (int) (sizeof(TsErrmsg) / sizeof(TsErrmsg[0])))
    err_code = 0;
  return TsErrmsg[err_code];
}

int ErrorInfo::setError(const ErrorInfo &rhs) {
  lock();
  errcode = rhs.errcode;
  errmsg = rhs.errmsg;
  unlock();
  return errcode;
}

int ErrorInfo::setError(int err_code, const string &str) {
  lock();
  errmsg = errorCodeString(err_code) + str;
  unlock();
  return (errcode = err_code);
}

ErrorInfo & getDummyErrorInfo() { return dummy_error_info; }

int errnumToErrorCode(int errnum) {
  switch (errnum) {
    case EEXIST: return KWEEXIST;
    case ENOENT: return KWENOOBJ;
    case ENFILE: return KWENFILE;
    case ENOMEM: return KWENOMEM;
    case ENOSPC: {
      return KWENOSPC;
    }
    case EROFS:  return KWEROFS;
    case EACCES: return KWEPERM;
    default: break;
  }
  return (errno > 0) ? KWEOTHER : 0;
}

int errnoToErrorCode() {
  int errnum = errno;
  return errnumToErrorCode(errnum);
}
