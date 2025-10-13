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

#include <iostream>
#include "ts_object.h"
#include "utils/big_table_utils.h"
#include "lg_api.h"

enum {
  OBJ_RD_LOCK,
  OBJ_WR_LOCK,
  OBJ_RD_UNLOCK,
  OBJ_WR_UNLOCK,
  OBJ_RD_LOCK_ERROR,
  OBJ_WR_LOCK_ERROR,
};

void LogObjectLockError(int type, TSObject *obj) {
  switch(type) {
    case OBJ_RD_LOCK_ERROR:
      LOG_ERROR("%lx <Cannot lock(r)>: \"%s\", db: \"%s\"\n", pthread_self(),
                obj->path().c_str(), obj->tbl_sub_path().c_str());
      break;
    case OBJ_WR_LOCK_ERROR:
      LOG_ERROR("%lx <Cannot lock(w)>: \"%s\", db: \"%s\"\n", pthread_self(),
                obj->path().c_str(), obj->tbl_sub_path().c_str());
      break;
  }
}

TSObject::TSObject() {
  pthread_mutex_init(&obj_mutex_, NULL);
  pthread_cond_init(&obj_mtx_cv_, NULL);
  pthread_mutex_init(&ref_cnt_mtx_, NULL);
  pthread_cond_init(&ref_cnt_cv_, NULL);
  status_ = OBJ_NOT_READY;
  ref_count_ = 0;
  ins_ref_cnt_ = 0;
#if !defined(NDEBUG)
  rd_cnt_ = wr_cnt_ = 0;
#endif
}

TSObject::~TSObject() {
  pthread_cond_destroy(&ref_cnt_cv_);
  pthread_mutex_destroy(&ref_cnt_mtx_);
  pthread_cond_destroy(&obj_mtx_cv_);
  pthread_mutex_destroy(&obj_mutex_);
}

int TSObject::type() const {
    return 0;
}

void TSObject::incRefCount() {
  refMutexLock();
  ++(ref_count_);
  refMutexUnlock();
}

int TSObject::decRefCount() {
  int ref_count;
  refMutexLock();
  ref_count = --(ref_count_);
  refMutexUnlock();
  return ref_count;
}

string TSObject::path() const
{ return kwdbts::s_emptyString; }

string TSObject::name() const
{ return getTsObjectName(path()); }

const string & TSObject::tbl_sub_path() const
{ return kwdbts::s_emptyString; }

string TSObject::directory() const {
  const string &db = tbl_sub_path();
  if (db.empty())
    return kwdbts::s_kaiwudb;
  if (db.back() == '\\')
    return db.substr(0, db.size() - 1);
  return db;
}

int TSObject::open(const string &obj_path, const string& db_path, const string &tbl_sub_path, int flags,
                   ErrorInfo &err_info) { return 0; }

int TSObject::version() const { return 0; }

void TSObject::clear() {}

void TSObject::sync(int flags) {}

int TSObject::remove() { return -1; }

int TSObject::startRead() {
  rdLock();
  if (status_ != OBJ_READY) {
    unLock();
    LogObjectLockError(OBJ_RD_LOCK_ERROR, this);
    return KWERLOCK;
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_++;
  rw_mtx_.unlock();
#endif
  return 0;
}

int TSObject::startRead(ErrorInfo &err_info) {
  rdLock();
  if (status_ > OBJ_READY_TO_COMPACT) {
    unLock();
    LogObjectLockError(OBJ_RD_LOCK_ERROR, this);
    return err_info.setError(KWERLOCK, getTsObjectName(path()));
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_++;
  rw_mtx_.unlock();
#endif
  return 0;
}

int TSObject::startWrite() {
  wrLock();
  if (status_ != OBJ_READY) {
    unLock();
    LogObjectLockError(OBJ_WR_LOCK_ERROR, this);
    return KWEWLOCK;
  }
#if !defined(NDEBUG)
  rw_mtx_.lock();
  wr_cnt_++;
  rw_mtx_.unlock();
#endif
  return 0;
}

int TSObject::stopRead() {
  int ret_val = unLock();
#if !defined(NDEBUG)
  rw_mtx_.lock();
  rd_cnt_--;
  rw_mtx_.unlock();
#endif
  return ret_val;
}

int TSObject::stopWrite() {
  int ret_val = unLock();
#if !defined(NDEBUG)
  rw_mtx_.lock();
  wr_cnt_--;
  rw_mtx_.unlock();
#endif
  return ret_val;
}

bool TSObject::isTemporary() const { return false; }

string TSObject::toString() {
  return intToString(ref_count_);
}

std::string defaultNameServicePath()
{
  return "default";
}

void releaseObject(TSObject *obj, bool is_force) {
  if (obj) {
    if (obj->isTemporary()) {
      if (obj->decRefCount() == 0)
        delete obj;
      return;
    }
    delete obj;
  }
}
