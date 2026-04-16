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

#include <gtest/gtest.h>
#include <string>
#include "ts_object.h"
#include "ts_object_error.h"

class TestTSObject : public TSObject {
 public:
  int rdLock() override { return 0; }
  int wrLock() override { return 0; }
  int unLock() override { return 0; }

  std::string path() const override {
    return "/test/object";
  }

  const std::string& tbl_sub_path() const override {
    static std::string s = "/test/db";
    return s;
  }
};

class TempObject : public TestTSObject {
 public:
  bool isTemporary() const override { return true; }
};

class TSObjectTest : public ::testing::Test {
 protected:
  TestTSObject* obj;

  void SetUp() override {
    obj = new TestTSObject();
  }

  void TearDown() override {
    delete obj;
  }
};

TEST_F(TSObjectTest, Constructor) {
  EXPECT_EQ(obj->getObjectStatus(), OBJ_NOT_READY);
  EXPECT_EQ(obj->refCount(), 0);
}

TEST_F(TSObjectTest, RefCount) {
  obj->incRefCount();
  EXPECT_EQ(obj->refCount(), 1);
  obj->decRefCount();
  EXPECT_EQ(obj->refCount(), 0);
}

TEST_F(TSObjectTest, Status) {
  obj->setObjectReady();
  EXPECT_TRUE(obj->isValid());
  obj->setObjectStatus(OBJ_NOT_READY);
  EXPECT_FALSE(obj->isValid());
}

TEST_F(TSObjectTest, StartRead) {
  ErrorInfo err;
  int ret = obj->startRead(err);
  EXPECT_EQ(ret, KWERLOCK);

  obj->setObjectReady();
  ret = obj->startRead(err);
  EXPECT_EQ(ret, 0);
  obj->stopRead();
}

TEST_F(TSObjectTest, StartWrite) {
  int ret = obj->startWrite();
  EXPECT_EQ(ret, KWEWLOCK);

  obj->setObjectReady();
  ret = obj->startWrite();
  EXPECT_EQ(ret, 0);
  obj->stopWrite();
}

TEST_F(TSObjectTest, PathNameDir) {
  EXPECT_FALSE(obj->path().empty());
  EXPECT_FALSE(obj->name().empty());
  EXPECT_FALSE(obj->directory().empty());
}

TEST_F(TSObjectTest, BasicMethods) {
  EXPECT_EQ(obj->type(), 0);
  EXPECT_EQ(obj->version(), 0);
  ErrorInfo err;
  EXPECT_EQ(obj->open("", "", "", 0, err), 0);
  EXPECT_EQ(obj->remove(), -1);
  obj->clear();
  obj->sync(0);
}

TEST_F(TSObjectTest, IsUsed) {
  EXPECT_FALSE(obj->isTemporary());
  obj->incRefCount();
  obj->incRefCount();
  EXPECT_TRUE(obj->isUsed());
}

TEST_F(TSObjectTest, IsUsedTemporary) {
  TempObject tobj;
  EXPECT_TRUE(tobj.isTemporary());
  tobj.incRefCount();
  tobj.incRefCount();
  tobj.incRefCount();
  EXPECT_TRUE(tobj.isUsed());
}

TEST_F(TSObjectTest, ToString) {
  obj->incRefCount();
  std::string s = obj->toString();
  EXPECT_EQ(s, "1");
}

TEST_F(TSObjectTest, All) {
  releaseObject(nullptr);

  auto* o = new TestTSObject();
  releaseObject(o);

  auto* t = new TempObject();
  t->incRefCount();
  releaseObject(t);
}

TEST_F(TSObjectTest, Mutex) {
  obj->mutexLock();
  obj->mutexUnlock();
  obj->mutexSignal();
  obj->refMutexLock();
  obj->refMutexUnlock();
}

TEST_F(TSObjectTest, Path) {
  EXPECT_EQ(defaultNameServicePath(), "default");
}
