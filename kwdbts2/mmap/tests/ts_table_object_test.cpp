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
#include <cstdio>
#include <unistd.h>
#include <sys/stat.h>
#include "ts_table_object.h"

using namespace kwdbts;

class TsTableObjectTest : public ::testing::Test {
protected:
  char file_[256];
  char dir_[256];
  const int magic_ = 0x54535442;

  void SetUp() override {
    snprintf(dir_, sizeof(dir_), "/tmp");
    snprintf(file_, sizeof(file_), "/tmp/ts_table_safe_%p.tmp", this);

    FILE* f = fopen(file_, "wb");
    if (f) {
      fputc(0, f);
      fclose(f);
    }
  }

  void TearDown() override {
    if (access(file_, F_OK) == 0) {
      unlink(file_);
    }
  }
};

TEST_F(TsTableObjectTest, OpenCloseFlow) {
  TsTableObject obj;
  int ret = obj.open(std::string(dir_), std::string(file_), magic_, O_CREAT | O_RDWR);
  ASSERT_EQ(ret, 0);

  ret = obj.close();
  EXPECT_EQ(ret, 0);
}

TEST_F(TsTableObjectTest, InitMetaDataAfterOpen) {
  TsTableObject obj;
  ASSERT_EQ(obj.open(std::string(dir_), std::string(file_), magic_, O_CREAT | O_RDWR), 0);

  int ret = obj.initMetaData();
  ASSERT_EQ(ret, 0);

  EXPECT_NE(obj.metaData(), nullptr);
  if (obj.metaData() != nullptr) {
    EXPECT_EQ(obj.metaData()->schema_version, 1);
  }

  obj.close();
}

TEST_F(TsTableObjectTest, MemExtendSafeCall) {
  TsTableObject obj;
  ASSERT_EQ(obj.open(std::string(dir_), std::string(file_), magic_, O_CREAT | O_RDWR), 0);

  off_t old_len = obj.metaDataLen();
  int ret = obj.memExtend(4096);
  ASSERT_EQ(ret, 0);
  EXPECT_EQ(obj.metaDataLen(), old_len + 4096);

  obj.close();
}

TEST_F(TsTableObjectTest, ColumnIndexQuerySafe) {
  TsTableObject obj;
  ASSERT_EQ(obj.open(std::string(dir_), std::string(file_), magic_, O_CREAT | O_RDWR), 0);
  ASSERT_EQ(obj.initMetaData(), 0);

  EXPECT_EQ(obj.getColumnIndex(9999), -1);

  obj.colsInfoWithHidden();
  obj.getIdxForValidCols();

  obj.close();
}
