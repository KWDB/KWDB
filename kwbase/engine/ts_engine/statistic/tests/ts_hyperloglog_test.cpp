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

#include <gtest/gtest.h>
#include <memory>
#include "ts_hyperloglog.h"

namespace kwdbts {

class TestSketch : public ::testing::Test {
 protected:
    int p_;
    bool sparse_;

    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    void SetUp() {
        p_ = 14;
        sparse_ = true;
    }

    void TearDown() {}
};

TEST_F(TestSketch, InsertHash) {
    std::shared_ptr<Sketch> sketch = std::make_shared<Sketch>(p_, sparse_);
    uint64_t hash1 = 123;
    sketch->InsertHash(hash1);
    uint64_t hash2 = 789;
    sketch->InsertHash(hash2);
    EXPECT_EQ(sketch->GetTmpSet().size(), 2);
}

TEST_F(TestSketch, Insert) {
    std::vector<byte> buf;
    std::shared_ptr<Sketch> sketch = std::make_shared<Sketch>(p_, sparse_);
    for (int i = 1; i <= 10; ++i) {
        PutUint64LittleEndian(&buf, static_cast<u_int64_t>(i));
        sketch->Insert(buf);
    }
    EXPECT_EQ(sketch->GetTmpSet().size(), 10);
}

TEST_F(TestSketch, TransitionToNormal) {
    std::shared_ptr<Sketch> sketch = std::make_shared<Sketch>(p_, true);

    std::vector<byte> buf;
    for (uint64_t i = 1; i < 10000; ++i) {
       PutUint64LittleEndian(&buf, static_cast<u_int64_t>(i));
       sketch->Insert(buf);
    }

    // The Sketch should now be in normal mode due to the number of inserts
    EXPECT_FALSE(sketch->isSparse());

    EXPECT_NE(sketch->GetRegs().get(), nullptr);
}

TEST_F(TestSketch, MarshalBinary) {
    std::shared_ptr<Sketch> sketch = std::make_shared<Sketch>(p_, sparse_);

    sketch->InsertHash(123456789);
    sketch->InsertHash(987654321);

    // Serialize the Sketch object
    std::vector<byte> serializedData = sketch->MarshalBinary();

    // Expect the serialized data to be non-empty
    EXPECT_GT(serializedData.size(), 0);

    EXPECT_EQ(serializedData[0], version);
    EXPECT_EQ(serializedData[1], p_);
}

TEST_F(TestSketch, MergeSparse) {
  std::shared_ptr<Sketch> sketch = std::make_shared<Sketch>(p_, true);
  for (uint64_t i = 0; i < 100; ++i) {
    sketch->InsertHash(i);
  }

  sketch->mergeSparse();

  EXPECT_TRUE(sketch->GetTmpSet().empty());
  EXPECT_GT(sketch->GetSparseList()->len(), 0);
}

}  // namespace kwdbts
