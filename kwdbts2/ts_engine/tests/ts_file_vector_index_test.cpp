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
#include "ts_file_vector_index.h"

using namespace kwdbts;  // NOLINT

class TestMemFile : public FileWithIndex {
 private:
  uint32_t length_;
  char* start_{nullptr};
  uint32_t free_offset_{4};
 public:
  explicit TestMemFile(uint32_t file_max_length) : length_(file_max_length) {
    start_ = reinterpret_cast<char*>(malloc(length_));
  }
  ~TestMemFile() {
    free(start_);
  }
  uint64_t AllocateAssigned(size_t size, uint8_t fill_number) override {
    auto extend = free_offset_ % 8;
    uint32_t skip = 0;
    if (extend != 0) {
      skip = 8 - extend;
    }
    free_offset_ += skip;
    if (free_offset_ + size > length_) {
      return INVALID_POSITION;
    }
    auto ret = free_offset_;
    memset(start_ + free_offset_, fill_number, size);
    free_offset_ += size;
    return ret;
  }
  char* GetAddrForOffset(uint64_t offset, uint32_t reading_bytes) override {
    if (offset >= length_) {
      return nullptr;
    }
    return start_ + offset;
  }
};

class TsFileVectorIndexTest : public ::testing::Test {
 public:
  struct IndexInfo {
    uint64_t id;
    uint64_t value;
  };
};

TEST_F(TsFileVectorIndexTest, noSpaceForRootNode) {
  TestMemFile file(10);
  uint64_t offset = INVALID_POSITION;
  VectorIndexForFile<uint64_t> file_index;
  file_index.Init(&file, &offset);

  auto ret = file_index.GetIndexObject(0, false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(0, true);
  EXPECT_EQ(ret, nullptr);
}

TEST_F(TsFileVectorIndexTest, noSpaceForNode) {
  TestMemFile file(NUM_PER_INDEX_BLOCK * 8 + 8 + 8);
  uint64_t offset = INVALID_POSITION;
  VectorIndexForFile<uint64_t> file_index;
  file_index.Init(&file, &offset);

  auto ret = file_index.GetIndexObject(0, false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(0, true);
  EXPECT_NE(ret, nullptr);

  // no space left for new leaf node.
  ret = file_index.GetIndexObject(1, false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(1, true);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(NUM_PER_INDEX_BLOCK * (1 - INDIRECT_INDEX_LEVEL_RATIO) - 1, false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(NUM_PER_INDEX_BLOCK * (1 - INDIRECT_INDEX_LEVEL_RATIO) - 1, true);
  EXPECT_EQ(ret, nullptr);

  // no space left for new block node.
  ret = file_index.GetIndexObject(NUM_PER_INDEX_BLOCK * (1 - INDIRECT_INDEX_LEVEL_RATIO), false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(NUM_PER_INDEX_BLOCK * (1 - INDIRECT_INDEX_LEVEL_RATIO), true);
  EXPECT_EQ(ret, nullptr);
}

TEST_F(TsFileVectorIndexTest, extendMaxId) {
  TestMemFile file(10 << 20);
  uint64_t offset = INVALID_POSITION;
  VectorIndexForFile<uint64_t> file_index;
  file_index.Init(&file, &offset);

  auto ret = file_index.GetIndexObject(file_index.GetMaxId(), false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(file_index.GetMaxId(), true);
  EXPECT_NE(ret, nullptr);
  ret = file_index.GetIndexObject(file_index.GetMaxId() + 1, false);
  EXPECT_EQ(ret, nullptr);
  ret = file_index.GetIndexObject(file_index.GetMaxId() + 1, true);
  EXPECT_EQ(ret, nullptr);
}

TEST_F(TsFileVectorIndexTest, insertAndSelect) {
  TestMemFile file(10 << 20);
  uint64_t offset = INVALID_POSITION;
  VectorIndexForFile<uint64_t> file_index;
  file_index.Init(&file, &offset);
  int number = 10000;
  int start_value = 12345;
  for (size_t i = 0; i < number; i++) {
    auto ret = file_index.GetIndexObject(i, true);
    *ret = start_value + i;
  }
  
  for (size_t i = 0; i < number; i++) {
    auto ret = file_index.GetIndexObject(i, false);
    EXPECT_NE(ret, nullptr);
    EXPECT_EQ(*ret, start_value + i);
  }
  for (size_t i = 0; i < number; i++) {
    auto ret = file_index.GetIndexObject(file_index.GetMaxId() - i, true);
    *ret = start_value + i;
  }
  
  for (size_t i = 0; i < number; i++) {
    auto ret = file_index.GetIndexObject(file_index.GetMaxId() - i, false);
    EXPECT_NE(ret, nullptr);
    EXPECT_EQ(*ret, start_value + i);
  }
}

TEST_F(TsFileVectorIndexTest, multiIndexinsertAndSelect) {
  TestMemFile file(10 << 20);
  uint64_t offset = INVALID_POSITION;
  VectorIndexForFile<uint64_t> file_index;
  file_index.Init(&file, &offset);
  uint64_t offset1 = INVALID_POSITION;
  VectorIndexForFile<uint32_t> file_index_1;
  file_index_1.Init(&file, &offset1);

  int number = 10000;
  int start_value = 12345;
  int index_number = 3;

  std::vector<VectorIndexForFile<uint64_t>*> indexs;
  std::vector<uint64_t> offsets;
  offsets.resize(index_number);
  for (size_t i = 0; i < index_number; i++) {
    offsets[i] = INVALID_POSITION;
    VectorIndexForFile<uint64_t>* file_index = new VectorIndexForFile<uint64_t>();
    file_index->Init(&file, &offsets[i]);
    indexs.push_back(file_index);
  }
  

  for (size_t i = 0; i < number; i++) {
    for (size_t j = 0; j < indexs.size(); j++) {
      auto ret = indexs[j]->GetIndexObject(i, true);
      *ret = start_value + i + j;
    }
  }
  
  for (size_t i = 0; i < number; i++) {
    for (size_t j = 0; j < indexs.size(); j++) {
      auto ret = indexs[j]->GetIndexObject(i, false);
      EXPECT_NE(ret, nullptr);
      ASSERT_EQ(*ret, start_value + i + j);
    }
  }
  for (size_t i = 0; i < number; i++) {
    for (size_t j = 0; j < indexs.size(); j++) {
      auto ret = indexs[j]->GetIndexObject(file_index.GetMaxId() - i, true);
      *ret = start_value + i + j;
    }
  }
  
  for (size_t i = 0; i < number; i++) {
    for (size_t j = 0; j < indexs.size(); j++) {
      auto ret = indexs[j]->GetIndexObject(file_index.GetMaxId() - i, false);
      EXPECT_NE(ret, nullptr);
      ASSERT_EQ(*ret, start_value + i + j);
    }
  }
  for (size_t i = 0; i < indexs.size(); i++) {
    delete indexs[i];
  }
  indexs.clear();
}
