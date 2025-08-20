#if false
#include "ts_column_block.h"

#include <gtest/gtest.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <iterator>
#include <memory>
#include <numeric>
#include <random>
#include <string>
#include <string_view>
#include <vector>

#include "data_type.h"
#include "kwdb_type.h"
#include "libkwdbts2.h"
#include "ts_bitmap.h"

using namespace kwdbts;

TEST(ColumnBlockBuilder, FixLen) {
  AttributeInfo col_schema;
  col_schema.type = DATATYPE::INT64;
  col_schema.size = 8;
  kwdbts::TsColumnBlockBuilder builder(col_schema);
  std::vector<int> block_len{1234, 1, 1324, 1432};

  std::vector<int64_t> full_data;
  std::vector<DataFlags> full_bitmaps;
  for (auto n : block_len) {
    std::default_random_engine drng(n);
    std::vector<int64_t> data(n);
    TsBitmap bitmap(n);
    for (int i = 0; i < n; ++i) {
      data[i] = drng();
      DataFlags flag = drng() % 2 == 0 ? kValid : kNone;
      bitmap[i] = flag;
      full_bitmaps.push_back(flag);
    }
    TSSlice input;
    input.data = reinterpret_cast<char *>(data.data());
    input.len = data.size() * sizeof(uint64_t);
    builder.AppendFixLenData(input, data.size(), bitmap);

    std::copy(data.begin(), data.end(), std::back_inserter(full_data));
  }

  auto col_block1 = builder.GetColumnBlock();
  EXPECT_EQ(col_block1->GetRowNum(), std::accumulate(block_len.begin(), block_len.end(), 0));

  // compress & decompress
  TsColumnCompressInfo compress_info;
  std::string out;
  EXPECT_EQ(col_block1->GetCompressedData(&out, &compress_info, true), SUCCESS);

  std::unique_ptr<TsColumnBlock> col_block2;
  ASSERT_EQ(TsColumnBlock::ParseColumnData(col_schema, {out.data(), out.size()},
                                                     compress_info, &col_block2),
            SUCCESS);

  {
    const char *value = col_block1->GetColAddr();
    TsBitmap bitmap;
    EXPECT_EQ(col_block1->GetColBitmap(bitmap), SUCCESS);
    const int64_t *data1 = reinterpret_cast<const int64_t *>(value);
    for (int i = 0; i < full_data.size(); ++i) {
      EXPECT_EQ(data1[i], full_data[i]);
      EXPECT_EQ(bitmap[i], full_bitmaps[i]);
    }
  }
  {
    const char *value = col_block2->GetColAddr();
    TsBitmap bitmap;
    EXPECT_EQ(col_block2->GetColBitmap(bitmap), SUCCESS);
    const int64_t *data1 = reinterpret_cast<const int64_t *>(value);
    for (int i = 0; i < full_data.size(); ++i) {
      if (bitmap[i] == kValid) {
        EXPECT_EQ(data1[i], full_data[i]);
      }
    }
  }

  kwdbts::TsColumnBlockBuilder builder2(col_schema);
  builder2.AppendColumnBlock(*col_block1);
  builder2.AppendColumnBlock(*col_block2);
  auto col_block3 = builder2.GetColumnBlock();
  EXPECT_EQ(col_block3->GetRowNum(), std::accumulate(block_len.begin(), block_len.end(), 0) * 2);
  ASSERT_EQ(col_block3->GetCompressedData(&out, &compress_info, true), SUCCESS);
  std::unique_ptr<TsColumnBlock> col_block4;
  ASSERT_EQ(TsColumnBlock::ParseColumnData(col_schema, {out.data(), out.size()},
                                                     compress_info, &col_block4),
            SUCCESS);
  {
    const char *value = col_block3->GetColAddr();
    TsBitmap bitmap;
    EXPECT_EQ(col_block3->GetColBitmap(bitmap), SUCCESS);
    const int64_t *data1 = reinterpret_cast<const int64_t *>(value);
    for (int i = 0; i < full_data.size() * 2; ++i) {
      if (bitmap[i] == kValid) {
        EXPECT_EQ(data1[i], full_data[i % full_data.size()]);
      }
    }
  }

  {
    const char *value = col_block4->GetColAddr();
    TsBitmap bitmap;
    EXPECT_EQ(col_block4->GetColBitmap(bitmap), SUCCESS);
    const int64_t *data1 = reinterpret_cast<const int64_t *>(value);
    for (int i = 0; i < full_data.size() * 2; ++i) {
      if (bitmap[i] == kValid) {
        EXPECT_EQ(data1[i], full_data[i % full_data.size()]);
      }
    }
  }
}

static inline std::string RandomStr(int n, int seed = 0) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";
  std::string s;
  s.reserve(n);
  std::default_random_engine drng(seed);
  for (int i = 0; i < n; ++i) {
    s += alphanum[drng() % (sizeof(alphanum) - 1)];
  }
  return s;
}

TEST(ColumnBlockBuilder, VarLen) {
  AttributeInfo col_schema;
  col_schema.type = DATATYPE::VARSTRING;
  col_schema.size = 8;
  kwdbts::TsColumnBlockBuilder builder(col_schema);
  std::vector<int> block_len{1234, 1, 1324, 1432};

  std::vector<std::string> full_data;
  std::vector<DataFlags> full_bitmaps;
  for (auto n : block_len) {
    std::default_random_engine drng(n);
    std::vector<std::string> data(n);
    TsBitmap bitmap(n);
    for (int i = 0; i < n; ++i) {
      DataFlags flag = drng() % 2 == 0 ? kValid : kNone;
      bitmap[i] = flag;
      full_bitmaps.push_back(flag);

      int len = drng() % 100;
      data[i] = RandomStr(len, i);

      TSSlice input;
      input.data = data[i].data();
      input.len = data[i].size();
      builder.AppendVarLenData(input, flag);
    }

    std::copy(data.begin(), data.end(), std::back_inserter(full_data));
  }

  auto col_block1 = builder.GetColumnBlock();
  EXPECT_EQ(col_block1->GetRowNum(), std::accumulate(block_len.begin(), block_len.end(), 0));

  // compress & decompress
  TsColumnCompressInfo compress_info;
  std::string out;
  EXPECT_EQ(col_block1->GetCompressedData(&out, &compress_info, true), SUCCESS);

  std::unique_ptr<TsColumnBlock> col_block2;
  ASSERT_EQ(TsColumnBlock::ParseColumnData(col_schema, {out.data(), out.size()},
                                                     compress_info, &col_block2),
            SUCCESS);

  {
    TsBitmap bitmap;
    EXPECT_EQ(col_block1->GetColBitmap(bitmap), SUCCESS);
    for (int i = 0; i < full_data.size(); ++i) {
      EXPECT_EQ(bitmap[i], full_bitmaps[i]);
      if (bitmap[i] == kValid) {
        TSSlice value;
        EXPECT_EQ(col_block1->GetValueSlice(i, value), SUCCESS);
        ASSERT_EQ(value.len, full_data[i].size()) << i;
        EXPECT_EQ(std::string_view(value.data, value.len), full_data[i]);
      }
    }
  }
  {
    TsBitmap bitmap;
    EXPECT_EQ(col_block2->GetColBitmap(bitmap), SUCCESS);
    for (int i = 0; i < full_data.size(); ++i) {
      EXPECT_EQ(bitmap[i], full_bitmaps[i]);
      TSSlice value;
      if (bitmap[i] == kValid) {
        EXPECT_EQ(col_block2->GetValueSlice(i, value), SUCCESS);
        ASSERT_EQ(value.len, full_data[i].size()) << i;
        EXPECT_EQ(std::string_view(value.data, value.len), full_data[i]);
      }
    }
  }

  kwdbts::TsColumnBlockBuilder builder2(col_schema);
  builder2.AppendColumnBlock(*col_block1);
  builder2.AppendColumnBlock(*col_block2);
  auto col_block3 = builder2.GetColumnBlock();
  EXPECT_EQ(col_block3->GetRowNum(), std::accumulate(block_len.begin(), block_len.end(), 0) * 2);
  ASSERT_EQ(col_block3->GetCompressedData(&out, &compress_info, true), SUCCESS);
  std::unique_ptr<TsColumnBlock> col_block4;
  ASSERT_EQ(TsColumnBlock::ParseColumnData(col_schema, {out.data(), out.size()},
                                                     compress_info, &col_block4),
            SUCCESS);

  {
    TsBitmap bitmap;
    EXPECT_EQ(col_block3->GetColBitmap(bitmap), SUCCESS);
    for (int i = 0; i < full_data.size() * 2; ++i) {
      EXPECT_EQ(bitmap[i], full_bitmaps[i % full_data.size()]);
      if (bitmap[i] == kValid) {
        TSSlice value;
        EXPECT_EQ(col_block3->GetValueSlice(i, value), SUCCESS);
        ASSERT_EQ(value.len, full_data[i % full_data.size()].size()) << i;
        EXPECT_EQ(std::string_view(value.data, value.len), full_data[i % full_data.size()]);
      }
    }
  }

  {
    TsBitmap bitmap;
    EXPECT_EQ(col_block4->GetColBitmap(bitmap), SUCCESS);
    for (int i = 0; i < full_data.size() * 2; ++i) {
      EXPECT_EQ(bitmap[i], full_bitmaps[i % full_data.size()]);
      if (bitmap[i] == kValid) {
        TSSlice value;
        EXPECT_EQ(col_block4->GetValueSlice(i, value), SUCCESS);
        ASSERT_EQ(value.len, full_data[i % full_data.size()].size()) << i;
        EXPECT_EQ(std::string_view(value.data, value.len), full_data[i % full_data.size()]);
      }
    }
  }
}
#endif