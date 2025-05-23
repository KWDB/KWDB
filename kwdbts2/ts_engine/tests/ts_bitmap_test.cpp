#include "ts_bitmap.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <random>
TEST(TsBitmap, Write) {
  {
    kwdbts::TsBitmap b1(4);
    ASSERT_EQ(b1.GetData().len, 1);
    kwdbts::TsBitmap b2(3);
    ASSERT_EQ(b2.GetData().len, 1);
    kwdbts::TsBitmap b3(0);
    ASSERT_EQ(b3.GetData().len, 0);
  }
  kwdbts::TsBitmap bm(997);
  ASSERT_EQ(bm.GetData().len, 250);
  for (int i = 0; i < 997; ++i) {
    bm[i] = static_cast<kwdbts::DataFlags>(i % 3);
  }
  EXPECT_EQ(bm.GetValidCount(), 333);
  for (int i = 0; i < 997; ++i) {
    EXPECT_TRUE(bm[i] == static_cast<kwdbts::DataFlags>(i % 3));
  }
  const kwdbts::TsBitmap &const_ref_bm = bm;
  for (int i = 0; i < 997; ++i) {
    EXPECT_EQ(bm[i], const_ref_bm[i]);
  }
  bm.SetAll(kwdbts::DataFlags::kNone);
  for (int i = 0; i < 997; ++i) {
    EXPECT_EQ(bm[i], kwdbts::kNone);
  }
  EXPECT_EQ(bm.GetValidCount(), 0);

  EXPECT_FALSE(bm.IsAllValid());
  bm.SetAll(kwdbts::kValid);
  EXPECT_TRUE(bm.IsAllValid());
}

TEST(TsBitmap, Rep) {
  kwdbts::TsBitmap bm(10);
  for (int i = 0; i < 10; ++i) {
    bm[i] = static_cast<kwdbts::DataFlags>(i % 3);
  }
  auto data = bm.GetData();
  std::string val = std::string{data.data, data.len};
  std::string exp("\x24\x49\x02");
  EXPECT_EQ(val, exp);

  const kwdbts::TsBitmap bm2({exp.data(), exp.size()}, 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(bm[i], bm2[i]);
  }
}

TEST(TsBitmap, Assign) {
  int n = 10000;
  int offset = 7;
  std::default_random_engine drng(0);
  std::vector<kwdbts::DataFlags> flags(n);
  std::array<kwdbts::DataFlags, 3> choises{kwdbts::DataFlags::kValid, kwdbts::DataFlags::kNone,
                                           kwdbts::DataFlags::kNull};
  for (int i = 0; i < n; ++i) {
    flags[i] = choises[drng() % choises.size()];
  }
  kwdbts::TsBitmap bitmap1(n);
  for (int i = 0; i < n; ++i) {
    bitmap1[i] = flags[i];
  }
  for (int i = 0; i < n; ++i) {
    EXPECT_EQ(bitmap1[i], flags[i]);
  }

  kwdbts::TsBitmap bitmap2(offset + n);
  for (int i = 0; i < n; ++i) {
    bitmap2[i + offset] = bitmap1[i];
    EXPECT_EQ(bitmap1[i], bitmap2[i + offset]);
  }
}