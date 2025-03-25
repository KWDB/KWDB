#include "ts_bitmap.h"

#include <gtest/gtest.h>
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
  for (int i = 0; i < 997; ++i) {
    EXPECT_TRUE(bm[i] == static_cast<kwdbts::DataFlags>(i % 3));
  }
  bm.SetAll(kwdbts::DataFlags::kNone);
  for (int i = 0; i < 997; ++i) {
    EXPECT_EQ(bm[i], kwdbts::kNone);
  }
}

TEST(TsBitmap, Rep) {
  kwdbts::TsBitmap bm(10);
  for (int i = 0; i < 10; ++i) {
    bm[i] = static_cast<kwdbts::DataFlags>(i % 3);
  }
  auto data = bm.GetData();
  std::string val = std::string{data.data, data.len};
  std::string exp("\x18\x61\x80");
  EXPECT_EQ(val, exp);
}