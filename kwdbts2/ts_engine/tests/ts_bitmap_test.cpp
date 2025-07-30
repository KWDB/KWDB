#include "ts_bitmap.h"

#include <gtest/gtest.h>
#include <algorithm>
#include <random>

using namespace kwdbts;
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

TEST(TsBitmap, View) {
  int n = 10000;
  std::default_random_engine drng(0);
  std::vector<kwdbts::DataFlags> flags(n);
  std::array<kwdbts::DataFlags, 3> choises{kwdbts::DataFlags::kValid, kwdbts::DataFlags::kNone,
                                           kwdbts::DataFlags::kNull};
  for (int i = 0; i < n; ++i) {
    flags[i] = choises[drng() % choises.size()];
  }
  kwdbts::TsBitmap bm(n);
  for (int i = 0; i < n; ++i) {
    bm[i] = flags[i];
  }

  struct Case {
    int start;
    int count;
  };

  std::vector<Case> cases{{0, 1}, {2, 2}, {3, 1}, {0, 40}, {0, 39}, {1, 39}, {2, 38}, {3, 35}};

  for (auto c : cases) {
    auto view = bm.Slice(c.start, c.count);
    for (int i = 0; i < c.count; ++i) {
      EXPECT_EQ(view[i], flags[i + c.start]);
    }

    int nvalid = std::count(flags.begin() + c.start, flags.begin() + c.start + c.count, kwdbts::DataFlags::kValid);
    EXPECT_EQ(view.GetValidCount(), nvalid);
  }
}

TEST(TsBitmap, SliceOfView) {
  int n = 10000;
  std::default_random_engine drng(0);
  std::vector<kwdbts::DataFlags> flags(n);
  std::array<kwdbts::DataFlags, 3> choises{kwdbts::DataFlags::kValid, kwdbts::DataFlags::kNone,
                                           kwdbts::DataFlags::kNull};
  TsBitmap bm(n);
  for (int i = 0; i < n; ++i) {
    flags[i] = choises[drng() % choises.size()];
    bm[i] = flags[i];
  }

  TsBitmapView bm_v = bm.Slice(30, 300).Slice(40, 100).Slice(50, 10);
  for (int i = 0; i < 10; ++i) {
    EXPECT_EQ(bm_v[i], flags[i + 30 + 40 + 50]);
  }

  EXPECT_EQ(bm_v.GetValidCount(),
            std::count(flags.begin() + 30 + 40 + 50, flags.begin() + 30 + 40 + 50 + 10, kwdbts::DataFlags::kValid));
}

TEST(TsBitmap, ValidCount) {
  int n = 100;
  std::default_random_engine drng(0);
  std::vector<kwdbts::DataFlags> flags(n);
  std::array<kwdbts::DataFlags, 3> choises{kwdbts::DataFlags::kValid, kwdbts::DataFlags::kNone,
                                           kwdbts::DataFlags::kNull};
  for (int i = 1; i < n; ++i) {
    TsBitmap bm(i);
    for (int k = 0; k < i; ++k) {
      flags[k] = choises[drng() % choises.size()];
      bm[k] = flags[k];
    }

    EXPECT_EQ(bm.GetValidCount(), std::count(flags.begin(), flags.begin() + i, kwdbts::DataFlags::kValid));
  }
}