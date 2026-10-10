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

#include "row_sampling.h"
#include <gtest/gtest.h>
#include <memory>

namespace kwdbts {

class TestSampleReservoir : public ::testing::Test {
 protected:
    int sample_size_;

    static void SetUpTestCase() {}

    static void TearDownTestCase() {}

    void SetUp() override {
        sample_size_ = 10;
    }

    void TearDown() override {}
};

TEST_F(TestSampleReservoir, SampleRow) {
    std::shared_ptr<SampleReservoir> sr = std::make_shared<SampleReservoir>(sample_size_);
    for (k_int64 i = 0; i < sample_size_; ++i) {
        SampledRow row{};
        row.data = i;
        row.rank = sr->Int63();
        sr->SampleRow(row);
    }
    EXPECT_EQ(sr->GetSamples().size(), sample_size_);

    sr.reset(new SampleReservoir(sample_size_));
    for (k_int64 i = 0; i < sample_size_ - 1; ++i) {
        SampledRow row{};
        row.data = i;
        row.rank = sr->Int63();
        sr->SampleRow(row);
    }
    EXPECT_EQ(sr->GetSamples().size(), sample_size_ - 1);

    sr.reset(new SampleReservoir(sample_size_));
    for (k_int64 i = 0; i < sample_size_ + 1; ++i) {
        SampledRow row{};
        row.data = i;
        row.rank = sr->Int63();
        sr->SampleRow(row);
    }
    EXPECT_EQ(sr->GetSamples().size(), sample_size_);
}

}  // namespace kwdbts
