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

namespace kwdbts {

void SampleReservoir::SampleRow(const SampledRow& row) {
    if (samples.size() < numSamples) {
        samples.push(row);
    } else if (row.rank < samples.top().rank) {
        samples.pop();
        samples.push(row);
    }
}

std::vector<SampledRow> SampleReservoir::GetSamples() {
    std::vector<SampledRow> result;
    result.reserve(samples.size());
    while (!samples.empty()) {
        result.push_back(samples.top());
        samples.pop();
    }
    return result;
}

// generate a random 64-bit integer
int64_t SampleReservoir::Int63() {
    // Define a uniform distribution ranging from 0 to INT64_MAX
    std::uniform_int_distribution<std::int64_t> distribution(0, INT64_MAX);

    // Generate random numbers
    int64_t rank = distribution(rng);
    return rank;
}

}  // namespace kwdbts

