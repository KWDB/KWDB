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

#ifndef KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_ROW_SAMPLING_H_
#define KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_ROW_SAMPLING_H_

#include <vector>
#include <queue>
#include <random>
#include <functional>
#include <optional>
#include "ts_utils.h"

namespace kwdbts {

struct SampledRow {
    // store data
    std::optional<DataVariant> data;
    k_uint64 rank{};
};

// Comparison function for max heap
struct CompareSample {
    bool operator()(const SampledRow& lhs, const SampledRow& rhs) {
        return lhs.rank < rhs.rank;  // max heap
    }
};

// Reservoir sampling
class SampleReservoir {
 public:
    explicit SampleReservoir(uint32_t numSamples) : numSamples(numSamples), rng(std::random_device {}()) {}

    /*
     * @Description : Adds a row to the reservoir samples. If the reservoir is not full, the row is simply added.
     *                If the reservoir is full, the row is added only if its rank is lower than the highest rank in the reservoir.
     * @IN row : The row to be sampled, which contains a rank attribute.
     * @OUT samples : The reservoir where samples are stored. It may be updated by adding a new row or replacing the highest rank row.
    */
    void SampleRow(const SampledRow& row);

    std::vector<SampledRow> GetSamples();

    int64_t Int63();

    size_t GetSampleSize() const {return samples.size();}

 private:
    uint32_t numSamples;
    // priority queue is used to expresses max-heap
    std::priority_queue<SampledRow, std::vector<SampledRow>, CompareSample> samples;
    // random number generator
    std::mt19937_64 rng;
};

}  // namespace kwdbts

#endif  // KWBASE_ENGINE_TS_ENGINE_STATISTIC_INCLUDE_ROW_SAMPLING_H_

