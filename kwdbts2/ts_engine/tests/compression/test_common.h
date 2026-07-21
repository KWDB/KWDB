#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <type_traits>
#include <vector>

#include "libkwdbts2.h"

#ifndef TEST_DATA_DIR
#error "TEST_DATA_DIR must be defined via CMake (target_compile_definitions)"
#endif

/// Wrap a typed array as a TSSlice for Compress/Decompress calls.
/// The cast strips const — callers must ensure the compressor treats data as read-only.
template <typename T>
static TSSlice MakeSlice(const T *data, size_t count) {
  return {reinterpret_cast<char *>(const_cast<T *>(data)), count * sizeof(T)};
}

/// Read a CSV file into a vector of T.
/// Trims trailing commas from each line (some reference CSV files have them).
template <typename T>
static std::vector<T> ReadCSV(const std::string &path, size_t max_count = SIZE_MAX) {
    std::vector<T> result;
    std::ifstream file(path);
    if (!file.is_open()) {
        // ADD_FAILURE() << "Cannot open: " << path;
        return result;
    }
    std::string line;
    while (std::getline(file, line) && result.size() < max_count) {
        if (!line.empty() && line.back() == ',') line.pop_back();
        if constexpr (std::is_same_v<T, float>) {
            result.push_back(std::stof(line));
        } else {
            result.push_back(std::stod(line));
        }
    }
    return result;
}

/// Compare two decoded values, handling NaN bit-exact or via EXPECT_FLOAT_EQ / EXPECT_DOUBLE_EQ.
template <typename T>
static void ExpectValueEq(T expected, T actual, size_t i) {
    if (std::isnan(expected)) {
        EXPECT_TRUE(std::isnan(actual)) << "i=" << i << " expected NaN";
    } else if constexpr (std::is_same_v<T, float>) {
        EXPECT_FLOAT_EQ(actual, expected) << "i=" << i;
    } else {
        EXPECT_DOUBLE_EQ(actual, expected) << "i=" << i;
    }
}

// ============================================================
// Named dataset — carries a name + inline data (no file I/O)
// ============================================================

template <typename T>
struct NamedDataset {
    std::string name;
    std::vector<T> data;
};

template <typename T>
void PrintTo(const NamedDataset<T> &dataset, std::ostream *os) {
  *os << "Dataset<" << typeid(T).name() << ">: " << dataset.name.c_str() << " (size=" << dataset.data.size() << ")";
}

// ============================================================
// Inline data generators
// ============================================================

/// Generate 1024 doubles all equal to max_uint_for_bitwidth(bw).
inline std::vector<double> MakeGeneratedDoubles(int bw) {
    uint64_t max_val = (bw == 64) ? UINT64_MAX : ((1ULL << bw) - 1);
    return std::vector<double>(1024, static_cast<double>(max_val));
}

// ============================================================
// Shared dataset lists (used by all codec tests)
// ============================================================

/// Real-world time-series samples (23 datasets, 1024 double values each).
inline const std::vector<std::string> kSampleCSVs = {
    "samples/neon_air_pressure.csv",
    "samples/city_temperature_f.csv",
    "samples/food_prices.csv",
    "samples/bird_migration_f.csv",
    "samples/bitcoin_f.csv",
    "samples/stocks_de.csv",
    "samples/stocks_uk.csv",
    "samples/cms1.csv",
    "samples/nyc29.csv",
    "samples/gov10.csv",
    "samples/medicare1.csv",
    "samples/neon_wind_dir.csv",
    "samples/neon_bio_temp_c.csv",
    "samples/basel_temp_f.csv",
    "samples/basel_wind_f.csv",
    "samples/ssd_hdd_benchmarks_f.csv",
    "samples/poi_lat.csv",
    "samples/poi_lon.csv",
    "samples/arade4.csv",
    "samples/cms25.csv",
    "samples/bitcoin_transactions_f.csv",
    "samples/neon_dew_point_temp.csv",
    "samples/neon_pm10_dust.csv",
};

/// Synthetic generated columns at various bit-widths (9 datasets, 1024 double values each).
inline const std::vector<NamedDataset<double>> kGeneratedDatasets = {
    {"generated/bw0",  MakeGeneratedDoubles(0)},
    {"generated/bw8",  MakeGeneratedDoubles(8)},
    {"generated/bw16", MakeGeneratedDoubles(16)},
    {"generated/bw24", MakeGeneratedDoubles(24)},
    {"generated/bw32", MakeGeneratedDoubles(32)},
    {"generated/bw40", MakeGeneratedDoubles(40)},
    {"generated/bw48", MakeGeneratedDoubles(48)},
    {"generated/bw56", MakeGeneratedDoubles(56)},
    {"generated/bw64", MakeGeneratedDoubles(64)},
};

/// Edge-case datasets, double type (1 dataset).
inline const std::vector<std::string> kEdgeCaseDoubleCSVs = {
    "edge_case/edge_case.csv",
};

/// Edge-case datasets, float type (1 dataset).
inline const std::vector<std::string> kEdgeCaseFloatCSVs = {
    "edge_case/avx512dq.csv",
};

/// Float-specific inline datasets (3 datasets, 1024 float values each).
inline const std::vector<NamedDataset<float>> kFloatInlineDatasets = {
    {"float/test_0", std::vector<float>(1024, 1.5f)},
    {"float/test_1", std::vector<float>(1024, 10.23f)},
    {"float/test_2", std::vector<float>(1024, 1235.64f)},
};

// ============================================================
// Shared helpers: run a roundtrip function over a dataset
// ============================================================

/// Read CSV from data_dir/<rel_path>, then invoke roundtrip_fn (file-based).
template <typename T, typename F>
static void RunDatasetRoundtrip(const std::string &data_dir,
                                const std::string &rel_path,
                                size_t max_count, F &&roundtrip_fn) {
    std::string path = data_dir + "/" + rel_path;
    auto input = ReadCSV<T>(path, max_count);
    // ASSERT_FALSE(input.empty()) << "Failed to load: " << path;
    roundtrip_fn(input, rel_path);
}

/// Use inline NamedDataset data directly (no file I/O).
template <typename T, typename F>
static void RunDatasetRoundtrip(const NamedDataset<T> &dataset,
                                size_t max_count, F &&roundtrip_fn) {
    ASSERT_FALSE(dataset.data.empty()) << "Empty dataset: " << dataset.name;
    size_t n = std::min(dataset.data.size(), max_count);
    std::vector<T> input(dataset.data.begin(), dataset.data.begin() + n);
    roundtrip_fn(input, dataset.name);
}
