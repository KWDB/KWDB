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

#include <string>
#include <vector>

#include "ee_block_compress.h"
#include "gtest/gtest.h"

namespace kwdbts {
namespace {

KSlice MakeWritableSlice(std::string& buffer) {
  return KSlice(buffer.data(), buffer.size());
}

}  // namespace

TEST(BlockCompressTest, GetBlockCompressorAndPoolUsage) {
  const BlockCompressor* codec = reinterpret_cast<const BlockCompressor*>(0x1);
  EXPECT_EQ(GetBlockCompressor(CompressionTypePB::NO_COMPRESSION, &codec),
            KStatus::SUCCESS);
  EXPECT_EQ(codec, nullptr);

  EXPECT_EQ(GetBlockCompressor(CompressionTypePB::LZ4_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);
  EXPECT_EQ(codec->GetCompressionType(), CompressionTypePB::LZ4_COMPRESSION);

  EXPECT_EQ(GetBlockCompressor(CompressionTypePB::SNAPPY_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);
  EXPECT_EQ(codec->GetCompressionType(), CompressionTypePB::SNAPPY_COMPRESSION);

  EXPECT_EQ(GetBlockCompressor(CompressionTypePB::LZ4_FRAME_COMPRESSION, &codec),
            KStatus::FAIL);

  EXPECT_TRUE(UseCompressionPool(CompressionTypePB::LZ4_COMPRESSION));
  EXPECT_TRUE(UseCompressionPool(CompressionTypePB::LZ4_FRAME_COMPRESSION));
  EXPECT_TRUE(UseCompressionPool(CompressionTypePB::ZSTD_COMPRESSION));
  EXPECT_FALSE(UseCompressionPool(CompressionTypePB::SNAPPY_COMPRESSION));
  EXPECT_FALSE(UseCompressionPool(CompressionTypePB::NO_COMPRESSION));
}

TEST(BlockCompressTest, Lz4CompressAndDecompressVariants) {
  const BlockCompressor* codec = nullptr;
  ASSERT_EQ(GetBlockCompressor(CompressionTypePB::LZ4_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);

  const std::string input =
      "alpha-beta-gamma-alpha-beta-gamma-alpha-beta-gamma";

  std::string compressed_buffer(codec->CalculateMaxCompressedLength(input.size()),
                                '\0');
  KSlice compressed = MakeWritableSlice(compressed_buffer);
  EXPECT_EQ(codec->CompressBlock(KSlice(input), &compressed), KStatus::SUCCESS);
  EXPECT_GT(compressed.data_size_, 0U);

  std::string decompressed_buffer(input.size(), '\0');
  KSlice decompressed = MakeWritableSlice(decompressed_buffer);
  EXPECT_EQ(codec->DecompressBlock(
                KSlice(compressed_buffer.data(), compressed.data_size_),
                &decompressed),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(decompressed.data_, decompressed.data_size_), input);

  QuickString compressed_fast;
  KSlice fast_output;
  EXPECT_EQ(codec->CompressBlock(KSlice(input), &fast_output, true, input.size(),
                                 &compressed_fast, nullptr),
            KStatus::SUCCESS);
  EXPECT_GT(compressed_fast.Size(), 0U);

  std::string fast_decoded(input.size(), '\0');
  KSlice fast_decompressed = MakeWritableSlice(fast_decoded);
  EXPECT_EQ(
      codec->DecompressBlock(
          KSlice(reinterpret_cast<const char*>(compressed_fast.data()),
                 compressed_fast.Size()),
          &fast_decompressed),
      KStatus::SUCCESS);
  EXPECT_EQ(std::string(fast_decompressed.data_, fast_decompressed.data_size_),
            input);

  std::string compressed_std;
  KSlice std_output;
  EXPECT_EQ(codec->CompressBlock(KSlice(input), &std_output, true, input.size(),
                                 nullptr, &compressed_std),
            KStatus::SUCCESS);
  EXPECT_FALSE(compressed_std.empty());

  std::string std_decoded(input.size(), '\0');
  KSlice std_decompressed = MakeWritableSlice(std_decoded);
  EXPECT_EQ(codec->DecompressBlock(KSlice(compressed_std), &std_decompressed),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(std_decompressed.data_, std_decompressed.data_size_),
            input);

  std::vector<KSlice> slices{KSlice("left-"), KSlice("middle-"),
                             KSlice("right")};
  std::string vector_compressed(codec->CalculateMaxCompressedLength(17), '\0');
  KSlice vector_output = MakeWritableSlice(vector_compressed);
  EXPECT_EQ(codec->CompressBlock(slices, &vector_output), KStatus::SUCCESS);

  std::string vector_decoded(17, '\0');
  KSlice vector_decompressed = MakeWritableSlice(vector_decoded);
  EXPECT_EQ(codec->DecompressBlock(
                KSlice(vector_compressed.data(), vector_output.data_size_),
                &vector_decompressed),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(vector_decompressed.data_,
                        vector_decompressed.data_size_),
            "left-middle-right");

  std::string bogus = "not-a-valid-lz4-frame";
  std::string bogus_output(input.size(), '\0');
  KSlice bogus_decompressed = MakeWritableSlice(bogus_output);
  EXPECT_EQ(codec->DecompressBlock(KSlice(bogus), &bogus_decompressed),
            KStatus::FAIL);
}

TEST(BlockCompressTest, Lz4HandlesSingleSliceVectorsAndLargeBuffers) {
  const BlockCompressor* codec = nullptr;
  ASSERT_EQ(GetBlockCompressor(CompressionTypePB::LZ4_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);

  const std::string input = "single-slice-vector-input";
  const std::vector<KSlice> single_slice{KSlice(input)};

  std::string compressed_direct(codec->CalculateMaxCompressedLength(input.size()),
                                '\0');
  KSlice direct_output = MakeWritableSlice(compressed_direct);
  EXPECT_EQ(codec->CompressBlock(single_slice, &direct_output),
            KStatus::SUCCESS);
  EXPECT_GT(direct_output.data_size_, 0U);

  QuickString compressed_fast;
  KSlice fast_output;
  EXPECT_EQ(codec->CompressBlock(single_slice, &fast_output, true,
                                 11 * 1024 * 1024, &compressed_fast, nullptr),
            KStatus::SUCCESS);
  EXPECT_GT(compressed_fast.Size(), 0U);

  std::string decoded_fast(input.size(), '\0');
  KSlice decoded_fast_slice = MakeWritableSlice(decoded_fast);
  EXPECT_EQ(
      codec->DecompressBlock(
          KSlice(reinterpret_cast<const char*>(compressed_fast.data()),
                 compressed_fast.Size()),
          &decoded_fast_slice),
      KStatus::SUCCESS);
  EXPECT_EQ(std::string(decoded_fast_slice.data_, decoded_fast_slice.data_size_),
            input);

  std::string compressed_std;
  KSlice std_output;
  EXPECT_EQ(codec->CompressBlock(single_slice, &std_output, true,
                                 11 * 1024 * 1024, nullptr, &compressed_std),
            KStatus::SUCCESS);
  EXPECT_FALSE(compressed_std.empty());

  std::string decoded_std(input.size(), '\0');
  KSlice decoded_std_slice = MakeWritableSlice(decoded_std);
  EXPECT_EQ(codec->DecompressBlock(KSlice(compressed_std), &decoded_std_slice),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(decoded_std_slice.data_, decoded_std_slice.data_size_),
            input);

  EXPECT_FALSE(codec->CheckIfExceedsMaxInputSize(codec->GetMaxInputSize()));
  EXPECT_TRUE(
      codec->CheckIfExceedsMaxInputSize(codec->GetMaxInputSize() + 1));
}

TEST(BlockCompressTest, SnappyCompressAndDecompressVariants) {
  const BlockCompressor* codec = nullptr;
  ASSERT_EQ(GetBlockCompressor(CompressionTypePB::SNAPPY_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);

  const std::string input = "snappy-data-snappy-data-snappy-data";

  std::string compressed_buffer(codec->CalculateMaxCompressedLength(input.size()),
                                '\0');
  KSlice compressed = MakeWritableSlice(compressed_buffer);
  EXPECT_EQ(codec->CompressBlock(KSlice(input), &compressed), KStatus::SUCCESS);

  std::string decompressed_buffer(input.size(), '\0');
  KSlice decompressed = MakeWritableSlice(decompressed_buffer);
  EXPECT_EQ(codec->DecompressBlock(
                KSlice(compressed_buffer.data(), compressed.data_size_),
                &decompressed),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(decompressed.data_, decompressed.data_size_), input);

  std::vector<KSlice> slices{KSlice("snappy-"), KSlice("vector-"),
                             KSlice("payload")};
  std::string vector_compressed(codec->CalculateMaxCompressedLength(21), '\0');
  KSlice vector_output = MakeWritableSlice(vector_compressed);
  EXPECT_EQ(codec->CompressBlock(slices, &vector_output), KStatus::SUCCESS);

  std::string vector_decoded(21, '\0');
  KSlice vector_decompressed = MakeWritableSlice(vector_decoded);
  EXPECT_EQ(codec->DecompressBlock(
                KSlice(vector_compressed.data(), vector_output.data_size_),
                &vector_decompressed),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(vector_decompressed.data_,
                        vector_decompressed.data_size_),
            "snappy-vector-payload");

  std::string bogus = "not-snappy";
  std::string bogus_output(input.size(), '\0');
  KSlice bogus_decompressed = MakeWritableSlice(bogus_output);
  EXPECT_EQ(codec->DecompressBlock(KSlice(bogus), &bogus_decompressed),
            KStatus::FAIL);
}

TEST(BlockCompressTest, SnappyIgnoresNullSlicesAndEmptyInputs) {
  const BlockCompressor* codec = nullptr;
  ASSERT_EQ(GetBlockCompressor(CompressionTypePB::SNAPPY_COMPRESSION, &codec),
            KStatus::SUCCESS);
  ASSERT_NE(codec, nullptr);

  std::vector<KSlice> slices{KSlice("snappy-"),
                             KSlice(static_cast<const char*>(nullptr),
                                    static_cast<size_t>(0)),
                             KSlice("payload")};
  std::string compressed(codec->CalculateMaxCompressedLength(14), '\0');
  KSlice output = MakeWritableSlice(compressed);
  EXPECT_EQ(codec->CompressBlock(slices, &output), KStatus::SUCCESS);

  std::string decoded(14, '\0');
  KSlice decoded_slice = MakeWritableSlice(decoded);
  EXPECT_EQ(codec->DecompressBlock(
                KSlice(compressed.data(), output.data_size_), &decoded_slice),
            KStatus::SUCCESS);
  EXPECT_EQ(std::string(decoded_slice.data_, decoded_slice.data_size_),
            "snappy-payload");

  std::vector<KSlice> empty_slices;
  std::string empty_compressed(codec->CalculateMaxCompressedLength(0), '\0');
  KSlice empty_output = MakeWritableSlice(empty_compressed);
  EXPECT_EQ(codec->CompressBlock(empty_slices, &empty_output),
            KStatus::SUCCESS);
}

}  // namespace kwdbts
