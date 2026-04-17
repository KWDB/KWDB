#include "ts_coding.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include "ts_bufferbuilder.h"

// Verifies fixed-width integer helpers encode and decode values in canonical
// little-endian byte order.
TEST(FixedCoding, FixedIntegersUseLittleEndianEncoding) {
  char u16[sizeof(uint16_t)];
  char u32[sizeof(uint32_t)];
  char u64[sizeof(uint64_t)];

  kwdbts::EncodeFixed16(u16, 0x1234);
  kwdbts::EncodeFixed32(u32, 0x12345678U);
  kwdbts::EncodeFixed64(u64, 0x0102030405060708ULL);

  const std::string expected16{"\x34\x12", sizeof(u16)};
  const std::string expected32{"\x78\x56\x34\x12", sizeof(u32)};
  const std::string expected64{"\x08\x07\x06\x05\x04\x03\x02\x01", sizeof(u64)};

  EXPECT_EQ(std::string(u16, sizeof(u16)), expected16);
  EXPECT_EQ(std::string(u32, sizeof(u32)), expected32);
  EXPECT_EQ(std::string(u64, sizeof(u64)), expected64);

  EXPECT_EQ(kwdbts::DecodeFixed16(expected16.data()), 0x1234);
  EXPECT_EQ(kwdbts::DecodeFixed32(expected32.data()), 0x12345678U);
  EXPECT_EQ(kwdbts::DecodeFixed64(expected64.data()), 0x0102030405060708ULL);
}

// Verifies the timestamp64 helpers preserve the raw 64-bit bit pattern while
// encoding and decoding little-endian bytes.
TEST(FixedCoding, Timestamp64UsesLittleEndianBitPatternEncoding) {
  const timestamp64 ts = static_cast<timestamp64>(0x8122334455667788LL);
  char encoded[sizeof(ts)];

  kwdbts::EncodeFixedTimestamp64(encoded, ts);
  const std::string expected{"\x88\x77\x66\x55\x44\x33\x22\x81", sizeof(encoded)};
  EXPECT_EQ(std::string(encoded, sizeof(encoded)), expected);
  EXPECT_EQ(kwdbts::DecodeFixedTimestamp64(expected.data()), ts);
}

TEST(BitWriter, WriteBits) {
  kwdbts::TsBufferBuilder buffer;
  {
    kwdbts::TsBitWriter writer(&buffer);
    EXPECT_TRUE(writer.WriteBits(3, 0b101));
    EXPECT_TRUE(writer.WriteBits(4, 0b1100));
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0b10111000);

    EXPECT_TRUE(writer.WriteBits(9, 0b110110111));
    ASSERT_EQ(buffer.size(), 2);
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0b10111001);
    EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0b10110111);
  }
  {
    kwdbts::TsBitWriter writer(&buffer);
    EXPECT_TRUE(writer.WriteBits(10, 0b101));
    ASSERT_EQ(buffer.size(), 2);
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0b1);
    EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0b01000000);
    EXPECT_TRUE(writer.WriteBits(10, 0b10101101));
    ASSERT_EQ(buffer.size(), 3);
    EXPECT_EQ(static_cast<uint8_t>(buffer[1]), 0b01001010);
    EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0b11010000);
  }

  for (int i = 0; i < 8; ++i) {
    kwdbts::TsBufferBuilder buf;
    for (int j = 1; j <= 64; ++j) {
      kwdbts::TsBitWriter w{&buf};
      for (int n = 0; n < i; ++n) {
        w.WriteBit(0);
      }
      if (j != 64) {
        ASSERT_TRUE(w.WriteBits(j, (1ULL << j) - 1));
      } else {
        ASSERT_TRUE(w.WriteBits(j, -1));
      }

      ASSERT_EQ(buf.size(), (i + j - 1) / 8 + 1);

      kwdbts::TsBitReader r{buf.AsStringView()};
      bool b;
      for (int k = 0; k < i; ++k) {
        ASSERT_TRUE(r.ReadBit(&b));
        EXPECT_EQ(b, 0);
      }
      for(int k = 0; k < j; ++k){
        ASSERT_TRUE(r.ReadBit(&b));
        EXPECT_EQ(b, 1) << i << " , " << j << buf.AsStringView();
      }
      for (int k = 0; k < (8 - (i + j) % 8) % 8; ++k) {
        ASSERT_TRUE(r.ReadBit(&b)) << (i + j);
        EXPECT_EQ(b, 0);
      }
      ASSERT_FALSE(r.ReadBit(&b));
    }
  }
  {
    kwdbts::TsBufferBuilder buf;
    kwdbts::TsBitWriter w{&buf};
    w.WriteBits(64, 0x12345678);
    std::string exp{"\0\0\0\0\x12\x34\x56\x78", 8};
    ASSERT_EQ(buf.size(), 8);
    EXPECT_EQ(buf.AsStringView(), exp);
  }
}

TEST(BitWriter, WriteBit) {
  kwdbts::TsBufferBuilder buffer;
  struct TestCase {
    std::string binary;
    std::vector<uint8_t> number;
  };
  std::vector<TestCase> cases{{"00000001", {0x1}},
                              {"00001000", {0x8}},
                              {"01110001", {0x71}},
                              {"11111111", {0xFF}},
                              {"101010010010", {0xA9, 0x20}}};
  for (const auto &t : cases) {
    kwdbts::TsBitWriter writer(&buffer);
    for (auto c : t.binary) {
      writer.WriteBit(c == '1');
    }
    ASSERT_EQ(buffer.size(), t.number.size());
    for (int i = 0; i < t.number.size(); ++i) {
      EXPECT_EQ(static_cast<uint8_t>(buffer[i]), t.number[i]) << t.binary << " at: " << i;
    }
  }
}

TEST(BitWriter, WriteByte) {
  kwdbts::TsBufferBuilder buffer;
  {  // aligned write;
    kwdbts::TsBitWriter writer(&buffer);
    std::string expected;
    for (int i = 0; i < 256; ++i) {
      ASSERT_TRUE(writer.WriteByte(i)) << i;
      expected.push_back(i);
    }
    ASSERT_EQ(buffer.size(), expected.size());
    EXPECT_EQ(buffer.AsStringView(), expected);
  }
  {  // non-aligned write;
    kwdbts::TsBitWriter writer(&buffer);
    writer.WriteBit(1);
    writer.WriteBit(0);
    for (int i = 0; i < 256; ++i) {
      writer.WriteByte(i);
    }
    ASSERT_EQ(buffer.size(), 257);
    EXPECT_EQ(buffer[0], '\x80');
    for (uint32_t i = 1; i < 256; ++i) {
      EXPECT_EQ(static_cast<uint8_t>(buffer[i]), (((i - 1) & 0x03) << 6) | ((i & 0xFC) >> 2))
          << "idx " << i;
    }
    EXPECT_EQ(static_cast<uint8_t>(buffer[256]), 0xC0);
  }
}

TEST(BitReader, ReadByte) {
  std::string rep;
  for (int i = 0; i < 256; ++i) {
    rep.push_back(i);
  }
  {
    kwdbts::TsBitReader reader(rep);
    uint8_t c;
    for (int i = 0; i < 256; ++i) {
      ASSERT_TRUE(reader.ReadByte(&c)) << i;
      ASSERT_EQ(c, i);
    }
    ASSERT_FALSE(reader.ReadByte(&c));
  }
  {
    kwdbts::TsBitReader reader(rep, 3);
    uint8_t c;
    ASSERT_TRUE(reader.ReadByte(&c));
    ASSERT_EQ(c, 0);
    for (int i = 1; i < 255; ++i) {
      ASSERT_TRUE(reader.ReadByte(&c));
      ASSERT_EQ(c, (i << 3 & 0xFF) | (((i + 1) & 0xE0) >> 5));
    }
    ASSERT_FALSE(reader.ReadByte(&c));
  }
}

TEST(BitReader, ReadBit) {
  std::string rep = "\x12\x34\x56";
  std::string expected = "000100100011010001010110";
  kwdbts::TsBitReader reader(rep);
  for (int i = 0; i < 24; ++i) {
    bool bit;
    ASSERT_TRUE(reader.ReadBit(&bit));
    EXPECT_EQ(bit, expected[i] - '0');
  }
}

TEST(BitReader, ReadBits) {
  // that is 00110001_00110010_00110011_00110100_00110101_00110110_00110111_00111000_00111001
  std::string rep = "1234567890";
  kwdbts::TsBitReader reader(rep);
  uint64_t v;

  // split as
  // 0011'0001_0'011001'0_001100'11_001101'00_0011010'1_00110110_0'0110111_0011'1000_00111001
  EXPECT_TRUE(reader.ReadBits(4, &v));
  EXPECT_EQ(v, 0b0011);

  EXPECT_TRUE(reader.ReadBits(5, &v));
  EXPECT_EQ(v, 0b00010);

  EXPECT_TRUE(reader.ReadBits(6, &v));
  EXPECT_EQ(v, 0b011001);

  EXPECT_TRUE(reader.ReadBits(7, &v));
  EXPECT_EQ(v, 0b0001100);

  EXPECT_TRUE(reader.ReadBits(8, &v));
  EXPECT_EQ(v, 0b11001101);

  EXPECT_TRUE(reader.ReadBits(9, &v));
  EXPECT_EQ(v, 0b000011010);

  EXPECT_TRUE(reader.ReadBits(10, &v));
  EXPECT_EQ(v, 0b1001101100);

  EXPECT_TRUE(reader.ReadBits(11, &v));
  EXPECT_EQ(v, 0b01101110011);

  EXPECT_TRUE(reader.ReadBits(12, &v));
  EXPECT_EQ(v, 0b100000111001);

  std::string rep2 = "12345678";
  kwdbts::TsBitReader reader2(rep2);
  uint64_t v2;
  ASSERT_TRUE(reader2.ReadBits(64, &v2));
  EXPECT_EQ(v2, 0x3132333435363738);
}
