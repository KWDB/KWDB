#include "ts_coding.h"

#include <gtest/gtest.h>

#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

TEST(BitWriter, WriteBits) {
  std::string buffer;
  {
    kwdbts::TsBitWriter writer(&buffer);
    EXPECT_TRUE(writer.WriteBits(3, 0b101));
    EXPECT_TRUE(writer.WriteBits(4, 0b1100));
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[0]), 0b10111000);

    EXPECT_TRUE(writer.WriteBits(9, 0b110110111));
    ASSERT_EQ(writer.Str().size(), 2);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[0]), 0b10111001);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[1]), 0b10110111);
  }
  {
    kwdbts::TsBitWriter writer(&buffer);
    EXPECT_TRUE(writer.WriteBits(10, 0b101));
    ASSERT_EQ(writer.Str().size(), 2);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[0]), 0b1);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[1]), 0b01000000);
    EXPECT_TRUE(writer.WriteBits(10, 0b10101101));
    ASSERT_EQ(writer.Str().size(), 3);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[1]), 0b01001010);
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[2]), 0b11010000);
  }

  for (int i = 0; i < 8; ++i) {
    std::string buf;
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

      ASSERT_EQ(w.Str().size(), (i + j - 1) / 8 + 1);

      kwdbts::TsBitReader r{buf};
      bool b;
      for (int k = 0; k < i; ++k) {
        ASSERT_TRUE(r.ReadBit(&b));
        EXPECT_EQ(b, 0);
      }
      for(int k = 0; k < j; ++k){
        ASSERT_TRUE(r.ReadBit(&b));
        EXPECT_EQ(b, 1) << i << " , " << j << buf;
      }
      for (int k = 0; k < (8 - (i + j) % 8) % 8; ++k) {
        ASSERT_TRUE(r.ReadBit(&b)) << (i + j);
        EXPECT_EQ(b, 0);
      }
      ASSERT_FALSE(r.ReadBit(&b));
    }
  }
  {
    std::string buf;
    kwdbts::TsBitWriter w{&buf};
    w.WriteBits(64, 0x12345678);
    std::string exp{"\0\0\0\0\x12\x34\x56\x78", 8};
    ASSERT_EQ(w.Str().size(), 8);
    EXPECT_EQ(buf, exp);
  }
}

TEST(BitWriter, WriteBit) {
  std::string buffer;
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
    ASSERT_EQ(writer.Str().size(), t.number.size());
    for (int i = 0; i < t.number.size(); ++i) {
      EXPECT_EQ(static_cast<uint8_t>(writer.Str()[i]), t.number[i]) << t.binary << " at: " << i;
    }
  }
}

TEST(BitWriter, WriteByte) {
  std::string buffer;
  {  // aligned write;
    kwdbts::TsBitWriter writer(&buffer);
    std::string expected;
    for (int i = 0; i < 256; ++i) {
      ASSERT_TRUE(writer.WriteByte(i)) << i;
      expected.push_back(i);
    }
    ASSERT_EQ(writer.Str().size(), expected.size());
    EXPECT_EQ(writer.Str(), expected);
  }
  {  // non-aligned write;
    kwdbts::TsBitWriter writer(&buffer);
    writer.WriteBit(1);
    writer.WriteBit(0);
    for (int i = 0; i < 256; ++i) {
      writer.WriteByte(i);
    }
    ASSERT_EQ(writer.Str().size(), 257);
    EXPECT_EQ(writer.Str()[0], '\x80');
    for (uint32_t i = 1; i < 256; ++i) {
      EXPECT_EQ(static_cast<uint8_t>(writer.Str()[i]), (((i - 1) & 0x03) << 6) | ((i & 0xFC) >> 2))
          << "idx " << i;
    }
    EXPECT_EQ(static_cast<uint8_t>(writer.Str()[256]), 0xC0);
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