#pragma once

#include <string>

#include "libkwdbts2.h"
#include "snappy.h"
#include "ts_bitmap.h"
#include "ts_compressor.h"

namespace kwdbts {

class TsCompressorBase {
 public:
  virtual bool Compress(const TSSlice &raw, const TsBitmap *bitmap, uint32_t count,
                        std::string *out) const = 0;
  virtual bool Decompress(const TSSlice &raw, const TsBitmap *bitmap, uint32_t count,
                          std::string *out) const = 0;
};

class GenCompressorBase {
 public:
  virtual bool Compress(const TSSlice &raw, std::string *out) const = 0;
  virtual bool Decompress(const TSSlice &raw, std::string *out) const = 0;
};

class CompressorImpl {
 protected:
  CompressorImpl() = default;

 public:
  CompressorImpl(const CompressorImpl &) = delete;
  void operator=(const CompressorImpl &) = delete;
  virtual bool Compress(const TSSlice &data, uint64_t count, std::string *out) const = 0;
  virtual bool Decompress(const TSSlice &data, uint64_t count, std::string *out) const = 0;
};

class GorillaInt : public CompressorImpl {
 private:
  GorillaInt() = default;

 public:
  static GorillaInt &GetInstance() {
    static GorillaInt inst;
    return inst;
  }
  static constexpr int stride = 8;
  bool Compress(const TSSlice &data, uint64_t count, std::string *out) const override;
  bool Decompress(const TSSlice &data, uint64_t count, std::string *out) const override;
};

class GorillaIntV2 : public CompressorImpl {
 private:
  GorillaIntV2() = default;

 public:
  static GorillaIntV2 &GetInstance() {
    static GorillaIntV2 inst;
    return inst;
  }
  static constexpr int stride = 8;
  bool Compress(const TSSlice &data, uint64_t count, std::string *out) const override;
  bool Decompress(const TSSlice &data, uint64_t count, std::string *out) const override;
};

template <class T>
class Simple8BInt : public CompressorImpl {
 private:
  Simple8BInt() = default;

 public:
  static Simple8BInt &GetInstance() {
    static Simple8BInt inst;
    return inst;
  }
  static constexpr int stride = sizeof(T);
  bool Compress(const TSSlice &data, uint64_t count, std::string *out) const override;
  bool Decompress(const TSSlice &data, uint64_t count, std::string *out) const override;
};

class SnappyString : public CompressorImpl {
 private:
  SnappyString() = default;

 public:
  static constexpr int stride = -1;
  static SnappyString &GetInstance() {
    static SnappyString inst;
    return inst;
  }
  bool Compress(const TSSlice &data, uint64_t count, std::string *out) const override {
    snappy::Compress(data.data, data.len, out);
    return true;
  }
  bool Decompress(const TSSlice &data, uint64_t count, std::string *out) const override {
    snappy::Uncompress(data.data, data.len, out);
    return true;
  }
};

template <class Compressor>
class ConcreateTsCompressor : public TsCompressorBase {
 private:
  ConcreateTsCompressor() = default;

 public:
  static TsCompressorBase &GetInstance() {
    static ConcreateTsCompressor inst;
    return inst;
  }
  bool Compress(const TSSlice &raw, const TsBitmap *bitmap, uint32_t count,
                std::string *out) const override {
    assert(raw.len == count * 8);
    assert(bitmap == nullptr || bitmap->GetCount() == count);
    int stride = Compressor::stride;
    if (stride < 0 || bitmap == nullptr || bitmap->IsAllValid()) {
      return Compressor::GetInstance().Compress(raw, count, out);
    }

    std::string valid_data;
    const char *data = raw.data;
    out->reserve(bitmap->GetValidCount() * stride);
    for (int i = 0; i < count; ++i) {
      if ((*bitmap)[i] != kValid) continue;
      valid_data.append(data + i * stride, stride);
    }

    TSSlice input{reinterpret_cast<char *>(valid_data.data()), valid_data.size()};
    return Compressor::GetInstance().Compress(input, valid_data.size(), out);
  }

  bool Decompress(const TSSlice &raw, const TsBitmap *bitmap, uint32_t count,
                  std::string *out) const override {
    int stride = Compressor::stride;
    if (stride < 0 || bitmap == nullptr || bitmap->IsAllValid()) {
      return Compressor::GetInstance().Decompress(raw, count, out);
    }
    std::string buf;
    bool ok = Compressor::GetInstance().Decompress(raw, bitmap->GetCount(), &buf);
    if (!ok) {
      return false;
    }

    assert(buf.size() == bitmap->GetValidCount() * stride);
    const char *ptr = buf.data();
    for (int i = 0; i < count; ++i) {
      if ((*bitmap)[i] == kValid) {
        out->append(ptr, stride);
        ptr += stride;
      } else {
        out->append(stride, 0);
      }
    }

    return true;
  }
};

template <class Compressor>
class ConcreateGenCompressor : public GenCompressorBase {
 private:
  ConcreateGenCompressor() = default;

 public:
  static GenCompressorBase &GetInstance() {
    static ConcreateGenCompressor inst;
    return inst;
  }
  bool Compress(const TSSlice &raw, std::string *out) const override {
    const CompressorImpl &comp = Compressor::GetInstance();
    return comp.Compress(raw, 0, out);
  }
  bool Decompress(const TSSlice &raw, std::string *out) const override {
    const CompressorImpl &comp = Compressor::GetInstance();
    return comp.Decompress(raw, 0, out);
  }
};

}  // namespace kwdbts