// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan
// PSL v2. You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
// NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
// Mulan PSL v2 for more details.

#include "ee_block_compress.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <deque>

#include "lg_api.h"

namespace kwdbts {

static int COMPRESSION_BUFFER_THRESHOLD = 1024 * 1024 * 10;

struct LZ4CompressContext {
  LZ4CompressContext() : ctx(LZ4_createStream()) {
  }

  // LZ4 compression context
  LZ4_stream_t* ctx{nullptr};

  k_bool compression_fail{false};
  k_uint32 compression_count{0};
  QuickString compression_buffer;
  ~LZ4CompressContext() {
    LZ4_freeStream(ctx);
  }

  // reset
  void Resetter() {
    LZ4_resetStream(ctx);
  }
};
// single instance
class LZ4StreamPool {
 private:
  std::deque<std::unique_ptr<LZ4CompressContext>> resources;
  std::mutex mutex;
  size_t max_pool_size = 16;
  struct ContextDeleter {
    LZ4StreamPool* pool;

    explicit ContextDeleter(LZ4StreamPool* p) : pool(p) {
    }
    void operator()(LZ4CompressContext* ctx) const {
      if (pool) {
        pool->Add(std::unique_ptr<LZ4CompressContext>(ctx));
      }
    }
  };

  LZ4StreamPool() = default;
  ~LZ4StreamPool() = default;

 public:
  LZ4StreamPool(const LZ4StreamPool&) = delete;
  LZ4StreamPool& operator=(const LZ4StreamPool&) = delete;

 public:
  using Ref = std::unique_ptr<LZ4CompressContext, ContextDeleter>;

  static LZ4StreamPool& GetInstance() {
    static LZ4StreamPool instance;
    return instance;
  }

  // pool size
  void SetMaxPoolSize(size_t size) {
    std::lock_guard<std::mutex> lock(mutex);
    max_pool_size = size;
  }

  // get stream
  Ref GetLZ4Ctx() {
    std::lock_guard<std::mutex> lock(mutex);
    if (!resources.empty()) {
      auto ptr = std::move(resources.back());
      resources.pop_back();
      return Ref(ptr.release(), ContextDeleter(this));
    }
    return Ref(new LZ4CompressContext(), ContextDeleter(this));
  }

  void Add(std::unique_ptr<LZ4CompressContext>&& ctx) {
    if (ctx) {
      std::lock_guard<std::mutex> lock(mutex);
      if (resources.size() < max_pool_size) {
        ctx->Resetter();
        resources.emplace_back(std::move(ctx));
      }
    }
  }
};

KStatus BlockCompressor::CompressBlock(const std::vector<KSlice>& inputs,
                                        KSlice* output,
                                        k_bool use_compressed_buff,
                                        size_t uncompressed_size,
                                        QuickString* compressed_fast,
                                        std::string* compressed_std) const {
  if (inputs.size() == 1) {
    return CompressBlock(inputs[0], output, use_compressed_buff,
                    uncompressed_size, compressed_fast, compressed_std);
  }
  std::string buf;
  // we compute total size to avoid more memory copy
  size_t total_size = KSlice::ComputeTotalSize(inputs);
  buf.reserve(total_size);
  for (auto& input : inputs) {
    buf.append(input.data_, input.data_size_);
  }
  return CompressBlock(KSlice(buf), output, use_compressed_buff, uncompressed_size,
                  compressed_fast, compressed_std);
}

class Lz4BlockCompressor : public BlockCompressor {
 public:
  Lz4BlockCompressor()
      : BlockCompressor(CompressionTypePB::LZ4_COMPRESSION) {}

  explicit Lz4BlockCompressor(CompressionTypePB type) : BlockCompressor(type) {}

  static const Lz4BlockCompressor* Instance() {
    static Lz4BlockCompressor s_instance;
    return &s_instance;
  }

  ~Lz4BlockCompressor() override {
    if (context) {
      if (context->ctx) {
        LZ4_freeStream(context->ctx);
        context->ctx = nullptr;
      }
      delete context;
      context = nullptr;
    }
  };

  KStatus CompressBlock(const KSlice& input, KSlice* output,
                   k_bool use_compressed_buff, size_t uncompressed_size,
                   QuickString* compressed_fast,
                   std::string* compressed_std) const override {
    return DoCompressBlock(input, output, use_compressed_buff, uncompressed_size,
                      compressed_fast, compressed_std);
  }

  KStatus DecompressBlock(const KSlice& input, KSlice* output) const override {
    auto decompressed_len =
        LZ4_decompress_safe(input.data_, output->data_, input.data_size_, output->data_size_);
    if (decompressed_len < 0) {
      LOG_ERROR(
          "decompress faild , decompress_len %d, error=$0, input.size %ld, "
          "output->size %ld",
          decompressed_len, input.data_size_, output->data_size_);
      return KStatus::FAIL;
    }
    output->data_size_ = decompressed_len;
    return KStatus::SUCCESS;
  }

  size_t CalculateMaxCompressedLength(size_t len) const override {
    return LZ4_compressBound(len);
  }

  k_bool CheckIfExceedsMaxInputSize(size_t len) const override {
    return len > LZ4_MAX_INPUT_SIZE;
  }

  size_t GetMaxInputSize() const override {
    return LZ4_MAX_INPUT_SIZE;
  }
  LZ4_stream_t* GetLz4StreamObject() const override {
    return context->ctx;
  }

 private:
  KStatus DoCompressBlock(const KSlice& input, KSlice* output,
                     k_bool use_compressed_buff, size_t uncompressed_size,
                     QuickString* compressed_fast,
                     std::string* compressed_std) const {
    auto context = LZ4StreamPool::GetInstance().GetLZ4Ctx();
    [[maybe_unused]] QuickString* compression_buffer = nullptr;
    [[maybe_unused]] size_t max_len = 0;
    if (use_compressed_buff) {
      max_len = CalculateMaxCompressedLength(uncompressed_size);
      if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
        compression_buffer = &context->compression_buffer;
        compression_buffer->resize(max_len);
        output->data_ = reinterpret_cast<char*>(compression_buffer->data());
        output->data_size_ = max_len;
      } else {
        if (compressed_fast) {
          compressed_fast->resize(max_len);
          output->data_ = reinterpret_cast<char*>(compressed_fast->data());
        } else {
          compressed_std->resize(max_len);
          output->data_ = reinterpret_cast<char*>(compressed_std->data());
        }
        output->data_size_ = max_len;
      }
    }

    k_int32 acceleration = 1;
    size_t compressed_size =
        LZ4_compress_fast_continue(context->ctx, input.data_, output->data_,
                                   input.data_size_, output->data_size_, acceleration);
    if (compressed_size <= 0) {
      LOG_ERROR("compressed_size %ld, failed", compressed_size);
      return KStatus::FAIL;
    }
    output->data_size_ = compressed_size;

    if (use_compressed_buff) {
      if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
        compression_buffer->resize(output->data_size_);
        if (compressed_fast) {
          compressed_fast->AssignCopy(compression_buffer->data(),
                                        compression_buffer->Size());
        } else {
          compressed_std->clear();
          compressed_std->resize(compression_buffer->Size());
          memcpy(compressed_std->data(), compression_buffer->data(),
                 compression_buffer->Size());
        }
        compression_buffer->resize(0);
      } else {
        if (compressed_fast) {
          compressed_fast->resize(output->data_size_);
        } else {
          compressed_std->resize(output->data_size_);
        }
      }
    }
    // LZ4_resetStream(context->ctx);
    return KStatus::SUCCESS;
  }

 private:
  LZ4CompressContext *context = nullptr;
};

class SnappySlicesSource : public snappy::Source {
 public:
  explicit SnappySlicesSource(const std::vector<KSlice>& slices) {
    for (auto& slice : slices) {
      // We filter empty slice here to avoid complicated process
      if (slice.data_ == 0) {
        continue;
      }
      available_ += slice.data_size_;
      slices_.push_back(slice);
    }
  }

  ~SnappySlicesSource() override = default;

  // Return the number of bytes left to read from the source
  size_t Available() const override { return available_; }

  // Peek at the next flat region of the source.  Does not reposition
  // the source.  The returned region is empty iff Available()==0.
  //
  // Returns a pointer to the beginning of the region and store its
  // length in *len.
  //
  // The returned region is valid until the next call to Skip() or
  // until this object is destroyed, whichever occurs first.
  //
  // The returned region may be larger than Available() (for example
  // if this ByteSource is a view on a substring of a larger source).
  // The caller is responsible for ensuring that it only reads the
  // Available() bytes.
  const char* Peek(size_t* len) override {
    if (available_ == 0) {
      *len = 0;
      return nullptr;
    }
    // we should assure that *len is not 0
    *len = slices_[cur_slice_].data_size_ - slice_off_;
    return slices_[cur_slice_].data_ + slice_off_;
  }

  // Skip the next n bytes.  Invalidates any buffer returned by
  // a previous call to Peek().
  // REQUIRES: Available() >= n
  void Skip(size_t n) override {
    // DCHECK(available_ >= n);
    available_ -= n;
    while (n > 0) {
      auto left = slices_[cur_slice_].data_size_ - slice_off_;
      if (left > n) {
        // n can be digest in current slice
        slice_off_ += n;
        return;
      }
      slice_off_ = 0;
      cur_slice_++;
      n -= left;
    }
  }

 private:
  std::vector<KSlice> slices_;
  size_t available_{0};
  size_t cur_slice_{0};
  size_t slice_off_{0};
};

class SnappyBlockCompressor : public BlockCompressor {
 public:
  SnappyBlockCompressor() : BlockCompressor(CompressionTypePB::SNAPPY_COMPRESSION) {}

  static const SnappyBlockCompressor* Instance() {
    static SnappyBlockCompressor s_instance;
    return &s_instance;
  }

  ~SnappyBlockCompressor() override = default;

  KStatus CompressBlock(const KSlice& input, KSlice* output, k_bool use_compressed_buff, size_t uncompressed_size,
                        QuickString* compressed_fast, std::string* compressed_std) const override {
    snappy::RawCompress(input.data_, input.data_size_, output->data_, &output->data_size_);
    return KStatus::SUCCESS;
  }

  KStatus DecompressBlock(const KSlice& input, KSlice* output) const override {
    if (!snappy::RawUncompress(input.data_, input.data_size_, output->data_)) {
      return KStatus::FAIL;
    }
    // NOTE: GetUncompressedLength only takes O(1) time
    snappy::GetUncompressedLength(input.data_, input.data_size_, &output->data_size_);
    return KStatus::SUCCESS;
  }

  KStatus CompressBlock(const std::vector<KSlice>& inputs, KSlice* output, k_bool use_compression_buffer,
                        size_t uncompressed_size, QuickString* compressed_fast,
                        std::string* compressed_std) const override {
    SnappySlicesSource source(inputs);
    snappy::UncheckedByteArraySink sink(output->data_);
    output->data_size_ = snappy::Compress(&source, &sink);
    return KStatus::SUCCESS;
  }

  size_t CalculateMaxCompressedLength(size_t len) const override { return snappy::MaxCompressedLength(len); }
};

KStatus GetBlockCompressor(CompressionTypePB type, const BlockCompressor** compressor, int compression_level) {
  switch (type) {
    case CompressionTypePB::NO_COMPRESSION:
      *compressor = nullptr;
      break;
    case CompressionTypePB::LZ4_COMPRESSION:
      *compressor = Lz4BlockCompressor::Instance();
      break;
    case CompressionTypePB::SNAPPY_COMPRESSION:
      *compressor = SnappyBlockCompressor::Instance();
      break;
    default:
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}
k_bool UseCompressionPool(CompressionTypePB type) {
  if (type == CompressionTypePB::LZ4_FRAME_COMPRESSION || type == CompressionTypePB::ZSTD_COMPRESSION ||
      type == CompressionTypePB::LZ4_COMPRESSION) {
    return true;
  }
  return false;
}

}  // namespace kwdbts
