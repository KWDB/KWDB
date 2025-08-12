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
  faststring compression_buffer;
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

KStatus BlockCompressionCodec::Compress(const std::vector<KSlice>& inputs,
                                        KSlice* output,
                                        k_bool use_compression_buffer,
                                        size_t uncompressed_size,
                                        faststring* compressed_body1,
                                        std::string* compressed_body2) const {
  if (inputs.size() == 1) {
    return Compress(inputs[0], output, use_compression_buffer,
                    uncompressed_size, compressed_body1, compressed_body2);
  }
  std::string buf;
  // we compute total size to avoid more memory copy
  size_t total_size = KSlice::ComputeTotalSize(inputs);
  buf.reserve(total_size);
  for (auto& input : inputs) {
    buf.append(input.data, input.size);
  }
  return Compress(KSlice(buf), output, use_compression_buffer, uncompressed_size,
                  compressed_body1, compressed_body2);
}

class Lz4BlockCompression : public BlockCompressionCodec {
 public:
  Lz4BlockCompression()
      : BlockCompressionCodec(CompressionTypePB::LZ4_COMPRESSION) {}

  explicit Lz4BlockCompression(CompressionTypePB type) : BlockCompressionCodec(type) {}

  static const Lz4BlockCompression* Instance() {
    static Lz4BlockCompression s_instance;
    return &s_instance;
  }

  ~Lz4BlockCompression() override {
    if (context) {
      if (context->ctx) {
        LZ4_freeStream(context->ctx);
        context->ctx = nullptr;
      }
      delete context;
      context = nullptr;
    }
  };

  KStatus Compress(const KSlice& input, KSlice* output,
                   k_bool use_compression_buffer, size_t uncompressed_size,
                   faststring* compressed_body1,
                   std::string* compressed_body2) const override {
    return DoCompress(input, output, use_compression_buffer, uncompressed_size,
                      compressed_body1, compressed_body2);
  }

  KStatus Decompress(const KSlice& input, KSlice* output) const override {
    auto decompressed_len =
        LZ4_decompress_safe(input.data, output->data, input.size, output->size);
    if (decompressed_len < 0) {
      LOG_ERROR(
          "decompress faild , decompress_len %d, error=$0, input.size %ld, "
          "output->size %ld",
          decompressed_len, input.size, output->size);
      return KStatus::FAIL;
    }
    output->size = decompressed_len;
    return KStatus::SUCCESS;
  }

  size_t MaxCompressedLen(size_t len) const override {
    return LZ4_compressBound(len);
  }

  k_bool ExceedMaxInputSize(size_t len) const override {
    return len > LZ4_MAX_INPUT_SIZE;
  }

  size_t MaxInputSize() const override { return LZ4_MAX_INPUT_SIZE; }
  LZ4_stream_t* GetLz4() const override { return context->ctx; }

 private:
  KStatus DoCompress(const KSlice& input, KSlice* output,
                     k_bool use_compression_buffer, size_t uncompressed_size,
                     faststring* compressed_body1,
                     std::string* compressed_body2) const {
    auto context = LZ4StreamPool::GetInstance().GetLZ4Ctx();
    [[maybe_unused]] faststring* compression_buffer = nullptr;
    [[maybe_unused]] size_t max_len = 0;
    if (use_compression_buffer) {
      max_len = MaxCompressedLen(uncompressed_size);
      if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
        compression_buffer = &context->compression_buffer;
        compression_buffer->resize(max_len);
        output->data = reinterpret_cast<char*>(compression_buffer->data());
        output->size = max_len;
      } else {
        if (compressed_body1) {
          compressed_body1->resize(max_len);
          output->data = reinterpret_cast<char*>(compressed_body1->data());
        } else {
          compressed_body2->resize(max_len);
          output->data = reinterpret_cast<char*>(compressed_body2->data());
        }
        output->size = max_len;
      }
    }

    k_int32 acceleration = 1;
    size_t compressed_size =
        LZ4_compress_fast_continue(context->ctx, input.data, output->data,
                                   input.size, output->size, acceleration);
    if (compressed_size <= 0) {
      LOG_ERROR("compressed_size %ld, failed", compressed_size);
      return KStatus::FAIL;
    }
    output->size = compressed_size;

    if (use_compression_buffer) {
      if (max_len <= COMPRESSION_BUFFER_THRESHOLD) {
        compression_buffer->resize(output->size);
        if (compressed_body1) {
          compressed_body1->AssignCopy(compression_buffer->data(),
                                        compression_buffer->Size());
        } else {
          compressed_body2->clear();
          compressed_body2->resize(compression_buffer->Size());
          memcpy(compressed_body2->data(), compression_buffer->data(),
                 compression_buffer->Size());
        }
        compression_buffer->resize(0);
      } else {
        if (compressed_body1) {
          compressed_body1->resize(output->size);
        } else {
          compressed_body2->resize(output->size);
        }
      }
    }
    // LZ4_resetStream(context->ctx);
    return KStatus::SUCCESS;
  }

 private:
  LZ4CompressContext* context = nullptr;
};
KStatus GetBlockCompressionCodec(CompressionTypePB type, const BlockCompressionCodec** codec, int compression_level) {
  switch (type) {
    case CompressionTypePB::NO_COMPRESSION:
      *codec = nullptr;
      break;
    case CompressionTypePB::LZ4_COMPRESSION:
      *codec = Lz4BlockCompression::Instance();
      break;
    default:
      return KStatus::FAIL;
  }
  return KStatus::SUCCESS;
}

k_bool UseCompressionPool(CompressionTypePB type) {
  if (type == CompressionTypePB::LZ4_FRAME_COMPRESSION ||
      type == CompressionTypePB::ZSTD_COMPRESSION ||
      type == CompressionTypePB::LZ4_COMPRESSION) {
    return true;
  }
  return false;
}

}  // namespace kwdbts
