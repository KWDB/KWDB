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

#include <endian.h>

#include "data_type.h"
#include "ts_common.h"
#include "ts_compressor.h"

namespace kwdbts {

#define REGISTER_DECOMPRESSOR(class_name, id)           \
  {                                                     \
    decompressor_[id] = std::make_unique<class_name>(); \
    decompressor_[id]->SetID(id);                       \
  }

#define REGISTER_POLICY_COMPRESSOR(data_type, compressor_id) { \
  compressor_[data_type] = decompressor_[compressor_id].get(); \
}

#define is_bigendian() (__BYTE_ORDER == __BIG_ENDIAN)
#define SIMPLE8B_MAX_INT64 ((uint64_t)2305843009213693951L)

#define safeInt64Add(a, b) (((a >= 0) && (b <= INT64_MAX - a)) || ((a < 0) && (b >= INT64_MIN - a)))
#define ZIGZAG_ENCODE(T, v) ((u##T)((v) >> (sizeof(T) * 8 - 1))) ^ (((u##T)(v)) << 1)  // zigzag encode
#define ZIGZAG_DECODE(T, v) ((v) >> 1) ^ -((T)((v)&1))                                 // zigzag decode

void ColumnCompressorMgr::initDeCompressor() {
  decompressor_.clear();
  // WARNNING: only append, cannot modify lines which already exists.
  // const number is compressor id for that compressor class.
  REGISTER_DECOMPRESSOR(ColumnCompressorPlain,  0);
  REGISTER_DECOMPRESSOR(ColumnCompressorInt32,  1);
  REGISTER_DECOMPRESSOR(ColumnCompressorDouble, 2);
  REGISTER_DECOMPRESSOR(ColumnCompressorTS,     3);
}

bool ColumnCompressorMgr::ResetPolicy(ColumnCompressorPolicy policy) {
  RW_LATCH_X_LOCK(compressor_mutex_);
  Defer defer([&]() { RW_LATCH_UNLOCK(compressor_mutex_); });
  compress_policy_ = policy;
  compressor_.clear();
  switch (compress_policy_) {
  case ColumnCompressorPolicy::PLAIN:
    // no need
    return true;
  case ColumnCompressorPolicy::LIGHT_COMRESS:
    REGISTER_POLICY_COMPRESSOR(DATATYPE::INT32, 1);
    REGISTER_POLICY_COMPRESSOR(DATATYPE::DOUBLE, 2);
    REGISTER_POLICY_COMPRESSOR(DATATYPE::TIMESTAMP64, 3);
    return true;
  case ColumnCompressorPolicy::HEAVY_COMRESS:
    LOG_ERROR("not implimented.");
    return false;

  default:
    break;
  }
  LOG_ERROR("can not found policy.");
  return false;
}

ColumnCompressorMgr::ColumnCompressorMgr() {
  compressor_mutex_ = new KRWLatch(RWLATCH_ID_COMPRESSOR_RWLOCK);
}

ColumnCompressorMgr::~ColumnCompressorMgr() {
  compressor_.clear();
  delete compressor_mutex_;
  compressor_mutex_ = nullptr;
  decompressor_.clear();
}

bool ColumnCompressorMgr::Init() {
  initDeCompressor();
  return ResetPolicy(compress_policy_);
}

TSSlice ColumnCompressorMgr::Encode(TSSlice plain, TSSlice bitmap, uint32_t count, DATATYPE type) {
  RW_LATCH_S_LOCK(compressor_mutex_);
  Defer defer([&]() { RW_LATCH_UNLOCK(compressor_mutex_); });
  switch (type) {
    case TIMESTAMP64:
    case TIMESTAMP64_MICRO:
    case TIMESTAMP64_NANO:
    case TIMESTAMP64_LSN:
    case TIMESTAMP64_LSN_MICRO:
    case TIMESTAMP64_LSN_NANO:
      type = TIMESTAMP64;
    default:
      break;
  }
  auto iter = compressor_.find(type);
  if (iter != compressor_.end()) {
    return iter->second->Compress(plain, bitmap, count);
  }
  // LOG_WARN("cannot found compressor for type[%d], we using plain_compressor.", type);
  return decompressor_[0]->Compress(plain, bitmap, count);
}

bool ColumnCompressorMgr::Decode(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) {
  auto compressor_id = KUint32(data.data);
  auto iter = decompressor_.find(compressor_id);
  if (iter != decompressor_.end()) {
    return iter->second->DeCompress(data, count, plain, bitmap);
  }
  LOG_WARN("cannot found decompressor[%u].", compressor_id);
  return false;
}

char* ColumnBlockCompressor::mallocWithID(size_t size, TSSlice* col_compressed) {
  auto mall_size = size + sizeof(uint32_t);
  char* mem = reinterpret_cast<char*>(malloc(mall_size));
  if (mem != nullptr) {
    KUint32(mem) = compressor_id_;
    col_compressed->data = mem;
    col_compressed->len = mall_size;
    return mem + sizeof(uint32_t);
  }
  return mem;
}

TSSlice ColumnCompressorPlain::Compress(TSSlice plain, TSSlice bitmap, uint32_t count) {
  uint32_t bitmap_len = (count + 7) / 8;
  TSSlice ret;
  char* mem = mallocWithID(plain.len + bitmap_len, &ret);
  if (mem != nullptr) {
    memcpy(mem, bitmap.data, bitmap_len);
    memcpy(mem + bitmap_len, plain.data, plain.len);
    return ret;
  }
  LOG_ERROR("malloc memory failed.");
  return TSSlice{nullptr, 0};
}

bool ColumnCompressorPlain::DeCompress(TSSlice data, uint32_t count, TSSlice* plain, TSSlice* bitmap) {
  assert(KUint32(data.data) == compressor_id_);
  char* data_begin = data.data + sizeof(uint32_t);
  uint32_t bitmap_len = (count + 7) / 8;
  char* bitmap_addr = reinterpret_cast<char*>(malloc(bitmap_len));
  if (bitmap_addr != nullptr) {
    memcpy(bitmap_addr, data_begin, bitmap_len);
    auto col_data_len = data.len - bitmap_len - sizeof(uint32_t);
    char* mem = reinterpret_cast<char*>(malloc(col_data_len));
    if (mem != nullptr) {
      memcpy(mem, data_begin + bitmap_len, col_data_len);
      bitmap->data = bitmap_addr;
      bitmap->len = bitmap_len;
      plain->data = mem;
      plain->len = col_data_len;
      return true;
    }
  }
  LOG_ERROR("malloc memory failed.");
  if (bitmap_addr != nullptr) {
    free(bitmap_addr);
  }
  return false;
}

TSSlice ColumnCompressorTS::Compress(TSSlice plain, TSSlice bitmap, uint32_t count) {
  int _pos = 1;
  if (count == 0) {
    LOG_WARN("The size of data to be compressed is 0.");
    return TSSlice{nullptr, 0};
  }
  uint32_t bitmap_len = (count + 7) / 8;
  TSSlice ret;
  char *mem = mallocWithID(plain.len + bitmap_len + 1, &ret);
  if (mem != nullptr) {
    memcpy(mem, bitmap.data, bitmap_len);
    char* output = mem + bitmap_len;
    char* input = plain.data;
    int64_t *input_stream = reinterpret_cast<int64_t *>(input);

    int64_t prev_value = input_stream[0];

    int64_t prev_delta = -prev_value;
    uint8_t flags = 0, flag1 = 0, flag2 = 0;
    uint64_t dd1 = 0, dd2 = 0;
    if (prev_value >= 0x8000000000000000) {
      //        uWarn("compression timestamp is over signed long long range. ts = 0x%"PRIx64" \n", prev_value);
      goto _exit_over;
    }

    for (int i = 0; i < count; i++) {
      int64_t curr_value = input_stream[i];
      if (!safeInt64Add(curr_value, -prev_value)) goto _exit_over;
      int64_t curr_delta = curr_value - prev_value;
      if (!safeInt64Add(curr_delta, -prev_delta)) goto _exit_over;
      int64_t delta_of_delta = curr_delta - prev_delta;
      // zigzag encode the value.
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, delta_of_delta);
      if (i % 2 == 0) {
        flags = 0;
        dd1 = zigzag_value;
        if (dd1 == 0) {
          flag1 = 0;
        } else {
          flag1 = (uint8_t) (LONG_BYTES - BUILDIN_CLZL(dd1) / BITS_PER_BYTE);
        }
      } else {
        dd2 = zigzag_value;
        if (dd2 == 0) {
          flag2 = 0;
        } else {
          flag2 = (uint8_t) (LONG_BYTES - BUILDIN_CLZL(dd2) / BITS_PER_BYTE);
        }
        flags = flag1 | (flag2 << 4);
        // Encode the flag.
        if ((_pos + CHAR_BYTES - 1) >= count * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, &flags, CHAR_BYTES);
        _pos += CHAR_BYTES;
        /* Here, we assume it is little endian encoding method. */
        // Encode dd1
        if (is_bigendian()) {
          if ((_pos + flag1 - 1) >= count * LONG_BYTES) goto _exit_over;
          memcpy(output + _pos, reinterpret_cast<char *>(&dd1) + LONG_BYTES - flag1, flag1);
        } else {
          if ((_pos + flag1 - 1) >= count * LONG_BYTES) goto _exit_over;
          memcpy(output + _pos, reinterpret_cast<char *>(&dd1), flag1);
        }
        _pos += flag1;
        // Encode dd2;
        if (is_bigendian()) {
          if ((_pos + flag2 - 1) >= count * LONG_BYTES) goto _exit_over;
          memcpy(output + _pos, reinterpret_cast<char *>(&dd2) + LONG_BYTES - flag2, flag2);
        } else {
          if ((_pos + flag2 - 1) >= count * LONG_BYTES) goto _exit_over;
          memcpy(output + _pos, reinterpret_cast<char *>(&dd2), flag2);
        }
        _pos += flag2;
      }
      prev_value = curr_value;
      prev_delta = curr_delta;
    }

    if (count % 2 == 1) {
      flag2 = 0;
      flags = flag1 | (flag2 << 4);
      // Encode the flag.
      if ((_pos + CHAR_BYTES - 1) >= count * LONG_BYTES) goto _exit_over;
      memcpy(output + _pos, &flags, CHAR_BYTES);
      _pos += CHAR_BYTES;
      // Encode dd1;
      if (is_bigendian()) {
        if ((_pos + flag1 - 1) >= count * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, reinterpret_cast<char *>(&dd1) + LONG_BYTES - flag1, flag1);
      } else {
        if ((_pos + flag1 - 1) >= count * LONG_BYTES) goto _exit_over;
        memcpy(output + _pos, reinterpret_cast<char *>(&dd1), flag1);
      }
      _pos += flag1;
    }

    output[0] = 1;  // Means the string is compressed
    ret.len = _pos;
    return ret;

  _exit_over:
    output[0] = 0;  // Means the string is not compressed
    memcpy(output + 1, input, count * LONG_BYTES);
    return ret;
  }
  LOG_ERROR("malloc memory failed.");
  return TSSlice{nullptr, 0};
}

bool ColumnCompressorTS::DeCompress(TSSlice data, uint32_t count, TSSlice *plain, TSSlice *bitmap) {
  assert(KUint32(data.data) == compressor_id_);
  if (data.data == nullptr || data.len == 0) {
    LOG_ERROR("TSSlice data is nullptr or data length is 0.");
    return false;
  }
  if (count == 0) {
    LOG_ERROR("Data count is 0.");
    return false;
  }

  char *data_begin = data.data + sizeof(uint32_t);
  uint32_t bitmap_len = (count + 7) / 8;
  char *bitmap_addr = reinterpret_cast<char *>(malloc(bitmap_len));
  if (bitmap_addr == nullptr) {
    LOG_ERROR("malloc bitmap memory failed.");
    return false;
  }
  memcpy(bitmap_addr, data_begin, bitmap_len);
  bitmap->data = bitmap_addr;
  bitmap->len = bitmap_len;
  auto col_data_len = data.len - bitmap_len - sizeof(uint32_t) - 1;  // extra 1 is compressed flag
  char *mem = reinterpret_cast<char *>(malloc(col_data_len));
  if (mem == nullptr) {
    LOG_ERROR("malloc column data memory failed.");
    free(bitmap_addr);
    return false;
  }
  char *input = data_begin + bitmap_len;
  if (input[0] == 0) {
    // not compressed
    // memcpy(output, input + 1, count * LONG_BYTES);
    memcpy(mem, data_begin + bitmap_len, col_data_len);
    bitmap->data = bitmap_addr;
    bitmap->len = bitmap_len;
    plain->data = mem;
    plain->len = col_data_len;
    return true;
  }
  // need Decompress
  int64_t *out_stream = reinterpret_cast<int64_t *>(mem);

  int ipos = 1, opos = 0;
  int8_t nbytes = 0;
  int64_t prev_value = 0;
  int64_t prev_delta = 0;
  int64_t delta_of_delta = 0;

  while (1) {
    uint8_t flags = input[ipos++];
    // Decode dd1
    uint64_t dd1 = 0;
    nbytes = flags & INT8MASK(4);
    if (nbytes == 0) {
      delta_of_delta = 0;
    } else {
      if (is_bigendian()) {
        memcpy(reinterpret_cast<char *>(&dd1) + LONG_BYTES - nbytes, input + ipos, nbytes);
      } else {
        memcpy(&dd1, input + ipos, nbytes);
      }
      delta_of_delta = ZIGZAG_DECODE(int64_t, dd1);
    }
    ipos += nbytes;
    if (opos == 0) {
      prev_value = delta_of_delta;
      prev_delta = 0;
      out_stream[opos++] = delta_of_delta;
    } else {
      prev_delta = delta_of_delta + prev_delta;
      prev_value = prev_value + prev_delta;
      out_stream[opos++] = prev_value;
    }
    if (opos == count) {
      plain->data = reinterpret_cast<char *>(out_stream);
      plain->len = count * LONG_BYTES;
      return true;
    }

    // Decode dd2
    uint64_t dd2 = 0;
    nbytes = (flags >> 4) & INT8MASK(4);
    if (nbytes == 0) {
      delta_of_delta = 0;
    } else {
      if (is_bigendian()) {
        memcpy(reinterpret_cast<char *>(&dd2) + LONG_BYTES - nbytes, input + ipos, nbytes);
      } else {
        memcpy(&dd2, input + ipos, nbytes);
      }
      // zigzag_decoding
      delta_of_delta = ZIGZAG_DECODE(int64_t, dd2);
    }
    ipos += nbytes;
    prev_delta = delta_of_delta + prev_delta;
    prev_value = prev_value + prev_delta;
    out_stream[opos++] = prev_value;
    if (opos == count) {
      plain->data = reinterpret_cast<char *>(out_stream);
      plain->len = count * LONG_BYTES;
      return true;
    }
  }
}

TSSlice ColumnCompressorInt32::Compress(TSSlice plain, TSSlice bitmap, uint32_t count) {
  if (count == 0) {
    LOG_WARN("The size of data to be compressed is 0.");
    return TSSlice{nullptr, 0};
  }
  uint32_t bitmap_len = (count + 7) / 8;
  TSSlice ret;
  char *mem = mallocWithID(plain.len + bitmap_len + 1, &ret);
  if (mem == nullptr) {
    LOG_ERROR("malloc memory failed.");
    return TSSlice{nullptr, 0};
  }
  memcpy(mem, bitmap.data, bitmap_len);
  char *output = mem + bitmap_len;
  char *input = plain.data;
  // Selector value: 0    1   2   3   4   5   6   7   8  9  10  11 12  13  14  15
  char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};
  char bit_to_selector[] = {
    0, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10, 11, 11, 12, 12, 12, 13, 13, 13, 13, 13,
    14, 14, 14, 14, 14, 14, 14, 14, 14, 14, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15,
    15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15, 15
  };
  // get the byte limit.
  int word_length = INT_BYTES;
  int byte_limit = count * word_length + 1;
  int opos = 1;
  int64_t prev_value = 0;
  for (int i = 0; i < count;) {
    char selector = 0;
    char bit = 0;
    int elems = 0;
    int64_t prev_value_tmp = prev_value;
    uint64_t buffer = 0;
    for (int j = i; j < count; j++) {
      // Read data from the input stream and convert it to INT64 type.
      int64_t curr_value = 0;
      curr_value = (int64_t) (*(reinterpret_cast<int32_t *>(input) + j));
      // Get difference.
      if (!safeInt64Add(curr_value, -prev_value_tmp)) goto _copy_and_exit;
      int64_t diff = curr_value - prev_value_tmp;
      // Zigzag encode the value.
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);
      if (zigzag_value >= SIMPLE8B_MAX_INT64) goto _copy_and_exit;
      int64_t tmp_bit;
      if (zigzag_value == 0) {
        // Take care here, __builtin_clzl give wrong anser for value 0;
        tmp_bit = 0;
      } else {
        tmp_bit = (LONG_BYTES * BITS_PER_BYTE) - BUILDIN_CLZL(zigzag_value);
      }

      if (elems + 1 <= selector_to_elems[static_cast<int>(selector)]
        && elems + 1 <= selector_to_elems[static_cast<int>(bit_to_selector[static_cast<int>(tmp_bit)])]) {
        // If can hold another one.
        selector = selector > bit_to_selector[static_cast<int>(tmp_bit)] ?
          selector : bit_to_selector[static_cast<int>(tmp_bit)];
        elems++;
        bit = bit_per_integer[static_cast<int>(selector)];
      } else {
        // if cannot hold another one.
        while (elems < selector_to_elems[static_cast<int>(selector)]) selector++;
        elems = selector_to_elems[static_cast<int>(selector)];
        bit = bit_per_integer[static_cast<int>(selector)];
        break;
      }
      prev_value_tmp = curr_value;
    }
    buffer |= (uint64_t) selector;
    for (int k = 0; k < elems; k++) {
      int64_t curr_value = 0; /* get current values */
      curr_value = (int64_t) (*(reinterpret_cast<int32_t *>(input) + i));
      int64_t diff = curr_value - prev_value;
      uint64_t zigzag_value = ZIGZAG_ENCODE(int64_t, diff);
      buffer |= ((zigzag_value & INT64MASK(bit)) << (bit * k + 4));
      i++;
      prev_value = curr_value;
    }
    // Output the encoded value to the output.
    if (opos + sizeof(buffer) <= byte_limit) {
      memcpy(output + opos, &buffer, sizeof(buffer));
      opos += sizeof(buffer);
    } else {
    _copy_and_exit:
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      ret.len = plain.len + bitmap_len + 1;
      return ret;
    }
  }
  free(ret.data);
  // LOG_ERROR("ColumnCompressorInt32 compress failed.");
  return TSSlice{nullptr, 0};
}

bool ColumnCompressorInt32::DeCompress(TSSlice data, uint32_t count, TSSlice *plain, TSSlice *bitmap) {
  assert(KUint32(data.data) == compressor_id_);
  if (data.data == nullptr || data.len == 0) {
    LOG_ERROR("TSSlice data is nullptr or data length is 0.");
    return false;
  }
  if (count == 0) {
    LOG_ERROR("Data count is 0.");
    return false;
  }

  char *data_begin = data.data + sizeof(uint32_t);
  uint32_t bitmap_len = (count + 7) / 8;
  char *bitmap_addr = reinterpret_cast<char *>(malloc(bitmap_len));
  if (bitmap_addr == nullptr) {
    LOG_ERROR("malloc bitmap memory failed.");
    return false;
  }
  memcpy(bitmap_addr, data_begin, bitmap_len);
  bitmap->data = bitmap_addr;
  bitmap->len = bitmap_len;
  auto col_data_len = data.len - bitmap_len - sizeof(uint32_t) - 1;  // extra 1 is compressed flag
  char *mem = static_cast<char *>(malloc(col_data_len));
  if (mem == nullptr) {
    LOG_ERROR("malloc column data memory failed.");
    free(bitmap_addr);
    return false;
  }
  char *input = data_begin + bitmap_len;
  int word_length = INT_BYTES;
  // If not compressed.
  if (input[0] == 0) {
    // memcpy(output, input + 1, count * word_length);
    // return count * word_length;
    memcpy(mem, data_begin + bitmap_len, col_data_len);
    bitmap->data = bitmap_addr;
    bitmap->len = bitmap_len;
    plain->data = mem;
    plain->len = col_data_len;
    return true;
  }

  // Selector value: 0    1   2   3   4   5   6   7   8  9  10  11 12  13  14  15
  char bit_per_integer[] = {0, 0, 1, 2, 3, 4, 5, 6, 7, 8, 10, 12, 15, 20, 30, 60};
  int selector_to_elems[] = {240, 120, 60, 30, 20, 15, 12, 10, 8, 7, 6, 5, 4, 3, 2, 1};

  const char *ip = input + 1;
  int count1 = 0;
  int _pos = 0;
  int64_t prev_value = 0;

  while (1) {
    if (count1 == count) break;
    uint64_t w = 0;
    memcpy(&w, ip, LONG_BYTES);

    char selector = static_cast<char>(w & INT64MASK(4));  // selector = 4
    char bit = bit_per_integer[static_cast<int>(selector)];  // bit = 3
    int elems = selector_to_elems[static_cast<int>(selector)];

    for (int i = 0; i < elems; i++) {
      uint64_t zigzag_value;

      if (selector == 0 || selector == 1) {
        zigzag_value = 0;
      } else {
        zigzag_value = ((w >> (4 + bit * i)) & INT64MASK(bit));
      }
      int64_t diff = ZIGZAG_DECODE(int64_t, zigzag_value);
      int64_t curr_value = diff + prev_value;
      prev_value = curr_value;

      *(reinterpret_cast<int32_t *>(mem) + _pos) = static_cast<int32_t>(curr_value);
      _pos++;
      count1++;
      if (count1 == count) break;
    }
    ip += LONG_BYTES;
  }
  plain->data = mem;
  plain->len = count * word_length;
  return true;
}

void encodeDoubleValue(uint64_t diff, uint8_t flag, char *const output, int *const pos) {
  uint8_t nbytes = (flag & INT8MASK(3)) + 1;
  int nshift = (LONG_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff >>= nshift;

  while (nbytes) {
    output[(*pos)++] = (int8_t) (diff & INT64MASK(8));
    diff >>= BITS_PER_BYTE;
    nbytes--;
  }
}

uint64_t decodeDoubleValue(const char *const input, int *const ipos, uint8_t flag) {
  uint64_t diff = 0ul;
  int nbytes = (flag & INT8MASK(3)) + 1;
  for (int i = 0; i < nbytes; i++) {
    diff = diff | ((INT64MASK(8) & input[(*ipos)++]) << BITS_PER_BYTE * i);
  }
  int shift_width = (LONG_BYTES * BITS_PER_BYTE - nbytes * BITS_PER_BYTE) * (flag >> 3);
  diff <<= shift_width;

  return diff;
}

TSSlice ColumnCompressorDouble::Compress(TSSlice plain, TSSlice bitmap, uint32_t count) {
  if (count == 0) {
    LOG_WARN("The size of data to be compressed is 0.");
    return TSSlice{nullptr, 0};
  }
  uint32_t bitmap_len = (count + 7) / 8;
  TSSlice ret;
  char *mem = mallocWithID(plain.len + bitmap_len + 1, &ret);
  if (mem == nullptr) {
    LOG_ERROR("malloc memory failed.");
    return TSSlice{nullptr, 0};
  }
  memcpy(mem, bitmap.data, bitmap_len);
  char *output = mem + bitmap_len;
  char *input = plain.data;

  int byte_limit = count * DOUBLE_BYTES + 1;
  int opos = 1;

  uint64_t prev_value = 0;
  uint64_t prev_diff = 0;
  uint8_t prev_flag = 0;


  double *input_stream = &KDouble64(input);

  // Main loop
  for (int i = 0; i < count; i++) {
    union {
      double real;
      uint64_t bits;
    } curr;

    curr.real = input_stream[i];

    // Here we assume the next value is the same as previous one.
    uint64_t predicted = prev_value;
    uint64_t diff = curr.bits ^ predicted;

    int leading_zeros = LONG_BYTES * BITS_PER_BYTE;
    int trailing_zeros = leading_zeros;

    if (diff) {
      trailing_zeros = BUILDIN_CTZL(diff);
      leading_zeros = BUILDIN_CLZL(diff);
    }

    uint8_t nbytes = 0;
    uint8_t flag;

    if (trailing_zeros > leading_zeros) {
      nbytes = (uint8_t) (LONG_BYTES - trailing_zeros / BITS_PER_BYTE);

      if (nbytes > 0) nbytes--;
      flag = ((uint8_t) 1 << 3) | nbytes;
    } else {
      nbytes = (uint8_t) (LONG_BYTES - leading_zeros / BITS_PER_BYTE);
      if (nbytes > 0) nbytes--;
      flag = nbytes;
    }

    if (i % 2 == 0) {
      prev_diff = diff;
      prev_flag = flag;
    } else {
      int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
      int nbyte2 = (flag & INT8MASK(3)) + 1;
      if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
        uint8_t flags = prev_flag | (flag << 4);
        output[opos++] = flags;
        encodeDoubleValue(prev_diff, prev_flag, output, &opos);
        encodeDoubleValue(diff, flag, output, &opos);
      } else {
        output[0] = 1;
        memcpy(output + 1, input, byte_limit - 1);
        ret.len = byte_limit + bitmap_len;
        return ret;
      }
    }
    prev_value = curr.bits;
  }

  if (count % 2) {
    int nbyte1 = (prev_flag & INT8MASK(3)) + 1;
    int nbyte2 = 1;
    if (opos + 1 + nbyte1 + nbyte2 <= byte_limit) {
      uint8_t flags = prev_flag;
      output[opos++] = flags;
      encodeDoubleValue(prev_diff, prev_flag, output, &opos);
      encodeDoubleValue(0ul, 0, output, &opos);
    } else {
      output[0] = 1;
      memcpy(output + 1, input, byte_limit - 1);
      ret.len = byte_limit + bitmap_len;
      return ret;
    }
  }

  free(ret.data);
  LOG_ERROR(" ColumnCompressorDouble compress failed.");
  return TSSlice{nullptr, 0};
}

bool ColumnCompressorDouble::DeCompress(TSSlice data, uint32_t count, TSSlice *plain, TSSlice *bitmap) {
  assert(KUint32(data.data) == compressor_id_);
  if (data.data == nullptr || data.len == 0) {
    LOG_ERROR("TSSlice data is nullptr or data length is 0.");
    return false;
  }
  if (count == 0) {
    LOG_ERROR("Data count is 0.");
    return false;
  }

  char *data_begin = data.data + sizeof(uint32_t);
  uint32_t bitmap_len = (count + 7) / 8;
  char *bitmap_addr = reinterpret_cast<char *>(malloc(bitmap_len));
  if (bitmap_addr == nullptr) {
    LOG_ERROR("malloc bitmap memory failed.");
    return false;
  }
  memcpy(bitmap_addr, data_begin, bitmap_len);
  bitmap->data = bitmap_addr;
  bitmap->len = bitmap_len;
  auto col_data_len = data.len - bitmap_len - sizeof(uint32_t) - 1;  // extra 1 is compressed flag
  char *mem = static_cast<char *>(malloc(col_data_len));
  if (mem == nullptr) {
    LOG_ERROR("malloc column data memory failed.");
    free(bitmap_addr);
    return false;
  }
  char *input = data_begin + bitmap_len;
  // output stream
  double *output_stream = &KDouble64(mem);

  if (input[0] == 0) {
    // memcpy(output, input + 1, count * DOUBLE_BYTES);
    // return count * DOUBLE_BYTES;
    memcpy(mem, data_begin + bitmap_len, col_data_len);
    bitmap->data = bitmap_addr;
    bitmap->len = bitmap_len;
    plain->data = mem;
    plain->len = col_data_len;
    return true;
  }

  uint8_t flags = 0;
  int ipos = 1;
  int opos = 0;
  uint64_t prev_value = 0;

  for (int i = 0; i < count; i++) {
    if (i % 2 == 0) {
      flags = input[ipos++];
    }

    uint8_t flag = flags & INT8MASK(4);
    flags >>= 4;

    uint64_t diff = decodeDoubleValue(input, &ipos, flag);
    union {
      uint64_t bits;
      double real;
    } curr;

    uint64_t predicted = prev_value;
    curr.bits = predicted ^ diff;
    prev_value = curr.bits;

    output_stream[opos++] = curr.real;
  }
  plain->data = mem;
  plain->len = count * DOUBLE_BYTES;
  return true;
}



}  //  namespace kwdbts

