
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

#pragma once
#include <memory>
#include <string>
#include <utility>

#include "ee_fast_mem.h"
#include "kwdb_type.h"
namespace kwdbts {

#if defined(__GNUC__)
#define LIKELY_FALSE(x) (__builtin_expect(x, 0))
#define LIKELY_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define LIKELY_FALSE(x) x
#define LIKELY_TRUE(x) x
#endif

class QuickString {
 public:
  enum { kInlineBufInitialCapacity = 32 };

  QuickString() : data_(inline_initial_buffer_) {
  }

  ~QuickString() {
    if (data_ != inline_initial_buffer_) {
      delete[] data_;
    }
  }

  // make a QuickString with given capacity.
  explicit QuickString(size_t capacity) : data_(inline_initial_buffer_) {
    if (capacity > capacity_) {
      data_ = new uint8_t[capacity];
      capacity_ = capacity;
    }
  }
  // clear the buffer.
  void ClearBuffer() {
    resize(0);
  }

  // Get the byte at index i.
  const uint8_t& At(size_t i) const {
    return data_[i];
  }

  // resize buffer
  void resize(size_t newsize) {
    if (newsize > capacity_) {
      ReserveBuffer(newsize);
    }
    len_ = newsize;
  }

  void ReserveBuffer(size_t size) {
    if (LIKELY_TRUE(size <= capacity_)) return;
    ResizeInternalArray(size);
  }
  // append a string to the buffer.
  void AppendString(const std::string& str) {
    AppendInternal(str.data(), str.size());
  }

  // Get the data pointer.
  const uint8_t* data() const {
    return &data_[0];
  }

  // Append data to the buffer.
  void AppendInternal(const void* ptr, size_t size) {
    const auto* src = reinterpret_cast<const uint8_t*>(ptr);
    EnsureCapacityForAppend(size);
    if (size <= 4) {
      uint8_t* p = &data_[len_];
      for (int i = 0; i < size; i++) {
        *p++ = *src++;
      }
    } else {
      kwdbts::memcpy_inlined(&data_[len_], src, size);
    }
    len_ += size;
  }

  // convert the buffer to a string.
  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data()), len_);
  }

  // swap the buffer with other.
  void Swap(QuickString& other) {
    std::swap(len_, other.len_);
    std::swap(capacity_, other.capacity_);
    std::swap(data_, other.data_);
    std::swap_ranges(inline_initial_buffer_, inline_initial_buffer_ + kInlineBufInitialCapacity,
                     other.inline_initial_buffer_);
  }

  // prepare write
  uint8_t* PrepareWrite(size_t write_size) {
    EnsureCapacityForAppend(write_size);
    return data_ + len_;
  }

  // finish write
  void FinishWrite(size_t actual_written) {
    if (actual_written > 0) {
      len_ += actual_written;
    }
  }

  // Get the length of the buffer.
  size_t Length() const {
    return len_;
  }
  // Append a byte to the buffer.
  void PushBack(const char b) {
    EnsureCapacityForAppend(1);
    data_[len_] = b;
    len_++;
  }

  // Get the size of the buffer.
  size_t Size() const {
    return len_;
  }

  // Get the data pointer.
  uint8_t* data() {
    return &data_[0];
  }

  // Get the byte at index i.
  const uint8_t& operator[](size_t i) const {
    return data_[i];
  }
  // find the first occurrence of byte in the buffer.
  size_t Find(uint8_t byte, size_t start = 0) const {
    if (start >= len_) {
      return std::string::npos;
    }
    for (size_t i = start; i < len_; ++i) {
      if (data_[i] == byte) {
        return i;
      }
    }
    return std::string::npos;
  }
  // find the last occurrence of byte in the buffer.
  size_t FindLast(uint8_t byte) const {
    for (size_t i = len_; i > 0; --i) {
      if (data_[i - 1] == byte) {
        return i - 1;
      }
    }
    return std::string::npos;
  }
  // Get the byte at index i.
  uint8_t& operator[](size_t i) {
    return data_[i];
  }

  // Get the capacity of the buffer.
  size_t Capacity() const {
    return capacity_;
  }
  // Assign a copy of the given data to the buffer.
  void AssignCopy(const uint8_t* src, size_t len) {
    len_ = 0;
    resize(len);
    kwdbts::memcpy_inlined(data(), src, len);
  }
  // assign a copy of the given string to the buffer.
  void AssignCopy(const std::string& str) {
    AssignCopy(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
  }

 private:
  QuickString(const QuickString&) = delete;
  const QuickString& operator=(const QuickString&) = delete;

 private:
  // expand the buffer to at least size.
  void ExpandToAtLeast(size_t size) {
    if (size < capacity_ * 3 / 2) {
      size = capacity_ * 3 / 2;
    }
    ResizeInternalArray(size);
  }
  // ensure the buffer has at least size bytes.
  void EnsureCapacityForAppend(size_t size) {
    if (LIKELY_TRUE(len_ + size <= capacity_)) {
      return;
    }
    ExpandToAtLeast(len_ + size);
  }
  // resize the buffer to size.
  void ResizeInternalArray(size_t size) {
    std::unique_ptr<uint8_t[]> temp(new uint8_t[size]);
    if (len_ > 0) {
      memcpy(&temp[0], &data_[0], len_);
    }
    capacity_ = size;
    if (data_ != inline_initial_buffer_) {
      delete[] data_;
    }

    data_ = temp.release();
  }

 public:
  size_t len_{0};
  uint8_t* data_;
  uint8_t inline_initial_buffer_[kInlineBufInitialCapacity];  // NOLINT
  size_t capacity_{kInlineBufInitialCapacity};
};
}  // namespace kwdbts
