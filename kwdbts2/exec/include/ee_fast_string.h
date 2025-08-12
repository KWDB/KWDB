
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

#include <cstdint>
#include <cstring>
#include <string>

#include "ee_fast_mem.h"

namespace kwdbts {

#if defined(__GNUC__)
#define PREDICT_FALSE(x) (__builtin_expect(x, 0))
#define PREDICT_TRUE(x) (__builtin_expect(!!(x), 1))
#else
#define PREDICT_FALSE(x) x
#define PREDICT_TRUE(x) x
#endif

class faststring {
 public:
  enum { kInitialCapacity = 32 };

  faststring() : data_(initial_data_) {}

  // Construct a string with the given capacity, in bytes.
  explicit faststring(size_t capacity) : data_(initial_data_) {
    if (capacity > capacity_) {
      data_ = new uint8_t[capacity];
      capacity_ = capacity;
    }
  }

  ~faststring() {
    if (data_ != initial_data_) {
      delete[] data_;
    }
  }

  // Reset the valid length of the string to 0.
  void Clear() {
    resize(0);
  }

  void resize(size_t newsize) {
    if (newsize > capacity_) {
      Reserve(newsize);
    }
    len_ = newsize;
  }

  void Reserve(size_t newcapacity) {
    if (PREDICT_TRUE(newcapacity <= capacity_)) return;
    GrowArray(newcapacity);
  }

  void Append(const void* src_v, size_t count) {
    const auto* src = reinterpret_cast<const uint8_t*>(src_v);
    EnsureRoomForAppend(count);
    if (count <= 4) {
      uint8_t* p = &data_[len_];
      for (int i = 0; i < count; i++) {
        *p++ = *src++;
      }
    } else {
      kwdbts::memcpy_inlined(&data_[len_], src, count);
    }
    len_ += count;
  }

  void Append(const std::string& str) { Append(str.data(), str.size()); }

  void push_back(const char byte) {
    EnsureRoomForAppend(1);
    data_[len_] = byte;
    len_++;
  }

  size_t Length() const { return len_; }

  size_t Size() const { return len_; }

  size_t Capacity() const { return capacity_; }

  const uint8_t* data() const { return &data_[0]; }

  uint8_t* data() { return &data_[0]; }

  const uint8_t& At(size_t i) const { return data_[i]; }

  const uint8_t& operator[](size_t i) const { return data_[i]; }

  uint8_t& operator[](size_t i) { return data_[i]; }

  void AssignCopy(const uint8_t* src, size_t len) {
    len_ = 0;
    resize(len);
    kwdbts::memcpy_inlined(data(), src, len);
  }

  void AssignCopy(const std::string& str) {
    AssignCopy(reinterpret_cast<const uint8_t*>(str.c_str()), str.size());
  }

  void ShrinkToFit() {
    if (data_ == initial_data_ || capacity_ == len_) return;
    ShrinkToFitInternal();
  }

  std::string ToString() const {
    return std::string(reinterpret_cast<const char*>(data()), len_);
  }

 private:
  faststring(const faststring&) = delete;
  const faststring& operator=(const faststring&) = delete;

  void EnsureRoomForAppend(size_t count) {
    if (PREDICT_TRUE(len_ + count <= capacity_)) {
      return;
    }
    GrowToAtLeast(len_ + count);
  }

  void GrowToAtLeast(size_t newcapacity);

  void GrowArray(size_t newcapacity);

  void ShrinkToFitInternal();

  uint8_t* data_;
  uint8_t initial_data_[kInitialCapacity];
  size_t len_{0};
  size_t capacity_{kInitialCapacity};
};
}  // namespace kwdbts
