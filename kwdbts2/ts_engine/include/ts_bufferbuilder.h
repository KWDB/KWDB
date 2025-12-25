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

#pragma once

#include <algorithm>
#include <cassert>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <new>
#include <string_view>
#include <utility>

#include "ts_sliceguard.h"

namespace kwdbts {

class TsBufferBuilder {
 private:
  char *head_ = nullptr;
  char *tail_ = nullptr;
  char *limit_ = nullptr;

  void ExtendTo(size_t sz) {
    size_t old_size = size();
    size_t old_capacity = capacity();
    assert(sz >= old_size);
    head_ = static_cast<char *>(std::realloc(head_, sz));
    if (head_ == nullptr) {
      throw std::bad_alloc();
    }
    tail_ = head_ + old_size;
    limit_ = head_ + sz;
    std::fill(head_ + old_capacity, limit_, 0);
  }

 public:
  TsBufferBuilder() = default;
  explicit TsBufferBuilder(size_t initial_size) { resize(initial_size); }
  explicit TsBufferBuilder(const TsSliceGuard &guard) { this->append(guard.AsSlice()); }
  // explicit TsBufferBuilder(TsSliceGuard &&guard) {
  //   if (guard.own_data()) {
  //     head_ = guard.release();
  //     tail_ = limit_ = head_ + guard.size();
  //   } else {
  //     ExtendTo(guard.size());
  //     append(guard.AsSlice());
  //   }
  // }

  TsBufferBuilder(const TsBufferBuilder &) = delete;
  TsBufferBuilder &operator=(const TsBufferBuilder &) = delete;

  TsBufferBuilder(TsBufferBuilder &&other) { *this = std::move(other); }
  TsBufferBuilder &operator=(TsBufferBuilder &&other) {
    if (this == &other) {
      return *this;
    }
    this->~TsBufferBuilder();
    head_ = other.head_;
    tail_ = other.tail_;
    limit_ = other.limit_;
    other.head_ = other.tail_ = other.limit_ = nullptr;
    return *this;
  }

  ~TsBufferBuilder() {
    if (head_ != nullptr) {
      std::free(head_);
      head_ = tail_ = limit_ = nullptr;
    }
  }

  void reserve(size_t size) {
    if (size > capacity()) {
      ExtendTo(size);
    }
  }

  void resize(size_t size) {
    if (size > capacity()) {
      ExtendTo(size);
    }
    tail_ = head_ + size;
  }

  void append(const char *data, size_t size) { append(std::string_view{data, size}); }
  void append(TSSlice s) { append(std::string_view{s.data, s.len}); }
  void append(const TsSliceGuard &guard) { append(guard.AsSlice()); }
  void append(const TsBufferBuilder &builder) { append(builder.AsSlice()); }
  void append(std::string_view sv) {
    if (sv.size() == 0) return;
    size_t target_size = size() + sv.size();
    size_t current_capacity = capacity();
    if (target_size > capacity()) {
      size_t new_size = 0;
      if (target_size < (32U << 20)) {
        size_t tmp_new_size = current_capacity == 0 ? 64 : current_capacity * 2;
        while (tmp_new_size < target_size) {
          tmp_new_size *= 2;
        }
        new_size = tmp_new_size;
      } else {
        size_t new_size_mb = ((target_size - 1) >> 20) + 1;  // round up to MB
        new_size = new_size_mb << 20;
      }
      assert(new_size >= target_size);
      ExtendTo(new_size);
    }
    std::copy(sv.begin(), sv.end(), tail_);
    tail_ += sv.size();
  }

  void push_back(char c) { append(std::string_view{&c, 1}); }


  TSSlice AsSlice() const { return {head_, size()}; }
  TSSlice SubSlice(size_t start, size_t len) const {
    assert(start + len <= size());
    return {head_ + start, len};
  }
  std::string_view AsStringView() const & { return {head_, size()}; }
  std::string_view AsStringView() && = delete;

  TsSliceGuard GetBuffer() {
    TsSliceGuard::BufferPtr buffer(head_, &std::free);
    TsSliceGuard result{std::move(buffer), size()};
    head_ = tail_ = limit_ = nullptr;
    return result;
  }

  void assign(const char *data, size_t size) { assign(std::string_view{data, size}); }
  void assign(std::string_view sv) {
    if (sv.size() > capacity()) {
      reserve(sv.size());
    }
    std::copy(sv.begin(), sv.end(), head_);
    tail_ = head_ + sv.size();
  }

  const char *data() const { return head_; }
  char *data() { return head_; }

  char &operator[](size_t idx) { return head_[idx]; }
  char operator[](size_t idx) const { return head_[idx]; }
  char &back() { return tail_[-1]; }
  char back() const { return tail_[-1]; }

  size_t size() const { return tail_ - head_; }
  size_t capacity() const { return limit_ - head_; }
  void clear() { tail_ = head_; }
  bool empty() const { return size() == 0; }

  char *begin() { return head_; }
  char *end() { return tail_; }
  const char *begin() const { return head_; }
  const char *end() const { return tail_; }
};
}  // namespace kwdbts
