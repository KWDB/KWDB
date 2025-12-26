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
#include <cstddef>
#include <cstdlib>
#include <memory>
#include <string_view>
#include <utility>

#include "libkwdbts2.h"
namespace kwdbts {

class TsSliceGuard {
  friend class TsBufferBuilder;
  using BufferPtr = std::shared_ptr<char>;

 private:
  BufferPtr mem_;
  TSSlice slice;

  TsSliceGuard(BufferPtr&& buffer, size_t len) : mem_(std::move(buffer)), slice{mem_.get(), len} {}

  // constructor for sub-slice
  TsSliceGuard(const TsSliceGuard* base, size_t start, size_t len)
      : mem_(base->mem_), slice{base->slice.data + start, len} {}

 public:
  TsSliceGuard() : mem_(nullptr), slice{nullptr, 0} {}
  explicit TsSliceGuard(TSSlice s) : mem_(nullptr), slice{s} {}
  explicit TsSliceGuard(char* data, size_t len) : mem_{nullptr}, slice{data, len} {}

  // NOTE: delete copy constructor and assignment operator for performance consideration
  TsSliceGuard(const TsSliceGuard&) = delete;
  TsSliceGuard& operator=(const TsSliceGuard&) = delete;

  TsSliceGuard(TsSliceGuard&& other) noexcept = default;
  TsSliceGuard& operator=(TsSliceGuard&& other) noexcept = default;

  TSSlice AsSlice() const& { return slice; }
  TSSlice AsSlice() && = delete;
  std::string_view AsStringView() const& { return {slice.data, slice.len}; }
  std::string_view AsStringView() && = delete;

  TsSliceGuard SubSliceGuard(size_t start, size_t len) const { return TsSliceGuard(this, start, len); }
  TSSlice SubSlice(size_t start, size_t len) const { return {slice.data + start, len}; }

  void RemovePrefix(size_t n) {
    slice.data += n;
    slice.len -= n;
  }

  size_t size() const { return slice.len; }
  const char* data() const { return slice.data; }
  char* data() { return slice.data; }
  bool empty() const { return slice.len == 0; }

  bool own_data() const { return mem_ != nullptr; }

  const char* begin() const { return slice.data; }
  const char* end() const { return slice.data + slice.len; }

  char front() const { return slice.data[0]; }
  char back() const { return slice.data[slice.len - 1]; }
};

};  // namespace kwdbts
