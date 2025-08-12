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

#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "kwdb_type.h"
namespace kwdbts {
class faststring;
class KSlice {
 public:
  char* data;
  size_t size{0};

  static void Init();
  static const KSlice& MaxValue();
  static const KSlice& MinValue();

  KSlice() : data(const_cast<char*>("")) {
  }

  KSlice(const char* d, size_t n) : data(const_cast<char*>(d)), size(n) {
  }

  KSlice(const uint8_t* s, size_t n) : data(const_cast<char*>(reinterpret_cast<const char*>(s))), size(n) {
  }

  explicit KSlice(const std::string& s) : data(const_cast<char*>(s.data())), size(s.size()) {
  }

  explicit KSlice(const faststring& s);

  explicit KSlice(const char* s) : data(const_cast<char*>(s)), size(strlen(s)) {
  }

  operator std::string_view() const {
    return {data, size};
  }

  const char* GetData() const {
    return data;
  }
  char* MutableData() {
    return const_cast<char*>(data);
  }

  size_t GetSize() const {
    return size;
  }

  k_bool Empty() const {
    return size == 0;
  }

  const char& operator[](size_t n) const {
    assert(n < size);
    return data[n];
  }

  void Clear() {
    data = const_cast<char*>("");
    size = 0;
  }
  void RemovePrefix(size_t n) {
    assert(n <= size);
    data += n;
    size -= n;
  }

  void RemoveSuffix(size_t n) {
    assert(n <= size);
    size -= n;
  }

  void Truncate(size_t n) {
    assert(n <= size);
    size = n;
  }

  std::string ToString() const {
    return std::string(data, size);
  }

  k_int32 Compare(const KSlice& b) const;
  KSlice Tolower(std::string& buf) {
    buf.assign(GetData(), GetSize());
    std::transform(buf.begin(), buf.end(), buf.begin(), [](unsigned char c) { return std::tolower(c); });
    return KSlice(buf.data(), buf.size());
  }

  struct Comparator {
    bool operator()(const KSlice& a, const KSlice& b) const {
      return a.Compare(b) < 0;
    }
  };

  void Relocate(char* d) {
    if (data != d) {
      memcpy(d, data, size);
      data = d;
    }
  }

  friend k_bool operator==(const KSlice& x, const KSlice& y);

  friend std::ostream& operator<<(std::ostream& os, const KSlice& slice);

  static size_t ComputeTotalSize(const std::vector<KSlice>& slices) {
    size_t total_size = 0;
    for (auto& slice : slices) {
      total_size += slice.size;
    }
    return total_size;
  }

  static std::string ToString(const std::vector<KSlice>& slices) {
    std::string buf;
    for (auto& slice : slices) {
      buf.append(slice.data, slice.size);
    }
    return buf;
  }
};

inline std::ostream& operator<<(std::ostream& os, const KSlice& slice) {
  os << slice.ToString();
  return os;
}

inline bool operator!=(const KSlice& x, const KSlice& y) {
  return !(x == y);
}
}  // namespace kwdbts
