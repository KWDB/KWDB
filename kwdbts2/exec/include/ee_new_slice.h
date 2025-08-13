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
#include <iostream>
#include <map>
#include <string>
#include <string_view>
#include <vector>

#include "ee_fast_string.h"
#include "kwdb_type.h"
namespace kwdbts {
class KSlice {
 public:
  KSlice() : data_(const_cast<char*>("")) {
  }
  explicit KSlice(const char* s) : data_(const_cast<char*>(s)), data_size_(strlen(s)) {
  }
  explicit KSlice(const QuickString& s)
      : data_(const_cast<char*>(reinterpret_cast<const char*>(s.data()))), data_size_(s.Size()) {
  }
  explicit KSlice(const std::string& s) : data_(const_cast<char*>(s.data())), data_size_(s.size()) {
  }
  KSlice(const uint8_t* s, size_t n) : data_(const_cast<char*>(reinterpret_cast<const char*>(s))), data_size_(n) {
  }
  KSlice(const char* d, size_t n) : data_(const_cast<char*>(d)), data_size_(n) {
  }
  operator std::string_view() const {
    return {data_, data_size_};
  }

  const char* GetData() const {
    return data_;
  }

  void ClearData() {
    data_ = const_cast<char*>("");
    data_size_ = 0;
  }

  k_bool IsEmpty() const {
    return data_size_ == 0;
  }

  std::string ToString() const {
    return std::string(data_, data_size_);
  }
  void TruncData(size_t n) {
    assert(n <= data_size_);
    data_size_ = n;
  }

  const char& operator[](size_t n) const {
    assert(n < data_size_);
    return data_[n];
  }

  size_t GetSize() const {
    return data_size_;
  }
  static size_t ComputeTotalSize(const std::vector<KSlice>& slices) {
    size_t total_size = 0;
    for (auto& slice : slices) {
      total_size += slice.data_size_;
    }
    return total_size;
  }

  friend k_bool operator==(const KSlice& a, const KSlice& b);

  friend std::ostream& operator<<(std::ostream& os, const KSlice& s);

  void Relocate(char* d) {
    if (data_ != d) {
      memcpy(d, data_, data_size_);
      data_ = d;
    }
  }

  static std::string ToString(const std::vector<KSlice>& slices) {
    std::string buf;
    for (auto& slice : slices) {
      buf.append(slice.data_, slice.data_size_);
    }
    return buf;
  }

 public:
  char* data_;
  size_t data_size_{0};
};

inline std::ostream& operator<<(std::ostream& os, const KSlice& s) {
  os << s.ToString();
  return os;
}

inline bool operator!=(const KSlice& a, const KSlice& b) {
  return !(a == b);
}
}  // namespace kwdbts
