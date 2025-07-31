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
namespace kwdbts {

// template <typename T>
// inline int compare(T lhs, T rhs) {
//   if (lhs < rhs) {
//     return -1;
//   } else if (lhs > rhs) {
//     return 1;
//   } else {
//     return 0;
//   }
// }
// inline bool memequal(const char* p1, size_t size1, const char* p2, size_t
// size2) {
//     return (size1 == size2) && (memcmp(p1, p2, size1) == 0);
// }

// inline int memcompare(const char* p1, size_t size1, const char* p2, size_t
// size2) {
//     size_t min_size = std::min(size1, size2);
//     auto res = memcmp(p1, p2, min_size);
//     if (res != 0) {
//         return res > 0 ? 1 : -1;
//     }
//     return compare(size1, size2);
// }
class faststring;
class KSlice {
 public:
  char* data;
  size_t size{0};

  static void init();
  static const KSlice& max_value();
  static const KSlice& min_value();

  // Intentionally copyable

  /// Create an empty slice.
  KSlice() : data(const_cast<char*>("")) {}

  /// Create a slice that refers to a @c char byte array.
  KSlice(const char* d, size_t n) : data(const_cast<char*>(d)), size(n) {}

  // Create a slice that refers to a @c uint8_t byte array.
  //
  // @param [in] d
  //   The input array.
  // @param [in] n
  //   Number of bytes in the array.
  KSlice(const uint8_t* s, size_t n)
      : data(const_cast<char*>(reinterpret_cast<const char*>(s))), size(n) {}

  /// Create a slice that refers to the contents of the given string.
  explicit KSlice(const std::string& s)
      :  // NOLINT(runtime/explicit)
        data(const_cast<char*>(s.data())),
        size(s.size()) {}

  explicit KSlice(const faststring& s);

  /// Create a slice that refers to a C-string s[0,strlen(s)-1].
  explicit KSlice(const char* s)
      :  // NOLINT(runtime/explicit)
        data(const_cast<char*>(s)),
        size(strlen(s)) {}

  operator std::string_view() const { return {data, size}; }

  /// @return A pointer to the beginning of the referenced data.
  const char* get_data() const { return data; }

  /// @return A mutable pointer to the beginning of the referenced data.
  char* mutable_data() { return const_cast<char*>(data); }

  /// @return The length (in bytes) of the referenced data.
  size_t get_size() const { return size; }

  /// @return @c true iff the length of the referenced data is zero.
  bool empty() const { return size == 0; }

  /// @return the n-th byte in the referenced data.
  const char& operator[](size_t n) const {
    assert(n < size);
    return data[n];
  }

  /// Change this slice to refer to an empty array.
  void clear() {
    data = const_cast<char*>("");
    size = 0;
  }

  /// Drop the first "n" bytes from this slice.
  ///
  /// @pre n <= size
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   Number of bytes that should be dropped from the beginning.
  void remove_prefix(size_t n) {
    assert(n <= size);
    data += n;
    size -= n;
  }

  /// Drop the last "n" bytes from this slice.
  ///
  /// @pre n <= size
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   Number of bytes that should be dropped from the tail.
  void remove_suffix(size_t n) {
    assert(n <= size);
    size -= n;
  }

  /// Truncate the slice to the given number of bytes.
  ///
  /// @pre n <= size
  ///
  /// @note Only the base and bounds of the slice are changed;
  ///   the data is not modified.
  ///
  /// @param [in] n
  ///   The new size of the slice.
  void truncate(size_t n) {
    assert(n <= size);
    size = n;
  }

  /// @return A string that contains a copy of the referenced data.
  std::string to_string() const { return std::string(data, size); }

  /// Do a three-way comparison of the slice's data.
  int compare(const KSlice& b) const;

  // /// Check whether the slice starts with the given prefix.
  // bool starts_with(const Slice& x) const { return ((size >= x.size) &&
  // (memequal(data, x.size, x.data, x.size))); }

  // bool ends_with(const Slice& x) const {
  //     return ((size >= x.size) && memequal(data + (size - x.size), x.size,
  //     x.data, x.size));
  // }

  KSlice tolower(std::string& buf) {
    // copy this slice into buf
    buf.assign(get_data(), get_size());
    std::transform(buf.begin(), buf.end(), buf.begin(),
                   [](unsigned char c) { return std::tolower(c); });
    return KSlice(buf.data(), buf.size());
  }

  /// @brief Comparator struct, useful for ordered collections (like STL maps).
  struct Comparator {
    /// Compare two slices using Slice::compare()
    ///
    /// @param [in] a
    ///   The slice to call Slice::compare() at.
    /// @param [in] b
    ///   The slice to use as a parameter for Slice::compare().
    /// @return @c true iff @c a is less than @c b by Slice::compare().
    bool operator()(const KSlice& a, const KSlice& b) const {
      return a.compare(b) < 0;
    }
  };

  /// Relocate/copy the slice's data into a new location.
  ///
  /// @param [in] d
  ///   The new location for the data. If it's the same location, then no
  ///   relocation is done. It is assumed that the new location is
  ///   large enough to fit the data.
  void relocate(char* d) {
    if (data != d) {
      memcpy(d, data, size);
      data = d;
    }
  }

  friend bool operator==(const KSlice& x, const KSlice& y);

  friend std::ostream& operator<<(std::ostream& os, const KSlice& slice);

  static size_t compute_total_size(const std::vector<KSlice>& slices) {
    size_t total_size = 0;
    for (auto& slice : slices) {
      total_size += slice.size;
    }
    return total_size;
  }

  static std::string to_string(const std::vector<KSlice>& slices) {
    std::string buf;
    for (auto& slice : slices) {
      buf.append(slice.data, slice.size);
    }
    return buf;
  }
};

inline std::ostream& operator<<(std::ostream& os, const KSlice& slice) {
  os << slice.to_string();
  return os;
}

/// Check whether two slices are identical.
// inline bool operator==(const KSlice& x, const KSlice& y) {
//     return memequal(x.data, x.size, y.data, y.size);
// }

/// Check whether two slices are not identical.
inline bool operator!=(const KSlice& x, const KSlice& y) { return !(x == y); }

// inline int KSlice::compare(const KSlice& b) const {
//     return memcompare(data, size, b.data, b.size);
// }

// inline bool operator<(const KSlice& lhs, const KSlice& rhs) {
//     return lhs.compare(rhs) < 0;
// }

// inline bool operator<=(const KSlice& lhs, const KSlice& rhs) {
//     return lhs.compare(rhs) <= 0;
// }

// inline bool operator>(const KSlice& lhs, const KSlice& rhs) {
//     return lhs.compare(rhs) > 0;
// }

// inline bool operator>=(const KSlice& lhs, const KSlice& rhs) {
//     return lhs.compare(rhs) >= 0;
// }
}  // namespace kwdbts
