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

#include <string>

#include "kwdb_type.h"

namespace kwdbts {
struct String {
  String()
      : ptr_(nullptr),
        length_(0),
        alloc_length_(0),
        is_null_(false),
        is_alloc_(false) {}

  explicit String(size_t len) : ptr_(nullptr), length_(0), is_null_(false) {
    // ptr_ = static_cast<k_char *>(malloc(len + 1));
    // alloc_length_ = len + 1;
    // is_alloc_ = true;
    // ptr_[0] = 0;
    real_alloc(len);
  }

  String(k_char *str, size_t len, k_bool null)
      : ptr_(str),
        length_(len),
        alloc_length_(0),
        is_null_(null),
        is_alloc_(false) {}
  String(k_char *str, size_t len)
      : ptr_(str), length_(len), alloc_length_(0), is_alloc_(false) {}
  explicit String(const k_char *str)
      : ptr_(const_cast<k_char *>(str)),
        length_(strlen(str)),
        alloc_length_(0),
        is_null_(false),
        is_alloc_(false) {}
  explicit String(size_t max_len, const k_char *str)
      : ptr_(const_cast<k_char *>(str)),
        length_(strnlen(str, max_len)),
        alloc_length_(0),
        is_null_(false),
        is_alloc_(false) {}
  ~String() { mem_free(); }
  String operator=(const String &s) {
    if (&s != this) {
      mem_free();
      ptr_ = s.ptr_;
      length_ = s.length_;
      alloc_length_ = s.alloc_length_;
      is_null_ = s.is_null_;
      is_alloc_ = s.is_alloc_;
      (const_cast<String *>(&s))->is_alloc_ = false;
    }
    return *this;
  }
  String(const String &str)
      : ptr_(str.ptr_),
        length_(str.length_),
        alloc_length_(static_cast<k_uint32>(str.alloc_length_)),
        is_null_(str.is_null_),
        is_alloc_(false) {}
  String(String &&str) noexcept
      : ptr_(str.ptr_),
        length_(str.length_),
        alloc_length_(str.alloc_length_),
        is_null_(str.is_null_),
        is_alloc_(str.is_alloc_) {
    str.is_alloc_ = false;
  }
  void mem_free() {
    if (is_alloc_) {
      free(ptr_);
      ptr_ = nullptr;
      length_ = 0;
      is_null_ = false;
      alloc_length_ = 0;
      is_alloc_ = false;
    }
  }

  bool alloc(size_t len) {
    if (len < alloc_length_) return false;
    return real_alloc(len);
  }

  bool empty() { return length_ == 0; }

  bool real_alloc(size_t len) {
    const size_t length = len + 1;
    length_ = 0;
    if ((alloc_length_ < length) || (!is_alloc_)) {
      mem_free();
      if (!(ptr_ = static_cast<k_char *>(malloc(length)))) return true;
      alloc_length_ = length;
      is_alloc_ = true;
    }
    ptr_[0] = 0;
    return false;
  }

  bool copy_string(const String &str) {
    /**
     * @brief Copies content from source string to current string object
     *
     * Copies the entire content of the source string (including null
     * terminator) into the internal buffer of the current String object.
     * Requires the current object to have sufficient pre-allocated memory
     * (alloc_length_ >= source length + 1) and dynamic memory allocation flag
     * (is_alloc_ == true) to succeed.
     *
     * @param[in] str Source String object containing data to copy
     * @return bool True if copy succeeds, false otherwise
     * @retval true Memory allocation is sufficient and copy completed
     * @retval false Insufficient memory or non-dynamically allocated buffer
     *
     * @note 1. Copies full content including length_ bytes of data and null
     * terminator
     *       2. Ensure current object has completed memory allocation before
     * calling
     *       3. Null terminator is copied even if source string length is 0
     */
    const size_t length = str.length_ + 1;
    if ((alloc_length_ < length) || (!is_alloc_)) {
      return false;
    }
    std::memcpy(ptr_, str.ptr_, length);
    length_ = str.length_;
    return true;
  }

  k_char *ptr_{nullptr};
  size_t length_{0};
  k_uint32 alloc_length_{0};
  k_bool is_null_{false};
  k_bool is_alloc_{false};

  k_char *getptr() { return ptr_; }
  const k_char *c_str() { return ptr_; }
  k_bool isNull() { return is_null_; }
  size_t size() { return length_; }
  size_t length() { return length_; }
  k_int32 compare(String &str) {
    k_int32 min_len = length_ < str.length_ ? length_ : str.length_;
    k_int32 ret = strncmp(ptr_, str.ptr_, min_len);
    if (ret == 0 && length_ != str.length_) {
      if (length_ < str.length_) {
        return -1;
      }
      return 1;
    }
    return ret;
  }
  k_int32 compare(const String &str) const {
    k_int32 min_len = length_ < str.length_ ? length_ : str.length_;
    k_int32 ret = strncmp(ptr_, str.ptr_, min_len);
    if (ret == 0 && length_ != str.length_) {
      if (length_ < str.length_) {
        return -1;
      }
      return 1;
    }
    return ret;
  }

  k_int32 compare(const char *ptr, size_t length) const {
    k_int32 min_len = length_ < length ? length_ : length;
    k_int32 ret = strncmp(ptr_, ptr, min_len);
    if (ret == 0 && length_ != length) {
      if (length_ < length) {
        return -1;
      }
      return 1;
    }
    return ret;
  }
};
}  // namespace kwdbts
