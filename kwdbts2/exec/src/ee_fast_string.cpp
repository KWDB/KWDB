
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

#include "ee_fast_string.h"

#include <memory>

namespace kwdbts {
void faststring::GrowToAtLeast(size_t newcapacity) {
  // Not enough space, need to reserve more.
  // Don't reserve exactly enough space for the new string -- that makes it
  // too easy to write perf bugs where you get O(n^2) append.
  // Instead, alwayhs expand by at least 50%.

  if (newcapacity < capacity_ * 3 / 2) {
    newcapacity = capacity_ * 3 / 2;
  }
  GrowArray(newcapacity);
}

void faststring::GrowArray(size_t newcapacity) {
  // DCHECK_GE(newcapacity, capacity_);
  std::unique_ptr<uint8_t[]> newdata(new uint8_t[newcapacity]);
  if (len_ > 0) {
    memcpy(&newdata[0], &data_[0], len_);
  }
  capacity_ = newcapacity;
  if (data_ != initial_data_) {
    delete[] data_;
  } else {
    // ASAN_POISON_MEMORY_REGION(initial_data_, arraysize(initial_data_));
  }

  data_ = newdata.release();
  // ASAN_POISON_MEMORY_REGION(data_ + len_, capacity_ - len_);
}

void faststring::ShrinkToFitInternal() {
  // DCHECK_NE(data_, initial_data_);
  if (len_ <= kInitialCapacity) {
    // ASAN_UNPOISON_MEMORY_REGION(initial_data_, len_);
    memcpy(initial_data_, &data_[0], len_);
    delete[] data_;
    data_ = initial_data_;
    capacity_ = kInitialCapacity;
  } else {
    std::unique_ptr<uint8_t[]> newdata(new uint8_t[len_]);
    memcpy(&newdata[0], &data_[0], len_);
    delete[] data_;
    data_ = newdata.release();
    capacity_ = len_;
  }
}
}  // namespace kwdbts
