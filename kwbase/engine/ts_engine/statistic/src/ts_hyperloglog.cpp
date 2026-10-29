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

#include <cmath>
#include "ts_hyperloglog.h"
#include "ts_utils.h"

namespace kwdbts {

Sketch::Sketch(byte precision, bool sparse) : p_(precision) {
    // Initialize a HyperLogLog Sketch with precision p
    // The value of p must be between 4 and 18
    m_ = static_cast<uint32_t>(std::pow(2, precision));
    alpha_ = alpha(m_);
    if (sparse) {
        sparse_ = true;
        tmpSet_ = Set{};
        sparseList_ = std::make_shared<CompressedList>(m_);
    } else {
        regs_ = std::make_shared<Registers>(m_);
    }
    b_ = 0;
}

void Sketch::Insert(const std::vector<byte> &e) {
    uint64_t hash = Hash64(e, 1337);
    InsertHash(hash);
}

void Sketch::insert(uint32_t index, byte rank) {
  if (rank - b_ >= capacity) {
    uint8_t db = regs_->min();
    if (db > 0) {
      b_ += db;
      regs_->rebase(db);
    }
  }
  if (rank > b_) {
    uint8_t val = rank - b_;
    if (val > capacity - 1) {
      val = capacity - 1;
    }

    // Constantly update the value in the hash bucket(key point)
    if (val > regs_->get(index)) {
      regs_->set(index, val);
    }
  }
}

void Sketch::InsertHash(uint64_t hash) {
  if (sparse_) {
    tmpSet_.insert(encodeHash(hash, p_, pp));
    if (tmpSet_.size() * 100 > m_ / 2) {
      mergeSparse();
      // Convert to normal mode
      if (sparseList_->len() > m_ / 2) {
        toNormal();
      }
    }
  } else {
    // Processed in non-sparse mode
    auto [i, r] = getPosVal(hash, p_);
    insert(static_cast<uint32_t>(i), r);
  }
}

std::vector<byte> Sketch::MarshalBinary() const  {
    std::vector<byte> data;
    // Add default elements
    data.push_back(version);
    data.push_back(p_);
    data.push_back(b_);

    if (sparse_) {
        // Mark as sparse
        data.push_back(1);

        // Encode tmpSet_ and add to data
        auto tsdata = marshalBinary_Set(tmpSet_);
        data.insert(data.end(), tsdata.begin(), tsdata.end());

        // Encode sparseList_ and add to data
        auto sdata = sparseList_->MarshalBinary();
        data.insert(data.end(), sdata.begin(), sdata.end());
    } else {
        // Mark as dense
        data.push_back(0);

        // Add binary representation of regs_
        size_t sz = regs_->tailcuts_.size();
        data.push_back(static_cast<byte>(sz >> 24));
        data.push_back(static_cast<byte>(sz >> 16));
        data.push_back(static_cast<byte>(sz >> 8));
        data.push_back(static_cast<byte>(sz));

        for (auto& val : regs_->tailcuts_) {
            data.push_back(static_cast<byte>(val));
        }
    }
    return data;
}

std::vector<byte> CompressedList::MarshalBinary() const {
    std::vector<byte> bdata;
    bdata = marshalBinary_Byte(b_);
    std::vector<byte> data;
    // 8 bytes for count_ and last_, plus size of b_
    data.reserve(8 + bdata.size());

    // Add count_
    data.push_back(static_cast<byte>(count_ >> 24));
    data.push_back(static_cast<byte>(count_ >> 16));
    data.push_back(static_cast<byte>(count_ >> 8));
    data.push_back(static_cast<byte>(count_));

    // Add last_
    data.push_back(static_cast<byte>(last_ >> 24));
    data.push_back(static_cast<byte>(last_ >> 16));
    data.push_back(static_cast<byte>(last_ >> 8));
    data.push_back(static_cast<byte>(last_));

    // Add b_
    data.insert(data.end(), bdata.begin(), bdata.end());

    return data;
}

void Sketch::mergeSparse() {
  if (tmpSet_.empty()) {
    return;
  }

  // Copies the elements in the temp set into a vector and sorts them
  std::vector<uint32_t> keys(tmpSet_.begin(), tmpSet_.end());
  std::sort(keys.begin(), keys.end());

  // Create a new compressed list
  std::shared_ptr<CompressedList> newList = std::make_shared<CompressedList>(m_);

  // Merge old compressed list and sorted temp set into new compressed list
  std::unique_ptr<Iterator> iter = sparseList_->Iter();
  size_t i = 0;
  while (iter->HasNext() || i < keys.size()) {
    if (!iter->HasNext()) {
      newList->Append(keys[i]);
      ++i;
      continue;
    }

    if (i >= keys.size()) {
      newList->Append(iter->Next());
      continue;
    }

    uint32_t x1 = iter->Peek();
    uint32_t x2 = keys[i];
    if (x1 == x2) {
      newList->Append(iter->Next());
      ++i;
    } else if (x1 > x2) {
      newList->Append(x2);
      ++i;
    } else {
      newList->Append(iter->Next());
    }
  }

  // Release the old compressed list and update it to the new one
  sparseList_ = newList;

  // Clear the temp set
  tmpSet_.clear();
}

void Sketch::toNormal() {
  if (!tmpSet_.empty()) {
    mergeSparse();
  }

  // Create Registers object
  regs_ = std::make_unique<Registers>(m_);

  // Extract data from sparseList and insert into registers
  auto iter = sparseList_->Iter();
  while (iter->HasNext()) {
    uint32_t nextValue = iter->Next();
    auto decoded = decodeHash(nextValue, p_, pp);
    insert(decoded.first, decoded.second);
  }

  // Convert to normal mode
  sparse_ = false;
  tmpSet_.clear();
  sparseList_ = nullptr;
}

void CompressedList::Append(uint32_t x) {
  count_++;
  // Calculate the difference from the previous element(Key point)
  uint32_t delta = x - last_;
  last_ = x;

  // Variable-length coding(Key point)
  while (delta >= 128) {
    // When delta is greater than or equal to 128, the lower 7 bits of delta are stored after an operation with 0x80
    b_.push_back(static_cast<byte>((delta & 0x7F) | 0x80));
    delta >>= 7;
  }
  b_.push_back(static_cast<byte>(delta));
}

// Get length
size_t CompressedList::len() const {
  return b_.size();
}

std::pair<uint32_t, int> CompressedList::decode(int i, uint32_t last) const {
  uint32_t x = 0;
  int j = i;
  int shift = 0;
  while (b_[j] & 0x80) {
    x |= static_cast<uint32_t>(b_[j] & 0x7F) << shift;
    j++;
    shift+=7;
  }
  x |= static_cast<uint32_t>(b_[j]) << shift;
  return {x + last, j + 1};
}

std::unique_ptr<Iterator> CompressedList::Iter() const {
  return std::make_unique<Iterator>(this);
}

uint32_t Iterator::Next() {
  auto result = list->decode(index, lastValue);
  lastValue = result.first;
  index = result.second;
  return lastValue;
}

uint32_t Iterator::Peek() const {
  auto result = list->decode(index, lastValue);
  return result.first;
}

bool Iterator::HasNext() const {
  return index < list->len();
}

// Gets the smallest nonzero value stored in the tailcuts_
uint8_t Registers::min() const {
  if (nz_ > 0) {
    return 0;
  }
  uint8_t minValue = 255;
  for (auto r : tailcuts_) {
    if (r == 0 || minValue == 0) {
      return 0;
    }
    uint8_t val1 = static_cast<uint8_t>(r << 4 >> 4);
    uint8_t val2 = static_cast<uint8_t>(r >> 4);
    minValue = std::min({minValue, val1, val2});
  }
  return minValue;
}

uint8_t Registers::get(uint32_t i) const {
  // Determines the index in the array
  // because of each byte contains the values of two registers
  uint32_t index = i / 2;
  // Determine whether it's the high 4-bit or the low 4-bit
  uint8_t offset = uint8_t(i) & 1;
  // Get the corresponding 8-bit value
  const uint8_t& r = tailcuts_[index];

  if (offset == 0) {
    // high 4-bit
    return r >> 4;
  } else {
    // lower 4-bit
    return r & 0x0F;
  }
}

void Registers::set(uint32_t i, uint8_t val) {
  // Determines the index in the array
  // because of each byte contains the values of two registers
  uint32_t index = i / 2;
  // Determine whether it's the high 4-bit or the low 4-bit
  uint8_t offset = uint8_t(i) & 1;
  uint8_t& r = tailcuts_[index];
  bool isZeroBefore = false;
  if (offset == 0) {
    // Determine whether the high 4 bits are zero
    isZeroBefore = (r >> 4) == 0;
    // Update the high value and keep the low value
    r = (r & 0x0F) | (val << 4);
  } else {
    // Determine whether the lower 4 bits are zero
    isZeroBefore = (r & 0x0F) == 0;
    // Update the lower value and keep the high value
    r = (r & 0xF0) | val;
  }
  // If it was zero before, but is not now zero (judging by the new value),
  // we need to update the non-zero counter
  bool isZeroAfter = (offset == 0) ? (r >> 4) == 0 : (r & 0x0F) == 0;
  if (isZeroBefore && !isZeroAfter) {
    nz_++;
  } else if (!isZeroBefore && isZeroAfter) {
    nz_--;
  }
}

void Registers::rebase(uint8_t delta) {
  // Initialize non-zero count based on the total number of half-bytes
  uint32_t nz = static_cast<uint32_t>(tailcuts_.size()) * 2;
  for (auto& r : tailcuts_) {
    for (uint8_t offset = 0; offset < 2; offset++) {
      // Extract value for each half-byte
      uint8_t val = offset == 0 ? uint8_t(r >> 4) : uint8_t(r << 4 >> 4);
      if (val >= delta) {
        // Subtract delta from value
        val -= delta;
        if (val > 0) {
          // Decrease non-zero count if the result is zero
          nz--;
        }
        // Update the half-byte with the new value
        if (offset == 0) {
          r = reg(uint8_t(r << 4 >> 4) | (val << 4));
        } else {
          r = reg(uint8_t(r >> 4 << 4) | val);
        }
      }
    }
  }
  nz_ = nz;  // Update the non-zero count to match the Go version logic
}

}  // namespace kwdbts
