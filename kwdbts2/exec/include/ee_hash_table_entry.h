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
#include "kwdb_type.h"

namespace kwdbts {

//! The ht_entry_t struct represents an individual entry within a hash table.
/*!
    This struct is used by the JoinHashTable and AggregateHashTable to store
   entries within the hash table. It stores a pointer to the data and a salt
   value in a single hash_t and can return or modify the pointer and salt
    individually.
*/
struct hash_table_entry_t {  // NOLINT
 public:
  //! Upper 16 bits are salt, lower 48 bits are the pointer
  static constexpr const k_uint64 SALT_MASK = 0xFFFF000000000000;
  static constexpr const k_uint64 POINTER_MASK = 0x0000FFFFFFFFFFFF;

  hash_table_entry_t() noexcept : value(0) {}

  explicit hash_table_entry_t(k_uint64 value_p) noexcept : value(value_p) {}

  hash_table_entry_t(const k_uint64& salt, const void*& pointer)
      : value(static_cast<k_uint64>(reinterpret_cast<uintptr_t>(pointer)) |
              (salt & SALT_MASK)) {}

  inline bool IsOccupied() const { return value != 0; }

  //! Returns a pointer based on the stored value (asserts if the cell is
  //! occupied)
  inline DatumPtr GetPointer() const { return GetPointerOrNull(); }

  //! Returns a pointer based on the stored value
  inline DatumPtr GetPointerOrNull() const {
    return reinterpret_cast<DatumPtr>(value & POINTER_MASK);
  }

  inline void SetPointer(const DatumPtr& pointer) {
    // Set upper bits to 1 in pointer so the salt stays intact
    value |= static_cast<k_uint64>(reinterpret_cast<uintptr_t>(pointer)) &
             POINTER_MASK;
  }

  // Returns the salt, leaves upper salt bits intact, sets lower bits to all 1's
  static inline k_uint64 ExtractSalt(const k_uint64& hash) {
    return hash | POINTER_MASK;
  }

  inline k_uint64 GetSalt() const { return ExtractSalt(value); }

  inline void SetSalt(const k_uint64& salt) {
    // No need to mask, just put the whole thing there
    value = salt & SALT_MASK;
  }

 private:
  k_uint64 value;
};

// uses an AND operation to apply the modulo operation instead of an if
// condition that could be branch mispredicted
inline void IncrementAndWrap(k_uint64& offset, const k_uint64& capacity_mask) {
  ++offset &= capacity_mask;
}

}  // namespace kwdbts
