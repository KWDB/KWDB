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

#include <functional>
#include <vector>
#include <string>
#include <variant>

#include "ee_pb_plan.pb.h"
#include "kwdb_type.h"
#include "ee_data_chunk.h"

#ifndef UINT64_C
#define UINT64_C(c) c ## ULL
#endif

namespace kwdbts {

/*
    hash_value is called without qualification, so that overloads can be found
via ADL.

This is an extension to TR1
*/
// template<class T>
// inline void hash_combine(std::size_t& seed, const T& v) {
//   std::hash<T> hasher;
//   seed ^= hasher(v) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
// }


// FIXME: fix me if a better combined hash function is found
template<class T>
inline void hash_combine(std::size_t& h, const T& v) {
  std::hash<T> hasher;
  h = (hasher(v) * UINT64_C(0xbf58476d1ce4e5b9)) ^ h;
}

/*
    The GroupKey combination of the aggregate function is used as the key value
   of the hash table in the aggregation operator implementation
*/
class CombinedGroupKey {
  struct GroupKeyValue {
    char* data{nullptr};
    bool is_null{true};
    roachpb::DataType typ;
    k_int32 len{0};
    bool allow_null{false};
  };

 public:
  CombinedGroupKey() = default;
  ~CombinedGroupKey() { SafeDeleteArray(group_key_values_); }

  CombinedGroupKey(const CombinedGroupKey& other) {
    group_key_values_ = other.group_key_values_;
    group_key_size_ = other.group_key_size_;
  }

  bool Init(ColumnInfo* col_info, const std::vector<k_uint32>& group_cols) {
    k_int32 col_num = group_cols.size();
    group_key_size_ = col_num;
    group_key_values_ = KNEW GroupKeyValue[col_num];
    if (!group_key_values_) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_OUT_OF_MEMORY,
                                    "Insufficient memory");
      return false;
    }
    for (k_int32 i = 0; i < col_num; i++) {
      group_key_values_[i].len = col_info[group_cols[i]].storage_len;
      group_key_values_[i].typ = col_info[group_cols[i]].storage_type;
      group_key_values_[i].allow_null = col_info[group_cols[i]].allow_null;
    }
    is_init_ = true;
    return true;
  }

  void AddGroupKey(DatumPtr ptr, k_int32 index);

  bool is_null(k_uint32 i) const { return group_key_values_[i].is_null == true; }

  // overload ==
  bool operator==(const CombinedGroupKey& other) const;

  std::string to_string();

  /**
   * @brief whether the row of data chunk belongs to a new group
   * @param[in] chunk
   * @param[in] row
   * @param[in] group_cols index of group columns
   */
  bool IsNewGroup(DataChunk *chunk, k_uint32 row,
                  const std::vector<k_uint32>& group_cols);

  // the real data，Otherwise, the data pointed to by the pointer may be
  // released
  GroupKeyValue* group_key_values_{nullptr};
  k_int32 group_key_size_{0};
  DataChunkPtr chunk_{nullptr};
  bool is_init_{false};
};

// calculat hash value
struct GroupKeyHasher {
  std::size_t operator()(const CombinedGroupKey& gkc) const {
    std::size_t h = 13;
    for (int i = 0; i < gkc.group_key_size_; i++) {
      if (gkc.is_null(i)) {
        continue;
      }

      // calculate the hash value of each column
      switch (gkc.group_key_values_[i].typ) {
        case roachpb::DataType::BOOL: {
          if (const k_bool* pval = static_cast<k_bool*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::SMALLINT: {
          if (const k_int16* pval = static_cast<k_int16*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::INT: {
          if (const k_int32* pval = static_cast<k_int32*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          if (const k_int64* pval = static_cast<k_int64*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::FLOAT: {
          if (const k_float32* pval = static_cast<k_float32*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::DOUBLE: {
          if (const k_double64* pval = static_cast<k_double64*>(
                  static_cast<void*>(gkc.group_key_values_[i].data))) {
            hash_combine(h, *pval);
          }
          break;
        }
        case roachpb::DataType::CHAR:
        case roachpb::DataType::VARCHAR:
        case roachpb::DataType::NCHAR:
        case roachpb::DataType::NVARCHAR:
        case roachpb::DataType::BINARY:
        case roachpb::DataType::VARBINARY: {
          std::string pval = std::string(
              gkc.group_key_values_[i].data + 2,
              *static_cast<k_uint16*>(
                  static_cast<void*>(gkc.group_key_values_[i].data)));
          hash_combine(h, pval);
          break;
        }
        default:
          // Handle unsupported data types here
          LOG_ERROR("unsupported data type");
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type");
          break;
      }
    }
    // printf("hash value: %lu\n", h);
    return h;
  }
};

}  // namespace kwdbts
