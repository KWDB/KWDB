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

#include "ee_combined_group_key.h"
#include <numeric>
#include <limits>
#include <cmath>

#include "ee_common.h"

namespace kwdbts {

/*
    to_string
*/
std::string CombinedGroupKey::to_string() {
  std::vector<std::string> vec;
  for (int i = 0; i < group_key_size_; i++) {
    if (is_null(i)) {
      vec.push_back("NULL");
    } else {
      GroupKeyValue& key = group_key_values_[i];
      switch (key.typ) {
        case roachpb::DataType::BOOL: {
          if (const k_bool* pval = static_cast<k_bool*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        }
        case roachpb::DataType::SMALLINT: {
          if (const k_int16* pval = static_cast<k_int16*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        }
        case roachpb::DataType::INT: {
          if (const k_int32* pval = static_cast<k_int32*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        }
        case roachpb::DataType::TIMESTAMP:
        case roachpb::DataType::TIMESTAMPTZ:
        case roachpb::DataType::TIMESTAMP_MICRO:
        case roachpb::DataType::TIMESTAMP_NANO:
        case roachpb::DataType::TIMESTAMPTZ_MICRO:
        case roachpb::DataType::TIMESTAMPTZ_NANO:
        case roachpb::DataType::DATE:
        case roachpb::DataType::BIGINT: {
          if (const k_int64* pval = static_cast<k_int64*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        }
        case roachpb::DataType::FLOAT: {
          if (const k_float32* pval = static_cast<k_float32*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
          }
          break;
        }
        case roachpb::DataType::DOUBLE: {
          if (const k_double64* pval = static_cast<k_double64*>(
                  static_cast<void*>(key.data))) {
            vec.push_back(std::to_string(*pval));
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
              key.data + 2,
              *static_cast<k_uint16*>(static_cast<void*>(key.data)));
          vec.push_back(pval);
          break;
        }
        default:
          break;
      }
    }
  }
  std::string result = std::accumulate(vec.begin(), vec.end(), std::string(),
                                       [](string& ss, string& s) {
                                         return ss.empty() ? s : ss + "," + s;
                                       });
  return result;
}

bool CombinedGroupKey::operator==(const CombinedGroupKey& other) const {
  if (group_key_size_ != other.group_key_size_) {
    return false;
  }

  // compare the value of each col
  for (int i = 0; i < group_key_size_; i++) {
    if (is_null(i) != other.is_null(i)) {
      // is_null indicates is inconsistent
      return false;
    } else if (is_null(i) && other.is_null(i)) {
      // all null
      continue;
    }
    GroupKeyValue& key = group_key_values_[i];
    if (key.typ != other.group_key_values_[i].typ) {
      return false;
    }

    if (IsStringType(key.typ)) {
      k_uint16* len = static_cast<k_uint16*>(
          static_cast<void*>(other.group_key_values_[i].data));
      k_uint16* key_len = static_cast<k_uint16*>(static_cast<void*>(key.data));
      if ((*len) == (*key_len) &&
          memcmp(other.group_key_values_[i].data, key.data, (*len) + 2) == 0) {
        continue;
      } else {
        return false;
      }
    } else {
      if (memcmp(key.data, other.group_key_values_[i].data, key.len) != 0) {
        return false;
      }
    }
  }
  return true;
}

bool CombinedGroupKey::IsNewGroup(DataChunk *chunk, k_uint32 row,
                                  const std::vector<k_uint32>& group_cols) {
  for (int i = 0; i < group_key_size_; i++) {
    k_uint32 col = group_cols[i];
    GroupKeyValue& key = group_key_values_[i];
    if (key.allow_null) {
      auto is_null = chunk->IsNull(row, col);
      if (key.is_null != is_null) {
        return true;
      } else if (key.is_null && is_null) {
        continue;
      }
    }

    DatumPtr ptr = chunk->GetData(row, col);
    if (key.data == nullptr && ptr != nullptr) {
      return true;
    }
    if (IsStringType(key.typ)) {
      k_uint16* len = static_cast<k_uint16*>(static_cast<void*>(ptr));
      k_uint16* key_len = static_cast<k_uint16*>(static_cast<void*>(key.data));
      if ((*len) == (*key_len) && memcmp(ptr, key.data, (*len) + 2) == 0) {
        continue;
      } else {
        return true;
      }
    } else {
      if (memcmp(key.data, ptr, key.len) != 0) {
        return true;
      }
    }
  }
  return false;
}

void CombinedGroupKey::AddGroupKey(DatumPtr ptr, k_int32 index) {
  if (ptr == nullptr) {
    group_key_values_[index].is_null = true;
    return;
  }
  GroupKeyValue &key = group_key_values_[index];
  key.data = ptr;
  key.is_null = false;
}

}  // namespace kwdbts
