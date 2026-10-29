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

#include "ee_data_container.h"

#include <parallel/algorithm>

#include <queue>
#include <chrono>
#include <cmath>

#include "ee_exec_pool.h"
#include "ee_aggregate_func.h"
#include "lg_api.h"

namespace kwdbts {

// a, b represents the indexes of two rows of data in the container,
// respectively
bool OrderColumnCompare::operator()(k_uint32 a, k_uint32 b) {
  // ColunnInfo
  ColumnInfo *col_info = container_->GetColumnInfo();

  // compare
  for (int i = 0; i < order_info_.size(); i++) {
    int col_idx = order_info_[i].col_idx;

    // dispose Null
    bool is_a_null = container_->IsNull(a, col_idx);
    bool is_b_null = container_->IsNull(b, col_idx);

    // a,b is null
    if (is_a_null && is_b_null) {
      continue;
    }

    if (is_a_null && !is_b_null) {
      // a is null，b is not null
      return order_info_[i].direction == TSOrdering_Column::ASC ? true : false;
    } else if (!is_a_null && is_b_null) {
      // a is not null，b is null
      return order_info_[i].direction == TSOrdering_Column::ASC ? false : true;
    }

    // compare not null
    switch (col_info[col_idx].storage_type) {
      case roachpb::DataType::BOOL: {
        auto* a_data = reinterpret_cast<k_bool*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_bool*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::SMALLINT: {
        auto* a_data = reinterpret_cast<k_int16*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int16*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::INT: {
        auto* a_data = reinterpret_cast<k_int32*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int32*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BIGINT: {
        auto* a_data = reinterpret_cast<k_int64*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_int64*>(container_->GetData(b, col_idx));
        if (*a_data == *b_data) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::FLOAT: {
        auto* a_data = reinterpret_cast<k_float32*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_float32*>(container_->GetData(b, col_idx));
        // float
        if (fabs(*a_data - *b_data) < std::numeric_limits<float>::epsilon()) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::DOUBLE: {
        auto* a_data = reinterpret_cast<k_double64*>(container_->GetData(a, col_idx));
        auto* b_data = reinterpret_cast<k_double64*>(container_->GetData(b, col_idx));
        // double
        if (fabs(*a_data - *b_data) < std::numeric_limits<double>::epsilon()) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               *a_data < *b_data : *a_data > *b_data;
      }
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::VARBINARY: {
        auto* a_data = reinterpret_cast<char*>(container_->GetData(a, col_idx));
        k_uint16 a_len;
        std::memcpy(&a_len, a_data, sizeof(k_uint16));
        std::string a_str = std::string{a_data + sizeof(k_uint16), a_len};

        auto* b_data = reinterpret_cast<char*>(container_->GetData(b, col_idx));
        k_uint16 b_len;
        std::memcpy(&b_len, b_data, sizeof(k_uint16));
        std::string b_str = std::string{b_data + sizeof(k_uint16), b_len};

        if (a_str.compare(b_str) == 0) {
          continue;
        }

        return order_info_[i].direction == TSOrdering_Column::ASC ?
               a_str.compare(b_str) < 0 : a_str.compare(b_str) > 0;
      }
      case roachpb::DataType::DECIMAL: {
        DatumPtr a_data = container_->GetData(a, col_idx);
        DatumPtr b_data = container_->GetData(b, col_idx);

        int cmp_result = AggregateFunc::CompareDecimal(a_data, b_data);
        if (cmp_result == 0) {
          continue;
        }
        return order_info_[i].direction == TSOrdering_Column::ASC ?
               cmp_result < 0 : cmp_result > 0;
      }
      default:
        return true;
    }
  }
  return a < b;
}

}  // namespace kwdbts
