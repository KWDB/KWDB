// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
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
#include "data_type.h"
#include "utils/big_table_utils.h"


AttributeInfo::AttributeInfo() {
  id = 0;
  type = INVALID;
  offset = size = 0;
  length = 1;
  encoding = 0;
  flag = 0;
  max_len = 0;
  version = 0;
  col_flag = COL_INVALID;
}

bool AttributeInfo::operator==(AttributeInfo& rhs) const {
  if (strcmp(name, rhs.name) != 0)
    return false;
  return true;
}

std::string getDataTypeName(int32_t data_type) {
  switch (data_type)
  {
    case DATATYPE::INT16:   return "INT16";
    case DATATYPE::INT32:   return "INT32";
    case DATATYPE::INT64:   return "INT64";
    case DATATYPE::FLOAT:   return "FLOAT";
    case DATATYPE::DOUBLE:  return "DOUBLE";
    case DATATYPE::CHAR:    return "CHAR";
    case DATATYPE::BINARY:  return "BINARY";
    case DATATYPE::VARSTRING: return "VARSTRING";
    case DATATYPE::VARBINARY: return "VARBINARY";
    default:
      break;
  }
  return "UNKNOW_TYPE";
}
