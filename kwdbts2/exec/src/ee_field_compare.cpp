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

#include "ee_field_compare.h"

#include <regex.h>

#include <regex>

#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

static k_int64 getRegexValue(k_bool negation, k_bool is_case, String strvalue1, String strvalue2) {
  std::string str = std::string(strvalue1.getptr(), strvalue1.length_);
  std::string partstr = std::string(strvalue2.getptr(), strvalue2.length_);
  if (is_case) {
    // Convert all letters in the string to lowercase
    std::transform(str.begin(), str.end(), str.begin(), [](unsigned char c) { return std::tolower(c); });
    std::transform(partstr.begin(), partstr.end(), partstr.begin(), [](unsigned char c) { return std::tolower(c); });
  }
  try {
    std::regex pattern(partstr);
    k_int64 result = 0;
    if (std::regex_search(str, pattern)) {
      result = 1;
    }
    if (negation) {
      return !result;
    }
    return result;
  } catch (std::regex_error &e) {
    KString et = "regex_error";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_REGEXP_MISMATCH, et.data());
    return 0;
  }
}

char *FieldFuncComparison::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncEq::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncComparison::ValInt(ptr);
  } else {
    COMPARE_CHECK_NULL;
    return cmp.compare(nullptr, nullptr, false, false) == 0;
  }
}

k_double64 FieldFuncEq::ValReal() { return ValInt(); }

String FieldFuncEq::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncEq::field_to_copy() {
  FieldFuncEq *field = new FieldFuncEq(*this);

  return field;
}

k_int64 FieldFuncNotEq::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncComparison::ValInt(ptr);
  } else {
    COMPARE_CHECK_NULL;
    return cmp.compare(nullptr, nullptr, false, false) != 0;
  }
}

k_double64 FieldFuncNotEq::ValReal() { return ValInt(); }

String FieldFuncNotEq::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
  // return std::to_string(ValInt());
}

Field *FieldFuncNotEq::field_to_copy() {
  FieldFuncNotEq *field = new FieldFuncNotEq(*this);

  return field;
}

k_int64 FieldFuncLess::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncComparison::ValInt(ptr);
  } else {
    COMPARE_CHECK_NULL;
    return cmp.compare(nullptr, nullptr, false, false) < 0;
  }
}

k_double64 FieldFuncLess::ValReal() { return ValInt(); }

String FieldFuncLess::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncLess::field_to_copy() {
  FieldFuncLess *field = new FieldFuncLess(*this);

  return field;
}


k_int64 FieldFuncLessEq::ValInt() {
  COMPARE_CHECK_NULL;
  return cmp.compare(nullptr, nullptr, false, false) <= 0;
}

k_double64 FieldFuncLessEq::ValReal() { return ValInt(); }

String FieldFuncLessEq::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncLessEq::field_to_copy() {
  FieldFuncLessEq *field = new FieldFuncLessEq(*this);

  return field;
}

k_int64 FieldFuncGt::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncComparison::ValInt(ptr);
  } else {
    COMPARE_CHECK_NULL;
    return cmp.compare(nullptr, nullptr, false, false) > 0;
  }
}

k_double64 FieldFuncGt::ValReal() { return ValInt(); }

String FieldFuncGt::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncGt::field_to_copy() {
  FieldFuncGt *field = new FieldFuncGt(*this);

  return field;
}

k_int64 FieldFuncGtEq::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncComparison::ValInt(ptr);
  } else {
    COMPARE_CHECK_NULL;
    return cmp.compare(nullptr, nullptr, false, false) >= 0;
  }
}

k_double64 FieldFuncGtEq::ValReal() { return ValInt(); }

String FieldFuncGtEq::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncGtEq::field_to_copy() {
  FieldFuncGtEq *field = new FieldFuncGtEq(*this);

  return field;
}

char *FieldFuncLike::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncLike::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldLikeComparison::ValInt(ptr);
  } else {
    k_int32 ret = 1;
    if (args_[0]->CheckNull() || args_[1]->CheckNull()) {
      return 0;
    }
    cmp.set_case(is_case_);
    ret = cmp.likecompare();
    if (negation_) {
      return !ret;
    }
    return ret;
  }
}

k_double64 FieldFuncLike::ValReal() { return ValInt(); }

String FieldFuncLike::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncLike::field_to_copy() {
  FieldFuncLike *field = new FieldFuncLike(*this);

  return field;
}

char *FieldCondAnd::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  intvalue_ = 1;
  for (auto it : list_) {
    switch (it->get_storage_type()) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BOOL:
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BIGINT:
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE:
      case roachpb::DataType::CHAR:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::VARBINARY: {
        char *ptr = it->get_ptr(batch);
        if (ptr == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                        "could not parse \"\" field cond_and, get null value");
          return const_cast<char *>("");
        }
        if (0 == it->ValInt(ptr)) {
          intvalue_ = 0;
          return reinterpret_cast<char *>(&intvalue_);
        }
        break;
      }
      default:
        err = FAIL;
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func cond_and.");
        break;
    }
  }
  if (err != SUCCESS) {
    return const_cast<char *>("");
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldCondAnd::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldCond::ValInt(ptr);
  } else {
    k_int64 ret = 1;
    for (auto it : list_) {
      if (0 == it->ValInt()) {
        ret = 0;
        break;
      }
    }
    return ret;
  }
}

k_double64 FieldCondAnd::ValReal() { return ValInt(); }

String FieldCondAnd::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldCondAnd::field_to_copy() {
  FieldCondAnd *field = new FieldCondAnd(*this);

  return field;
}

char *FieldCondOr::get_ptr(RowBatch *batch) {
  std::string ret = "";
  char *ptrResult = nullptr;

  switch (storage_type_) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE: {
      intvalue_ = 0;
      for (auto it : list_) {
        char *ptr = it->get_ptr(batch);
        if (ptr == nullptr) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                        "could not parse \"\" field cond_or, get null value");
          return const_cast<char *>("");
        }
        if (it->ValInt(ptr)) {
          intvalue_ = 1;
          break;
        }
      }
      ptrResult = reinterpret_cast<char *>(&intvalue_);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func cond_or.");
      return const_cast<char *>("");
  }

  return ptrResult;
}

k_int64 FieldCondOr::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldCond::ValInt(ptr);
  } else {
    k_int64 ret = 0;
    for (auto it : list_) {
      if (it->ValInt()) {
        ret = 1;
        break;
      }
    }

    return ret;
  }
}

k_double64 FieldCondOr::ValReal() { return ValInt(); }

String FieldCondOr::ValStr() { return String(""); }

Field *FieldCondOr::field_to_copy() {
  FieldCondOr *field = new FieldCondOr(*this);

  return field;
}

FieldFuncOptNeg::~FieldFuncOptNeg() {
  if (nullptr != in_list_) {
    SafeDeletePointer(in_list_);
  }
  negation_ = 0;
}

void FieldFuncOptNeg::fix_fields(const std::list<Field **> &values,
                                 size_t count) {
  in_list_ = FieldInList::get_field_in_list(values, count);
}

char *FieldFuncIn::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncIn::ValInt() {
  if (have_null_ && negation_) {
    return 0;
  }
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncOptNeg::ValInt(ptr);
  } else {
    k_bool ret = in_list_->compare(args_);
    if (negation_) {
      return 1 == ret ? 1 : 0;
    }

    return 0 == ret ? 1 : 0;
  }
}

k_double64 FieldFuncIn::ValReal() { return ValInt(); }

String FieldFuncIn::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncIn::field_to_copy() {
  FieldFuncIn *field = new FieldFuncIn(*this);

  return field;
}

char *FieldCondIsNull::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldCondIsNull::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  if (negation_) {
    return !args_[0]->CheckNull();
  } else {
    return args_[0]->CheckNull();
  }
}

k_double64 FieldCondIsNull::ValReal() { return ValInt(); }

String FieldCondIsNull::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldCondIsNull::field_to_copy() {
  FieldCondIsNull *field = new FieldCondIsNull(*this);

  return field;
}

char *FieldCondIsUnknown::get_ptr(RowBatch *batch) {
  intvalue_ = ValInt();
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldCondIsUnknown::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  if (negation_) {
    return !args_[0]->CheckNull();
  } else {
    return args_[0]->CheckNull();
  }
}

k_double64 FieldCondIsUnknown::ValReal() { return ValInt(); }

String FieldCondIsUnknown::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldCondIsUnknown::field_to_copy() {
  FieldCondIsUnknown *field = new FieldCondIsUnknown(*this);

  return field;
}

char *FieldCondIsNan::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  switch (args_[0]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      char *ptr = args_[0]->get_ptr(batch);
      if (ptr == nullptr) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field isnan, get null value");
        return const_cast<char *>("");
      }
      std::string uppercaseStr;
      String s1 = args_[0]->ValStr(ptr);
      std::string original = {s1.getptr(), s1.length_};
      for (char c : original) {
        uppercaseStr += std::toupper(c);
      }
      if (negation_) {
        intvalue_ = !(uppercaseStr == "NAN");
      } else {
        intvalue_ = uppercaseStr == "NAN";
      }
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func isnan.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldCondIsNan::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  std::string uppercaseStr;
  String s1 = args_[0]->ValStr();
  std::string original = {s1.getptr(), s1.length_};
  k_int64 result = 0;
  for (char c : original) {
    uppercaseStr += std::toupper(c);
  }
  if (negation_) {
    result = !(uppercaseStr == "NAN");
    return result;
  } else {
    return uppercaseStr == "NAN";
  }
}

k_double64 FieldCondIsNan::ValReal() { return ValInt(); }

String FieldCondIsNan::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldCondIsNan::field_to_copy() {
  FieldCondIsNan *field = new FieldCondIsNan(*this);

  return field;
}

char *FieldFuncRegex::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  if (args_[0]->CheckNull() || args_[1]->CheckNull()) {
    return const_cast<char *>("");
  }

  switch (args_[0]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      char *ptr0 = args_[0]->get_ptr(batch);
      char *ptr1 = args_[1]->get_ptr(batch);
      if ((ptr0 == nullptr) || (ptr1 == nullptr)) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                      "could not parse \"\" field regex, get null value");
        return const_cast<char *>("");
      }
      String s0 = args_[0]->ValStr(ptr0);
      String s1 = args_[1]->ValStr(ptr1);
      intvalue_ = getRegexValue(negation_, is_case_, s0, s1);
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func regex.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncRegex::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncBool::ValInt(ptr);
  } else {
    if (args_[0]->CheckNull() || args_[1]->CheckNull()) {
      return 0;
    }
    return getRegexValue(negation_, is_case_, args_[0]->ValStr(), args_[1]->ValStr());
  }
}

k_double64 FieldFuncRegex::ValReal() { return ValInt(); }

String FieldFuncRegex::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncRegex::field_to_copy() {
  FieldFuncRegex *field = new FieldFuncRegex(*this);

  return field;
}
k_bool FieldFuncRegex::field_is_nullable() {
  if (args_[0]->CheckNull() || args_[1]->CheckNull()) {
    return true;
  }
  return false;
}

char *FieldFuncAny::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  intvalue_ = 0;

  for (size_t i = 0; i < arg_count_; i++) {
    if (args_[i]->CheckNull()) {
      continue;
    }
    char *ptr = args_[i]->get_ptr(batch);
    if (ptr == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    "could not parse \"\" field func_any, get null value");
      return const_cast<char *>("");
    }
    switch (args_[1]->get_storage_type()) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BOOL:
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BIGINT:
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE:
      case roachpb::DataType::CHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARBINARY: {
        if (args_[i]->ValInt(ptr) == 1) {
          intvalue_ = 1;
          return reinterpret_cast<char *>(&intvalue_);
        }
        break;
      }
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func_any.");
        err = FAIL;
        break;
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncAny::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  for (size_t i = 0; i < arg_count_; i++) {
    if (args_[i]->CheckNull()) {
      continue;
    }
    if (args_[i]->ValInt() == 1) {
      return 1;
    }
  }
  return 0;
}

k_double64 FieldFuncAny::ValReal() { return ValInt(); }

String FieldFuncAny::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncAny::field_to_copy() {
  FieldFuncAny *field = new FieldFuncAny(*this);

  return field;
}

char *FieldFuncAll::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  intvalue_ = 1;

  for (size_t i = 0; i < arg_count_; i++) {
    if (args_[i]->CheckNull()) {
      continue;
    }
    char *ptr = args_[i]->get_ptr(batch);
    if (ptr == nullptr) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                    "could not parse \"\" field func all, get null value");
      return const_cast<char *>("");
    }
    switch (args_[1]->get_storage_type()) {
      case roachpb::DataType::TIMESTAMP:
      case roachpb::DataType::TIMESTAMPTZ:
      case roachpb::DataType::TIMESTAMP_MICRO:
      case roachpb::DataType::TIMESTAMP_NANO:
      case roachpb::DataType::TIMESTAMPTZ_MICRO:
      case roachpb::DataType::TIMESTAMPTZ_NANO:
      case roachpb::DataType::DATE:
      case roachpb::DataType::BOOL:
      case roachpb::DataType::SMALLINT:
      case roachpb::DataType::INT:
      case roachpb::DataType::BIGINT:
      case roachpb::DataType::FLOAT:
      case roachpb::DataType::DOUBLE:
      case roachpb::DataType::CHAR:
      case roachpb::DataType::BINARY:
      case roachpb::DataType::NCHAR:
      case roachpb::DataType::VARCHAR:
      case roachpb::DataType::NVARCHAR:
      case roachpb::DataType::VARBINARY: {
        if (args_[i]->ValInt(ptr) != 1) {
          intvalue_ = 0;
          return reinterpret_cast<char *>(&intvalue_);
        }
        break;
      }
      default:
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func all.");
        err = FAIL;
        break;
    }
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncAll::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  for (size_t i = 0; i < arg_count_; i++) {
    if (args_[i]->CheckNull()) {
      continue;
    }
    if (args_[i]->ValInt() != 1) {
      return 0;
    }
  }
  return 1;
}

k_double64 FieldFuncAll::ValReal() { return ValInt(); }

String FieldFuncAll::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncAll::field_to_copy() {
  FieldFuncAll *field = new FieldFuncAll(*this);

  return field;
}

char *FieldFuncNot::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field  func not, get null value");
    return const_cast<char *>("");
  }
  intvalue_ = 0;
  switch (args_[0]->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY: {
      if (args_[0]->ValInt(ptr) == 0) {
        intvalue_ = 1;
      }
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func not.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncNot::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFuncBool::ValInt(ptr);
  }
  if (args_[0]->ValInt() == 0) {
    return 1;
  }
  return 0;
}

k_double64 FieldFuncNot::ValReal() { return ValInt(); }

String FieldFuncNot::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncNot::field_to_copy() {
  FieldFuncNot *field = new FieldFuncNot(*this);

  return field;
}
}  // namespace kwdbts
