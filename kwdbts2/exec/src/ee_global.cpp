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

#include "ee_global.h"

namespace kwdbts {

std::string EEIteratorErrCodeToString(EEIteratorErrCode code) {
  switch (code) {
    case EEIteratorErrCode::EE_OK:
      return "EEIteratorErrCode::EE_OK";
    case EEIteratorErrCode::EE_ERROR:
      return "EEIteratorErrCode::EE_ERROR";
    case EEIteratorErrCode::EE_DATA_ERROR:
      return "EEIteratorErrCode::EE_DATA_ERROR";
    case EEIteratorErrCode::EE_END_OF_RECORD:
      return "EEIteratorErrCode::EE_END_OF_RECORD";
    case EEIteratorErrCode::EE_NEXT_CONTINUE:
      return "EEIteratorErrCode::EE_NEXT_CONTINUE";
    case EEIteratorErrCode::EE_KILLED:
      return "EEIteratorErrCode::EE_KILLED";
    case EEIteratorErrCode::EE_QUIT:
      return "EEIteratorErrCode::EE_QUIT";
    case EEIteratorErrCode::EE_Sample:
      return "EEIteratorErrCode::EE_Sample";
    case EEIteratorErrCode::EE_TIMESLICE_OUT:
      return "EEIteratorErrCode::EE_TIMESLICE_OUT";
    case EEIteratorErrCode::EE_ERROR_ZERO:
      return "EEIteratorErrCode::EE_ERROR_ZERO";
    case EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED:
      return "EEIteratorErrCode::EE_PTAG_COUNT_NOT_MATCHED";
    default:
      return "EEIteratorErrCode::UNKNOWN";
  }
}

std::string KWDBTypeFamilyToString(KWDBTypeFamily type) {
  switch (type) {
    case KWDBTypeFamily::BoolFamily:
      return "BOOL";
    case KWDBTypeFamily::IntFamily:
      return "INT";
    case KWDBTypeFamily::FloatFamily:
      return "FLOAT";
    case KWDBTypeFamily::DecimalFamily:
      return "Decimal";
    case KWDBTypeFamily::DateFamily:
      return "Date";
    case KWDBTypeFamily::TimestampFamily:
      return "Timestamp";
    case KWDBTypeFamily::IntervalFamily:
      return "Interval";
    case KWDBTypeFamily::StringFamily:
      return "String";
    case KWDBTypeFamily::BytesFamily:
      return "Bytes";
    case KWDBTypeFamily::TimestampTZFamily:
      return "TimestampTZ";
    case KWDBTypeFamily::CollatedStringFamily:
      return "CollatedString";
    case KWDBTypeFamily::OidFamily:
      return "Oid";
    case KWDBTypeFamily::UnknownFamily:
      return "Unknown";
    case KWDBTypeFamily::UuidFamily:
      return "Uuid";
    case KWDBTypeFamily::ArrayFamily:
      return "Array";
    case KWDBTypeFamily::INetFamily:
      return "INet";
    case KWDBTypeFamily::TimeFamily:
      return "Time";
    case KWDBTypeFamily::JsonFamily:
      return "Json";
    case KWDBTypeFamily::TimeTZFamily:
      return "TimeTZ";
    case KWDBTypeFamily::TupleFamily:
      return "Tuple";
    case KWDBTypeFamily::BitFamily:
      return "Bit";
    case KWDBTypeFamily::AnyFamily:
      return "Any";
  }

  return "UNKNOWN";
}

}  // namespace kwdbts
