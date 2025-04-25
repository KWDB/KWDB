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

#include "ee_field_typecast.h"

#include "ee_field.h"
#include "ee_global.h"
#include "ee_table.h"
#include "me_metadata.pb.h"
#include "ee_timestamp_utils.h"

namespace kwdbts {

FieldTypeCast::~FieldTypeCast() {}

k_int64 FieldTypeCast::ValInt(char *ptr) {
  if (roachpb::DataType::SMALLINT == storage_type_ ||
      roachpb::DataType::INT == storage_type_) {
    k_int32 val = 0;
    memcpy(&val, ptr, storage_len_);
    return val;
  } else if (roachpb::DataType::BOOL == storage_type_) {
    bool val = 0;
    memcpy(&val, ptr, storage_len_);
    return val;
  }
  k_int64 val = 0;
  memcpy(&val, ptr, storage_len_);
  return val;
}

k_double64 FieldTypeCast::ValReal(char *ptr) {
  if (roachpb::DataType::FLOAT == storage_type_) {
    k_float32 val = 0;
    memcpy(&val, ptr, storage_len_);
    return val;
  }

  k_double64 val = 0;
  memcpy(&val, ptr, storage_len_);

  return val;
}

String FieldTypeCast::ValStr(char *ptr) { return ValTempStr(ptr); }

char *FieldTypeCast::get_ptr() { return nullptr; }

k_bool FieldTypeCast::is_nullable() { return field_->is_nullable(); }

k_bool FieldTypeCast::fill_template_field(char *ptr) {
  if (field_->is_nullable()) {
    return 1;
  }

  switch (storage_type_) {
    case roachpb::DataType::BOOL: {
      k_bool val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::SMALLINT: {
      k_int16 val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::INT: {
      k_int32 val = ValInt();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::TIMESTAMP: {
      k_int64 val = ValInt();
      memcpy(ptr, &val, sizeof(k_int64));
      break;
    }
    case roachpb::DataType::FLOAT: {
      k_float32 val = ValReal();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::DOUBLE: {
      k_double64 val = ValReal();
      memcpy(ptr, &val, storage_len_);
      break;
    }
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      String str = ValStr();
      k_uint16 len = static_cast<k_uint16>(str.size());
      memcpy(ptr, &len, sizeof(k_uint16));
      memcpy(ptr + sizeof(k_uint16), str.c_str(), len);
      break;
    }
    default: {
      break;
    }
  }

  return 0;
}

template <typename T>
std::string getCustomTypeName() {
  if (std::is_same<T, k_int16>::value) {
    return "int2";
  } else if (std::is_same<T, k_int32>::value) {
    return "int4";
  } else if (std::is_same<T, k_int64>::value) {
    return "int8";
  }

  // return the original type name by default
  return typeid(T).name();
}

inline KStatus integerToInteger(Field *field, k_int64 &output) {
  output = field->ValInt();
  return SUCCESS;
}

inline KStatus doubleToInteger(Field *field, k_int64 &output) {
  k_double64 v = field->ValReal();
  if (isinf(v) || isnan(v)) {
    // Throw Error
    return FAIL;
  }
  output = v;
  return SUCCESS;
}

inline KStatus stringToInteger(Field *field, k_int64 &output) {
  String str = field->ValStr();
  if (str.empty()) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" as type int");
    return FAIL;
  }
  return strToInt64(
      const_cast<char *>(std::string(str.getptr(), str.length_).c_str()),
      output);
}

inline KStatus integerToDouble(Field *field, k_double64 &output) {
  output = field->ValInt();
  return SUCCESS;
}

inline KStatus doubleToDouble(Field *field, k_double64 &output) {
  output = field->ValReal();
  return SUCCESS;
}

inline KStatus stringToDouble(Field *field, k_double64 &output) {
  String str = field->ValStr();

  if (str.empty()) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" as type float");
    return FAIL;
  }
  return strToDouble(
      const_cast<char *>(std::string(str.getptr(), str.length_).data()),
      output);
}

inline KStatus timestamptzToString(Field *field, char *output, k_uint32 length, k_int64 type_scale,
                                   k_int8 time_zone) {
  k_int64 timestamp = field->ValInt();
  time_t time = static_cast<time_t>(timestamp / type_scale) + time_zone * 3600;
  struct tm local_time;
  memset(&local_time, 0, sizeof(local_time));
  gmtime_r(&time, &local_time);

  char buffer[30] = {0};
  std::strftime(buffer, 30, "%04Y-%m-%d %H:%M:%S", &local_time);

  int seconds_after_decimal = timestamp % type_scale;
  KString milli_second_str = ".";
  if (seconds_after_decimal == 0) {
    milli_second_str = "";
  } else {
    type_scale /= 10;
    while (type_scale > seconds_after_decimal) {
      milli_second_str += '0';
      type_scale /=10;
    }
    while (seconds_after_decimal % 10 == 0) {
      seconds_after_decimal /= 10;
    }
    milli_second_str += to_string(seconds_after_decimal);
  }
  KString time_zone_str = "";
  if (time_zone < 10) {
    time_zone_str = "0";
  }
  time_zone_str += to_string(time_zone);
  strncpy(output,
          (std::string(buffer) + milli_second_str + "+" + time_zone_str + ":00")
              .c_str(),
          length - 1);
  output[length - 1] = '\0';
  return SUCCESS;
}

inline KStatus timestampToString(Field *field, char *output, k_uint32 length, k_int64 type_scale) {
  return timestamptzToString(field, output, length, type_scale, 0);
}

inline KStatus integerToString(Field *field, char *output, k_uint32 length) {
  strncpy(output, std::to_string(field->ValInt()).c_str(), length);
  output[length - 1] = '\0';
  return SUCCESS;
}

inline KStatus doubleToString(Field *field, char *output, k_uint32 length) {
  return doubleToStr(field->ValReal(), output, length);
}

inline KStatus stringToString(Field *field, char *output, k_uint32 length) {
  String res = field->ValStr();
  k_uint32 tmp = length - 1;
  if (tmp > res.length_) tmp = res.length_;
  strncpy(output, res.c_str(), tmp);
  output[tmp] = '\0';
  return SUCCESS;
}

inline KStatus integerToTimestampTz(Field *field, k_int64 &output, k_int64 scale,
                                    k_int64 time_zone_diff, roachpb::DataType out_type) {
  output = field->ValInt();
  if (!I64_SAFE_ADD_CHECK(out_type, time_zone_diff)) {
    return FAIL;
  }
  output += time_zone_diff;
  return SUCCESS;
}
inline KStatus stringToTimestampTz(Field *field, k_int64 &output, k_int64 scale,
                                   k_int64 time_zone_diff, roachpb::DataType out_type) {
  String str = field->ValStr();
  if (str.empty()) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_DATETIME_FORMAT,
        "parsing as type timestamp: empty or blank input");
    return FAIL;
  }
  convertStringToTimestamp(std::string(str.getptr(), str.length_), scale, &output);
  return SUCCESS;
}
inline KStatus timestamptzToTimestampTz(Field *field, k_int64 &output, k_int64 scale,
                                    k_int64 time_zone_diff, roachpb::DataType out_type) {
  output = field->ValInt();
  if (!I64_SAFE_ADD_CHECK(out_type, time_zone_diff)) {
    return FAIL;
  }
  output += time_zone_diff;
  return convertTimePrecision(&output, field->get_storage_type(), out_type)
             ? SUCCESS
             : FAIL;
}
inline KStatus numToBool(Field *field, k_bool &output) {
  output = field->ValInt() != 0 ? 1 : 0;
  return SUCCESS;
}

inline KStatus stringToBool(Field *field, k_bool &output) {
  String s = field->ValStr();
  std::string str = std::string(s.getptr(), s.length_);
  if (str.empty()) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_TEXT_REPRESENTATION,
        "could not parse \"\" as type bool: invalid bool value");
    return FAIL;
  }
  if (str == "1" || str == "true") {
    output = 1;
    return SUCCESS;
  } else if (str == "0" || str == "false") {
    output = 0;
    return SUCCESS;
  } else {
    KString msg =
        "could not parse \"" + str + "\" as type bool: invalid bool value";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  msg.c_str());
    return FAIL;
  }
}

template class FieldTypeCastSigned<k_int16>;
template class FieldTypeCastSigned<k_int32>;
template class FieldTypeCastSigned<k_int64>;

template <typename T>
FieldTypeCastSigned<T>::FieldTypeCastSigned(Field *field)
    : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::IntFamily;
  storage_type_ = roachpb::DataType::BIGINT;
  storage_len_ = sizeof(k_int64);
  switch (field_->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
      storage_len_ = sizeof(k_int64);
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL:
      func_ = integerToInteger;
      break;
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      func_ = doubleToInteger;
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      func_ = stringToInteger;
      break;
    default:
      break;
  }
}

template <typename T>
k_int64 FieldTypeCastSigned<T>::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValInt(ptr);
  } else {
    k_int64 in_v = 0;
    auto err = func_(field_, in_v);
    if (err != SUCCESS) {
      return err;
    }
    if (field_->get_storage_type() == roachpb::DataType::TIMESTAMP ||
        field_->get_storage_type() == roachpb::DataType::TIMESTAMPTZ ||
        field_->get_storage_type() == roachpb::DataType::DATE) {
      return in_v;
    }
    if (in_v < std::numeric_limits<T>::lowest() ||
        in_v > std::numeric_limits<T>::max()) {
      KString msg = "value is out of range for type " + getCustomTypeName<T>();
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
                                    msg.c_str());
      return 0;
    }
    return (T)in_v;
  }
}

template <typename T>
k_double64 FieldTypeCastSigned<T>::ValReal() {
  return ValInt();
}

template <typename T>
String FieldTypeCastSigned<T>::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

template class FieldTypeCastReal<k_float32>;
template class FieldTypeCastReal<k_float64>;

template <typename T>
FieldTypeCastReal<T>::FieldTypeCastReal(Field *field) : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::FloatFamily;
  storage_type_ = roachpb::DataType::DOUBLE;
  storage_len_ = sizeof(k_double64);
  switch (field_->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL:
      func_ = integerToDouble;
      break;
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      func_ = doubleToDouble;
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      func_ = stringToDouble;
      break;
    default:
      break;
  }
}

template <typename T>
k_int64 FieldTypeCastReal<T>::ValInt() {
  return ValReal();
}

template <typename T>
k_double64 FieldTypeCastReal<T>::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValReal(ptr);
  } else {
    k_double64 in_v = 0;
    auto err = func_(field_, in_v);
    if (err != SUCCESS) {
      return err;
    }
    // if (in_v < std::numeric_limits<T>::lowest() ||
    //     in_v > std::numeric_limits<T>::max()) {
    //         KString msg = "value is out of range for type " +
    //         getCustomTypeName<T>();
    //   EEPgErrorInfo::SetPgErrorInfo(ERRCODE_NUMERIC_VALUE_OUT_OF_RANGE,
    //                                 msg.c_str());
    //   return TYPE_CAST_ERANGE;
    // }
    return (T)in_v;
  }
}

template <typename T>
String FieldTypeCastReal<T>::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

FieldTypeCastString::FieldTypeCastString(Field *field, k_uint32 field_length,
                                         KString &output_type)
    : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::StringFamily;
  if (output_type.find("VARCHAR") != std::string::npos) {
    if (output_type.find("NVARCHAR") != std::string::npos) {
      storage_type_ = roachpb::DataType::NVARCHAR;
      storage_len_ = field_length > 0 ? (4 * field_length) : 256;
    } else {
      storage_type_ = roachpb::DataType::VARCHAR;
      storage_len_ = field_length > 0 ? field_length : 256;
    }
  } else if (output_type.find("CHAR") != std::string::npos) {
    storage_type_ = roachpb::DataType::CHAR;
    if (output_type.find("NCHAR") != std::string::npos) {
      storage_type_ = roachpb::DataType::NCHAR;
      if (field_length > 0) {
        storage_len_ = 4 * field_length;
        letter_len_ = field_length;
      } else {
        storage_len_ = 256;
        letter_len_ = 1;
      }
    } else {
      if (field_length > 0) {
        storage_len_ = ++field_length;
        letter_len_ = field_length;
      } else {
        storage_len_ = 256;
        letter_len_ = 1;
      }
    }
  } else {
      storage_type_ = roachpb::DataType::VARCHAR;
      storage_len_ = field_length > 0 ? field_length : 256;
  }

  switch (field_->get_storage_type()) {
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL:
      func_ = integerToString;
      break;
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      func_ = doubleToString;
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      storage_len_ =
          field_length > 0 ? storage_len_ : field->get_storage_length();
      func_ = stringToString;
      break;
    default:
      KString msg =
        "invalid type " + field_->get_storage_type();
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  msg.c_str());
      break;
  }
}

k_int64 FieldTypeCastString::ValInt() { return 0; }

k_double64 FieldTypeCastString::ValReal() { return 0.0; }

String FieldTypeCastString::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValStr(ptr);
  } else {
    char in_v[storage_len_] = {0};
    auto err = func_(field_, in_v, storage_len_);
    if (err != SUCCESS) {
      return String("");
    }
    // truncate string
    if (letter_len_ > 0) {
      k_uint32 num = 0;
      for (size_t i = 0; i < storage_len_; i++) {
        if ((in_v[i] & 0xc0) != 0x80) {
          if (num >= letter_len_) {
            memset(in_v + i, 0, storage_len_ - 1 - i);
            break;
          }
          num++;
        }
      }
    }
    String s(storage_len_);
    snprintf(s.getptr(), storage_len_ + 1, "%s", in_v);
    s.length_ = strlen(in_v);
    return s;
  }
}

FieldTypeCastTimestamptz2String::FieldTypeCastTimestamptz2String(
    Field *field, k_uint32 field_length, KString &output_type, k_int8 time_zone)
    : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::StringFamily;
  // storage_type_ = roachpb::DataType::CHAR;
  // storage_len_ = field_length > 0 ? ++field_length : 256;
  time_zone_ = time_zone;

  switch (field->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
      type_scale_ = 1000;
      break;
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
      type_scale_ = 1000000;
      break;
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      type_scale_ = 1000000000;
      break;
  }
  if (output_type.find("VARCHAR") != std::string::npos) {
    if (output_type.find("NVARCHAR") != std::string::npos) {
      storage_type_ = roachpb::DataType::NVARCHAR;
      storage_len_ = field_length > 0 ? (4 * field_length) : 256;
    } else {
      storage_type_ = roachpb::DataType::VARCHAR;
      storage_len_ = field_length > 0 ? field_length : 256;
    }
  } else if (output_type.find("CHAR") != std::string::npos) {
    storage_type_ = roachpb::DataType::CHAR;
    if (output_type.find("NCHAR") != std::string::npos) {
      storage_type_ = roachpb::DataType::NCHAR;
    }
    if (field_length > 0) {
      storage_len_ = ++field_length;
    } else {
      storage_len_ = 2;
    }
  } else {
      storage_type_ = roachpb::DataType::VARCHAR;
      storage_len_ = field_length > 0 ? field_length : 256;
  }
}

k_int64 FieldTypeCastTimestamptz2String::ValInt() { return 0; }

k_double64 FieldTypeCastTimestamptz2String::ValReal() { return 0.0; }

String FieldTypeCastTimestamptz2String::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValStr(ptr);
  } else {
    char in_v[storage_len_] = {0};

    auto err = timestamptzToString(field_, in_v, storage_len_, type_scale_, time_zone_);
    if (err != SUCCESS) {
      return String("");
    }
    String s(storage_len_);
    snprintf(s.getptr(), storage_len_ + 1, "%s", in_v);
    s.length_ = strlen(in_v);
    return s;
  }
}

FieldTypeCastTimestampTz::FieldTypeCastTimestampTz(Field *field,
                                                   k_int8 type_num,
                                                   k_int8 timezone)
    : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::TimestampFamily;
  switch (type_num) {
    case 3:
      sql_type_ = roachpb::DataType::TIMESTAMP;
      storage_type_ = roachpb::DataType::TIMESTAMP;
      type_scale_ = 1;
      timezone_diff_ = timezone * 3600 * 1000;
      break;
    case 6:
      sql_type_ = roachpb::DataType::TIMESTAMP_MICRO;
      storage_type_ = roachpb::DataType::TIMESTAMP_MICRO;
      type_scale_ = 1000;
      timezone_diff_ = timezone * 3600 * 1000000;
      break;
    case 9:
      sql_type_ = roachpb::DataType::TIMESTAMP_NANO;
      storage_type_ = roachpb::DataType::TIMESTAMP_NANO;
      type_scale_ = 1000000;
      timezone_diff_ = timezone * 3600 * 1000000000;

      break;
    default:
      sql_type_ = roachpb::DataType::TIMESTAMP;
      storage_type_ = roachpb::DataType::TIMESTAMP;
      type_scale_ = 1;
      timezone_diff_ = timezone * 3600 * 1000;
      break;
  }
  storage_len_ = sizeof(k_int64);
  switch (field_->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
      func_ = timestamptzToTimestampTz;
      timezone_diff_ = 0;
      break;
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
      func_ = timestamptzToTimestampTz;
      break;
    case roachpb::DataType::DATE:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      func_ = integerToTimestampTz;
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      func_ = stringToTimestampTz;
      break;
    default:
      break;
  }
}

k_int64 FieldTypeCastTimestampTz::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValInt(ptr);
  } else {
    k_int64 in_v = 0;
    auto err = func_(field_, in_v, type_scale_, timezone_diff_, storage_type_);
    if (err != SUCCESS) {
      return 0;
    }
    return in_v;
  }
}

k_double64 FieldTypeCastTimestampTz::ValReal() { return ValInt(); }

String FieldTypeCastTimestampTz::ValStr() {
  String s(storage_len_);
  snprintf(s.getptr(), storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

FieldTypeCastBool::FieldTypeCastBool(Field *field) : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::BoolFamily;
  storage_type_ = roachpb::DataType::BOOL;
  storage_len_ = sizeof(k_bool);
  switch (field_->get_storage_type()) {
    case roachpb::DataType::TIMESTAMP:
    case roachpb::DataType::TIMESTAMPTZ:
    case roachpb::DataType::TIMESTAMP_MICRO:
    case roachpb::DataType::TIMESTAMP_NANO:
    case roachpb::DataType::TIMESTAMPTZ_MICRO:
    case roachpb::DataType::TIMESTAMPTZ_NANO:
    case roachpb::DataType::DATE:
    case roachpb::DataType::SMALLINT:
    case roachpb::DataType::INT:
    case roachpb::DataType::BIGINT:
    case roachpb::DataType::BOOL:
    case roachpb::DataType::FLOAT:
    case roachpb::DataType::DOUBLE:
      func_ = numToBool;
      break;
    case roachpb::DataType::CHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::VARBINARY:
      func_ = stringToBool;
      break;
    default:
      break;
  }
}

k_int64 FieldTypeCastBool::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValInt(ptr);
  } else {
    k_bool in_v = 0;
    auto err = func_(field_, in_v);
    if (err != SUCCESS) {
      return 0;
    }
    return in_v;
  }
}

k_double64 FieldTypeCastBool::ValReal() { return ValInt(); }

String FieldTypeCastBool::ValStr() {
  String s(storage_len_);
  snprintf(s.getptr(), storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

FieldTypeCastBytes::FieldTypeCastBytes(Field *field, k_uint32 field_length,
                                       k_uint32 bytes_length)
    : FieldTypeCast(field) {
  return_type_ = KWDBTypeFamily::StringFamily;
  storage_type_ = roachpb::DataType::VARBINARY;
  storage_len_ = field_length > 0 ? field_length : 256;
  bytes_len_ = bytes_length;
}

k_int64 FieldTypeCastBytes::ValInt() { return 0; }

k_double64 FieldTypeCastBytes::ValReal() { return 0.0; }

String FieldTypeCastBytes::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValStr(ptr);
  }
  String s = field_->ValStr();
  KString ret = std::string(s.getptr(), s.length_);
  if (bytes_len_ == 0) {
    return s;
  }
  if (ret.size() > bytes_len_) {
    String s(bytes_len_);
    snprintf(s.ptr_, bytes_len_ + 1, "%s", ret.substr(0, bytes_len_).c_str());
    s.length_ = bytes_len_;
    return s;
  }
  for (size_t i = ret.size(); i < bytes_len_; i++) {
    ret += "\0";
  }
  String s1(ret.length());
  snprintf(s1.ptr_, ret.length() + 1, "%s", ret.c_str());
  s1.length_ = ret.length();
  return s1;
}

FieldTypeCastDecimal::FieldTypeCastDecimal(Field *field)
    : FieldTypeCast(field) {
  storage_type_ = field->get_storage_type();
  sql_type_ = field->get_sql_type();
  storage_len_ = field->get_storage_length();
}

k_int64 FieldTypeCastDecimal::ValInt() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValInt(ptr);
  } else {
    return field_->ValInt();
  }
}

k_double64 FieldTypeCastDecimal::ValReal() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValReal(ptr);
  } else {
    return field_->ValReal();
  }
}

String FieldTypeCastDecimal::ValStr() {
  char *ptr = get_ptr();
  if (nullptr != ptr) {
    return FieldTypeCast::ValStr(ptr);
  } else {
    return field_->ValStr();
  }
}

}  // namespace kwdbts
