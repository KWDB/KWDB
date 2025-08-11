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

#include <arpa/inet.h>
#include <cstdio>
#include <sstream>
#include "data_value_handler.h"
#include "utils/date_time_util.h"
#include "utils/big_table_utils.h"
#include "var_string.h"
#include "utils/string_utils.h"


using namespace std;
using namespace kwdbts;

inline bool isEmpty(char *s) {
  //to-do(allison&thomas): distinguish  and ''
  if (s == NULL || s == nullptr) {
    return false;
  }

  return ((strcmp(s, "") == 0) || (*s == 0x0));
}

bool isEqualIgnoreCase(char *src, const char* dest) {
  if (src == NULL || src == nullptr) {
    return false;
  }
  size_t size = strlen(dest);
  if (strlen(src) != size) {
    return false;
  }

  const int CHAR_DIFF = 'a' - 'A';
  int diff = -1;
  for (size_t i = 0; i < size; ++i) {
    diff = abs(src[i] - dest[i]);
    if (diff != 0 && diff != CHAR_DIFF) {
      return false;
    }
  }

  return true;
}

class BYTEToString: public DataToString {
public:
    BYTEToString(): DataToString(1, sizeof(int8_t)) {};

    virtual ~BYTEToString() {};

    virtual string toString(void *data) {
        char c = *((char *)data);
        if (c)
            return string(1, *((char *)data));
        else
            return kwdbts::s_emptyString;
    }

    virtual int toString(void *data, char *str) {
        *str++ = *((char *)data);
        *str = 0;
        return 1;
    }
};

template <typename T>
class IntegerDataToStringZN: public DataToString {
public:
  explicit IntegerDataToStringZN(int max_len): DataToString(max_len, sizeof(T)) {}
  virtual ~IntegerDataToStringZN() {}

  virtual string toString(void *data) {
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((T *)data));
#else
    int32_t tmp;
    memcpy(&tmp , data , sizeof(T));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((T *)data));
    return strlen(str);
#else
    int32_t tmp;
    memcpy(&tmp , data , sizeof(T));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

template <typename T>
class IntegerDataToString: public DataToString {
public:
  explicit IntegerDataToString(int max_len): DataToString(max_len, sizeof(T)) {}
  virtual ~IntegerDataToString() {}

  virtual bool isNull (void *data) {
    if (kwdbts::EngineOptions::zeroIfNull())
       return false;
    return *(T *)data == std::numeric_limits<T>::min();
  }

  virtual string toString(void *data) {
    if (*(T *)data == std::numeric_limits<T>::min())
      return string(kwdbts::s_NULL);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((T *)data));
#else
    T tmp;
    memcpy(&tmp , data , sizeof(T));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(T *)data == std::numeric_limits<T>::min()) {
      strcpy(str, kwdbts::s_NULL.c_str());
      return 4;
    }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%d", *((T *)data));
    return strlen(str);
#else
    T tmp;
    memcpy(&tmp , data , sizeof(T));
    sprintf(str, "%d", tmp);
    return strlen(str);
#endif
  }
};

class INT64DataToStringZN: public DataToString {
public:
  explicit INT64DataToStringZN(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~INT64DataToStringZN() {};

  virtual string toString(void *data) {
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int64_t *)data));
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%ld", *((int64_t *)data));
    return strlen(str);
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    sprintf(str, "%ld", tmp);
    return strlen(str);
#endif
  }
};

class INT64DataToString: public DataToString {
public:
  explicit INT64DataToString(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~INT64DataToString() {};

  virtual bool isNull (void *data) {
    if (kwdbts::EngineOptions::zeroIfNull())
       return false;
    return *(int64_t *)data == std::numeric_limits<int64_t>::min();
  }

  virtual string toString(void *data) {
    if (*(int64_t *)data == std::numeric_limits<int64_t>::min())
      return string(kwdbts::s_NULL);
#if defined(__UNALIGNED_ACCESS__)
    return intToString(*((int64_t *)data));
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    return intToString(tmp);
#endif
  }

  virtual int toString(void *data, char *str) {
    if (*(int64_t *)data == std::numeric_limits<int64_t>::min()) {
      strcpy(str, kwdbts::s_NULL.c_str());
      return 4;
    }
#if defined(__UNALIGNED_ACCESS__)
    sprintf(str, "%ld", *((int64_t *)data));
    return strlen(str);
#else
    int64_t tmp;
    memcpy(&tmp , data , sizeof(int64_t));
    sprintf(str, "%ld", tmp);
    return strlen(str);
#endif
  }
};

class TimeStamp64ToString: public DataToString {
protected:
  TimeStamp64DateTime ts64_;
public:
  explicit TimeStamp64ToString(int max_len): DataToString(max_len, sizeof(int64_t))
  { ts64_.setPrecision(max_len); }
  virtual ~TimeStamp64ToString() {}

  virtual string toString(void *data) {
    return ts64_.toString(data);
  }

  virtual int toString(void *data, char *str) {
    if (*((int64_t*) data) < 0) {
      sprintf(str, "-%lu", abs(*((int64_t*) data)));
    } else {
      sprintf(str, "%lu", *((int64_t*) data));
    }
    return strlen(str);
  }
};


class TimeStamp64LSNToString: public DataToString {
 public:
  TimeStamp64LSNToString() : DataToString(16, 16) {
    ts64_.setPrecision(3);
  }
  virtual ~TimeStamp64LSNToString() {}

  virtual bool isNull (void *data) { return false; }

  virtual string toString(void *data) {
    TimeStamp64LSN* ts_lsn = (TimeStamp64LSN*)data;
    std::stringstream ss;
    ss << ts64_.toString(&ts_lsn->ts64) << "," << ts_lsn->lsn;
    return ss.str();
  }
 protected:
  TimeStamp64DateTime ts64_;
};


template <typename T>
class FloatDataToString: public DataToString {
protected:
  vector<char> str_;
  DoubleFormatToString dbl_to_str_;
public:
  FloatDataToString(int max_len, int prec, bool is_rm_tz):
    DataToString(max_len, sizeof(T)), dbl_to_str_(prec, is_rm_tz)
  { str_.resize(max_len + 1); }
  virtual ~FloatDataToString() {}

  virtual bool isNull (void *data) { return !(isfinite(*((T*)data))); }

  virtual string toString(void *data) {
    if (isfinite(*((T*)data))) {
      dbl_to_str_.toString((char *)str_.data(), (double)(*((T*) data)));
      return string(str_.data());
    } else {
    	// so it can handle inf
      if (isinf(*((T*)data))) {
        if (*((T*)data) >= 0) {
          return '"' + kwdbts::s_inf + '"';
        } else {
          return '"' + kwdbts::s_n_inf + '"';
        }
      }
      return kwdbts::s_NULL;       // "null" for json string.
  }
}

  virtual int toString(void *data, char *str) {
    return dbl_to_str_.toString(str, (double)(*((T*) data)));
  }
};

DataToString * getDataToString(int type, int max_len) {
  switch(type) {
    case INT8: return new IntegerDataToStringZN<int8_t>(max_len);
    case INT16: return new IntegerDataToStringZN<int16_t>(max_len);
    case INT32: return new IntegerDataToStringZN<int32_t>(max_len);
    case INT64: return new INT64DataToStringZN(max_len);
  }

  return new DataToString(max_len); // useless
}

class CHARToString: public DataToString
{
public:
    CHARToString(int max_len, int size): DataToString(max_len, size) {};

    virtual ~CHARToString() {};

    virtual string toString(void *data) {
        return ::toString((const char *)data, size_);
    }

    virtual int toString(void *data, char *str) {
        return mmap_strlcpy(str, (const char*) data, max_len_);
    }
};

class TimeToString: public DataToString {
protected:
  Time time_;
public:
  explicit TimeToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~TimeToString() {}
  virtual bool isNull (void *data) { return !time_.isValidTime(data); }
  virtual string toString(void *data) { return time_.toString(data); }
  virtual int toString(void *data, char *str)
  { return time_.BasicDateTime::strformat(data, str); }
};

class Time64ToString: public DataToString {
protected:
  Time *time_;
public:
  Time64ToString(int max_len, int precision): DataToString(max_len, sizeof(int64_t)) {
      time_ = new Time64(precision);
  }
  virtual ~Time64ToString() { delete time_; }
  virtual string toString(void *data) { return time_->toString(data); }
  virtual int toString(void *data, char *str)
  { return time_->BasicDateTime::strformat(data, str); }
};

class Date32ToString: public DataToString {
private:
  Date32 date_;
public:
  explicit Date32ToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~Date32ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return KWEVALUE;    // NULL
  }
};

class DateTimeDOSToString: public DataToString {
private:
  DateTimeDOS date_;
public:
  explicit DateTimeDOSToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}
  virtual ~DateTimeDOSToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return KWEVALUE;
  }
};

class DateTime32ToString: public DataToString {
private:
  DateTime32 date_;
public:
  explicit DateTime32ToString(int max_len): DataToString(max_len, sizeof(int32_t)) {}

  virtual ~DateTime32ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return KWEVALUE;    // NULL
  }
};

class DateTime64ToString: public DataToString {
private:
  DateTime64 date_;
public:
  explicit DateTime64ToString(int max_len): DataToString(max_len, sizeof(int64_t)) {}
  virtual ~DateTime64ToString() {}

  virtual bool isNull (void *data) { return !date_.isValidTime(data); }

  virtual string toString(void *data) { return date_.toString(data); }

  virtual int toString(void *data, char *str) {
    if (date_.isValidTime(data))
      return date_.strformat(data, str);
    return KWEVALUE;  // NULL
  }
};

class BINARYToString: public DataToString {
protected:
  string s_;
public:
  explicit BINARYToString(int max_len): DataToString(max_len, max_len)
  { s_.resize(max_len_ * 2); }
  virtual ~BINARYToString() {};

  virtual string toString(void *data) {
    char *s = (char *)s_.data();
    binaryToHexString((unsigned char *)data, s, max_len_);
    return string(s);
  }

  virtual int toString(void *data, char *str) {
    memcpy(str, data, max_len_);
    return max_len_;
  }
};

int setPrecision(const AttributeInfo &col_info, bool &is_rm_tz) {
//  if (col_info.extra < 0) {
    is_rm_tz = true;
    return kwdbts::EngineOptions::precision();
//  }
//  is_rm_tz = false;
//  return col_info.extra;
}

int setFloatPrecision(const AttributeInfo &col_info, bool &is_rm_tz) {
  is_rm_tz = true;
  return kwdbts::EngineOptions::float_precision();
}

class NULLToString: public DataToString {
public:
  NULLToString(): DataToString(0, 0) {}
  virtual ~NULLToString() {}
  virtual bool isNull (void *data) { return true; }
  virtual string toString(void *data) { return "NULL"; }
  virtual int toString(void *data, char *str) { return -1; }
};


DataToStringPtr getDataToStringHandler(const AttributeInfo &col_info,
  int encoding) {
  DataToString *vs;
  int max_len = dataTypeMaxTextLen(col_info);

  switch (col_info.type) {
    case BYTE_ARRAY:
    case BYTE:
      vs = new BYTEToString();
      break;
    case TIMESTAMP:
      vs = new IntegerDataToStringZN<int32_t>(max_len);
      break;
    case INT8_ARRAY:
    case INT8:
    case INT16_ARRAY:
    case INT16:
    case INT32_ARRAY:
    case INT32:
    case INT64_ARRAY:
    case INT64:
      vs = getDataToString(col_info.type, max_len);
      break;
    case TIMESTAMP64:
    case TIMESTAMP64_MICRO:
    case TIMESTAMP64_NANO:
      vs = new TimeStamp64ToString(col_info.max_len);
      break;
    case FLOAT_ARRAY:
    case FLOAT: {
      bool is_rm_tz;
      int prec = setFloatPrecision(col_info, is_rm_tz);
      vs = new FloatDataToString<float> (max_len, prec, is_rm_tz);
      break;
    }
    case DOUBLE_ARRAY:
    case DOUBLE: {
      bool is_rm_tz;
      int prec = setPrecision(col_info, is_rm_tz);
      vs = new FloatDataToString<double> (max_len, prec, is_rm_tz);
      break;
    }
    case DATE32:
      vs = new Date32ToString(max_len);
      break;
    case DATETIMEDOS:
      vs = new DateTimeDOSToString(max_len);
      break;
    case DATETIME32:
      vs = new DateTime32ToString(max_len);
      break;
    case DATETIME64:
      vs = new DateTime64ToString(max_len);
      break;
    case VARSTRING:
      vs = new VARSTRINGToString(max_len);
      break;
    case ROWID:
#ifndef ID64
      vs = new IntegerDataToString<int32_t>(max_len);
#else
      vs = new INT64DataToString(max_len);
#endif
      break;
    case STRING_CONST:
    case CHAR:
      vs = new CHARToString(max_len, col_info.size);
      break;
    case TIME: vs = new TimeToString(max_len); break;
    case TIME64: vs = new Time64ToString(max_len, col_info.max_len); break;
    //case TDIGEST: vs = new TDIGESTToString(max_len); break;
    case BINARY: vs = new BINARYToString(max_len); break;
    case VARBINARY:
      vs = new VARBINARYToString(max_len); break;
    case NULL_TYPE:
      vs = new NULLToString(); break;
    default:
      vs = new DataToString(max_len);
  }
  return DataToStringPtr(vs);
}

////////////////////////////////////////////////////////////////////
// DataToString

DataToString::~DataToString() {}

bool DataToString::isNull (void *data) { return false; }

string DataToString::toString(void *data) { return kwdbts::s_emptyString; }

void DataToString::unlock() {}

////////////////////////////////////////////////////////////////////
// StringToData

StringToData::StringToData() {
  has_default_ = false;
}

StringToData::~StringToData() { }
bool StringToData::isLatest() const { return false; }
void StringToData::setColumn(int col) {}

int StringToData::toData(char *str, void * addr) {
  return 0;
}

void StringToData::clear() {}

// integer to string:  NULL is converted to Zero
template<typename T, int d_type>
class IntegerStringToDataZN: public StringToData {
public:
  IntegerStringToDataZN() {
  }
  virtual ~IntegerStringToDataZN() {}
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    if (isEqualIgnoreCase(str, "true")) {
      str = const_cast<char*>("1\0");
    } else if (isEqualIgnoreCase(str, "false")) {
      str = const_cast<char*>("0\0");
    }
    char *end_ptr;
    errno = 0;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
      return KWEDATA;
    }
    if ((v < std::numeric_limits<T>::min())
      || (v > std::numeric_limits<T>::max()))
      return KWERANGE;
    *((T*)data) = (T)v;
    return 0;
  }
};

// integer to string:  NULL is converted to minimum integer number
template<typename T, int d_type>
class IntegerStringToData: public StringToData {
public:
  IntegerStringToData() {
  }
  virtual ~IntegerStringToData() {}
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    if (isEqualIgnoreCase(str, "true")) {
      strcpy(str, "1");
    } else if (isEqualIgnoreCase(str, "false")) {
      strcpy(str, "0");
    }
    char *end_ptr;
    errno = 0;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
      return KWEDATA;
    }
    if ((v < std::numeric_limits<T>::min())
      || (v > std::numeric_limits<T>::max()))
      return KWERANGE;
    *((T*)data) = (T)v;
    return 0;
  }

};

class LongStringToData: public StringToData {
public:
  LongStringToData(): StringToData() {
  }
  virtual ~LongStringToData() {}

  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    errno = 0;
    char *end_ptr;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
      return KWEDATA;
    }
    if ((v < std::numeric_limits<int64_t>::min())
      || (v > std::numeric_limits<int64_t>::max()))
      return KWERANGE;
    *((int64_t*)data) = (int64_t)v;
    return 0;
  }
};

class LongStringToDataZN: public StringToData {
public:
  LongStringToDataZN(): StringToData() {
  }
  virtual ~LongStringToDataZN() {}
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    errno = 0;
    char *end_ptr;
    int64_t v = strtol(str, &end_ptr, 10);
    if (errno == ERANGE) {
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
      return KWEDATA;
    }
    if ((v < std::numeric_limits<int64_t>::min())
      || (v > std::numeric_limits<int64_t>::max()))
      return KWERANGE;
    *((int64_t*)data) = (int64_t)v;
    return 0;
  }
};

class FLOATStringToData: public StringToData {
protected:
  timestamp64 ts_;
  BigTable *latest_bt_;
  int col_;
  bool isfinite_;      // will be set for each input data
public:
  FLOATStringToData() {
    latest_bt_ = nullptr;
  }
  virtual ~FLOATStringToData() {}
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE) {
      isfinite_ = false;
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
       isfinite_ = false;
       return KWEDATA;
     }
    if ((v < std::numeric_limits<float>::lowest())
      || (v > std::numeric_limits<float>::max())) {
      isfinite_ = false;
      return KWERANGE;
    }
    *((float*) data) = (float) v;
    isfinite_ = true;
    return 0;
  }
};

class DOUBLEStringToData: public StringToData {
protected:
  timestamp64 ts_ = 0;
  BigTable *latest_bt_ = nullptr;
  int col_ = 0;
  bool is_finite_ = false;      // will be set for each input data
public:
  DOUBLEStringToData() {}
  virtual ~DOUBLEStringToData() {}
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual void setLatestTable(timestamp64 ts, BigTable *bt) {
    ts_= ts;
    latest_bt_ = bt;
  }
  virtual void setRow(uint64_t row) {
    if (latest_bt_ && is_finite_) {
      latest_bt_->mutexLock();
      latest_bt_->setColumnValue(col_, 0, &ts_);
      latest_bt_->setColumnValue(col_, 1, &row);
      latest_bt_->mutexUnlock();
    }
  }
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE) {
      is_finite_ = false;
      return KWERANGE;
    }
    if (*end_ptr != '\0') {
      is_finite_ = false;
      return KWEDATA;
    }

    *((double*) data) = (double) v;
    is_finite_ = true;
    return 0;
  }
};

template<typename T, int d_type>
class FloatStringToData: public StringToData {
protected:
  timestamp64 ts_ = 0;
  BigTable *latest_bt_ = nullptr;
  int col_ = 0;
  bool is_finite_ = false;      // will be set for each input data
public:
  FloatStringToData() {}
  virtual ~FloatStringToData() {}
  virtual bool isLatest() const { return true; }
  virtual void setColumn(int col) { col_ = col; }
  virtual int toData(char *str, void *data) {
    if (isEmpty(str) && !has_default_) {
      return KWEDATA;
    }
    errno = 0;
    char *end_ptr;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE || *end_ptr != '\0') {
      is_finite_ = false;
      return KWERANGE;
    }
    if ((v < std::numeric_limits<T>::lowest())
      || (v > std::numeric_limits<T>::max())) {
      is_finite_ = false;
      return KWERANGE;
    }
    *((T*) data) = (T) v;
    is_finite_ = true;
    return 0;

  }

};

template<typename T, int d_type>
class FloatStringToDataZN: public FloatStringToData<T, d_type> {
public:
  FloatStringToDataZN(): FloatStringToData<T, d_type>() {}
  virtual ~FloatStringToDataZN() {}
  virtual int toData(char *str, void *data) {
    if (isEmpty(str)) {
      return KWEDATA;
    }
    char *end_ptr;
    errno = 0;
    double v = strtod(str, &end_ptr);
    if (errno == ERANGE)
      return KWERANGE;
    if (*end_ptr != '\0')
      return KWEDATA;
    *((T*)data) = (T)v;
    return 0;
  }
};

StringToData *getStringToData(int type) {
  if (kwdbts::EngineOptions::zeroIfNull()) {
    switch(type) {
      case INT8: return new IntegerStringToDataZN<int8_t, INT8>();
      case INT16: return new IntegerStringToDataZN<int16_t, INT16>();
      case INT32: return new IntegerStringToDataZN<int32_t,INT32>();
      case INT64: return new LongStringToDataZN();
      case FLOAT: return new FloatStringToDataZN<float, FLOAT>();
      case DOUBLE: return new FloatStringToDataZN<double, DOUBLE>();
    }
  } else {
    switch(type) {
      case INT8: return new IntegerStringToData<int8_t, INT8>();
      case INT16: return new IntegerStringToData<int16_t, INT16>();
      case INT32: return new IntegerStringToData<int32_t, INT32>();
      case INT64: return new LongStringToData();
      case FLOAT: return new FLOATStringToData();
      case DOUBLE: return new DOUBLEStringToData();
    }
  }
  return new StringToData(); // useless
}

class StringToIDTYPE: public StringToData {
protected:
  int max_len_;
public:
  explicit StringToIDTYPE(int max_len) {
    max_len_ = (max_len <= 0) ? DEFAULT_STRING_MAX_LEN : max_len;
  }

  virtual ~StringToIDTYPE() {}

  virtual int toData(char *str, void *data) {
    if (strlen(str) > max_len_)
      return KWEVALUE;
    return KWENAMESERVICE;
  }

};


class StringToCHAR: public StringToData {
protected:
  int size_;
public:
  explicit StringToCHAR(int s): size_(s) {
    string str;
    str.resize(size_, 0); // TODO: default value \x0 or ' '?
  }

  virtual ~StringToCHAR() {}

  virtual int toData(char *str, void *data) {
    if (strlen(str) > size_) {
      return KWEVALUE;
    }
    int cpy_len = CHARcpy((char *)data, str, size_);
    if (cpy_len > 0) {
      unsigned char c = ((unsigned char*) data)[cpy_len - 1] & 0xC0;
      if (c == 0xC0) { // 0x1100---- | 0x1110----
        cpy_len--;
      } else if (c == 0x80) {
        if (cpy_len < 2)
          cpy_len = 0;
        else if ((((unsigned char*) data)[cpy_len - 2] & 0xE0) == 0xE0) {
          cpy_len -= 2;     // 0x1110---- 0x10------
        }
      }
    }
    if (cpy_len < size_)
      memset(offsetAddr(data, cpy_len), 0, size_ - cpy_len);
    return 0;
  }
};

template <typename T>
class StringToDateTime: public StringToData {
protected:
  T dt_;
public:
  explicit StringToDateTime(int prec) {
    dt_.setPrecision(prec);
  }
  virtual ~StringToDateTime() {};
  virtual int toData(char *str, void *data) {
    if (!dt_.setDateTime(str, data)) {
      return KWEDATEVALUE;
    }
    return 0;
  }
};

class StringToBINARY: public StringToData {
protected:
  int max_len_;
public:
  explicit StringToBINARY(int max_len): max_len_(max_len) {
    string str;
    str.resize(max_len_, 0);
  }
  virtual ~StringToBINARY() {}
  virtual int toData(char *str, void *data) {
    int len = unHex(str, (unsigned char *)data, max_len_);
    if (len >= 0)
      memset(offsetAddr(data, len), 0, max_len_ - len);
    return len;
  }
};


StringToDataPtr getStringToDataHandler(BigTable *bt, int col,
  const AttributeInfo &col_info, int encoding,
  const string &time_format) {
  StringToData *vs;
  switch (col_info.type) {
    case STRING:
#if !defined(USE_SMART_INDEX)
      vs = new StringToIDTYPE(col_info.max_len);
#else
      vs =
        (encoding == DICTIONARY) ? new StringToIDTYPE(ns) :
          new StringToINDEX(ns, dim_attr);
#endif
      break;
    case CHAR:
      vs = new StringToCHAR(col_info.max_len);
      break;
    case VARSTRING:
      vs = new StringToVARSTRING(bt, col, col_info.max_len);
      break;
    case TIMESTAMP:
      vs = new IntegerStringToDataZN<int32_t, INT32>;
      break;
    case TIMESTAMP64:
      vs = new StringToDateTime<TimeStamp64DateTime>(col_info.max_len);
      break;
    case INT8_ARRAY:
    case INT8:
    case INT16_ARRAY:
    case INT16:
    case INT32_ARRAY:
    case INT32:
    case INT64_ARRAY:
    case INT64:
    case FLOAT_ARRAY:
    case FLOAT:
    case DOUBLE_ARRAY:
    case DOUBLE:
      vs = getStringToData(col_info.type);
      break;
    case DATE32:
      vs = new StringToDateTime<Date32>(col_info.max_len);
      break;
    case DATETIMEDOS:
      vs = new StringToDateTime<DateTimeDOS>(col_info.max_len);
      break;
    case DATETIME32:
      vs = new StringToDateTime<DateTime32>(col_info.max_len);
      break;
    case DATETIME64:
      vs = new StringToDateTime<DateTime64>(col_info.max_len);
      break;
    case ROWID:
#ifndef ID64
      vs = new IntegerStringToData<int32_t, INT32>;
#else
      vs = new IntegerStringToData<int64_t>;
#endif
      break;
    case BINARY: vs = new StringToBINARY(col_info.max_len); break;
    case VARBINARY:
      vs = new StringToVARBINARY(bt, col, col_info.max_len); break;
    default:
      vs = new StringToData;
  }

  return StringToDataPtr(vs);
//    }

}

CONVERT_DATA_FUNC getConvertFunc(int32_t old_data_type, int32_t new_data_type,
                                int32_t new_length, bool& is_digit_data, ErrorInfo& err_info) {
  is_digit_data = false;
  err_info.errcode = 0;
  switch (old_data_type)  {
  case DATATYPE::INT16:
    is_digit_data = true;
    if (new_data_type == DATATYPE::INT32) {
      return convertFixDataToData<int16_t, int32_t, false>;
    } else if (new_data_type == DATATYPE::INT64) {
      return convertFixDataToData<int16_t, int32_t, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int16_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::INT32:
    is_digit_data = true;
    if (new_data_type == DATATYPE::INT64) {
      return convertFixDataToData<int32_t, int64_t, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int32_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::INT64:
    is_digit_data = true;
    if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<int64_t, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::FLOAT:
    is_digit_data = true;
    if (new_data_type == DATATYPE::DOUBLE) {
      return convertFixDataToData<float, double, false>;
    } else if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<float, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::DOUBLE:
    is_digit_data = true;
    if (new_data_type == DATATYPE::VARSTRING) {
      return convertFixDataToData<double, char, true>;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::CHAR:
    if (new_data_type == DATATYPE::VARSTRING ||
        new_data_type == DATATYPE::BINARY ||
        new_data_type == DATATYPE::CHAR) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::BINARY:
    if (new_data_type == DATATYPE::VARSTRING ||
        new_data_type == DATATYPE::CHAR ||
        new_data_type == DATATYPE::BINARY) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::VARSTRING:
    if (new_data_type == DATATYPE::INT16 ) {
      return convertStringToFixData<DATATYPE::INT16>;   
    } else if (new_data_type == DATATYPE::INT32) {
      return convertStringToFixData<DATATYPE::INT32>;
    } else if (new_data_type == DATATYPE::INT64) {
      return convertStringToFixData<DATATYPE::INT64>;
    } else if (new_data_type == DATATYPE::FLOAT) {
      return convertStringToFixData<DATATYPE::FLOAT>;
    } else if (new_data_type == DATATYPE::DOUBLE) {
      return convertStringToFixData<DATATYPE::DOUBLE>;
    } else if (new_data_type == DATATYPE::CHAR ||
               new_data_type == DATATYPE::BINARY ||
               new_data_type == DATATYPE::VARSTRING) {
      return nullptr;
    }
    err_info.errcode = -1;
    break;
  case DATATYPE::VARBINARY:
  default:
    err_info.errcode = -1;
    break;
  }
  return nullptr;
}
