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

#include "ee_field_func_string.h"

#include <cmath>
#include <exception>
#include <iomanip>
#include <sstream>

#include "ee_field.h"
#include "ee_field_common.h"
#include "ee_global.h"
#include "pgcode.h"

namespace kwdbts {

//  determine if it is the first byte of utf8
inline bool IsUtf8Header(const char &c) { return (c & 0xc0) != 0x80; }
static std::list<KString> str2Utf8List(const KString &str) {
  std::list<KString> res_list;
  KString tmp_str = "";
  for (auto &c : str) {
    if (!tmp_str.empty() && IsUtf8Header(c)) {
      res_list.push_back(tmp_str);
      tmp_str = "";
    }
    tmp_str += c;
  }
  if (!tmp_str.empty()) {
    res_list.push_back(tmp_str);
  }
  return res_list;
}

static size_t strLength(const KString &str) { return str2Utf8List(str).size(); }

static String ltrim(const KString &in_str, const KString &trim_str) {
  auto trimlist = str2Utf8List(trim_str);
  size_t split_i = 0;
  KString tmp_str = "";
  for (size_t i = 0; i < in_str.length(); i++) {
    if (IsUtf8Header(in_str[i])) {
      if (!tmp_str.empty() && std::find(trimlist.begin(), trimlist.end(),
                                        tmp_str) == trimlist.end()) {
        break;
      }
      split_i = i;
      tmp_str = "";
    }
    tmp_str += in_str[i];
  }
  // determine if the last one needs to be deleted
  if (!tmp_str.empty() &&
      std::find(trimlist.begin(), trimlist.end(), tmp_str) != trimlist.end()) {
    return String("");
  }

  std::string str = in_str.substr(split_i);
  String s(str.length());
  snprintf(s.getptr(), str.length() + 1, "%s", str.c_str());
  s.length_ = str.length();
  return s;
}

static String ltrim1(Field **args, k_int32 arg_count) {
  KString tmp_str = "";
  size_t split_i = 0;
  String s = args[0]->ValStr();
  KString in_str = {s.getptr(), s.length_};
  for (auto &c : in_str) {
    if (!std::isspace(c)) {
      break;
    }
    split_i++;
  }

  std::string str = in_str.substr(split_i);
  String s1(str.length());
  snprintf(s1.getptr(), str.length() + 1, "%s", str.c_str());
  s1.length_ = str.length();
  return s1;
}

static String ltrim2(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  String s2 = args[1]->ValStr();
  return ltrim({s1.getptr(), s1.length_}, {s2.getptr(), s2.length_});
}

static String rtrim(const KString &in_str, const KString &trim_str) {
  auto trimlist = str2Utf8List(trim_str);
  auto in_str_list = str2Utf8List(in_str);
  auto it = in_str_list.rbegin();
  for (; it != in_str_list.rend(); it++) {
    if (trim_str.find(*it) == KString::npos) {
      break;
    }
  }
  KString res_str = "";
  for (; it != in_str_list.rend(); it++) {
    res_str = *it + res_str;
  }

  String s(res_str.length());
  snprintf(s.getptr(), res_str.length() + 1, "%s", res_str.c_str());
  s.length_ = res_str.length();
  return s;
  // return res_str;
}

static String rtrim1(Field **args, k_int32 arg_count) {
  KString tmp_str = "";
  String s = args[0]->ValStr();
  KString in_str = {s.getptr(), s.length_};
  size_t split_i = in_str.length();
  while (split_i > 0) {
    if (!std::isspace(in_str[split_i - 1])) {
      break;
    }
    --split_i;
  }
  std::string str = in_str.substr(0, split_i);
  String s1(str.length());
  snprintf(s1.getptr(), str.length() + 1, "%s", str.c_str());
  s1.length_ = str.length();
  return s1;
}

static String rtrim2(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  String s2 = args[1]->ValStr();
  return rtrim({s1.getptr(), s1.length_}, {s2.getptr(), s2.length_});
}

static KString pad(const KString &padstr, size_t sub_len) {
  std::list<KString> padstr_utf8 = str2Utf8List(padstr);
  auto pad_len = padstr_utf8.size();
  if (pad_len == 0) {
    return "";
  }
  KString res_str = "";
  for (size_t i = 0; i < static_cast<size_t>(sub_len / pad_len); i++) {
    res_str += padstr;
  }
  size_t len2append = sub_len % pad_len;
  for (auto s : padstr_utf8) {
    if (len2append == 0) {
      break;
    }
    res_str += s;
    len2append--;
  }

  return res_str;
}

static String lpad2(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto in_str = s.getptr();
  auto len = args[1]->ValInt();
  if (len <= 0) {
    return String("");
  }
  KString padstr = " ";
  auto in_len = strLength(in_str);
  if (in_len >= len) {
    return s;
  }
  KString res_str = pad(padstr, (len - in_len));
  res_str += in_str;

  String s1(res_str.length());
  snprintf(s1.getptr(), res_str.length() + 1, "%s", res_str.c_str());
  s1.length_ = res_str.length();
  return s1;
}

static String lpad3(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto in_str = std::string({s.getptr(), s.length_});
  auto len = args[1]->ValInt();
  if (len <= 0) {
    return String("");
  }
  auto in_len = strLength(in_str);
  if (in_len >= len) {
    return s;
  }
  String s2 = args[2]->ValStr();
  auto padstr = std::string({s2.getptr(), s2.length_});
  KString res_str = pad(padstr, (len - in_len));
  res_str += in_str;

  String s1(res_str.length());
  snprintf(s1.getptr(), res_str.length() + 1, "%s", res_str.c_str());
  s1.length_ = res_str.length();
  return s1;
}

static String rpad2(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto in_str = std::string({s.getptr(), s.length_});
  auto len = args[1]->ValInt();
  if (len <= 0) {
    return String("");
  }
  KString padstr = " ";
  auto in_len = strLength(in_str);
  if (in_len >= len) {
    return s;
  }
  KString res_str = in_str;
  res_str += pad(padstr, (len - in_len));

  String s1(res_str.length());
  snprintf(s1.getptr(), res_str.length() + 1, "%s", res_str.c_str());
  s1.length_ = res_str.length();
  return s1;
}

static String rpad3(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto len = args[1]->ValInt();
  if (len <= 0) {
    return String("");
  }
  KString res_str = std::string({s.getptr(), s.length_});
  auto in_len = strLength(res_str);
  if (in_len >= len) {
    return s;
  }
  String s2 = args[2]->ValStr();
  res_str += pad({s2.getptr(), s2.length_}, (len - in_len));

  String s1(res_str.length());
  snprintf(s1.getptr(), res_str.length() + 1, "%s", res_str.c_str());
  s1.length_ = res_str.length();
  return s1;
}

static String substrWithRegex(const std::string &input,
                                   const std::string &regexStr,
                                   const std::string &escapeChar) {
  std::wstring_convert<std::codecvt_utf8<wchar_t>> converter;

  std::wstring widestr = converter.from_bytes(input);

  std::wstring widestrp = converter.from_bytes(regexStr);
  std::wstring wescapeChar = converter.from_bytes(escapeChar);

  // firstly, replace the custom escape characters in regular expressions with
  // standard backslashes
  std::wstring adjustedRegexStr;
  for (size_t i = 0; i < widestrp.length(); ++i) {
    if (widestrp[i] == wescapeChar[0] && i + 1 < widestrp.length()) {
      adjustedRegexStr += '\\';  // add standard escape characters
      ++i;                       // skip escaped characters
    }
    adjustedRegexStr += widestrp[i];
  }

  // using adjusted regular expressions for matching
  try {
    std::wregex reg(adjustedRegexStr);
    std::wsmatch match;
    if (std::regex_search(widestr, match, reg) && match.size() > 0) {
      std::wstring ma;
      if (match.size() > 1) {
        ma = match.str(1);
      } else {
        ma = match.str(0);
      }
      std::string mastr = converter.to_bytes(ma);
      String s(mastr.length());
      snprintf(s.getptr(), mastr.length() + 1, "%s", mastr.c_str());
      s.length_ = mastr.length();
      return s;
    }
  } catch (const std::regex_error &e) {
    KString et = "regex_error";
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_REGEXP_MISMATCH, et.data());
    return String("");
  }
  /**
   * TODO(exec): If there is no match, return NULL. case：
   * create ts database ts_db;
   * create table ts_db.t1 (kt timestamptz not null, s1 varchar(8) not null,s2
   * char(50)) tags (t1 int2 not null) primary tags(t1); insert into ts_db.t1
   * values (now(), 'var', 'A132@4W5uk@jSG' ,1); select substr(s2,'[a-z]','u')
   * from ts_db.t1;
   * **/
  String s;
  s.is_null_ = true;
  return s;
}

static String substr2(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto in_str = std::string(s.getptr(), s.length_);
  if (args[1]->get_storage_type() == roachpb::DataType::CHAR) {
    String s1 = args[1]->ValStr();
    return substrWithRegex(in_str, {s1.getptr(), s1.length_}, "");
  }
  auto start_i = args[1]->ValInt();
  k_int64 count = 0;
  std::string substr = "";

  for (auto it = in_str.begin(); it != in_str.end(); ++it) {
    if (IsUtf8Header(
            *it)) {  // determine if it is the first byte encoded in UTF-8
      ++count;
    }
    if (count >= start_i) {
      substr += *it;
    }
  }
  s.alloc(substr.length());
  snprintf(s.getptr(), substr.length() + 1, "%s", substr.c_str());
  s.length_ = substr.length();
  return s;
}

static String substr3(Field **args, k_int32 arg_count) {
  String s = args[0]->ValStr();
  auto in_str = std::string(s.getptr(), s.length_);
  if (args[1]->get_storage_type() == roachpb::DataType::CHAR) {
    String s1 = args[1]->ValStr();
    String s2 = args[2]->ValStr();
    return substrWithRegex(in_str, {s1.getptr(), s1.length_},
                           {s2.getptr(), s2.length_});
  }
  auto start_i = args[1]->ValInt();
  auto len = args[2]->ValInt();
  if (len < 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  ("substr(): negative substring length " +
                                      std::to_string(len) + " not allowed")
                                      .c_str());
  }
  auto end_i = start_i + len;
  k_int64 count = 0, size_count = 0;
  std::string substr = "";
  for (auto it = in_str.begin(); it != in_str.end() && count < end_i; ++it) {
    if (IsUtf8Header(
            *it)) {  // determine if it is the first byte encoded in UTF-8
      ++count;
    }
    if (count >= start_i) {
      if (count >= end_i) {
        break;
      }
      substr += *it;
    }
  }
  String s1(substr.length());
  snprintf(s1.getptr(), substr.length() + 1, "%s", substr.c_str());
  s1.length_ = substr.length();
  return s1;
}

static String concat(Field **args, k_int32 arg_count) {
  std::string res = "";
  for (k_int32 i = 0; i < arg_count; i++) {
    String s = args[i]->ValStr();
    res += {s.getptr(), s.length_};
  }
  String s(res.length());
  snprintf(s.getptr(), res.length() + 1, "%s", res.c_str());
  s.length_ = res.length();
  return s;
}

static String lower(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  const std::string &originalStr = std::string{s1.getptr(), s1.length_};
  std::string lowerCaseStr;
  // Pre-allocate required space
  lowerCaseStr.reserve(originalStr.size());

  std::transform(originalStr.begin(), originalStr.end(),
                 std::back_inserter(lowerCaseStr),
                 [](unsigned char c) { return std::tolower(c); });

  String s0(lowerCaseStr.length());
  snprintf(s0.getptr(), lowerCaseStr.length() + 1, "%s", lowerCaseStr.c_str());
  s0.length_ = lowerCaseStr.length();
  return s0;
}

static String upper(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  const std::string &originalStr = std::string(s1.getptr(), s1.length_);
  std::string upperCaseStr;
  // Pre-allocate required space
  upperCaseStr.reserve(originalStr.size());

  std::transform(originalStr.begin(), originalStr.end(),
                 std::back_inserter(upperCaseStr),
                 [](unsigned char c) { return std::toupper(c); });
  String s0(upperCaseStr.length());
  snprintf(s0.getptr(), upperCaseStr.length() + 1, "%s", upperCaseStr.c_str());
  s0.length_ = upperCaseStr.length();
  return s0;
}

static String left(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  auto in_str_list = str2Utf8List(std::string(s1.getptr(), s1.length_));

  k_int64 len = args[1]->ValInt();
  if (len < 0) {
    if ((-len) >= in_str_list.size()) {
      return s1;
    }
    len = in_str_list.size() + len;
  } else if (len >= in_str_list.size()) {
    return s1;
  } else if (len == 0) {
    return String("");
  }
  k_int64 count = 0;
  KString new_str = "";

  for (auto &s : in_str_list) {
    new_str += s;
    if (++count >= len) {
      break;
    }
  }
  String s0(new_str.length());
  snprintf(s0.getptr(), new_str.length() + 1, "%s", new_str.c_str());
  s0.length_ = new_str.length();
  return s0;
}

static String right(Field **args, k_int32 arg_count) {
  String s1 = args[0]->ValStr();
  k_int64 len = args[1]->ValInt();
  auto in_str_list = str2Utf8List(std::string(s1.getptr(), s1.length_));

  if (len < 0) {
    if ((-len) >= in_str_list.size()) {
      return s1;
    }
    len = in_str_list.size() + len;
  } else if (len >= in_str_list.size()) {
    return s1;
  }
  k_int64 count = 0;
  KString new_str = "";

  for (auto s = in_str_list.rbegin(); count < len && s != in_str_list.rend();
       ++s, ++count) {
    new_str = *s + new_str;
  }
  String s0(new_str.length());
  snprintf(s0.getptr(), new_str.length() + 1, "%s", new_str.c_str());
  s0.length_ = new_str.length();
  return s0;
}

KStatus getConcatType(const std::list<Field *> &fields,
                      roachpb::DataType *sql_type,
                      roachpb::DataType *storage_type, k_uint32 *storage_len) {
  *sql_type = fields.front()->get_sql_type();
  *storage_type = fields.front()->get_sql_type();
  *storage_len = 0;
  for (auto f : fields) {
    *storage_len += f->get_storage_length();
  }
  return SUCCESS;
}

static String GetEncodeValue(String &strvalue1, String &strvalue2) {
  std::string data = std::string(strvalue1.getptr(), strvalue1.length_);
  std::string str = std::string(strvalue2.getptr(), strvalue2.length_);
  std::string format;
  for (char c : str) {
    format += std::toupper(c);
  }
  if (format == "HEX") {
    std::stringstream ss;
    for (unsigned char c : data) {
      ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(c);
    }
    String s(ss.str().length());
    snprintf(s.getptr(), ss.str().length() + 1, "%s", ss.str().c_str());
    s.length_ = ss.str().length();
    return s;
  } else if (format == "ESCAPE") {
    std::stringstream ss;
    for (unsigned char c : data) {
      if (c == '\\') {
        ss << "\\\\";
      } else if (c < 32 || c >= 127) {
        ss << '\\' << static_cast<char>('0' + (c >> 6)) << static_cast<char>('0' + ((c >> 3) & 7))
           << static_cast<char>('0' + (c & 7));
      } else {
        ss << c;
      }
    }
    String s(ss.str().length());
    snprintf(s.getptr(), ss.str().length() + 1, "%s", ss.str().c_str());
    s.length_ = ss.str().length();
    return s;
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "only 'hex', 'escape' formats are supported for encode()");
    return String("");
  }
}

static String GetDecodeValue(String &strvalue1, String &strvalue2) {
  std::string data = std::string(strvalue1.getptr(), strvalue1.length_);
  std::string str = std::string(strvalue2.getptr(), strvalue2.length_);
  std::string format;
  for (char c : str) {
    format += std::toupper(c);
  }
  if (format == "HEX") {
    std::string result;
    if (data.length() % 2 == 1) {
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "encoding/hex: invalid input parameter");
      return String("");
    }
    for (size_t i = 0; i + 1 < data.length(); i += 2) {
      if (!isxdigit(data[i]) || !isxdigit(data[i + 1])) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "encoding/hex: invalid input parameter");
        return String("");
      }
      unsigned char byte = std::stoi(data.substr(i, 2), nullptr, 16);
      result += byte;  // add to result
    }
    String s(result.length());
    snprintf(s.getptr(), result.length() + 1, "%s", result.c_str());
    s.length_ = result.length();
    return s;
  } else if (format == "ESCAPE") {
    std::stringstream ss;
    for (size_t i = 0; i < data.size(); i++) {
      char ch = data[i];
      if (ch != '\\') {
        ss << ch;
        continue;
      }
      if (i >= data.size() - 1) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_ESCAPE_SEQUENCE,
                                      "bytea encoded value ends with escape character");
        return String("");
      }
      if (data[i + 1] == '\\') {
        ss << '\\';
        i++;
        continue;
      }
      if (i + 3 >= data.size()) {
        EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_ESCAPE_SEQUENCE,
                                      "bytea encoded value ends with incomplete escape sequence");
        return String("");
      }
      unsigned char b = 0;
      for (int j = 1; j <= 3; j++) {
        char octDigit = data[i + j];
        if (octDigit < '0' || octDigit > '7' || (j == 1 && octDigit > '3')) {
          EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_ESCAPE_SEQUENCE, "invalid bytea escape sequence");
          return String("");
        }
        b = (b << 3) | (octDigit - '0');
      }
      ss << b;
      i += 3;
    }
    String s(ss.str().length());
    snprintf(s.getptr(), ss.str().length() + 1, "%s", ss.str().c_str());
    s.length_ = ss.str().length();
    return s;
  } else {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "only 'hex', 'escape' formats are supported for decode()");
    return String("");
  }
}

static String GetChrValue(k_int64 ascii, k_uint32 storagelen) {
  std::string result;
  if (ascii < 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE,
                                  "input value must be >= 0");
    return String("");
  }
  if (ascii > 1114111) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_PARAMETER_VALUE,
        "input value must be <= 1114111 (maximum Unicode code point)");
    return String("");
  }
  if (ascii <= 0x7F) {  // 1 byte encoding
    result += static_cast<char>(ascii);
  } else if (ascii <= 0x7FF) {  // 2 byte encoding
    result += static_cast<char>((ascii >> 6) | 0xC0);
    result += static_cast<char>((ascii & 0x3F) | 0x80);
  } else {  // 3 byte encoding
    result += static_cast<char>((ascii >> 12) | 0xE0);
    result += static_cast<char>(((ascii >> 6) & 0x3F) | 0x80);
    result += static_cast<char>((ascii & 0x3F) | 0x80);
  }
  String s(storagelen);
  snprintf(s.getptr(), storagelen + 1, "%s", result.c_str());
  s.length_ = result.length();
  return s;
}

const FieldStringFuncion funcs[] = {
    {
        .name = "ltrim",
        .func_type = FieldFunc::Functype::LTRIM_FUNC,
        .func1 = ltrim1,
        .func2 = ltrim2,
    },
    {
        .name = "rtrim",
        .func_type = FieldFunc::Functype::LTRIM_FUNC,
        .func1 = rtrim1,
        .func2 = rtrim2,
    },
    {
        .name = "lpad",
        .func_type = FieldFunc::Functype::LPAD_FUNC,
        .func1 = nullptr,
        .func2 = lpad2,
        .func3 = lpad3,
    },
    {
        .name = "rpad",
        .func_type = FieldFunc::Functype::RPAD_FUNC,
        .func1 = nullptr,
        .func2 = rpad2,
        .func3 = rpad3,
    },
    {
        .name = "substr",
        .func_type = FieldFunc::Functype::SUBSTR_FUNC,
        .func1 = nullptr,
        .func2 = substr2,
        .func3 = substr3,
    },
    {
        .name = "substring",
        .func_type = FieldFunc::Functype::SUBSTR_FUNC,
        .func1 = nullptr,
        .func2 = substr2,
        .func3 = substr3,
    },
    {
        .name = "concat",
        .func_type = FieldFunc::Functype::CONCAT_FUNC,
        .func1 = concat,
        .func2 = concat,
        .func3 = concat,
        .funcn = concat,
        .field_type_func = getConcatType,
    },
    {.name = "lower",
     .func_type = FieldFunc::Functype::LOWER_FUNC,
     .func1 = lower},
    {.name = "upper",
     .func_type = FieldFunc::Functype::UPPER_FUNC,
     .func1 = upper},
    {.name = "left",
     .func_type = FieldFunc::Functype::LEFT_FUNC,
     .func1 = nullptr,
     .func2 = left},
    {.name = "right",
     .func_type = FieldFunc::Functype::RIGHT_FUNC,
     .func1 = nullptr,
     .func2 = right}};

FieldFuncString::FieldFuncString(const KString &name,
                                 const std::list<Field *> &fields)
    : FieldFunc(fields) {
  type_ = FIELD_ARITHMETIC;

  for (auto f : funcs) {
    if (name == f.name) {
      func_type_ = f.func_type;
      switch (arg_count_) {
        case 1:
          func_ = f.func1;
          break;
        case 2:
          func_ = f.func2;
          break;
        case 3:
          func_ = f.func3;
          break;
        default:
          func_ = f.funcn;
      }
      if (f.field_type_func != nullptr) {
        f.field_type_func(fields, &sql_type_, &storage_type_, &storage_len_);
      } else {
        sql_type_ = fields.front()->get_sql_type();
        storage_type_ = fields.front()->get_storage_type();
        storage_len_ = fields.front()->get_storage_length();
      }
    }
  }
}

char *FieldFuncString::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  switch (storage_type_) {
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      strvalue_ = func_(args_, arg_count_);
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func string.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(strvalue_.ptr_);
}
k_int64 FieldFuncString::ValInt() { return 0; }

k_double64 FieldFuncString::ValReal() { return 0.0; }

String FieldFuncString::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    return func_(args_, arg_count_);
  }
}

Field *FieldFuncString::field_to_copy() {
  FieldFuncString *field = new FieldFuncString(*this);
  return field;
}

char *FieldFuncLength::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field length, get null value");
    return const_cast<char *>("");
  }
  String s = args_[0]->ValStr(ptr);
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
      intvalue_ = strLength(std::string(s.getptr(), s.length_));
      break;
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY:
      intvalue_ = s.size();
      break;
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func length.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncLength::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  }
  if (args_[0]->get_storage_type() == roachpb::DataType::BINARY ||
      args_[0]->get_storage_type() == roachpb::DataType::VARBINARY) {
    return args_[0]->ValStr().size();
  }
  String s = args_[0]->ValStr();
  return strLength(std::string(s.getptr(), s.length_));
}

k_double64 FieldFuncLength::ValReal() { return ValInt(); }

String FieldFuncLength::ValStr() {
  String str(storage_len_);
  snprintf(str.ptr_, storage_len_ + 1, "%ld", ValInt());
  str.length_ = strlen(str.ptr_);
  return str;
}

Field *FieldFuncLength::field_to_copy() {
  FieldFuncLength *field = new FieldFuncLength(*this);

  return field;
}

char *FieldFuncGetBit::get_ptr(RowBatch *batch) {
  intvalue_ = 0;
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field get bit, get null value");
    return const_cast<char *>("");
  }

  String s = args_[0]->ValStr(ptr);
  std::string str = std::string(s.ptr_, s.length_);
  std::vector<unsigned char> byteArray(str.begin(), str.end());
  int index = args_[0]->ValInt(ptr);
  // exceed array length
  if (index < 0 || index >= 8 * byteArray.size()) {
    EEPgErrorInfo::SetPgErrorInfo(
        ERRCODE_INVALID_PARAMETER_VALUE,
        ("bit index " + std::to_string(index) + "out of valid range (0..)" + std::to_string(8 * byteArray.size() - 1))
            .c_str());
    return reinterpret_cast<char *>(&intvalue_);
  }
  if ((byteArray[index / 8] & (1 << (8 - 1 - (index) % 8))) != 0) {
    intvalue_ = 1;
  }

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncGetBit::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    String s = args_[0]->ValStr();
    std::string str = std::string(s.ptr_, s.length_);
    std::vector<unsigned char> byteArray(str.begin(), str.end());
    int index = args_[0]->ValInt();
    // exceed array length
    if (index < 0 || index >= 8 * byteArray.size()) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_INVALID_PARAMETER_VALUE,
          ("bit index " + std::to_string(index) + "out of valid range (0..)" +
           std::to_string(8 * byteArray.size() - 1))
              .c_str());
      return 0;
    }
    if ((byteArray[index / 8] & (1 << (8 - 1 - (index) % 8))) != 0) {
      return 1;
    }
    return 0;
  }
}

k_double64 FieldFuncGetBit::ValReal() { return ValInt(); }

String FieldFuncGetBit::ValStr() {
  String s(storage_len_);
  snprintf(s.ptr_, storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.ptr_);
  return s;
}

Field *FieldFuncGetBit::field_to_copy() {
  FieldFuncGetBit *field = new FieldFuncGetBit(*this);
  return field;
}

char *FieldFuncInitCap::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  if (sizeof(args_) == 0) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_PARAMETER_VALUE, "can not use init_cap with an empty string");
    return const_cast<char *>("");
  }

  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field init_cap, get null value");
    return const_cast<char *>("");
  }

  switch (storage_type_) {
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      String s1 = args_[0]->ValStr(ptr);
      strvalue_ = String(s1.length());
      strvalue_.copy_string(s1);
      strvalue_.ptr_[0] = std::toupper(strvalue_.ptr_[0]);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func init_cap.");
      err = FAIL;
      break;
  }
  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(strvalue_.ptr_);
}

k_int64 FieldFuncInitCap::ValInt() { return 0; }

k_double64 FieldFuncInitCap::ValReal() { return 0.0; }

String FieldFuncInitCap::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    if (sizeof(args_) == 0) {
      EEPgErrorInfo::SetPgErrorInfo(
          ERRCODE_INVALID_PARAMETER_VALUE,
          "can not use init_cap with an empty string");
      return String("");
    }

    String s1 = args_[0]->ValStr();
    String result(s1.length());
    result.copy_string(s1);
    result.ptr_[0] = std::toupper(result.ptr_[0]);
    return result;
  }
}

Field *FieldFuncInitCap::field_to_copy() {
  FieldFuncInitCap *field = new FieldFuncInitCap(*this);
  return field;
}

char *FieldFuncChr::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field chr, get null value");
    return const_cast<char *>("");
  }
  k_int64 ascii = args_[0]->ValInt(ptr);
  switch (storage_type_) {
    case roachpb::DataType::CHAR:
    case roachpb::DataType::NCHAR:
    case roachpb::DataType::VARCHAR:
    case roachpb::DataType::NVARCHAR:
    case roachpb::DataType::BINARY:
    case roachpb::DataType::VARBINARY: {
      strvalue_ = GetChrValue(ascii, storage_len_);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func chr.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return const_cast<char *>(strvalue_.ptr_);
}

k_int64 FieldFuncChr::ValInt() { return 0; }

k_double64 FieldFuncChr::ValReal() { return 0.0; }

String FieldFuncChr::ValStr() {
  char *ptr = get_ptr();
  k_int64 ascii;
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    ascii = args_[0]->ValInt();
  }
  return GetChrValue(ascii, storage_len_);
}

Field *FieldFuncChr::field_to_copy() {
  FieldFuncChr *field = new FieldFuncChr(*this);
  return field;
}

char *FieldFuncEncode::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  switch (storage_type_) {
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
                                      "could not parse \"\" field encode, get null value");
        return const_cast<char *>("");
      }

      String s0 = args_[0]->ValStr(ptr0);
      String s1 = args_[1]->ValStr(ptr1);
      strvalue_ = GetEncodeValue(s0, s1);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func encode.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(strvalue_.ptr_);
}

k_int64 FieldFuncEncode::ValInt() { return 0; }

k_double64 FieldFuncEncode::ValReal() { return 0.0; }

String FieldFuncEncode::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    String s0 = args_[0]->ValStr();
    String s1 = args_[1]->ValStr();
    return GetEncodeValue(s0, s1);
  }
}

Field *FieldFuncEncode::field_to_copy() {
  FieldFuncEncode *field = new FieldFuncEncode(*this);
  return field;
}

char *FieldFuncDecode::get_ptr(RowBatch *batch) {
  KStatus err = SUCCESS;

  switch (storage_type_) {
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
                                      "could not parse \"\" field decode, get null value");
        return const_cast<char *>("");
      }
      String s0 = args_[0]->ValStr(ptr0);
      String s1 = args_[1]->ValStr(ptr1);
      strvalue_ = GetDecodeValue(s0, s1);
      break;
    }
    default:
      EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INDETERMINATE_DATATYPE, "unsupported data type for field func decode.");
      err = FAIL;
      break;
  }

  if (err != SUCCESS) {
    return const_cast<char *>("");
  }

  return reinterpret_cast<char *>(&strvalue_.ptr_);
}

k_int64 FieldFuncDecode::ValInt() { return 0; }

k_double64 FieldFuncDecode::ValReal() { return 0.0; }

String FieldFuncDecode::ValStr() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValStr(ptr);
  } else {
    String s0 = args_[0]->ValStr();
    String s1 = args_[1]->ValStr();
    return GetDecodeValue(s0, s1);
  }
}

Field *FieldFuncDecode::field_to_copy() {
  FieldFuncDecode *field = new FieldFuncDecode(*this);
  return field;
}

char *FieldFuncBitLength::get_ptr(RowBatch *batch) {
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field bit length, get null value");
    return const_cast<char *>("");
  }

  intvalue_ = args_[0]->ValStr(ptr).size() * 8;
  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncBitLength::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    return (args_[0]->ValStr()).size() * 8;
  }
}

k_double64 FieldFuncBitLength::ValReal() { return ValInt(); }

String FieldFuncBitLength::ValStr() {
  String s(storage_len_);
  snprintf(s.getptr(), storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.getptr());
  return s;
}

Field *FieldFuncBitLength::field_to_copy() {
  FieldFuncBitLength *field = new FieldFuncBitLength(*this);
  return field;
}

char *FieldFuncOctetLength::get_ptr(RowBatch *batch) {
  char *ptr = args_[0]->get_ptr(batch);
  if (ptr == nullptr) {
    EEPgErrorInfo::SetPgErrorInfo(ERRCODE_INVALID_TEXT_REPRESENTATION,
                                  "could not parse \"\" field octet length, get null value");
    return const_cast<char *>("");
  }

  intvalue_ = args_[0]->ValStr(ptr).size();

  return reinterpret_cast<char *>(&intvalue_);
}

k_int64 FieldFuncOctetLength::ValInt() {
  char *ptr = get_ptr();
  if (ptr) {
    return FieldFunc::ValInt(ptr);
  } else {
    return (args_[0]->ValStr()).size();
  }
}

k_double64 FieldFuncOctetLength::ValReal() { return ValInt(); }

String FieldFuncOctetLength::ValStr() {
  String s(storage_len_);
  snprintf(s.getptr(), storage_len_ + 1, "%ld", ValInt());
  s.length_ = strlen(s.getptr());
  return s;
}

Field *FieldFuncOctetLength::field_to_copy() {
  FieldFuncOctetLength *field = new FieldFuncOctetLength(*this);
  return field;
}

}  // namespace kwdbts
