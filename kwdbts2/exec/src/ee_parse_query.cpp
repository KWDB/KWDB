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

#include "ee_parse_query.h"

#include <math.h>

#include <chrono>
#include <iomanip>

#include "ee_common.h"

using kwdb::TokenType;
using kwdb::Element;
using kwdb::AstEleType;

namespace kwdbts {
//k_int16 forwardToTimeStringEnd(k_char *str) {
//  k_int16 i = 0;
//  k_int16 numOfSep = 0;
//
//  while (str[i] != 0 && numOfSep < 2) {
//    if (str[i++] == ':') {
//      numOfSep++;
//    }
//  }
//
//  while (str[i] >= '0' && str[i] <= '9') {
//    i++;
//  }
//
//  return i;
//}
//
//void ResolveTm(KString date, tm &t) {
//  // how many digits of year
//  int pos = date.find('-');
//  if (pos == 0) {  // '-2000-01-01 00:00:00'
//    pos = date.find('-', 1);
//    t.tm_year = 0 - stoi(date.substr(1, pos)) - 1900;
//  } else {  // '2000-01-01 00:00:00'
//    t.tm_year = stoi(date.substr(0, pos)) - 1900;
//  }
//
//  t.tm_mon = stoi(date.substr(pos+1, pos+3)) - 1;
//  t.tm_mday = stoi(date.substr(pos+4, pos+6));
//  t.tm_hour = stoi(date.substr(pos+7, pos+9));
//  t.tm_min = stoi(date.substr(pos+10, pos+12));
//  t.tm_sec = stoi(date.substr(pos+13, pos+15));
//}
//
///*
// * rfc3339 format:
// * 2013-04-12 15:52:01+08:00
// * 2013-04-12 15:52:01.123+08:00
// *
// * 2013-04-12 15:52:01Z
// * 2013-04-12 15:52:01.123Z
// *
// * iso-8601 format:
// * 2013-04-12T15:52:01+0800
// * 2013-04-12T15:52:01.123+0800
// */
//k_int64 getGMT(KString *value_) {
//  tm t = {};
//  // 19 means length of format 0000-00-00 00:00:00
//  if (value_->length() < 19) {
//    LOG_ERROR("date %s is not valid\n", (*value_).c_str())
//    return 0;
//  }
//
//  ResolveTm(*value_, t);
//  auto mt = mktime(&t);
//  // auto tp = std::chrono::system_clock::from_time_t(mt);
//  //  转换为Unix毫秒时间戳
//  // int64_t unixMs = std::chrono::duration_cast<std::chrono::milliseconds>(
//  //                      tp.time_since_epoch())
//  //                     .count();
//  int64_t unixMs = mt * 1000;
//  k_int16 pos;
//  pos = forwardToTimeStringEnd(value_->data());
//  k_char val = (*value_)[pos];
//  k_char *endptr;
//  if ((val == 'Z' || val == 'z') && val == '\0') {
//    /* utc time, no millisecond, return directly*/
//    return unixMs;
//  } else if (val == '.') {
//    pos++;
//    k_int16 nanoCOunt = 0;
//    k_int16 position = pos;
//    while ((*value_)[position] >= '0' && (*value_)[position] <= '9') {
//      position++;
//      if (nanoCOunt < 9) {
//        nanoCOunt++;
//      }
//    }
//    int64_t nanoSecond =
//        strtol(value_->substr(pos, nanoCOunt).c_str(), &endptr, 10);
//    pos = position;
//    while (nanoCOunt < 9) {
//      nanoSecond *= 10;
//      nanoCOunt++;
//    }
//    unixMs += nanoSecond;
//  }
//  k_char seg = (*value_)[pos];
//  if (seg == '+' || seg == '-') {
//    int64_t hour;
//    // parse the timezone
//    pos++;
//    KString subStr = value_->substr(pos, 2);
//    hour = strtol(value_->substr(pos, 2).c_str(), &endptr, 10);
//
//    if (seg == '+') {
//      unixMs -= hour * 3600 * 1000000000;
//    } else {
//      unixMs += hour * 3600 * 1000000000;
//    }
//    // change UTC to GMT
//    // auto offset = t.tm_gmtoff;
//    unixMs += t.tm_gmtoff * 1000000000;
//  }
//  return unixMs;
//}

//k_bool ParseQuery::ParseNumber(k_int64 factor) {
//  auto raw_sql = this->raw_sql_;
//  std::basic_string<k_char> read_buffer;
//  auto current_type = this->pos_->type;
//  if (current_type == TokenType::Number ||
//      current_type == TokenType::StringLiteral ||
//      current_type == TokenType::BareWord) {
//    auto size = read_buffer.size();
//    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//      size = read_buffer.size();
//    }
//    if (current_type == TokenType::BareWord) {
//      k_int64 value = 1;
//      if (read_buffer.find("TRUE") != std::string::npos ||
//          read_buffer.find("true") != std::string::npos) {
//        auto ele = Element(value);
//        ele.SetType(INT_TYPE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else if (read_buffer.find("FALSE") != std::string::npos ||
//                 read_buffer.find("false") != std::string::npos) {
//        value = 0;
//        auto ele = Element(value);
//        ele.SetType(INT_TYPE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else {
//        return false;
//      }
//    }
//  } else {
//    return false;
//  }
//  ++this->pos_;
//
//  /*
//   * dispose -1
//   */
//  if (this->pos_->type == TokenType::ClosingRoundBracket) {
//    ++this->pos_;
//  }
//
//  if (this->pos_->type == TokenType::TypeAnotation) {
//    ++this->pos_;
//    std::string data_str =
//        raw_sql.substr(this->pos_->current_depth_,
//                       this->pos_->end_depth_ - this->pos_->current_depth_);
//    if (data_str.find("INTERVAL") != std::string::npos) {
//      read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//      auto ele = Element(read_buffer);
//      ele.SetType(INTERVAL_TYPE);
//      node_list_.push_back(std::make_shared<Element>(ele));
//      return true;
//    } else if (data_str.find("INT") != std::string::npos) {
//      k_char *stop_string;
//      auto node_value = std::strtoull(read_buffer.c_str(), &stop_string, 10);
//      auto ele = Element(static_cast<k_int64>(node_value * factor));
//      ele.SetType(INT_TYPE);
//      node_list_.push_back(std::make_shared<Element>(ele));
//      return true;
//    } else {
//      if (data_str.find("DECIMAL") != std::string::npos) {
//        auto node_value = std::stod(read_buffer);
//        auto ele = Element(node_value * factor);
//        ele.SetType(DECIMAL);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        return true;
//      } else {
//        if (data_str.find("FLOAT8") != std::string::npos) {
//          k_float64 node_value;
//          if (read_buffer.compare("'NaN'") == 0) {
//            node_value = NAN;
//          } else {
//            node_value = std::stod(read_buffer);
//          }
//          auto ele = Element(node_value * factor);
//          ele.SetType(FLOAT_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("FLOAT") != std::string::npos) {
//          k_float32 node_value;
//          if (read_buffer.compare("'NaN'") == 0) {
//            node_value = NAN;
//          } else {
//            node_value = std::stof(read_buffer);
//          }
//          auto ele = Element(node_value * factor);
//          ele.SetType(FLOAT_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("TIMESTAMPTZ") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          // k_int64 tz = getGMT(&read_buffer);
//          k_int64 tz = 0;
//          k_int64 scale = 0;
//          AstEleType ele_type = TIMESTAMPTZ_TYPE;
//          char type = data_str[data_str.size() - 2];
//          if (type == '6') {
//            scale = 1000;
//            ele_type = TIMESTAMPTZ_MICRO_TYPE;
//          } else if (type == '9') {
//            scale = 1000000;
//            ele_type = TIMESTAMPTZ_NANO_TYPE;
//          } else {  // default or 3
//            scale = 1;
//          }
//          if (convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS) return false;
//          auto ele = Element(tz);
//          ele.SetType(ele_type);
//          node_list_.push_back(std::make_shared<Element>(ele));
//
//          return true;
//        } else if (data_str.find("TIMESTAMP") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          // k_int64 tz = getGMT(&read_buffer);
//          k_int64 tz = 0;
//          k_int64 scale = 0;
//          AstEleType ele_type = TIMESTAMP_TYPE;
//          char type = data_str[data_str.size() - 2];
//          if (type == '6') {
//            scale = 1000;
//            ele_type = TIMESTAMP_MICRO_TYPE;
//          } else if (type == '9') {
//            scale = 1000000;
//            ele_type = TIMESTAMP_NANO_TYPE;
//          } else {  // default or 3
//            scale = 1;
//          }
//          if (convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS) return false;
//          auto ele = Element(tz);
//          ele.SetType(ele_type);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (data_str.find("DATE") != std::string::npos) {
//          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
//          auto ele = Element(read_buffer);
//          ele.SetType(DATE_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (current_type == TokenType::StringLiteral &&
//                   data_str.find("STRING") != std::string::npos) {
//          read_buffer =
//              parseUnicode2Utf8(read_buffer.substr(1, read_buffer.size() - 2));
//          auto ele = Element(read_buffer);
//          ele.SetType(STRING_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else if (current_type == TokenType::StringLiteral &&
//                   data_str.find("BYTES") != std::string::npos) {
//          read_buffer =
//              parseHex2String(read_buffer.substr(1, read_buffer.size() - 2));
//          auto ele = Element(read_buffer);
//          ele.SetType(BYTES_TYPE);
//          node_list_.push_back(std::make_shared<Element>(ele));
//          return true;
//        } else {
//          return false;
//        }
//      }
//    }
//  }
//  return false;
//}

//k_bool ParseQuery::ParseSingleExpr() {
//  if (this->pos_->type == TokenType::Number ||
//      this->pos_->type == TokenType::StringLiteral ||
//      this->pos_->type == TokenType::BareWord) {
//    k_bool flag = ParseNumber(1);
//    return flag;
//  } else if (this->pos_->type == TokenType::At) {
//    std::string raw_sql = this->raw_sql_;
//    std::basic_string<k_char> read_buffer;
//    ++this->pos_;
//    if (this->pos_->type == TokenType::Number) {
//      auto size = read_buffer.size();
//      while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//        read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//        size = read_buffer.size();
//      }
//    } else {
//      return false;
//    }
//    k_uint32 node_value = atoi(read_buffer.c_str());
//    auto ele = Element(node_value);
//    ele.SetType(COLUMN_TYPE);
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::In) {
//    auto ele = Element(In, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//    std::string raw_sql = this->raw_sql_;
//    std::basic_string<k_char> read_buffer;
//    auto size = read_buffer.size();
//    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//      size = read_buffer.size();
//    }
//    ele = Element(read_buffer);
//    ele.SetType(STRING_TYPE);
//    node_list_.push_back(std::make_shared<Element>(ele));
//    return true;
//  } else if (this->pos_->type == TokenType::Not) {
//    ++this->pos_;
//    if (this->pos_->type == TokenType::Like ||
//        this->pos_->type == TokenType::ILike ||
//        this->pos_->type == TokenType::Is ||
//        this->pos_->type == TokenType::In) {
//      --this->pos_;
//      return true;
//    }
//    --this->pos_;
//    auto ele = Element(NOT, true);
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::Like) {
//    auto ele = Element(Like, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::ILike) {
//    auto ele = Element(ILike, true);
//    --this->pos_;
//    if (this->pos_->type == TokenType::Not) {
//      ele.SetNegative(KTRUE);
//    }
//    ++this->pos_;
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else if (this->pos_->type == TokenType::Is) {
//    ++this->pos_;
//    bool localNegative = KFALSE;
//    if (this->pos_->type == TokenType::Not) {
//      localNegative = KTRUE;
//      ++this->pos_;
//    }
//    if (this->pos_->type == TokenType::Null) {
//      auto ele = Element(IS_NULL, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    } else if (this->pos_->type == TokenType::Nan) {
//      auto ele = Element(IS_NAN, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    } else if (this->pos_->type == TokenType::Unknown) {
//      auto ele = Element(IS_UNKNOWN, true);
//      if (localNegative) {
//        ele.SetNegative(localNegative);
//      }
//      node_list_.push_back(std::make_shared<Element>(ele));
//    }
//  } else if (this->pos_->type == TokenType::Null) {
//    auto ele = Element(NULL_TYPE, false);
//    ++this->pos_;
//    if (this->pos_->type == TokenType::DoubleColon) {
//      // ship null
//      ++this->pos_;
//      // bareWords
//      ++this->pos_;
//      if (this->pos_->type == TokenType::ClosingRoundBracket) {
//        --this->pos_;
//      }
//      if (this->pos_->type == TokenType::OpeningRoundBracket) {
//        // (
//        ++this->pos_;
//        // number
//        ++this->pos_;
//      }
//    } else {
//      --this->pos_;
//    }
//    node_list_.push_back(std::make_shared<Element>(ele));
//  } else {
//    switch (this->pos_->type) {
//      case TokenType::Plus: {
//        node_list_.push_back(std::make_shared<Element>(PLUS, true));
//        break;
//      }
//      case TokenType::Minus: {
//        node_list_.push_back(std::make_shared<Element>(MINUS, true));
//        break;
//      }
//      case TokenType::Multiple: {
//        node_list_.push_back(std::make_shared<Element>(MULTIPLE, true));
//        break;
//      }
//      case TokenType::Divide: {
//        node_list_.push_back(std::make_shared<Element>(DIVIDE, true));
//        break;
//      }
//      case TokenType::Dividez: {
//        node_list_.push_back(std::make_shared<Element>(DIVIDEZ, true));
//        break;
//      }
//      case TokenType::Remainder: {
//        node_list_.push_back(std::make_shared<Element>(REMAINDER, true));
//        break;
//      }
//      case TokenType::Percent: {
//        node_list_.push_back(std::make_shared<Element>(PERCENT, true));
//        break;
//      }
//      case TokenType::Power: {
//        node_list_.push_back(std::make_shared<Element>(POWER, true));
//        break;
//      }
//      case TokenType::ANDCAL: {
//        node_list_.push_back(std::make_shared<Element>(ANDCAL, true));
//        break;
//      }
//      case TokenType::ORCAL: {
//        node_list_.push_back(std::make_shared<Element>(ORCAL, true));
//        break;
//      }
//      case TokenType::Tilde: {
//        node_list_.push_back(std::make_shared<Element>(TILDE, true));
//        break;
//      }
//      case TokenType::ITilde: {
//        node_list_.push_back(std::make_shared<Element>(IREGEX, true));
//        break;
//      }
//      case TokenType::NotRegex: {
//        node_list_.push_back(std::make_shared<Element>(NOTREGEX, true));
//        break;
//      }
//      case TokenType::NotIRegex: {
//        node_list_.push_back(std::make_shared<Element>(NOTIREGEX, true));
//        break;
//      }
//      case TokenType::Equals: {
//        node_list_.push_back(std::make_shared<Element>(EQUALS, true));
//        break;
//      }
//      case TokenType::NotEquals: {
//        node_list_.push_back(std::make_shared<Element>(NOT_EQUALS, true));
//        break;
//      }
//      case TokenType::LessOrEquals: {
//        node_list_.push_back(std::make_shared<Element>(LESS_OR_EQUALS, true));
//        break;
//      }
//      case TokenType::GreaterOrEquals: {
//        node_list_.push_back(
//            std::make_shared<Element>(GREATER_OR_EQUALS, true));
//        break;
//      }
//      case TokenType::LeftShift: {
//        node_list_.push_back(std::make_shared<Element>(LEFTSHIFT, true));
//        break;
//      }
//      case TokenType::RightShift: {
//        node_list_.push_back(std::make_shared<Element>(RIGHTSHIFT, true));
//        break;
//      }
//      case TokenType::Greater: {
//        node_list_.push_back(std::make_shared<Element>(GREATER, true));
//        break;
//      }
//      case TokenType::Comma: {
//        node_list_.push_back(std::make_shared<Element>(COMMA, false));
//        break;
//      }
//      case TokenType::Less: {
//        node_list_.push_back(std::make_shared<Element>(LESS, true));
//        break;
//      }
//      case TokenType::AND: {
//        node_list_.push_back(std::make_shared<Element>(AND, true));
//        break;
//      }
//      case TokenType::OR: {
//        node_list_.push_back(std::make_shared<Element>(OR, true));
//        break;
//      }
//      case TokenType::In: {
//        node_list_.push_back(std::make_shared<Element>(In, true));
//        break;
//      }
//      case TokenType::Case: {
//        node_list_.push_back(std::make_shared<Element>(CASE, true));
//        break;
//      }
//      case TokenType::When: {
//        node_list_.push_back(std::make_shared<Element>(WHEN, true));
//        break;
//      }
//      case TokenType::Then: {
//        node_list_.push_back(std::make_shared<Element>(THEN, true));
//        break;
//      }
//      case TokenType::Else: {
//        node_list_.push_back(std::make_shared<Element>(ELSE, true));
//        break;
//      }
//      case TokenType::End: {
//        node_list_.push_back(std::make_shared<Element>(CASE_END, true));
//        break;
//      }
//      case TokenType::COALESCE: {
//        node_list_.push_back(std::make_shared<Element>(COALESCE, true));
//        break;
//      }
//      case TokenType::DoubleColon: {
//        // CAST
//        ++this->pos_;
//        // bareWords
//        // ++this->pos_;
//        std::string raw_sql = this->raw_sql_;
//        std::basic_string<k_char> read_buffer;
//        auto size = read_buffer.size();
//        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//          size = read_buffer.size();
//        }
//        auto ele = Element(read_buffer);
//        ele.SetType(CAST);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        // if (this->pos_->type == TokenType::OpeningRoundBracket) {
//        //   // (
//        //   ++this->pos_;
//        //   // number
//        //   ++this->pos_;
//        // }
//        break;
//      }
//      case TokenType::TypeAnotation: {
//        ++this->pos_;
//        break;
//      }
//      case TokenType::Function: {
//        auto ele = Element(Function, true);
//        std::string raw_sql = this->raw_sql_;
//        std::basic_string<k_char> read_buffer;
//        auto size = read_buffer.size();
//        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
//          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
//          size = read_buffer.size();
//        }
//
//        ele = Element(read_buffer);
//        ele.SetType(Function);
//        ele.SetFunc(KTRUE);
//        node_list_.push_back(std::make_shared<Element>(ele));
//        break;
//      }
//      case TokenType::Cast: {
//        // Skip cast token.
//        // ++this->pos_;
//        break;
//      }
//      case TokenType::Any: {
//        node_list_.push_back(std::make_shared<Element>(ANY, true));
//        break;
//      }
//      case TokenType::All: {
//        node_list_.push_back(std::make_shared<Element>(ALL, true));
//        break;
//      }
//      default:
//        return false;
//    }
//  }
//  return true;
//}

KStatus ParseQuery::ConstructTree(std::size_t &i, void *head_node_void, void* user_data) {
  auto *head_node = static_cast<std::shared_ptr<BinaryExpr>*>(head_node_void);
  auto current_node = std::make_shared<BinaryExpr>(true);
  ExprPtr expr_ptr = nullptr;
  KStatus ret = FAIL;
  if (i >= node_list_.size()) {
    return SUCCESS;
  }
  if (node_list_[i]->is_operator) {
    current_node->is_leaf = false;
    switch (node_list_[i]->operators) {
      case CLOSING_BRACKET: {
        (i)++;
        return FAIL;
      }
      case OPENING_BRACKET: {
        (i)++;
        while (node_list_[i]->operators != CLOSING_BRACKET) {
          ret = ConstructTree(i, &current_node, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }
        (i)++;  // Skip CLOSING_BRACKET
        *head_node = current_node;
        return SUCCESS;
      }
      case WHEN: {
        current_node->operator_type = node_list_[i]->operators;
        // head_node :Case xxx When xxx
        if ((*head_node) == nullptr) {
          (i)++;
          while (node_list_[i]->operators != THEN) {
            ret = ConstructTree(i, head_node, user_data);
            if (ret != SUCCESS) {
              return ret;
            }
          }
          return SUCCESS;
        } else {
          (i)++;
          current_node->left = *head_node;
          while (node_list_[i]->operators != THEN) {
            ret = ConstructTree(i, &current_node->right, user_data);
            if (ret != SUCCESS) {
              return ret;
            }
          }
          *head_node = current_node;
          return SUCCESS;
        }
      }
      case CASE_END:
      case ELSE:
        return FAIL;
      case CASE: {
        current_node->is_leaf = true;  // CASE use args，dispose
        current_node->operator_type = node_list_[i]->operators;
        (i)++;
        ExprPtr cond_node = nullptr;
        while (node_list_[i]->operators != WHEN) {
          ret = ConstructTree(i, &cond_node, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }
        while (node_list_[i]->operators != CASE_END) {
          if (node_list_[i]->operators == WHEN) {
            expr_ptr = std::make_shared<BinaryExpr>(false);
            expr_ptr->operator_type = THEN;
            ExprPtr temp_node = cond_node;
            ret = ConstructTree(i, &temp_node, user_data);  // WHEN
            if (ret != SUCCESS) {
              return ret;
            }
            expr_ptr->left = temp_node;
            if (node_list_[i]->operators != THEN) {
              return FAIL;
            }
            (i)++;
            while (node_list_[i]->operators != CASE_END &&
                   node_list_[i]->operators != WHEN &&
                   node_list_[i]->operators != ELSE) {
              ret = ConstructTree(i, &expr_ptr->right, user_data);
              if (ret != SUCCESS) {
                return ret;
              }
            }
            current_node->args.push_back(expr_ptr);
          } else if (node_list_[i]->operators == ELSE) {
            expr_ptr = std::make_shared<BinaryExpr>(false);
            expr_ptr->operator_type = ELSE;
            (i)++;
            while (node_list_[i]->operators != CASE_END) {
              ret = ConstructTree(i, &expr_ptr->left, user_data);
              if (ret != SUCCESS) {
                return ret;
              }
            }
            current_node->args.push_back(expr_ptr);
          }
        }
        if (node_list_[i]->operators != CASE_END) {
          return FAIL;
        }
        (i)++;
        *head_node = current_node;
        return SUCCESS;
      }
      case IS_NULL:
      case IS_NAN: {
        current_node->operator_type = node_list_[i]->operators;
        if (node_list_[i]->is_negative) {
          current_node->SetNegative(node_list_[i]->is_negative);
        }
        current_node->left = *head_node;
        current_node->right = current_node->left;
        (i)++;
        *head_node = current_node;
        return SUCCESS;
      }
      case MINUS: {
        current_node->operator_type = MINUS;
        if ((*head_node) == nullptr || (*head_node)->const_ptr == nullptr) {
          k_int64 zero = 0;
          auto ele = Element(zero);
          ele.SetType(INT_TYPE);
          (*head_node)->const_ptr = std::make_shared<Element>(ele);
        }
        current_node->left = *head_node;
        (i)++;
        ret = ConstructTree(i, &current_node->right, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        *head_node = current_node;
        return SUCCESS;
      }
      case TILDE: {
        if ((*head_node) == nullptr) {
          current_node->operator_type = TILDE;
          (i)++;
          ret = ConstructTree(i, &current_node->left, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        } else {
          current_node->operator_type = REGEX;
          current_node->left = *head_node;
          if (node_list_[i]->is_negative) {
            current_node->SetNegative(node_list_[i]->is_negative);
          }
          (i)++;
          ret = ConstructTree(i, &current_node->right, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }

        *head_node = current_node;
        return SUCCESS;
      }
      case IREGEX: {
          current_node->operator_type = IREGEX;
          current_node->left = *head_node;
          if (node_list_[i]->is_negative) {
            current_node->SetNegative(node_list_[i]->is_negative);
          }
          (i)++;
          ret = ConstructTree(i, &current_node->right, user_data);
          if (ret != SUCCESS) {
            return ret;
          }

        *head_node = current_node;
        return SUCCESS;
      }
      case NOTREGEX: {
        current_node->operator_type = REGEX;
        current_node->left = *head_node;
        current_node->SetNegative(KTRUE);
        (i)++;
        ret = ConstructTree(i, &current_node->right, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        *head_node = current_node;
        return SUCCESS;
      }
      case NOTIREGEX: {
        current_node->operator_type = IREGEX;
        current_node->left = *head_node;
        current_node->SetNegative(KTRUE);
        (i)++;
        ret = ConstructTree(i, &current_node->right, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        *head_node = current_node;
        return SUCCESS;
      }
      case COALESCE: {
        // COALESCE(@1(column), normal value)
        current_node->operator_type = COALESCE;

        (i)++;
        if (node_list_[i]->operators != OPENING_BRACKET) {
          return FAIL;
        }
        (i)++;
        while (node_list_[i]->operators != CLOSING_BRACKET) {
          if (ConstructTree(i, &expr_ptr, user_data) != SUCCESS) {
            return FAIL;
          }
          if (node_list_[i]->operators == COMMA) {
            current_node->left = expr_ptr;
            (i)++;
          }
        }
        if (expr_ptr != nullptr) {
          current_node->right = expr_ptr;
        }
        (i)++;
        *head_node = current_node;
        return SUCCESS;
      }
      case ALL:
      case ANY: {
        current_node->operator_type = node_list_[i]->operators;
        if (node_list_[i]->is_negative) {
          current_node->SetNegative(node_list_[i]->is_negative);
        }
        (i)++;
        if (node_list_[i]->operators != OPENING_BRACKET) {
          return FAIL;
        }
        (i)++;
        while (node_list_[i]->operators != CLOSING_BRACKET) {
          expr_ptr = std::make_shared<BinaryExpr>(false);
          expr_ptr->operator_type = (*head_node)->operator_type;
          expr_ptr->left = (*head_node)->left;
          expr_ptr->is_negative = (*head_node)->is_negative;
          expr_ptr->is_leaf = (*head_node)->is_leaf;
          if (ConstructTree(i, &expr_ptr->right, user_data) != SUCCESS) {
            return FAIL;
          }
          current_node->args.push_back(expr_ptr);
          if (node_list_[i]->operators == COMMA) {
            (i)++;
          }
        }
        *head_node = current_node;
        (i)++;
        return SUCCESS;
      }
      case NOT: {
        current_node->operator_type = NOT;
        (i)++;
        ret = ConstructTree(i, &current_node->left, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        (i)++;
        *head_node = current_node;
        return SUCCESS;
      }
      default: {
        current_node->operator_type = node_list_[i]->operators;
        if (node_list_[i]->is_negative) {
          current_node->SetNegative(node_list_[i]->is_negative);
        }
        current_node->left = *head_node;
        (i)++;
        if (node_list_[i]->operators == ANY ||
            node_list_[i]->operators == ALL) {
          ret = ConstructTree(i, &current_node, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        } else {
          ret = ConstructTree(i, &current_node->right, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }
        *head_node = current_node;
        return SUCCESS;
      }
    }
  } else if (node_list_[i]->is_func) {
    current_node->is_leaf = true;
    current_node->operator_type = node_list_[i]->operators;
    current_node->const_ptr = node_list_[i];
    current_node->const_ptr->operators = AstEleType::STRING_TYPE;
    current_node->const_ptr->value.string_type =
        node_list_[i]->value.string_type.substr(
            node_list_[i]->value.string_type.find_last_of(':') + 1);
    (i)++;
    if (node_list_[i]->operators != OPENING_BRACKET) {
      return FAIL;
    }
    (i)++;
    while (node_list_[i]->operators != CLOSING_BRACKET) {
      if (ConstructTree(i, &expr_ptr, user_data) != SUCCESS) {
        return FAIL;
      }
      if (node_list_[i]->operators == COMMA) {
        current_node->args.push_back(expr_ptr);
        (i)++;
      }
    }
    if (expr_ptr != nullptr) {
      current_node->args.push_back(expr_ptr);
    }
    (i)++;
    *head_node = current_node;
    return SUCCESS;
  } else {
    switch (node_list_[i]->operators) {
      case CAST:
        current_node->is_leaf = false;
        current_node->operator_type = CAST;
        current_node->const_ptr = node_list_[i];
        current_node->const_ptr->operators = AstEleType::STRING_TYPE;
        current_node->const_ptr->value.string_type =
            node_list_[i]->value.string_type.substr(
                node_list_[i]->value.string_type.find_last_of(':') + 1);
        current_node->left = *head_node;
        (i)++;
        *head_node = current_node;
        return SUCCESS;
        break;
      case COLUMN_TYPE:
        current_node->is_leaf = true;
        current_node->reference_ptr = std::make_shared<VirtualField>(
            node_list_[i]->value.number.column_id);
        (i)++;
        while (i < node_list_.size()) {
          if (node_list_[i]->operators == CAST) {
            ret = ConstructTree(i, &current_node, user_data);
            if (ret != SUCCESS) {
              return ret;
            }
            break;
          } else {
            // (i)--;
            break;
          }
        }
        *head_node = current_node;
        return SUCCESS;
      case AstEleType::INT_TYPE:
      case AstEleType::FLOAT_TYPE:
      case AstEleType::DECIMAL:
      case AstEleType::STRING_TYPE:
      case AstEleType::TIMESTAMP_TYPE:
      case AstEleType::TIMESTAMPTZ_TYPE:
      case AstEleType::TIMESTAMP_MICRO_TYPE:
      case AstEleType::TIMESTAMPTZ_MICRO_TYPE:
      case AstEleType::TIMESTAMP_NANO_TYPE:
      case AstEleType::TIMESTAMPTZ_NANO_TYPE:
      case AstEleType::DATE_TYPE:
      case AstEleType::INTERVAL_TYPE:
      case AstEleType::BYTES_TYPE:
      case AstEleType::NULL_TYPE:
        current_node->is_leaf = true;
        current_node->const_ptr = node_list_[i];
        (i)++;
        *head_node = current_node;
        return SUCCESS;
      default:
        break;
    }
  }
  return FAIL;
}

//ExprPtr ParseQuery::ParseImpl() {
//  k_int64 bracket = 0;
//
//  while (true) {
//    if (this->pos_->type == TokenType::Error) {
//      return nullptr;
//    }
//    if (this->pos_->isEnd()) {
//      if (bracket != 0) {
//        return nullptr;
//      }
//      size_t i = 0;
//      ExprPtr expr = nullptr;
//      while (i < node_list_.size()) {
//        if (ConstructTree(&i, &expr) != SUCCESS) {
//          return nullptr;
//        }
//      }
//      return expr;
//    }
//    if (this->pos_->type == TokenType::OpeningRoundBracket) {
//      node_list_.push_back(std::make_shared<Element>(OPENING_BRACKET, true));
//      ++bracket;
//      ++this->pos_;
//      if (this->pos_->type == TokenType::Minus) {
//        k_int64 factor = -1;
//        ++this->pos_;
//
//        node_list_.pop_back();
//        --bracket;
//        k_bool flag = ParseNumber(factor);
//        if (!flag) {
//          return nullptr;
//        }
//      } else {
//        --this->pos_;
//      }
//    } else {
//      if (this->pos_->type == TokenType::ClosingRoundBracket) {
//        node_list_.push_back(std::make_shared<Element>(kwdb::CLOSING_BRACKET, true));
//        --bracket;
//      } else {
//        k_bool flag = ParseSingleExpr();
//        if (!flag) {
//          return nullptr;
//        }
//      }
//    }
//    ++this->pos_;
//  }
//return nullptr;
//}

}  // namespace kwdbts
