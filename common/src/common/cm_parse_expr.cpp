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
#include "cm_parse_expr.h"
#include <math.h>
#include "ee_cast_utils.h"
#include "cm_func.h"

using kwdbts::k_bool;
using kwdbts::k_float32;
using kwdbts::k_float64;
using kwdbts::SUCCESS;

namespace kwdb {
k_bool ParseExpr::ParseNumber(k_int64 factor) {
  auto raw_sql = this->raw_sql_;
  std::basic_string<k_char> read_buffer;
  auto current_type = this->pos_->type;
  if (current_type == TokenType::Number ||
      current_type == TokenType::StringLiteral ||
      current_type == TokenType::BareWord) {
    auto size = read_buffer.size();
    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
      size = read_buffer.size();
    }
    if (current_type == TokenType::BareWord) {
      k_int64 value = 1;
      if (read_buffer.find("TRUE") != std::string::npos ||
          read_buffer.find("true") != std::string::npos) {
        auto ele = Element(value);
        ele.SetType(INT_TYPE);
        node_list_.push_back(std::make_shared<Element>(ele));
        return true;
      } else if (read_buffer.find("FALSE") != std::string::npos ||
                 read_buffer.find("false") != std::string::npos) {
        value = 0;
        auto ele = Element(value);
        ele.SetType(INT_TYPE);
        node_list_.push_back(std::make_shared<Element>(ele));
        return true;
      } else {
        return false;
      }
    }
  } else {
    return false;
  }
  ++this->pos_;
  /*
   * dispose -1
   */
  if (this->pos_->type == TokenType::ClosingRoundBracket) {
    ++this->pos_;
  }
  if (this->pos_->type == TokenType::TypeAnotation) {
    ++this->pos_;
    std::string data_str =
        raw_sql.substr(this->pos_->current_depth_,
                       this->pos_->end_depth_ - this->pos_->current_depth_);
    if (data_str.find("INTERVAL") != std::string::npos) {
      read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
      auto ele = Element(read_buffer);
      ele.SetType(INTERVAL_TYPE);
      node_list_.push_back(std::make_shared<Element>(ele));
      return true;
    } else if (data_str.find("INT") != std::string::npos) {
      k_char *stop_string;
      auto node_value = std::strtoull(read_buffer.c_str(), &stop_string, 10);
      auto ele = Element(static_cast<k_int64>(node_value * factor));
      ele.SetType(INT_TYPE);
      node_list_.push_back(std::make_shared<Element>(ele));
      return true;
    } else {
      if (data_str.find("DECIMAL") != std::string::npos) {
        auto node_value = std::stod(read_buffer);
        auto ele = Element(node_value * factor);
        ele.SetType(DECIMAL);
        node_list_.push_back(std::make_shared<Element>(ele));
        return true;
      } else {
        if (data_str.find("FLOAT8") != std::string::npos) {
          k_float64 node_value;
          if (read_buffer.compare("'NaN'") == 0) {
            node_value = NAN;
          } else {
            node_value = std::stod(read_buffer);
          }
          auto ele = Element(node_value * factor);
          ele.SetType(FLOAT_TYPE);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (data_str.find("FLOAT") != std::string::npos) {
          k_float32 node_value;
          if (read_buffer.compare("'NaN'") == 0) {
            node_value = NAN;
          } else {
            node_value = std::stof(read_buffer);
          }
          auto ele = Element(node_value * factor);
          ele.SetType(FLOAT_TYPE);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (data_str.find("TIMESTAMPTZ") != std::string::npos) {
          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
          // k_int64 tz = getGMT(&read_buffer);
          k_int64 tz = 0;
          k_int64 scale = 0;
          AstEleType ele_type = TIMESTAMPTZ_TYPE;
          char type = data_str[data_str.size() - 2];
          if (type == '6') {
            scale = 1000;
            ele_type = TIMESTAMPTZ_MICRO_TYPE;
          } else if (type == '9') {
            scale = 1000000;
            ele_type = TIMESTAMPTZ_NANO_TYPE;
          } else {  // default or 3
            scale = 1;
          }
          if (kwdbts::convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS) return false;
          auto ele = Element(tz);
          ele.SetType(ele_type);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (data_str.find("TIMESTAMP") != std::string::npos) {
          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
          // k_int64 tz = getGMT(&read_buffer);
          k_int64 tz = 0;
          k_int64 scale = 0;
          AstEleType ele_type = TIMESTAMP_TYPE;
          char type = data_str[data_str.size() - 2];
          if (type == '6') {
            scale = 1000;
            ele_type = TIMESTAMP_MICRO_TYPE;
          } else if (type == '9') {
            scale = 1000000;
            ele_type = TIMESTAMP_NANO_TYPE;
          } else {  // default or 3
            scale = 1;
          }
          if (kwdbts::convertStringToTimestamp(read_buffer, scale, &tz) != SUCCESS) return false;
          auto ele = Element(tz);
          ele.SetType(ele_type);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (data_str.find("DATE") != std::string::npos) {
          read_buffer = read_buffer.substr(1, read_buffer.size() - 2);
          auto ele = Element(read_buffer);
          ele.SetType(DATE_TYPE);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (current_type == TokenType::StringLiteral &&
                   data_str.find("STRING") != std::string::npos) {
          read_buffer = kwdbts::parseUnicode2Utf8(read_buffer.substr(1, read_buffer.size() - 2));
          auto ele = Element(read_buffer);
          ele.SetType(STRING_TYPE);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else if (current_type == TokenType::StringLiteral &&
                   data_str.find("BYTES") != std::string::npos) {
          read_buffer = kwdbts::parseHex2String(read_buffer.substr(1, read_buffer.size() - 2));
          auto ele = Element(read_buffer);
          ele.SetType(BYTES_TYPE);
          node_list_.push_back(std::make_shared<Element>(ele));
          return true;
        } else {
          return false;
        }
      }
    }
  }
  return false;
}

k_bool ParseExpr::ParseSingleExpr() {
  if (this->pos_->type == TokenType::Number ||
      this->pos_->type == TokenType::StringLiteral ||
      this->pos_->type == TokenType::BareWord) {
    k_bool flag = ParseNumber(1);
    return flag;
  } else if (this->pos_->type == TokenType::At) {
    std::string raw_sql = this->raw_sql_;
    std::basic_string<k_char> read_buffer;
    ++this->pos_;
    if (this->pos_->type == TokenType::Number) {
      auto size = read_buffer.size();
      while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
        read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
        size = read_buffer.size();
      }
    } else {
      return false;
    }
    k_uint32 node_value = atoi(read_buffer.c_str());
    auto ele = Element(node_value);
    ele.SetType(COLUMN_TYPE);
    node_list_.push_back(std::make_shared<Element>(ele));
  } else if (this->pos_->type == TokenType::In) {
    auto ele = Element(In, true);
    --this->pos_;
    if (this->pos_->type == TokenType::Not) {
      ele.SetNegative(KTRUE);
    }
    ++this->pos_;
    node_list_.push_back(std::make_shared<Element>(ele));
    std::string raw_sql = this->raw_sql_;
    std::basic_string<k_char> read_buffer;
    auto size = read_buffer.size();
    while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
      read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
      size = read_buffer.size();
    }
    ele = Element(read_buffer);
    ele.SetType(STRING_TYPE);
    node_list_.push_back(std::make_shared<Element>(ele));
    return true;
  } else if (this->pos_->type == TokenType::Not) {
    ++this->pos_;
    if (this->pos_->type == TokenType::Like ||
        this->pos_->type == TokenType::ILike ||
        this->pos_->type == TokenType::Is ||
        this->pos_->type == TokenType::In) {
      --this->pos_;
      return true;
    }
    --this->pos_;
    auto ele = Element(NOT, true);
    node_list_.push_back(std::make_shared<Element>(ele));
  } else if (this->pos_->type == TokenType::Like) {
    auto ele = Element(Like, true);
    --this->pos_;
    if (this->pos_->type == TokenType::Not) {
      ele.SetNegative(KTRUE);
    }
    ++this->pos_;
    node_list_.push_back(std::make_shared<Element>(ele));
  } else if (this->pos_->type == TokenType::ILike) {
    auto ele = Element(ILike, true);
    --this->pos_;
    if (this->pos_->type == TokenType::Not) {
      ele.SetNegative(KTRUE);
    }
    ++this->pos_;
    node_list_.push_back(std::make_shared<Element>(ele));
  } else if (this->pos_->type == TokenType::Is) {
    ++this->pos_;
    bool localNegative = KFALSE;
    if (this->pos_->type == TokenType::Not) {
      localNegative = KTRUE;
      ++this->pos_;
    }
    if (this->pos_->type == TokenType::Null) {
      auto ele = Element(IS_NULL, true);
      if (localNegative) {
        ele.SetNegative(localNegative);
      }
      node_list_.push_back(std::make_shared<Element>(ele));
    } else if (this->pos_->type == TokenType::Nan) {
      auto ele = Element(IS_NAN, true);
      if (localNegative) {
        ele.SetNegative(localNegative);
      }
      node_list_.push_back(std::make_shared<Element>(ele));
    } else if (this->pos_->type == TokenType::Unknown) {
      auto ele = Element(IS_UNKNOWN, true);
      if (localNegative) {
        ele.SetNegative(localNegative);
      }
      node_list_.push_back(std::make_shared<Element>(ele));
    }
  } else if (this->pos_->type == TokenType::Null) {
    auto ele = Element(NULL_TYPE, false);
    ++this->pos_;
    if (this->pos_->type == TokenType::DoubleColon) {
      // ship null
      ++this->pos_;
      // bareWords
      ++this->pos_;
      if (this->pos_->type == TokenType::ClosingRoundBracket) {
        --this->pos_;
      }
      if (this->pos_->type == TokenType::OpeningRoundBracket) {
        // (
        ++this->pos_;
        // number
        ++this->pos_;
      }
    } else {
      --this->pos_;
    }
    node_list_.push_back(std::make_shared<Element>(ele));
  } else {
    switch (this->pos_->type) {
      case TokenType::Plus: {
        node_list_.push_back(std::make_shared<Element>(PLUS, true));
        break;
      }
      case TokenType::Minus: {
        node_list_.push_back(std::make_shared<Element>(MINUS, true));
        break;
      }
      case TokenType::Multiple: {
        node_list_.push_back(std::make_shared<Element>(MULTIPLE, true));
        break;
      }
      case TokenType::Divide: {
        node_list_.push_back(std::make_shared<Element>(DIVIDE, true));
        break;
      }
      case TokenType::Dividez: {
        node_list_.push_back(std::make_shared<Element>(DIVIDEZ, true));
        break;
      }
      case TokenType::Remainder: {
        node_list_.push_back(std::make_shared<Element>(REMAINDER, true));
        break;
      }
      case TokenType::Percent: {
        node_list_.push_back(std::make_shared<Element>(PERCENT, true));
        break;
      }
      case TokenType::Power: {
        node_list_.push_back(std::make_shared<Element>(POWER, true));
        break;
      }
      case TokenType::ANDCAL: {
        node_list_.push_back(std::make_shared<Element>(ANDCAL, true));
        break;
      }
      case TokenType::ORCAL: {
        node_list_.push_back(std::make_shared<Element>(ORCAL, true));
        break;
      }
      case TokenType::Tilde: {
        node_list_.push_back(std::make_shared<Element>(TILDE, true));
        break;
      }
      case TokenType::ITilde: {
        node_list_.push_back(std::make_shared<Element>(IREGEX, true));
        break;
      }
      case TokenType::NotRegex: {
        node_list_.push_back(std::make_shared<Element>(NOTREGEX, true));
        break;
      }
      case TokenType::NotIRegex: {
        node_list_.push_back(std::make_shared<Element>(NOTIREGEX, true));
        break;
      }
      case TokenType::Equals: {
        node_list_.push_back(std::make_shared<Element>(EQUALS, true));
        break;
      }
      case TokenType::NotEquals: {
        node_list_.push_back(std::make_shared<Element>(NOT_EQUALS, true));
        break;
      }
      case TokenType::LessOrEquals: {
        node_list_.push_back(std::make_shared<Element>(LESS_OR_EQUALS, true));
        break;
      }
      case TokenType::GreaterOrEquals: {
        node_list_.push_back(
            std::make_shared<Element>(GREATER_OR_EQUALS, true));
        break;
      }
      case TokenType::LeftShift: {
        node_list_.push_back(std::make_shared<Element>(LEFTSHIFT, true));
        break;
      }
      case TokenType::RightShift: {
        node_list_.push_back(std::make_shared<Element>(RIGHTSHIFT, true));
        break;
      }
      case TokenType::Greater: {
        node_list_.push_back(std::make_shared<Element>(GREATER, true));
        break;
      }
      case TokenType::Comma: {
        node_list_.push_back(std::make_shared<Element>(COMMA, false));
        break;
      }
      case TokenType::Less: {
        node_list_.push_back(std::make_shared<Element>(LESS, true));
        break;
      }
      case TokenType::AND: {
        node_list_.push_back(std::make_shared<Element>(AND, true));
        break;
      }
      case TokenType::OR: {
        node_list_.push_back(std::make_shared<Element>(OR, true));
        break;
      }
      case TokenType::In: {
        node_list_.push_back(std::make_shared<Element>(In, true));
        break;
      }
      case TokenType::Case: {
        node_list_.push_back(std::make_shared<Element>(CASE, true));
        break;
      }
      case TokenType::When: {
        node_list_.push_back(std::make_shared<Element>(WHEN, true));
        break;
      }
      case TokenType::Then: {
        node_list_.push_back(std::make_shared<Element>(THEN, true));
        break;
      }
      case TokenType::Else: {
        node_list_.push_back(std::make_shared<Element>(ELSE, true));
        break;
      }
      case TokenType::End: {
        node_list_.push_back(std::make_shared<Element>(CASE_END, true));
        break;
      }
      case TokenType::COALESCE: {
        node_list_.push_back(std::make_shared<Element>(COALESCE, true));
        break;
      }
      case TokenType::DoubleColon: {
        // CAST
        ++this->pos_;
        // bareWords
        // ++this->pos_;
        std::string raw_sql = this->raw_sql_;
        std::basic_string<k_char> read_buffer;
        auto size = read_buffer.size();
        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
          size = read_buffer.size();
        }
        auto ele = Element(read_buffer);
        ele.SetType(CAST);
        node_list_.push_back(std::make_shared<Element>(ele));
        // if (this->pos_->type == TokenType::OpeningRoundBracket) {
        //   // (
        //   ++this->pos_;
        //   // number
        //   ++this->pos_;
        // }
        break;
      }
      case TokenType::TypeAnotation: {
        ++this->pos_;
        break;
      }
      case TokenType::Function: {
        auto ele = Element(Function, true);
        std::string raw_sql = this->raw_sql_;
        std::basic_string<k_char> read_buffer;
        auto size = read_buffer.size();
        while (size + this->pos_->current_depth_ < this->pos_->end_depth_) {
          read_buffer.push_back(raw_sql[this->pos_->current_depth_ + size]);
          size = read_buffer.size();
        }
        ele = Element(read_buffer);
        ele.SetType(Function);
        ele.SetFunc(KTRUE);
        node_list_.push_back(std::make_shared<Element>(ele));
        break;
      }
      case TokenType::Cast: {
        // Skip cast token.
        // ++this->pos_;
        break;
      }
      case TokenType::Any: {
        node_list_.push_back(std::make_shared<Element>(ANY, true));
        break;
      }
      case TokenType::All: {
        node_list_.push_back(std::make_shared<Element>(ALL, true));
        break;
      }
      default:
        return false;
    }
  }
  return true;
}

bool ParseExpr::ParseNode() {
  k_int64 bracket = 0;
  while (true) {
    if (this->pos_->type == TokenType::Error) {
      return false;
    }
    if (this->pos_->isEnd()) {
      if (bracket != 0) {
        return false;
      }
      break;
    }
    if (this->pos_->type == TokenType::OpeningRoundBracket) {
      node_list_.push_back(std::make_shared<Element>(OPENING_BRACKET, true));
      ++bracket;
      ++this->pos_;
      if (this->pos_->type == TokenType::Minus) {
        k_int64 factor = -1;
        ++this->pos_;
        node_list_.pop_back();
        --bracket;
        k_bool flag = ParseNumber(factor);
        if (!flag) {
          return false;
        }
      } else {
        --this->pos_;
      }
    } else {
      if (this->pos_->type == TokenType::ClosingRoundBracket) {
        node_list_.push_back(std::make_shared<Element>(CLOSING_BRACKET, true));
        --bracket;
      } else {
        k_bool flag = ParseSingleExpr();
        if (!flag) {
          return false;
        }
      }
    }
    ++this->pos_;
  }
  
  return true;
}

bool ParseExpr::ConstructExpr(void *expr, void *user_data) {
  size_t i = 0;
  while (i < node_list_.size()) {
    if (ConstructTree(i, expr, user_data) != SUCCESS) {
      return false;
    }
  }
  return true;
}

bool ParseExpr::ParseImpl(void *expr, void* user_data) {
  if (!ParseNode()) {
    return false;
  }
  
  return ConstructExpr(expr, user_data);
}

}  // namespace kwdb

