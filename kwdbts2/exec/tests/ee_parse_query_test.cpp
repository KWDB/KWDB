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
// Created by wuchan on 2022/11/1.

#include "ee_parse_query.h"

#include "cm_lexer.h"
#include "ee_token_iterator.h"
#include "ee_ast_element_type.h"
#include "gtest/gtest.h"

namespace kwdbts {
class TestParseQuery : public ::testing::Test {};

TEST_F(TestParseQuery, TestParseQueryFunction) {
  // KString query =
  //     "(3:::INT + 5:::INT < 1:::INT) && (2:::INT - 1:::INT > 5:::INT) || "
  //     "(9.7:::FLOAT * 2.5:::FLOAT / 3.0:::FLOAT == 5.0:::FLOAT)";
  KString query = "(3:::INT + 5:::INT < 1:::INT)";
  auto max_query_size = 0;
  auto max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);

  kwdbts::IParser::Pos pos(tokens_ptr, max_parser_depth);
  kwdbts::ParseQuery parser(query, pos);
  auto expr = parser.ParseImpl();
  ASSERT_TRUE(expr != nullptr);
  // auto left = expr->left;
  // auto right = expr->right;
  // ASSERT_TRUE(left != nullptr);
  // ASSERT_TRUE(right != nullptr);
  // ASSERT_EQ(expr->operator_type, kwdbts::OR);
  // ASSERT_EQ(left->operator_type, kwdbts::AND);
  // ASSERT_EQ(right->operator_type, kwdbts::MULTIPLE);
}
TEST_F(TestParseQuery, TestParseStringCharQueryFunction) {
  KString query = "'a ':::STRING";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos pos(tokens_ptr, max_parser_depth);
  kwdbts::ParseQuery parser(query, pos);
  auto expr = parser.ParseImpl();
  ASSERT_TRUE(expr != nullptr);
  ASSERT_TRUE(expr->const_ptr != nullptr);
  ASSERT_TRUE(expr->is_leaf);
  ASSERT_EQ(expr->const_ptr->operators, kwdbts::STRING_TYPE);
}
TEST_F(TestParseQuery, TestParseBytesCharQueryFunction) {
  KString query = "'\\xbbffee':::BYTES";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos pos(tokens_ptr, max_parser_depth);
  kwdbts::ParseQuery parser(query, pos);
  auto expr = parser.ParseImpl();
  ASSERT_TRUE(expr != nullptr);
  ASSERT_TRUE(expr != nullptr);
  ASSERT_TRUE(expr->const_ptr != nullptr);
  ASSERT_TRUE(expr->is_leaf);
  ASSERT_EQ(expr->const_ptr->operators, kwdbts::BYTES_TYPE);
}
}  // namespace kwdbts
