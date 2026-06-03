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
#include "ee_lexer.h"
#include "ee_iparser.h"

#include <vector>

#include "string"

#include "gtest/gtest.h"

namespace kwdbts {
namespace {

std::vector<TokenType> CollectSignificantTokens(const KString& query,
                                                k_int64 max_query_size = 0) {
  Lexer lexer(query.data(), query.data() + query.size(), max_query_size);
  std::vector<TokenType> tokens;
  while (true) {
    Token token = lexer.nextToken();
    if (token.isSignificant()) {
      tokens.push_back(token.type);
    }
    if (token.isEnd() || token.type == TokenType::ErrorMaxQuerySizeExceeded) {
      break;
    }
  }
  return tokens;
}

}  // namespace

class TestLexer : public ::testing::Test {};

// verify lexer
TEST_F(TestLexer, TestNotEqualsLexerFunction) {
  KString query = "3:::FLOAT != 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::NotEquals);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestEqualsLexerFunction) {
  KString query = "3:::FLOAT == 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Equals);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLessOrEqualsLexerFunction) {
  KString query = "3:::FLOAT <= 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::LessOrEquals);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestGreaterOrEqualsLexerFunction) {
  KString query = "3:::FLOAT >= 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::GreaterOrEquals);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestGreaterLexerFunction) {
  KString query = "3:::FLOAT > 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Greater);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLessFunction) {
  KString query = "3:::FLOAT < 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Less);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestAndFunction) {
  KString query = "3:::FLOAT and 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::AND);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestOrFunction) {
  KString query = "3:::FLOAT or 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::OR);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestPlusFunction) {
  KString query = "3:::FLOAT + 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Plus);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestMinusFunction) {
  KString query = "3:::FLOAT - 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Minus);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestMultipleFunction) {
  KString query = "3:::FLOAT * 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Multiple);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestDivideFunction) {
  KString query = "3:::FLOAT / 2:::FLOAT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                       max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Divide);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLexerAtFunction) {
  KString query = "@6 = '\\xbbffee':::BYTES";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::At);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Equals);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::StringLiteral);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

// Test for char with blank space
TEST_F(TestLexer, TestLexerStringCharFunction) {
  KString query = "'a ':::STRING";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::StringLiteral);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLexerBytesCharFunction) {
  KString query = "'\\xbbffee':::BYTES";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::StringLiteral);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLexerIntFunction) {
  KString query = "1:::INT8";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}

TEST_F(TestLexer, TestLexerRoundFunction) {
  KString query = "1:::()INT8";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::OpeningRoundBracket);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::ClosingRoundBracket);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
}


TEST_F(TestLexer, TestLexerCommaFunction) {
  KString query = "1:::INT8,";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Comma);
}

TEST_F(TestLexer, TestLexerQuestionMarkFunction) {
  KString query = "1:::INT8?";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;
  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::BareWord);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::QuestionMark);
}

TEST_F(TestLexer, TestLexerNotFunction) {
  KString query = "1:::NOT";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Not);
}

TEST_F(TestLexer, TestLexerLikeFunction) {
  KString query = "1:::LIKE";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Like);
}

TEST_F(TestLexer, TestLexerIsFunction) {
  KString query = "1:::IS";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Is);
}

TEST_F(TestLexer, TestLexerNullFunction) {
  KString query = "1:::NULL";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Null);
}
TEST_F(TestLexer, TestLexerPercentFunction) {
  KString query = "1:::%";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::TypeAnotation);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Percent);
}

TEST_F(TestLexer, TestLexerDoubleColonFunction) {
  KString query = "1::";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::DoubleColon);
}

TEST_F(TestLexer, TestLexerDotFunction) {
  KString query = "1::.";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::DoubleColon);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Dot);
}

TEST_F(TestLexer, TestLexerErrorFunction) {
  KString query = "/";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Error);
}
TEST_F(TestLexer, TestLexerArrowFunction) {
  KString query = "->2--";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Arrow);
  ++token_iterator;
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Number);
}

TEST_F(TestLexer, TestLexerStringLiteralFunction) {
  KString query = "e'\\':::STRING";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::StringLiteral);
}

TEST_F(TestLexer, TestLexerEndofFunction) {
  KString query = "";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::EndOfStream);
}
TEST_F(TestLexer, TestLexerfunctionFunction) {
  KString query = "Function()";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::Function);
}
TEST_F(TestLexer, TestLexerInFunction) {
  KString query = "IN ( )";
  k_int64 max_query_size = 0;
  k_int64 max_parser_depth = 0;

  kwdbts::Tokens tokens(query.data(), query.data() + query.size(),
                        max_query_size);
  auto tokens_ptr = std::make_shared<kwdbts::Tokens>(tokens);
  ASSERT_TRUE(tokens_ptr != nullptr);
  kwdbts::IParser::Pos token_iterator(tokens_ptr, max_parser_depth);
  ASSERT_EQ(token_iterator->type, kwdbts::TokenType::In);
}

TEST_F(TestLexer, TestLexerAdvancedOperatorsFunction) {
  const auto tokens = CollectSignificantTokens(
      "0x1f 12.5e-2 @@1 // # !~ !~* << >> | || & && ~ ~*");
  const std::vector<TokenType> expected = {
      TokenType::Number,      TokenType::Number,      TokenType::DoubleAt,
      TokenType::Number,      TokenType::Dividez,     TokenType::Remainder,
      TokenType::NotRegex,    TokenType::NotIRegex,   TokenType::LeftShift,
      TokenType::RightShift,  TokenType::ORCAL,       TokenType::OR,
      TokenType::ANDCAL,      TokenType::AND,         TokenType::Tilde,
      TokenType::ITilde,      TokenType::EndOfStream};
  EXPECT_EQ(tokens, expected);
}

TEST_F(TestLexer, TestLexerKeywordFamiliesFunction) {
  const auto tokens = CollectSignificantTokens(
      "LIKE ILIKE IS UNKNOWN NULL CASE WHEN THEN ELSE END COALESCE ANY ALL");
  const std::vector<TokenType> expected = {
      TokenType::Like,    TokenType::ILike, TokenType::Is,
      TokenType::Unknown, TokenType::Null,  TokenType::Case,
      TokenType::When,    TokenType::Then,  TokenType::Else,
      TokenType::End,     TokenType::COALESCE, TokenType::Any,
      TokenType::All,     TokenType::EndOfStream};
  EXPECT_EQ(tokens, expected);
}

TEST_F(TestLexer, TestLexerMaxQuerySizeExceededFunction) {
  Lexer limited("1234", "1234" + 4, 3);
  EXPECT_EQ(limited.nextToken().type, TokenType::ErrorMaxQuerySizeExceeded);
}

TEST_F(TestLexer, TestLexerKeywordsTypedStringsAndErrorsFunction) {
  {
    const auto tokens = CollectSignificantTokens(
        "e'abc':::STRING IN((1)) IS NULL NOT ILIKE LIKE NAN SOME CASE WHEN "
        "THEN ELSE END CAST COALESCE Functiondemo(");
    const std::vector<TokenType> expected = {
        TokenType::StringLiteral, TokenType::TypeAnotation,
        TokenType::BareWord,      TokenType::In,
        TokenType::ClosingRoundBracket,
        TokenType::Is,            TokenType::Null,
        TokenType::Not,           TokenType::ILike,
        TokenType::Like,          TokenType::Nan,
        TokenType::Any,           TokenType::Case,
        TokenType::When,          TokenType::Then,
        TokenType::Else,          TokenType::End,
        TokenType::Cast,          TokenType::COALESCE,
        TokenType::Function,      TokenType::OpeningRoundBracket,
        TokenType::EndOfStream};
    EXPECT_EQ(tokens, expected);
  }

  {
    Lexer lexer(":", ":" + 1, 0);
    EXPECT_EQ(lexer.nextToken().type, TokenType::Error);
  }

  {
    Lexer lexer("/*", "/*" + 2, 0);
    EXPECT_EQ(lexer.nextToken().type, TokenType::Error);
  }

  {
    Lexer lexer("e'unterminated", "e'unterminated" + 14, 0);
    EXPECT_EQ(lexer.nextToken().type, TokenType::Error);
  }
}
}  // namespace kwdbts
