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

#define private public
#include "ee_parse_query.h"
#undef private

#include <ctime>
#include <vector>

#include "ee_lexer.h"
#include "ee_token_iterator.h"
#include "ee_ast_element_type.h"
#include "gtest/gtest.h"

namespace kwdbts {
k_int16 forwardToTimeStringEnd(k_char* str);
void ResolveTm(KString date, tm& t);
k_int64 getGMT(KString* value_);

namespace {

struct ParserHarness {
  explicit ParserHarness(const KString& query_text)
      : query(query_text),
        tokens(std::make_shared<kwdbts::Tokens>(
            query.data(), query.data() + query.size(), 0)),
        pos(tokens, 0),
        parser(query, pos) {}

  KString query;
  std::shared_ptr<kwdbts::Tokens> tokens;
  kwdbts::IParser::Pos pos;
  ParseQuery parser;
};

ParserHarness MakeParser(const KString& query) { return ParserHarness(query); }

}  // namespace

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

TEST_F(TestParseQuery, ParseHelpersHandleTimesAndNegativeYears) {
  char time_str[] = "12:34:56.789+08";
  EXPECT_EQ(forwardToTimeStringEnd(time_str), 8);

  tm parsed = {};
  ResolveTm("-2024-03-05 06:07:08", parsed);
  EXPECT_EQ(parsed.tm_year, -3924);
  EXPECT_EQ(parsed.tm_mon, 2);
  EXPECT_EQ(parsed.tm_mday, 5);
  EXPECT_EQ(parsed.tm_hour, 6);
  EXPECT_EQ(parsed.tm_min, 7);
  EXPECT_EQ(parsed.tm_sec, 8);

  KString invalid = "2024-01-01";
  EXPECT_EQ(getGMT(&invalid), 0);

  KString plus = "2024-01-02 03:04:05+08";
  KString minus = "2024-01-02 03:04:05-08";
  EXPECT_EQ(getGMT(&minus) - getGMT(&plus),
            static_cast<k_int64>(16) * 3600 * 1000000000LL);
}

TEST_F(TestParseQuery, ParseNumberHandlesBooleansAndTypedLiterals) {
  {
    auto parser_ctx = MakeParser("TRUE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, INT_TYPE);
    EXPECT_EQ(parser.node_list_[0]->value.number.int_type, 1);
  }

  {
    auto parser_ctx = MakeParser("FALSE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, INT_TYPE);
    EXPECT_EQ(parser.node_list_[0]->value.number.int_type, 0);
  }

  {
    auto parser_ctx = MakeParser("'1 day':::INTERVAL");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, INTERVAL_TYPE);
  }

  {
    auto parser_ctx = MakeParser("'2024-01-02 03:04:05':::TIMESTAMP6");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, TIMESTAMP_TYPE);
  }

  {
    auto parser_ctx = MakeParser("'2024-01-02 03:04:05':::TIMESTAMPTZ9");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, TIMESTAMPTZ_TYPE);
  }

  {
    auto parser_ctx = MakeParser("'2024-01-02':::DATE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseNumber(1));
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, DATE_TYPE);
  }
}

TEST_F(TestParseQuery, ParseImplHandlesRegexCaseAnyAndCoalesceBranches) {
  {
    auto parser_ctx = MakeParser("//");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, DIVIDEZ);
  }

  {
    auto parser_ctx = MakeParser("#");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, REMAINDER);
  }

  {
    auto parser_ctx = MakeParser("<<");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, LEFTSHIFT);
  }

  {
    auto parser_ctx = MakeParser(">>");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, RIGHTSHIFT);
  }

  {
    auto parser_ctx = MakeParser("!~");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, NOTREGEX);
  }

  {
    auto parser_ctx = MakeParser("COALESCE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, COALESCE);
  }

  {
    auto parser_ctx = MakeParser("CASE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, CASE);
  }

  {
    auto parser_ctx = MakeParser("ANY");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, ANY);
  }
}

TEST_F(TestParseQuery, ParseSingleExprCoversPredicatesOperatorsAndFunctionTokens) {
  {
    auto parser_ctx = MakeParser("@12");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, COLUMN_TYPE);
    EXPECT_EQ(parser.node_list_[0]->value.number.column_id, 12U);
  }

  {
    auto parser_ctx = MakeParser("LIKE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, Like);
  }

  {
    auto parser_ctx = MakeParser("ILIKE");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, ILike);
  }

  {
    auto parser_ctx = MakeParser("IS NULL");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, IS_NULL);
  }

  {
    auto parser_ctx = MakeParser("IS NOT NAN");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, IS_NAN);
    EXPECT_TRUE(parser.node_list_[0]->is_negative);
  }

  {
    auto parser_ctx = MakeParser("IS UNKNOWN");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, IS_UNKNOWN);
  }

  {
    auto parser_ctx = MakeParser("NULL::INT8");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, NULL_TYPE);
  }

  {
    auto parser_ctx = MakeParser("::INT8");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, CAST);
    EXPECT_EQ(parser.node_list_[0]->value.string_type, "INT8");
  }

  {
    auto parser_ctx = MakeParser("Function:sum(");
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr());
    ASSERT_EQ(parser.node_list_.size(), 1U);
    EXPECT_EQ(parser.node_list_[0]->operators, Function);
    EXPECT_TRUE(parser.node_list_[0]->is_func);
  }

  struct OperatorCase {
    const char* sql;
    AstEleType expected;
  };
  const std::vector<OperatorCase> cases = {
      {"+", PLUS},       {"-", MINUS},        {"*", MULTIPLE},
      {"%", PERCENT},    {"^", POWER},        {"&", ANDCAL},
      {"|", ORCAL},      {"=", EQUALS},       {"!=", NOT_EQUALS},
      {"<=", LESS_OR_EQUALS},
      {">=", GREATER_OR_EQUALS},
      {"<", LESS},
      {">", GREATER},
      {"AND", AND},
      {"OR", OR},
      {"ALL", ALL},
  };
  for (const auto& test_case : cases) {
    auto parser_ctx = MakeParser(test_case.sql);
    auto& parser = parser_ctx.parser;
    ASSERT_TRUE(parser.ParseSingleExpr()) << test_case.sql;
    ASSERT_EQ(parser.node_list_.size(), 1U) << test_case.sql;
    EXPECT_EQ(parser.node_list_[0]->operators, test_case.expected)
        << test_case.sql;
  }
}

TEST_F(TestParseQuery, ConstructTreeBuildsCaseCoalesceAnyCastAndFunctionNodes) {
  {
    auto parser_ctx = MakeParser("");
    auto& parser = parser_ctx.parser;
    auto col = std::make_shared<Element>(static_cast<k_uint32>(1));
    col->SetType(COLUMN_TYPE);
    auto int_val = std::make_shared<Element>(static_cast<k_int64>(2));
    int_val->SetType(INT_TYPE);
    parser.node_list_ = {
        std::make_shared<Element>(COALESCE, true),
        std::make_shared<Element>(OPENING_BRACKET, true),
        col,
        std::make_shared<Element>(COMMA, false),
        int_val,
        std::make_shared<Element>(CLOSING_BRACKET, true),
    };

    size_t i = 0;
    ExprPtr expr = nullptr;
    ASSERT_EQ(parser.ConstructTree(&i, &expr), SUCCESS);
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->operator_type, COALESCE);
    ASSERT_NE(expr->left, nullptr);
    ASSERT_NE(expr->right, nullptr);
  }

  {
    auto parser_ctx = MakeParser("");
    auto& parser = parser_ctx.parser;
    auto col = std::make_shared<Element>(static_cast<k_uint32>(1));
    col->SetType(COLUMN_TYPE);
    auto one = std::make_shared<Element>(static_cast<k_int64>(1));
    one->SetType(INT_TYPE);
    auto two = std::make_shared<Element>(static_cast<k_int64>(2));
    two->SetType(INT_TYPE);
    parser.node_list_ = {
        col,
        std::make_shared<Element>(EQUALS, true),
        std::make_shared<Element>(ANY, true),
        std::make_shared<Element>(OPENING_BRACKET, true),
        one,
        std::make_shared<Element>(COMMA, false),
        two,
        std::make_shared<Element>(CLOSING_BRACKET, true),
    };

    size_t i = 0;
    ExprPtr expr = nullptr;
    while (i < parser.node_list_.size()) {
      ASSERT_EQ(parser.ConstructTree(&i, &expr), SUCCESS);
    }
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->operator_type, ANY);
    EXPECT_EQ(expr->args.size(), 2U);
  }

  {
    auto parser_ctx = MakeParser("");
    auto& parser = parser_ctx.parser;
    auto col = std::make_shared<Element>(static_cast<k_uint32>(1));
    col->SetType(COLUMN_TYPE);
    auto when_val = std::make_shared<Element>(static_cast<k_int64>(1));
    when_val->SetType(INT_TYPE);
    auto then_val = std::make_shared<Element>(static_cast<k_int64>(2));
    then_val->SetType(INT_TYPE);
    auto else_val = std::make_shared<Element>(static_cast<k_int64>(3));
    else_val->SetType(INT_TYPE);
    parser.node_list_ = {
        std::make_shared<Element>(CASE, true),
        col,
        std::make_shared<Element>(WHEN, true),
        when_val,
        std::make_shared<Element>(THEN, true),
        then_val,
        std::make_shared<Element>(ELSE, true),
        else_val,
        std::make_shared<Element>(CASE_END, true),
    };

    size_t i = 0;
    ExprPtr expr = nullptr;
    ASSERT_EQ(parser.ConstructTree(&i, &expr), SUCCESS);
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->operator_type, CASE);
    EXPECT_EQ(expr->args.size(), 2U);
  }

  {
    auto parser_ctx = MakeParser("");
    auto& parser = parser_ctx.parser;
    auto col = std::make_shared<Element>(static_cast<k_uint32>(1));
    col->SetType(COLUMN_TYPE);
    auto cast = std::make_shared<Element>(KString("INT8"));
    cast->SetType(CAST);
    parser.node_list_ = {col, cast};

    size_t i = 0;
    ExprPtr expr = nullptr;
    while (i < parser.node_list_.size()) {
      ASSERT_EQ(parser.ConstructTree(&i, &expr), SUCCESS);
    }
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->operator_type, CAST);
    ASSERT_NE(expr->left, nullptr);
  }

  {
    auto parser_ctx = MakeParser("");
    auto& parser = parser_ctx.parser;
    auto func = std::make_shared<Element>(KString("Function:sum"));
    func->SetType(Function);
    func->SetFunc(KTRUE);
    auto col = std::make_shared<Element>(static_cast<k_uint32>(1));
    col->SetType(COLUMN_TYPE);
    auto int_val = std::make_shared<Element>(static_cast<k_int64>(2));
    int_val->SetType(INT_TYPE);
    parser.node_list_ = {
        func,
        std::make_shared<Element>(OPENING_BRACKET, true),
        col,
        std::make_shared<Element>(COMMA, false),
        int_val,
        std::make_shared<Element>(CLOSING_BRACKET, true),
    };

    size_t i = 0;
    ExprPtr expr = nullptr;
    ASSERT_EQ(parser.ConstructTree(&i, &expr), SUCCESS);
    ASSERT_NE(expr, nullptr);
    EXPECT_EQ(expr->operator_type, Function);
    EXPECT_EQ(expr->args.size(), 2U);
  }
}
}  // namespace kwdbts
