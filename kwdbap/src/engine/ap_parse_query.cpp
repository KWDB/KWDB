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

#include "duckdb/engine/ap_parse_query.h"

#include "duckdb/catalog/catalog.hpp"
#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/catalog/entry_lookup_info.hpp"
#include "duckdb/execution/executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/planner/expression/bound_between_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_comparison_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_reference_expression.hpp"

using namespace duckdb;
using namespace kwdb;
using kwdbts::KStatus;

namespace kwdbap {
KStatus makeBewteenArgs(const unique_ptr<duckdb::Expression> *left_node,
                        const unique_ptr<duckdb::Expression> *right_node,
                        unique_ptr<duckdb::Expression> *lower_ptr,
                        unique_ptr<duckdb::Expression> *upper_ptr,
                        unique_ptr<duckdb::Expression> *input_col) {
  auto &lower = left_node->get()->Cast<BoundComparisonExpression>();
  if (lower.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
    *lower_ptr = std::move(lower.left);
    *input_col = std::move(lower.right);
  } else {
    *lower_ptr = std::move(lower.right);
    *input_col = std::move(lower.left);
  }
  auto &upper = right_node->get()->Cast<BoundComparisonExpression>();
  if (upper.left->GetExpressionClass() == ExpressionClass::BOUND_CONSTANT) {
    *upper_ptr = std::move(upper.left);
    if (nullptr == (*input_col).get() ||
        duckdb::Expression::Equals(*input_col, upper.right)) {
      return FAIL;
    }
  } else {
    *upper_ptr = std::move(upper.right);
    if (nullptr == (*input_col).get() ||
        !duckdb::Expression::Equals(*input_col, upper.left)) {
      return FAIL;
    }
  }
  return SUCCESS;
}

void makeConstantInt(const LogicalType &col_type, const k_int64 &int_value,
                     unique_ptr<BoundConstantExpression> &int_expr) {
  auto is_int16 = false;
  auto is_int32 = false;
  if (int_value >= INT16_MIN && int_value <= INT16_MAX) {
    is_int16 = true;
  } else if (int_value >= INT32_MIN && int_value <= INT32_MAX) {
    is_int32 = true;
  }

  switch (col_type.id()) {
    case LogicalType::SMALLINT:
      break;
    case LogicalType::INTEGER: {
      if (is_int16) {
        is_int16 = false;
        is_int32 = true;
      }
      break;
    }
    case LogicalType::BIGINT: {
      is_int16 = false;
      is_int32 = false;
      break;
    }
    default:
      return;
  }

  if (is_int16) {
    int_expr = make_uniq<BoundConstantExpression>(
        Value::SMALLINT(static_cast<int16_t>(int_value)));
  } else if (is_int32) {
    int_expr = make_uniq<BoundConstantExpression>(
        Value::INTEGER(static_cast<int32_t>(int_value)));
  } else {
    int_expr = make_uniq<BoundConstantExpression>(Value::BIGINT(int_value));
  }
}

KStatus APParseQuery::ConstructTree(std::size_t &i, void *head_node_ptr,
                                    void *user_data) {
  auto param = static_cast<ParseExprParam *>(user_data);
  auto *head_node =
      static_cast<unique_ptr<duckdb::Expression> *>(head_node_ptr);
  unique_ptr<duckdb::Expression> current_node;
  unique_ptr<duckdb::Expression> expr_ptr = nullptr;
  KStatus ret = FAIL;
  if (i >= node_list_.size()) {
    return SUCCESS;
  }
  if (node_list_[i]->is_operator) {
    switch (node_list_[i]->operators) {
      case CLOSING_BRACKET: {
        (i)++;
        return FAIL;
      }
      case OPENING_BRACKET: {
        (i)++;
        while (i < node_list_.size() &&
               node_list_[i]->operators != CLOSING_BRACKET) {
          ret = ConstructTree(i, &current_node, user_data);
          if (ret != SUCCESS) {
            return ret;
          }
        }
        (i)++;  // Skip CLOSING_BRACKET
        *head_node = std::move(current_node);
        return SUCCESS;
      }
      case AND: {
        (i)++;
        ret = ConstructTree(i, &current_node, user_data);
        if (ret != SUCCESS) {
          return ret;
        }
        auto head_expr = head_node->get();
        if (nullptr != head_expr) {
          bool lower_inclusive = false;
          bool upper_inclusive = false;
          bool head_lower = false;
          bool head_upper = false;
          bool current_lower = false;
          bool current_upper = false;
          auto head_type = head_expr->GetExpressionType();
          auto current_type = current_node->GetExpressionType();
          if (head_type == ExpressionType::COMPARE_GREATERTHAN ||
              head_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            head_lower = true;
            lower_inclusive =
                head_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          } else if (head_type == ExpressionType::COMPARE_LESSTHAN ||
                     head_type == ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            head_upper = true;
            upper_inclusive =
                head_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
          }
          if (current_type == ExpressionType::COMPARE_GREATERTHAN ||
              current_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO) {
            current_lower = true;
            lower_inclusive =
                current_type == ExpressionType::COMPARE_GREATERTHANOREQUALTO;
          } else if (current_type == ExpressionType::COMPARE_LESSTHAN ||
                     current_type ==
                         ExpressionType::COMPARE_LESSTHANOREQUALTO) {
            current_upper = true;
            upper_inclusive =
                current_type == ExpressionType::COMPARE_LESSTHANOREQUALTO;
          }
          unique_ptr<duckdb::Expression> lower_ptr;
          unique_ptr<duckdb::Expression> upper_ptr;
          unique_ptr<duckdb::Expression> input_col;
          if (current_lower && head_upper) {
            if (makeBewteenArgs(&current_node, head_node, &lower_ptr,
                                &upper_ptr, &input_col) != SUCCESS) {
              return FAIL;
            }
          } else if (current_upper && head_lower) {
            if (makeBewteenArgs(head_node, &current_node, &lower_ptr,
                                &upper_ptr, &input_col) != SUCCESS) {
              return FAIL;
            }
          }
          if (nullptr != lower_ptr && nullptr != upper_ptr &&
              nullptr != input_col) {
            auto between = make_uniq<BoundBetweenExpression>(
                std::move(input_col), std::move(lower_ptr),
                std::move(upper_ptr), lower_inclusive, upper_inclusive);
            *head_node = std::move(between);
          } else {
            auto conjunction = make_uniq<BoundConjunctionExpression>(
                ExpressionType::CONJUNCTION_AND, std::move(*head_node),
                std::move(current_node));
            *head_node = std::move(conjunction);
          }
        } else {
          *head_node = std::move(current_node);
        }
        return SUCCESS;
      }
      case LESS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_LESSTHAN, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case GREATER: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_GREATERTHAN, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_EQUAL, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case NOT_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_NOTEQUAL, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case LESS_OR_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_LESSTHANOREQUALTO, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case GREATER_OR_EQUALS: {
        auto tmp_expr = make_uniq<BoundComparisonExpression>(
            ExpressionType::COMPARE_GREATERTHANOREQUALTO, nullptr, nullptr);
        tmp_expr->left = std::move(*head_node);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      case In: {
        if (node_list_[i]->is_negative) {
          auto tmp_expr = make_uniq<BoundComparisonExpression>(
              ExpressionType::COMPARE_NOT_IN, nullptr, nullptr);
          tmp_expr->left = std::move(*head_node);
          *head_node = std::move(tmp_expr);
          (i)++;
        } else {
          auto tmp_expr = make_uniq<BoundComparisonExpression>(
              ExpressionType::COMPARE_IN, nullptr, nullptr);
          tmp_expr->left = std::move(*head_node);
          *head_node = std::move(tmp_expr);
          (i)++;
        }
        return SUCCESS;
      }
      case PLUS: {
        std::string plus = "+";
        EntryLookupInfo lookup_info(CatalogType::SCALAR_FUNCTION_ENTRY, plus);
        auto entry_retry = CatalogEntryRetriever(*param->context_);
        auto func_entry = entry_retry.GetEntry("", "", lookup_info,
                                               OnEntryNotFound::RETURN_NULL);
        auto &func = func_entry->Cast<ScalarFunctionCatalogEntry>();
        (i)++;
        if (ConstructTree(i, &current_node, user_data) != SUCCESS) {
          return FAIL;
        }
        vector<LogicalType> in_type;
        in_type.push_back(head_node->get()->return_type);
        in_type.push_back(current_node->return_type);
        auto type_func =
            func.functions.GetFunctionByArguments(*param->context_, in_type);
        auto return_type = head_node->get()->return_type;
        if (head_node->get()->return_type != type_func.return_type) {
          *head_node = BoundCastExpression::AddCastToType(
              *param->context_, std::move(*head_node), type_func.return_type,
              head_node->get()->return_type.id() == LogicalTypeId::ENUM);
        }
        if (current_node->return_type != type_func.return_type) {
          current_node = BoundCastExpression::AddCastToType(
              *param->context_, std::move(current_node), type_func.return_type,
              current_node->return_type.id() == LogicalTypeId::ENUM);
        }
        vector<unique_ptr<duckdb::Expression>> type_args;
        type_args.push_back(std::move(*head_node));
        type_args.push_back(std::move(current_node));

        auto tmp_expr = make_uniq<BoundFunctionExpression>(
            type_func.return_type, type_func, std::move(type_args), nullptr);
        *head_node = std::move(tmp_expr);
        (i)++;
        return SUCCESS;
      }
      default: {
        return FAIL;
      }
    }
  } else if (node_list_[i]->is_func) {
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
        // current_node->args.push_back(expr_ptr);
        (i)++;
      }
    }
    if (expr_ptr != nullptr) {
      // current_node->args.push_back(expr_ptr);
    }
    (i)++;
    *head_node = std::move(current_node);
    return SUCCESS;
  } else {
    switch (node_list_[i]->operators) {
      case CAST:
        (i)++;
        *head_node = std::move(current_node);
        return SUCCESS;
      case COLUMN_TYPE: {
        auto origin_node = node_list_[i];
        auto index = origin_node->value.number.column_id - 1;
        current_node = make_uniq<BoundReferenceExpression>(
            "", param->col_typ_map_[index], param->col_map_[index]);
        (i)++;
        while (i < node_list_.size() &&
               node_list_[i]->operators != CLOSING_BRACKET &&
               node_list_[i]->operators != AND) {
          if (ConstructTree(i, &current_node, user_data) != SUCCESS) {
            return FAIL;
          }
        }
        *head_node = std::move(current_node);
        return SUCCESS;
      }
      case AstEleType::INT_TYPE: {
        auto int_value = node_list_[i]->value.number.int_type;
        unique_ptr<BoundConstantExpression> int_expr;
        auto &comparsion_expr =
            head_node->get()->Cast<BoundComparisonExpression>();
        makeConstantInt(comparsion_expr.left->return_type, int_value, int_expr);
        if (nullptr == int_expr) {
          return FAIL;
        }
        if (int_expr->return_type != comparsion_expr.left->return_type) {
          comparsion_expr.left = BoundCastExpression::AddCastToType(
              *param->context_, std::move(comparsion_expr.left),
              int_expr->return_type,
              int_expr->return_type.id() == LogicalTypeId::ENUM);
        }
        comparsion_expr.right = std::move(int_expr);
        (i)++;
        return SUCCESS;
      }
      default:
        break;
    }
  }
  return FAIL;
}
}  // namespace kwdbap