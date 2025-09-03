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

#pragma once

#include <utility>

#include "duckdb.h"
#include "cm_assert.h"
#include "kwdb_type.h"
#include "ee_pb_plan.pb.h"
#include "ee_comm_def.h"
#include "ee_ast_element_type.h"
#include "ee_iparser.h"
#include "kwdb_type.h"

#include "duckdb/main/capi/capi_internal.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/connection.hpp"
#include "duckdb/main/prepared_statement_data.hpp"

using namespace duckdb;

namespace kwdbts {
typedef std::shared_ptr<Element> ElementPtr;
typedef std::vector<ElementPtr> Elements;
class APParseQuery : public IParser {
 public:

  explicit APParseQuery(std::string sql, Pos pos)
      : raw_sql_(std::move(sql)), pos_(std::move(pos)) {}

  KStatus ConstructAPExpr(ClientContext &context,
                          TableCatalogEntry &table,
                          std::size_t *i,
                          unique_ptr<duckdb::Expression> *head_node, std::map<idx_t, idx_t> &col_map);
  k_bool ParseNumber(k_int64 factor);
  k_bool ParseSingleExpr();
  Elements APParseImpl();

 private:
  std::string raw_sql_;
  kwdbts::IParser::Pos pos_;
  Elements node_list_;
};
}