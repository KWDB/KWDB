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

#include <libkwdbap.h>

#include <regex>
#include <limits>
#include <thread>

#include "duckdb.hpp"

TSStatus APOpen(APEngine** engine, TSSlice dir, APOptions options) {
  *engine = new APEngine();
  (*engine)->db = new duckdb::DuckDB({dir.data, dir.len});
  return TSStatus{nullptr, 0};
}

TSStatus APClose(APEngine* engine) {
  if(engine){
    if(engine->db){
      delete (duckdb::DuckDB *)(engine->db);
    }
    delete engine;
  }
  return TSStatus{nullptr, 0};
}
Gitee - 基于 Git 的代码托管和研发协作平台