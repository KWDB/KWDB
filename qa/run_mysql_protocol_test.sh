#!/bin/bash
# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
#
# This software (KWDB) is licensed under Mulan PSL v2.
# You can use this software according to the terms and conditions of the Mulan PSL v2.
# You may obtain a copy of Mulan PSL v2 at:
#          http://license.coscl.org.cn/MulanPSL2
# THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
# EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
# MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
# See the Mulan PSL v2 for more details.
#
# MySQL 线协议测试入口，脚本本体在 qa/mysql_protocol_test/。
#
# 用法: qa/run_mysql_protocol_test.sh [protocol|jdbc] [参数...]
#
#   protocol  mysql 客户端逐项断言协议行为（默认）
#   jdbc      MySQL Connector/J 冒烟测试
#
# 其余参数原样传给对应脚本，拓扑取 1n / 5c / 5cr，默认 1n。例如：
#   qa/run_mysql_protocol_test.sh 1n 5c 5cr
#   qa/run_mysql_protocol_test.sh jdbc 5c

source "$(cd "$(dirname "$0")"; pwd)/env.sh" >/dev/null 2>&1
TEST_DIR="$QA_DIR/mysql_protocol_test"

case "$1" in
  protocol) shift; exec "$TEST_DIR/protocol_test.sh" "$@" ;;
  jdbc)     shift; exec "$TEST_DIR/jdbc_test.sh" "$@" ;;
  -h|--help) sed -n '/^# 用法/,/^# *qa\/run_mysql_protocol_test.sh jdbc/p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
  *)        exec "$TEST_DIR/protocol_test.sh" "$@" ;;
esac
