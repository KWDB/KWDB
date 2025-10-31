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

set -e

CUR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QA_DIR="$(dirname "$(dirname "${CUR}")")"
BIN_DIR="$QA_DIR/../install/bin"
KWBIN="$BIN_DIR/kwbase"
mkdir $BIN_DIR/restful_test
DEPLOY_DIR="$BIN_DIR/restful_test"
QA_TEST_DIR=$QA_DIR/stress_tests/restful_test

SLEEP_START=10

setup_cert() {
  cd ${BIN_DIR}
  ./setup_cert_file.sh root 127.0.0.1
}

start_single_node() {
    ${KWBIN} \
            start-single-node --certs-dir=../certs \
            --listen-addr=127.0.0.1:26257 \
            --brpc-addr=127.0.0.1:27257 \
            --http-addr=127.0.0.1:8080 \
            --store=$DEPLOY_DIR/kwbase-data \
            --background
}

cleanup_single_node() {
  echo "Stopping KaiwuDB..."
  ${KWBIN} quit --certs-dir=../certs --host=127.0.0.1:26257 --drain-wait -8s || true

  echo "Waiting for KaiwuDB to stop..."

}
cd "${QA_TEST_DIR}"
javac -cp ".:postgresql-42.7.8.jar" DatabaseService.java



cd "${BIN_DIR}"
echo "Starting KaiwuDB nodes..."
start_single_node &

echo "Waiting for KaiwuDB to start..."
sleep "${SLEEP_START}"

${KWBIN} sql --certs-dir=../certs --host=127.0.0.1:26257 -e 'create user u1 with password 'abc';'
${KWBIN} sql --certs-dir=../certs --host=127.0.0.1:26257 -e 'grant admin to u1;'

cd "${QA_TEST_DIR}"
if [ -f "jdbc_test.out" ];then
  rm -rf "jdbc_test.out"
fi
# 执行SQL查询
java -cp ".:postgresql-42.7.8.jar" DatabaseService "create database test"

read -p "请输入要创建的数据库数量: " n
if ! [[ "$n" =~ ^[0-9]+$ ]] || [ "$n" -le 0 ]; then
    echo "错误: 请输入有效的正整数！"
    exit 1
fi

read -p "请输入每个库下要创建的数据库数量: " m
if ! [[ "$n" =~ ^[0-9]+$ ]] || [ "$n" -le 0 ]; then
    echo "错误: 请输入有效的正整数！"
    exit 1
fi

read -p "请输入每个表要插入的数据行数: " q
if ! [[ "$n" =~ ^[0-9]+$ ]] || [ "$n" -le 0 ]; then
    echo "错误: 请输入有效的正整数！"
    exit 1
fi


cd "${QA_TEST_DIR}"
echo "Running tests..."
start=$(date +%s%3N)

stmt=""
for ((i=1; i<=n; i++))
do
  db_name="db${i}"
  create_db_stmt="create database $db_name;"
  stmt="$stmt$create_db_stmt"
done
java -cp ".:postgresql-42.7.8.jar" DatabaseService "$stmt" >> jdbc_test.out

if ((m != 0)); then
for ((i=1; i<=n; i++))
do
  db_name="db${i}"
  stmt=""
  for((j=1; j<=m; j++))
  do
    tb_name="${db_name}.tb${j}"
    create_tb_stmt="create table $tb_name(a int, b float, c string);"
    stmt="$stmt$create_tb_stmt"
  done
  java -cp ".:postgresql-42.7.8.jar" DatabaseService "$stmt" >> jdbc_test.out
done
fi

if ((q != 0)); then
insert_stmt=""
for ((i=1; i<=n; i++))
do
  db_name="db${i}"
  for((j=1; j<=m; j++))
  do
    tb_name="${db_name}.tb${j}"
    stmt=""
    for ((k=1; k<=q; k++))
    do
      insert_stmt="insert into $tb_name values($k,0.$k,'test$k');"
      stmt="$stmt$insert_stmt"
    done
    java -cp ".:postgresql-42.7.8.jar" DatabaseService "$stmt" >> jdbc_test.out
    echo -e "\n"
  done
done
fi
end=$(date +%s%3N)
elapsed_ms=$((end - start))
echo "总执行时间: ${elapsed_ms} 毫秒" >> jdbc_test.out

cd "${BIN_DIR}"
cleanup_single_node
sleep "${SLEEP_START}"

echo "Removing data directories..."
rm -rf "${DEPLOY_DIR}"
wait

cd "${QA_TEST_DIR}"
echo "Script completed."