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

cd "${BIN_DIR}"
echo "Starting KaiwuDB nodes..."
start_single_node &

echo "Waiting for KaiwuDB to start..."
sleep "${SLEEP_START}"

${KWBIN} sql --certs-dir=../certs --host=127.0.0.1:26257 -e 'create user u1 with password 'abc';'
${KWBIN} sql --certs-dir=../certs --host=127.0.0.1:26257 -e 'grant admin to u1;'


process_sql_file() {
    local sql_file="$1"
    local sql_type="$2"

    # 临时移除注释和空行
    temp_file=$(mktemp)
    grep -v '^--' "$sql_file" | grep -v '^$' | tr -d '\r' > "$temp_file"

    # 按分号分割
    while IFS= read -r -d ';' sql; do
        sql=$(echo "$sql" | sed 's/^[[:space:]]*//;s/[[:space:]]*$//')

        if [ -n "$sql" ] && [ "${sql:0:2}" != "--" ]; then
            echo "=== 执行SQL ==="
            echo "$sql;"
            echo "================"
            curl -L --cacert "${BIN_DIR}/../certs/ca.crt" -u u1:abc \
                -d "${sql}" "https://127.0.0.1:8080/restapi/$sql_type" >> restful_sql_test.out
            echo -e "\n"
        fi
    done < <(cat "$temp_file" && echo ";")

    rm -f "$temp_file"
}

cd "${QA_TEST_DIR}"
if [ -f "restful_sql_test.out" ];then
  rm -rf "restful_sql_test.out"
fi
echo "Running tests..."
start=$(date +%s%3N)

process_sql_file "restful_test_ddl.sql" ddl
process_sql_file "restful_test_insert.sql" insert
process_sql_file "restful_test_query.sql" query

end=$(date +%s%3N)
elapsed_ms=$((end - start))
echo "总执行时间: ${elapsed_ms} 毫秒" >> restful_sql_test.out

cd "${BIN_DIR}"
cleanup_single_node
sleep "${SLEEP_START}"

echo "Removing data directories..."
rm -rf "${DEPLOY_DIR}"
wait

cd "${QA_TEST_DIR}"
echo "Script completed."