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
BIN_DIR="$QA_DIR/../install/bin/"
KWBIN="$BIN_DIR/kwbase"
mkdir $BIN_DIR/iso_test
DEPLOY_DIR="$BIN_DIR/iso_test"
INSTALL_DIR=$BIN_DIR
QA_TEST_DIR=$QA_DIR/stress_tests/isolation_test

LISTEN_PORTS=(26257 26258 26259 26260 26261)
BRPC_PORTS=(27257 27258 27259 27260 27261)
HTTP_PORTS=(8081 8082 8083 8084 8085)
STORE_DIRS=("$DEPLOY_DIR/kwbase-data1" "$DEPLOY_DIR/kwbase-data2" "$DEPLOY_DIR/kwbase-data3" "$DEPLOY_DIR/kwbase-data4" "$DEPLOY_DIR/kwbase-data5")
JOIN_ADDR="127.0.0.1:26257"
MAX_OFFSET="0ms"

SLEEP_START=10

start_node() {
    local index=$1
    local listen_port=${LISTEN_PORTS[$index]}
    local brpc_port=${BRPC_PORTS[$index]}
    local http_port=${HTTP_PORTS[$index]}
    local store=${STORE_DIRS[$index]}
    echo "Starting KaiwuDB node ${store}"
    ${KWBIN} \
        start --insecure \
        --listen-addr=127.0.0.1:"${listen_port}" \
        --http-addr=127.0.0.1:"${http_port}" \
        --brpc-addr=127.0.0.1:"${brpc_port}" \
        --store=${store} \
        --max-offset="${MAX_OFFSET}" \
        --join="${JOIN_ADDR}" \
        --background
}

cleanup() {
  echo "Stopping KaiwuDB..."
  for port in {26257..26261}; do
    ${KWBIN} quit --insecure --host=127.0.0.1:$port --drain-wait -8s || true
  done

  echo "Waiting for KaiwuDB to stop..."

  echo "Removing data directories..."
  rm -rf $DEPLOY_DIR
}

start_single_node() {
    ${KWBIN} \
            start-single-node --insecure \
            --listen-addr=127.0.0.1:26257 \
            --brpc-addr=127.0.0.1:27257 \
            --http-addr=127.0.0.1:8080 \
            --store=$DEPLOY_DIR/kwbase-data \
            --background
}

cleanup_single_node() {
  echo "Stopping KaiwuDB..."
  ${KWBIN} quit --insecure --host=127.0.0.1:26257 --drain-wait -8s || true

  echo "Waiting for KaiwuDB to stop..."

  echo "Removing data directories..."
  rm -rf $DEPLOY_DIR
}

trap cleanup_single_node EXIT

cd "${INSTALL_DIR}"
echo "Starting KaiwuDB nodes..."
start_single_node &

echo "Waiting for KaiwuDB to start..."
sleep "${SLEEP_START}"

cd "${QA_TEST_DIR}"
echo "Running tests..."
make test

wait

trap cleanup EXIT

cd "${INSTALL_DIR}"
echo "Starting KaiwuDB nodes..."
for i in "${!LISTEN_PORTS[@]}"; do
    start_node "$i" &
done

echo "Initializing KaiwuDB cluster on host ${JOIN_ADDR}"
${KWBIN} init --insecure --host="${JOIN_ADDR}" &

echo "Waiting for KaiwuDB to start..."
sleep "${SLEEP_START}"

cd "${QA_TEST_DIR}"
echo "Running tests..."
make test

wait

cd "${QA_TEST_DIR}"
echo "Script completed."