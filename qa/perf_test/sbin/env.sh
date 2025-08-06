#!/bin/bash
# Copyright 2023 Shanghai Yunxi Technology Co., Ltd.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# File Name: env.sh
# Description: The environment configuration script file before running the test
#
# Usage:
#   Configure environment variables for other scripts: source env.sh&lt; <kwbase full path>;"
#
sbindir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export TEST_IMAGE_NAME=${TEST_IMAGE_NAME:-devops.inspur.com:80/isg/zdp/repo/local_repo/zdp/builder/kaiwudb_amd64_ub_2204}
export TEST_IMAGE_TAG=${TEST_IMAGE_TAG:-latest}
export CODE_PLATFORM=${CODE_PLATFORM:-gitee.com}
export HTTP_PORT=${HTTP_PORT:-8080}
export MAX_SQL_MEMORY=${MAX_SQL_MEMORY:-25%}
export SBIN_DIR=$sbindir

export ROOT_DIR="$sbindir/.."

export GOPATH_DIR=$(realpath "$ROOT_DIR/../../../")
export KWDB_ROOT=${GOPATH_DIR}/src/${CODE_PLATFORM}/kwbasedb

export WORKSPACE_DIR=$(dirname "$GOPATH_DIR")

export UUID_PATH=$(basename "$GOPATH_DIR")

if [ -z $WORKSPACE_DIR ]; then
  echo input workspace dir
fi

if [ -d "$KWDB_ROOT" ]; then
  echo ${KWDB_ROOT} exists
else
  echo ${KWDB_ROOT} does not exists
  exit 1
fi

color_purple='\033[0;35m '
color_red='\033[0;31m '
color_blue='\033[0;34m '
color_green='\033[0;32m '
color_disable=' \033[0m'

logdir="${WORKSPACE_DIR}/log/"

export KWQUERY_DIR="$ROOT_DIR/kaiwudb"
export TSQUERY_DIR="$ROOT_DIR/timescaledb"
export FLASK_DIR=${WORKSPACE_DIR}/flask
export FLASK_OUTPUT=${FLASK_DIR}/flask_output
CURRENT_TIME=$(date +"%Y-%m-%d-%H-%M-%S_PerfOutput")
export CUR_OUTPUT_DIR=${FLASK_OUTPUT}/${CURRENT_TIME}
export LOG_DIR=$logdir
export SUMMARY_FILE=$logdir/summary.log
export LOG_FILE=$logdir/sbin.log
export KWDB_ROOT=${KWDB_ROOT}/install
export DEPLOY_ROOT=${KWDB_ROOT}/deploy
export BIN_DIR=${KWDB_ROOT}/bin
export CONF_DIR=${KWDB_ROOT}/conf
export LIB_DIR=${KWDB_ROOT}/lib
export LD_LIBRARY_PATH=$LIB_DIR
export KWBASE_BIN=$BIN_DIR/kwbase
export KWBIN="env LD_LIBRARY_PATH=$(realpath ${LIB_DIR}):$LD_LIBRARY_PATH $KWBASE_BIN"
export DRAIN_WAIT=${DRAIN_WAIT:-8s}
export INFO=$color_purple
export PRIMARY=$color_blue
export FAIL=$color_red
export SUCC=$color_green
export NC=$color_disable
export KWDB_RETENTIONS_INTERVAL=10

# print result
echo -e "${SUCC}SBIN_DIR:${SBIN_DIR}${NC}"
echo -e "${SUCC}BIN_PATH:${BIN_DIR}${NC}"
echo -e "${SUCC}LIB_DIR:${LIB_DIR}${NC}"
echo -e "${SUCC}FLASK_DIR:${FLASK_DIR}${NC}"
echo -e "${SUCC}CUR_OUTPUT_DIR:${CUR_OUTPUT_DIR}${NC}"
echo -e "${SUCC}WORKSPACE_DIR:${WORKSPACE_DIR}${NC}"
echo -e "${SUCC}UUID_PATH:${UUID_PATH}${NC}"

if [ ! -f "$KWBASE_BIN" ]; then
  echo "Kwbase BIN file does not exist"
  exit 1
fi