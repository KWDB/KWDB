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

qadir="$(cd "$(dirname "$0")"; pwd)"
export QA_DIR=$qadir

color_purple='\033[0;35m '
color_red='\033[0;31m '
color_blue='\033[0;34m '
color_green='\033[0;32m '
color_disable=' \033[0m'

rootdir="$QA_DIR/.."
logdir="$QA_DIR/../log/regression"

export LOG_DIR=$logdir
export SUMMARY_FILE=$logdir/summary.log
export LOG_FILE=$logdir/qa.log
export KWDB_ROOT=${rootdir}/install
export DEPLOY_ROOT=${KWDB_ROOT}/deploy
export BIN_DIR=${KWDB_ROOT}/bin
export CONF_DIR=${KWDB_ROOT}/conf
export LIB_DIR=${KWDB_ROOT}/lib
#export KWBIN="$BIN_DIR/kwbase"
if [ "$(uname -o)" = "Msys" ] || [ "$(uname -o)" = "Cygwin" ]; then
    export KWBIN="$BIN_DIR/kwbase.exe"
else
    export KWBIN="$BIN_DIR/kwbase"
fi
export DRAIN_WAIT=${DRAIN_WAIT:-8s}
export INFO=$color_purple
export PRIMARY=$color_blue
export FAIL=$color_red
export SUCC=$color_green
export NC=$color_disable
export KWDB_RETENTIONS_INTERVAL=10

echo -e "${SUCC}QA_DIR:${QA_DIR}${NC}"
echo -e "${SUCC}BIN_PATH:${BIN_DIR}${NC}"
echo -e "${SUCC}LIB_DIR:${LIB_DIR}${NC}"
