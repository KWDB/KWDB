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

cur_dir="$(
  cd "$(dirname "$0")"
  pwd
)"
source $cur_dir/env.sh
source $QA_DIR/util/utils.sh

topology=${1:-"1n"}
scales=${2:-"100,4000,100000,1000000"}


echo "topology ${topology}"
echo "scales ${scales}"

$cur_dir/tsbs_test/setup.sh

$QA_DIR/util/setup_basic_v2.sh ${topology}

custer_node=1
if [ $topology = "1n" ]; then
  custer_node=1
elif [ $topology = "3c" ]; then
  custer_node=3
else
  echo "topology ${topology} not support"
  exit 1
fi

if [ $? = 1 ]; then
    echo_err "setup ${topology} failed"
    return 1
fi

$cur_dir/tsbs_test/excute_tsbs_test.sh ${custer_node} $scales

test_success=$?

$QA_DIR/util/stop_basic_v2.sh ${topology}
if [ $? = 1 ]; then
    echo_err "shutdown ${topology} failed"
    exit 1
fi
if [ $test_success = 1 ]; then
  echo "tsbs test failed"
  exit 1
fi
echo "tsbs test passed"
exit 0