#!/bin/bash -e
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

test_topologies=${@}

# read kernel.msgmax new value and kernel.msgmnb new value from /etc/sysctl.conf
sysctl -p

# usage: qa_timeout_killer timeout task_name &  (& can't be omitted)
# To avoid the ci-docker container still alive when timeout,
# use this trick to kill the process tree to force exit run_test.sh
qa_timeout_killer() {
    echo "************************************************************************"
    echo "* Begin run qa test with timeout ${1}"
    sleep ${1}
    echo "* Test timeout(${1} seconds), force quit!"
    ps aux | grep -ie "${2}" | grep -v grep | awk '{print $2}' | xargs -r pkill -P
}

#ci container entry
qadir="$(cd "$(dirname "$0")"; pwd)"
echo start regression test
mkdir -p $qadir/../log/regression
if [ "${SHUTDOWN_TIMEOUT_HANG}" != "yes" ]; then
  qa_timeout_killer ${QA_TIMEOUT} run_test_local_v3 &
else
  echo "Run the regression test with SHUTDOWN_TIMEOUT_HANG = yes."
fi
echo "Run the regression test with TEST_FAILED_HANG = ${TEST_FAILED_HANG}."
if [ "$REPEAT_REGRESSION_TEST" = "yes" ]; then
  $qadir/run_test_local_v3.sh $REGRESSION_TEST $SQL_FILE 2>&1 | tee $qadir/../log/regression/test_results.txt
else
  $qadir/run_test_local_v3.sh TEST_v3_integration_basic_v3.sh *.sql ${test_topologies} 2>&1 | tee $qadir/../log/regression/test_results.txt
fi
