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

SCRIPT_NAME=$(basename ${0})

source $QA_DIR/Integration/execute_regression_sql_v2.sh
topologies=${@:-5c 5cr}
export KW_ENGINE_VERSION=1
execute_regression_sql_cluster_v2 ${topologies}

if [ $? = 1 ]; then
  echo "${SCRIPT_NAME}_failed"
  force_clean_up
  exit 1
fi
echo "${SCRIPT_NAME}_passed"
