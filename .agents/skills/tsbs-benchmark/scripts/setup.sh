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

export PROJ_BASE_DIR
PROJ_BASE_DIR=$(git rev-parse --show-toplevel)

source $PROJ_BASE_DIR/.agents/skills/tsbs-benchmark/scripts/utils.sh

if ! "$QA_DIR"/tsbs_test/setup.sh; then
    die "$QA_DIR/tsbs_test/setup.sh not exist"
fi