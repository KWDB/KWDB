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

export QA_DIR=$(dirname $(dirname "$0"))
export TSBS_PATH=$QA_DIR/my_tsbs_test/bin

echo "TSBS_PATH: $TSBS_PATH"

# 检查二进制文件是否存在且可执行
for binary in "tsbs_generate_data" "tsbs_load_kwdb" "tsbs_run_queries_kwdb"; do
  binary_path="$TSBS_PATH/${binary}"
  if [ -f "$binary_path" ]; then
    echo "file exist: $binary_path"
    if [ -x "$binary_path" ]; then
      echo "file executable: $binary_path"
    else
      echo "file not executable: $binary_path"
    fi
  else
    echo "file not exist: $binary_path"
  fi
done