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

#! /bin/bash
set -e

export cur_path=`pwd`
echo $cur_path

curdate="`date +%Y-%m-%d,%H:%M:%S`"


# check if binary exists
export TSBS_PATH=$QA_DIR/tsbs_test/bin
arch=$(uname -m)

# check if required binaries exist and are executable
binary_available=true
required_binaries=(
  "tsbs_generate_data"
  "tsbs_generate_queries"
  "tsbs_load_kwdb"
  "tsbs_run_queries_kwdb"
)

for binary in "${required_binaries[@]}"; do
  binary_path="$TSBS_PATH/${binary}_${arch}"
  if [ ! -f "$binary_path" ]; then
    echo "binary $binary_path not exist"
    binary_available=false
    break
  fi
  
  if [ ! -x "$binary_path" ]; then
    echo "binary $binary_path not executable"
    binary_available=false
    break
  fi
done

# 如果二进制文件不可用，则重新编译
if [ "$binary_available" = false ]; then
  echo "binary missing or not executable, recompile..."
  tsbs_branch=${1:-master}
  echo "tsbs branch: ${tsbs_branch}"
  # 判读是否存在GOPATH
  if [ -z "$GOPATH" ];then
    echo "GOPATH is not set"
    exit 1
  fi

  # check if tsbs repo exists, if not, clone it
  if [ ! -d "$GOPATH/src/github.com/kwdb-tsbs" ]; then
    mkdir -p $GOPATH/src/github.com
    cd $GOPATH/src/github.com

    git clone https://gitee.com/kwdb/kwdb-tsbs.git -b ${tsbs_branch}
  else
    cd $GOPATH/src/github.com/kwdb-tsbs
    git fetch origin ${tsbs_branch}
    git checkout ${tsbs_branch}
    git pull origin ${tsbs_branch}
  fi
  cd $GOPATH/src/github.com/kwdb-tsbs
  make
  
  # copy binaries to $TSBS_PATH
  mkdir -p $TSBS_PATH
  
  # append arch to binary filename
  echo "copy binaries to $TSBS_PATH..."
  for filename in "${required_binaries[@]}"; do
    binary="$GOPATH/src/github.com/kwdb-tsbs/bin/${filename}"
    # 复制并在文件名后添加架构信息
    cp "$binary" "$TSBS_PATH/${filename}_${arch}"
    echo "copy ${filename} to ${filename}_${arch}"
  done
else
  echo "all required binaries exist and are executable"
fi

echo "TSBS_PATH: ${TSBS_PATH}"
exit $?
