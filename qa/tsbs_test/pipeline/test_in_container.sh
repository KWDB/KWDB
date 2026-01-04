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

scale=$1
format=$2
KWDB_CT_NAME=$3
BRANCH_NAME=$4
query_workers=$5
query_times=$6
enable_perf=$7
insert_type=$8
cluster_node_num=${9}
ip=${10}
wal=${11}
replica_mode=${12}
load_workers=${13}
insert_direct=${14}
parallel_degree=${15}
enable_buffer_pool=${16}
ts_automatic_collection=${17}
pipeline=${18}
vectorize=${19}
block_cache_memory_size=${20}
tsbs_result_dir=${21}

is_test_container=${22:-"false"}


listenport=${23:-"27257"}
httpport=${24:-"8181"}
brpcport=${25:-"37357"}
update_threshold=${26:-"false"}
license=${27:-""}
workspace=/home/inspur/src/gitee.com
data_dir=${workspace}/kwbasedb/tsbs_test

cd /home/inspur/src/gitee.com/kwbasedb/install/bin

if [[ ${is_test_container} == "true" ]]; then
  if [[ ${replica_mode} == 3 ]]; then
    LD_LIBRARY_PATH=../lib ./kwbase init --insecure --host=${ip}:$listenport
  fi
  
  if [[ -n ${license} ]]; then
    LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host=${ip}:$listenport --execute="set cluster setting cluster.license ='${license}';"
  fi

  UPDATE_THRESHOLD=${update_threshold} /home/inspur/src/gitee.com/kwbasedb/qa/tsbs_test/excute_tsbs_test.sh ${cluster_node_num} ${scale} ${tsbs_result_dir} ${KWDB_CT_NAME} ${ip} ${listenport} /home/inspur/src/gitee.com
  exit_code=$?
  if [ $exit_code -ne 0 ]; then
    exit 1
  fi
fi
exit 0