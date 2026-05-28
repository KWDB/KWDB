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

set -euo pipefail

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
query_types_param=${28:-""}
compare_threshold=${29:-"false"}

workspace=/home/inspur/src/gitee.com
bin_dir="${workspace}/kwbasedb/install/bin"
kwbin="${bin_dir}/kwbase"
execute_script="${workspace}/kwbasedb/qa/tsbs_test/execute_tsbs_test.sh"

cd "${bin_dir}"

if [[ ${cluster_node_num} == 3 ]]; then
  LD_LIBRARY_PATH=../lib ./kwbase init --insecure --host="${ip}:${listenport}"
fi

if [[ -n ${license} ]]; then
  LD_LIBRARY_PATH=../lib ./kwbase sql --insecure --host="${ip}:${listenport}" --execute="set cluster setting cluster.license ='${license}';"
fi

QUERY_TYPES_PARAM="${query_types_param}" \
COMPARE_THRESHOLD="${compare_threshold}" \
UPDATE_THRESHOLD="${update_threshold}" \
"${execute_script}" \
  --node-num "${cluster_node_num}" \
  --scales "${scale}" \
  --result-dir "${tsbs_result_dir}" \
  --cluster-name "${KWDB_CT_NAME}" \
  --host "${ip}" \
  --port "${listenport}" \
  --data-dir "${workspace}" \
  --query-workers "${query_workers}" \
  --query-types "${query_types_param}" \
  --insert-type "${insert_type}" \
  --insert-direct "${insert_direct}" \
  --wal "${wal}" \
  --replica-mode "${replica_mode}" \
  --parallel-degree "${parallel_degree}" \
  --format "${format}" \
  --query-times "${query_times}" \
  --load-workers "${load_workers}" \
  --branch-name "${BRANCH_NAME}"
