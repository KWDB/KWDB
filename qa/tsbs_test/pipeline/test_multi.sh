
#! /bin/bash
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

source "$(dirname -- "${BASH_SOURCE[0]}")/env.sh"
KWDB_CT_NAME="tsbs_test_226"
BRANCH_NAME="${1:-"master"}"
TSBS_SCALE="${2:-"100,4000,100000,1000000"}"
TSBS_FORMAT="${3:-"kwdb"}"
QUERY_WORKERS="${4:-"8"}"
QUERY_TIMES="${5:-"10"}"
ENABLE_PERF="${6:-"false"}"
INSERT_TYPE="${7:-"prepare"}"
CLUSTER_NODE_NUM="${8:-"3"}"
WAL="${9:-"2"}"
REPLICA_MODE="${10:-"3"}"
LOAD_WORKERS="${11:-"12"}"
INSERT_DIRECT="${12:-"1"}"
PARALLEL_DEGREE="${13:-"8"}"
ENABLE_BUFFER_POOL="${14:-"true"}"
TS_AUTOMATIC_COLLECTION="${15:-"false"}"
PIPELINE="${16:-"true"}"
VECTORIZE="${17:-"auto"}"
BLOCK_CACHE_MEMORY_SIZE="${18:-"16106127360"}"
PERF_TYPE="${19:-""}"
UPDATE_THRESHOLD="${UPDATE_THRESHOLD:-"false"}"
KWBASE_LICENSE="${KWBASE_LICENSE:-""}"

echo "KWDB_CT_NAME ${KWDB_CT_NAME}"
echo "TSBS_SCALE ${TSBS_SCALE}"
echo "TSBS_FORMAT ${TSBS_FORMAT}"
echo "QUERY_TIMES ${QUERY_TIMES}"
echo "ENABLE_PERF ${ENABLE_PERF}"
echo "INSERT TYPE ${INSERT_TYPE}"
echo "CLUSTER NODE NUM ${CLUSTER_NODE_NUM}"
echo "WAL ${WAL}"
echo "REPLICA MODE ${REPLICA_MODE}"
echo "INSERT_DIRECT ${INSERT_DIRECT}"
echo "PARALLEL_DEGREE ${PARALLEL_DEGREE}"
echo "PERF_TYPE ${PERF_TYPE}"
echo "ENABLE_BUFFER_POOL ${ENABLE_BUFFER_POOL}"

IFS=',' read -ra TSBS_SCALE_LIST <<< "${TSBS_SCALE}"
IFS=',' read -ra INSERT_TYPE_LIST <<< "${INSERT_TYPE}"
IFS=',' read -ra CLUSTER_NODE_NUM_LIST <<< "${CLUSTER_NODE_NUM}"
IFS=',' read -ra WAL_LIST <<< "${WAL}"
IFS=',' read -ra REPLICA_MODE_LIST <<< "${REPLICA_MODE}"
IFS=',' read -ra INSERT_DIRECT_LIST <<< "${INSERT_DIRECT}"

echo "TSBS_SCALE_LIST[@]: ${TSBS_SCALE_LIST[@]}"
echo "CLUSTER_NODE_NUM_LIST[@]: ${CLUSTER_NODE_NUM_LIST[@]}"
echo "INSERT_DIRECT_LIST[@]: ${INSERT_DIRECT_LIST[@]}"
echo "INSERT_TYPE_LIST[@]: ${INSERT_TYPE_LIST[@]}"
echo "WAL_LIST[@]: ${WAL_LIST[@]}"
echo "REPLICA_MODE_LIST[@]: ${REPLICA_MODE_LIST[@]}"
echo "*******************"


CURR_HOST_IP=`hostname -I | awk '{print $1}'`
for scale in "${TSBS_SCALE_LIST[@]}"; do
    sh -c "docker exec -i ${CONTAINER_NAME_1} /home/inspur/src/gitee.com/kwbasedb/qa/tsbs_test/pipeline/start_in_container.sh ${CLUSTER_NODE_NUM} ${CURR_HOST_IP} 27257 8181 37357 ${ENABLE_BUFFER_POOL}"
    sh -c "docker exec -i ${CONTAINER_NAME_2} /home/inspur/src/gitee.com/kwbasedb/qa/tsbs_test/pipeline/start_in_container.sh ${CLUSTER_NODE_NUM} ${CURR_HOST_IP} 27258 8182 37358 ${ENABLE_BUFFER_POOL}"
    sh -c "docker exec -i ${CONTAINER_NAME_3} /home/inspur/src/gitee.com/kwbasedb/qa/tsbs_test/pipeline/start_in_container.sh ${CLUSTER_NODE_NUM} ${CURR_HOST_IP} 27259 8183 37359 ${ENABLE_BUFFER_POOL}"
    sh -c "docker exec -i ${CONTAINER_NAME_1} /home/inspur/src/gitee.com/kwbasedb/qa/tsbs_test/pipeline/test_in_container.sh ${scale} ${TSBS_FORMAT} ${KWDB_CT_NAME} ${BRANCH_NAME} ${QUERY_WORKERS} ${QUERY_TIMES} ${ENABLE_PERF} ${INSERT_TYPE} ${CLUSTER_NODE_NUM} ${CURR_HOST_IP} ${WAL} ${REPLICA_MODE} ${LOAD_WORKERS} ${INSERT_DIRECT} ${PARALLEL_DEGREE} ${ENABLE_BUFFER_POOL} ${TS_AUTOMATIC_COLLECTION} ${PIPELINE} ${VECTORIZE} ${BLOCK_CACHE_MEMORY_SIZE} /home/inspur/src/reports true 27257 8181 37357 ${UPDATE_THRESHOLD} ${KWBASE_LICENSE}"
    
    exit_code=$?
    if [ $exit_code -ne 0 ]; then
        echo "FAILED: Multi node Scale ${scale} TSBS test failed"
        exit 1
    fi
done