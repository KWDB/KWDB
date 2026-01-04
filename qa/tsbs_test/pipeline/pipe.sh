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

start_time=$(date +%s)

cur_path="$(dirname -- "${BASH_SOURCE[0]}")"
echo "cur_path: ${cur_path}"
source ${cur_path}/env.sh
source ${cur_path}/run_containers.sh
echo "open_source: ${OPEN_SOURCE}"
echo "KWDB_DIR: ${WORKDIR}/${CLONE_DIR}"
echo "REPORT_DIR: ${REPORT_DIR}"

# 1. copy source code to node home
docker stop ${CONTAINER_NAME_SINGLE} || true
docker stop ${CONTAINER_NAME_1} || true
docker stop ${CONTAINER_NAME_2} || true
docker stop ${CONTAINER_NAME_3} || true
cp -r ${WORKDIR}/${CLONE_DIR} ${NODE_HOME_SINGLE}
cp -r ${WORKDIR}/${CLONE_DIR} ${NODE_HOME_1}
cp -r ${WORKDIR}/${CLONE_DIR} ${NODE_HOME_2}
cp -r ${WORKDIR}/${CLONE_DIR} ${NODE_HOME_3}

host_ip=$(hostname -I | awk '{print $1}')
echo "host_ip: ${host_ip}"
# 2. start containers
run_containers

# 3. test single node
${cur_path}/test_single.sh
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "FAILED: Single node TSBS test failed"
    end_time=$(date +%s)
    execution_time=$((end_time - start_time))
    echo "Pipeline execution time: ${execution_time} seconds"
    exit 1
fi
echo "SUCCESS: Single node TSBS test passed"

${cur_path}/test_multi.sh
exit_code=$?
if [ $exit_code -ne 0 ]; then
    echo "FAILED: Multi node TSBS test failed"
    end_time=$(date +%s)
    execution_time=$((end_time - start_time))
    echo "Pipeline execution time: ${execution_time} seconds"
    exit 1
fi
echo "SUCCESS: Multi node TSBS test passed"

end_time=$(date +%s)
execution_time=$((end_time - start_time))
echo "Pipeline execution time: ${execution_time} seconds"