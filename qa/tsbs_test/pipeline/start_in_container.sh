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

function wait_kwbase_exit() {
  IP=${1}
  LISTEN_PORT=${2}
  HTTP_PORT=${3}
  BRPC_PORT=${4}

  # waiting for previous kwbase complete exit
  port_is_open() {
    local ip=$1
    local port=$2
    (exec 3<>/dev/tcp/${ip}/${port}) 2>/dev/null
    local result=$?
    exec 3>&- 3<&-
    return ${result}
  }

  check_ports() {
    local ip=$1
    shift
    for port in "$@"; do
      if port_is_open ${ip} ${port}; then
        return 0  # at least one port is open
      fi
    done
    return 1  # all ports are closed
  }

  echo "Checking if ports ${LISTEN_PORT}, ${HTTP_PORT}, ${BRPC_PORT} are occupied on ${IP}..."
  
  check_ports ${IP} ${LISTEN_PORT} ${HTTP_PORT} ${BRPC_PORT}
  result=$?
  
  wait_times=0
  while [[ ${result} -eq 0 ]]; do
    wait_times=$((wait_times+1))
    if [[ ${wait_times} -gt 10 ]]; then
      echo "trying to kill kwbase again..."
      pkill -9 kwbase
      wait_times=0
    fi
    echo "waiting for kwbase to exit..."
    sleep 0.1
    check_ports ${IP} ${LISTEN_PORT} ${HTTP_PORT} ${BRPC_PORT}
    result=$?
  done
  
  echo "All ports are now free."
}

replica_mode=${1}
ip=${2}

listenport=${3:-"27257"}
httpport=${4:-"8181"}
brpcport=${5:-"37357"}
enable_buffer_pool=${6:-"true"}
workspace=/home/inspur/src/gitee.com
data_dir=${workspace}/kwbasedb/tsbs_test

wait_kwbase_exit ${ip} ${listenport} ${httpport} ${brpcport}
store=${data_dir}/kwbase-data1
rm -rf ${store}

cd /home/inspur/src/gitee.com/kwbasedb/install/bin
echo "#### Node Start ####"
if [[ ${replica_mode} == 3 ]]; then
    if [ ${enable_buffer_pool} == "true" ]; then
        LD_LIBRARY_PATH=../lib ./kwbase start --insecure --listen-addr=${ip}:$listenport --http-addr=${ip}:$httpport --brpc-addr=${ip}:$brpcport --store=$store --join=${ip}:27257 --log-file-verbosity=ERROR --buffer-pool-size=65314 --background
    else
        LD_LIBRARY_PATH=../lib ./kwbase start --insecure --listen-addr=${ip}:$listenport --http-addr=${ip}:$httpport --brpc-addr=${ip}:$brpcport --store=$store --join=${ip}:27257 --log-file-verbosity=ERROR --background
    fi
else
    if [ ${enable_buffer_pool} == "true" ]; then
        LD_LIBRARY_PATH=../lib ./kwbase start-single-node --insecure --listen-addr=${ip}:$listenport --http-addr=${ip}:$httpport --brpc-addr=${ip}:$brpcport --store=$store --log-file-verbosity=ERROR --buffer-pool-size=65314 --background 
    else
        LD_LIBRARY_PATH=../lib ./kwbase start-single-node --insecure --listen-addr=${ip}:$listenport --http-addr=${ip}:$httpport  --brpc-addr=${ip}:$brpcport --store=$store --log-file-verbosity=ERROR --background
    fi
fi
