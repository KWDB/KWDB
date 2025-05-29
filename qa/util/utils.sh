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

function test_failed_hang()
{
  if [[ ${TEST_FAILED_HANG} == "yes" ]]; then
    echo -e "Hanging because TEST_FAILED_HANG is yes ..."
    while true
    do
      sleep 60
    done
  else
    echo -e "Continue to run without hanging, please set TEST_FAILED_HANG to yes if you want to let it hang."
  fi
}

function force_clean_up() {  
  if ! check_errlog ; then
    echo -e "${FAIL} process exception, please check errlog.log ${NC}" | tee -a $SUMMARY_FILE
  fi
  if [[ $FAILED_HANG == "yes" ]]; then
    echo -e "UT Failed! You can open a new terminal and use ksql to debug! [CTRL+C] or [./qa/force_clear_all_nodes.sh] to clear all processes!"
    while true
    do
      sleep 1
    done
  fi
  ps aux | grep -ie "kwbase sql --insecure" | grep -v grep | awk '{print $2}' | xargs -r kill -9 
  ps aux | grep -ie "kwbase start --insecure" | grep -v grep | awk '{print $2}' | xargs -r kill -9
  ps aux | grep -ie "kwbase start" | grep -v grep | awk '{print $2}' | xargs -r kill -9
  ps aux | grep -ie kwdbts_master_agent | grep -v grep | awk '{print $2}' | xargs -r kill -9 
  ps aux | grep -ie AE_TS | grep -v grep | awk '{print $2}' | xargs -r kill -9
}

function check_errlog() {
  has_errlog=false
  cd $KWDB_ROOT
  for log in `find . -name errlog.log`
  do
    has_errlog=true
    echo -e "${FAIL} found errlog $log ${NC}" | tee -a $LOG_FILE
    cat $log | tee -a $LOG_FILE
    if grep -q "/kwbase()" $log; then
      echo -e "detected a crash in kwbase(GO)" | tee -a $LOG_FILE
      kwbase_log=$(dirname $log)/node1/logs/kwbase.log
      panic_start_loc=$(grep -n -m 1 "panic" "$kwbase_log" | awk -F: '{print $1}')
      tail -n +$panic_start_loc $kwbase_log | tee -a $LOG_FILE
    fi
  done
  if [ "$has_errlog" = "true" ]; then
    return 1
  fi
  return 0
}

function print_locallog() {
  echo -e "${INFO} print local log begin....${NC}"
  has_errlog=false
  cd $KWDB_ROOT
  for log in `find . -name 'kwdb*.log'`
  do
    echo -e "${INFO} print $log ${NC}"
    cat $log
  done  
  for log in `find . -name 'KMalloc.log'`
  do
    echo -e "${INFO} print $log ${NC}"
    cat $log | grep "shm" | tee -a $LOG_FILE
  done
  echo -e "${INFO} print local log end....${NC}"
  return 0
}

echo_info() {
  echo -e "${INFO}$(date)[---------- ${1} ----------]${NC}" | \
    tee -a ${2:-$LOG_FILE}
}

echo_succ() {
  echo -e "${SUCC}$(date)---------- ${1}----------${NC}" | \
    tee -a ${2:-$LOG_FILE}
}

echo_err() {
  echo -e "${FAIL}$(date)[----------${1}----------]${NC}" | \
    tee -a ${2:-$LOG_FILE}
}

get_listen_addr() {
  echo "$(head -n1 $(realpath ${DEPLOY_ROOT}/${1}/kwbase.listen-addr))"
}

function start_single_node() {
  local store=${1}
  local listenport=${2:-26257}
  local http_port=${3:-8080}
  local host_ip=${4:-127.0.102.145}
  rm -fr ${DEPLOY_ROOT}/${store}
  ${KWBIN} start-single-node --insecure --listen-addr=${host_ip}:${listenport} \
    --http-addr=${host_ip}:${http_port} \
    --store=${DEPLOY_ROOT}/${store} \
    --pid-file=${DEPLOY_ROOT}/${store}/kwbase.pid \
    --background
  for c in {1..30};do
    if [ "1" = "$(check_kwbase_available ${store})" ];then
      break
    fi
    sleep 1
  done
  ${KWBIN} sql --insecure --host=${host_ip}:${listenport} -e \
  "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 60;"
}

function check_kwbase_available() {
  local store=${1}
  if [ ! -d "${DEPLOY_ROOT}/${store}" ];then
    echo_info "${store} not found"
    echo 0 && return 1
  fi
  listenaddr=$(get_listen_addr ${store})
  echo $(${KWBIN} node status --insecure --format csv --host=${listenaddr} | \
      grep "${listenaddr}" | tail -n1 | grep -cP ",true,true$" )
}

function check_kwbase_alive() {
  local store=${1}
  if [ ! -d "${DEPLOY_ROOT}/${store}" ];then
    echo_info "${store} not found"
    echo 0 && return 1
  fi
  listenaddr=$(get_listen_addr ${store})
  echo $(${KWBIN} node status --insecure --format csv --host=${listenaddr} | \
      grep "${listenaddr}" | grep -cP ",(true|false),true$" )
}

function nice_stop_node_v2() {
  local store=${1}
  echo_info "stopping single node: ${store}"
  if [ ! -d "${DEPLOY_ROOT}/${store}" ];then
    echo_err "node ${store} didn't found"
    exit 1
  fi
  ${KWBIN} quit --insecure --host=$(get_listen_addr ${store}) \
    --drain-wait ${DRAIN_WAIT}
}

function stop_node_v2() {
  local store=${1}
  echo_info "stopping single node: ${store}"
  if [ ! -d "${DEPLOY_ROOT}/${store}" ];then
    echo_err "node ${store} didn't found"
    exit 1
  fi
  kill -s SIGKILL $(cat ${DEPLOY_ROOT}/${store}/kwbase.pid)
}

function kwbase_exec_on() {
  local node=${1}
  local test_sql=${2}
  local output_file=${3}
  ${KWBIN} sql --insecure --host=$(get_listen_addr ${node}) --format=table \
    --set "errexit=false" --echo-sql < ${2} 2>&1 | tee ${3} > /dev/null
}

function _cluster_start_() {
  local mode=${1}
  local node_count=${2}
  local name_prefix=${3}
  local tcp_port_start=${4:-26257}
  local http_port_start=${5:-8080}
  local host_ip=${6:-127.0.102.145}

  local address=""

  rm -fr ${DEPLOY_ROOT}/extern
  mkdir -p ${DEPLOY_ROOT}/extern
  for idx in $(seq 1 ${node_count});do
    rm -fr ${DEPLOY_ROOT}/${name_prefix}${idx}
    echo_info "starting cluster node: ${name_prefix}${idx}"
    local laddr=${host_ip}:$(($tcp_port_start+$idx-1))
    local haddr=${host_ip}:$(($http_port_start+$idx-1))
    ${KWBIN} \
    ${mode} --insecure \
    --listen-addr=${laddr} \
    --http-addr=${haddr} \
    --store=${DEPLOY_ROOT}/${name_prefix}${idx} \
    --locality=region=CN-100000-0$(printf "%02d" ${idx}) \
    --join=${host_ip}:${tcp_port_start} \
    --pid-file=${DEPLOY_ROOT}/${name_prefix}${idx}/kwbase.pid \
    --external-io-dir=${DEPLOY_ROOT}/extern \
    --background
    address+="|${laddr}"
  done

  echo_info "cluster init: ${node_count}${name_prefix}"
  ${KWBIN} init --insecure --host=${host_ip}:${tcp_port_start} 2>&1 | \
    tee -a $LOG_FILE > /dev/null
  
  while [ ${node_count} -ne \
    $(${KWBIN} node \
      status --insecure --format csv --host=${address##*|} | \
      grep -P "${address#*|}" | grep -cP ",true,true$") ];do
    echo_info "cluster node uninitialized, wating..." && sleep 1
  done
  ${KWBIN} sql --insecure --host=${host_ip}:${tcp_port_start}  -e \
  "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 60;"
}

function start_cluster() {
  _cluster_start_ start $@
}

function start_cluster_replica() {
  _cluster_start_ start-single-replica $@
}

function clean_single_node() {
  local topology=${1}
  if [ -d "${DEPLOY_ROOT}/${topology}" ];then
    local listen_addr="$(get_listen_addr ${topology})"
    if [ -n "${listen_addr}" ] && [ -n "$(pgrep -f ${listen_addr})" ];then
      ${KWBIN} quit --insecure --host=$(head -n1 "${listen_addr}") \
        --drain-wait ${DRAIN_WAIT} 2>&1 | tee -a $LOG_FILE > /dev/null
    fi
    rm -fr "${DEPLOY_ROOT}/${topology}"
  fi
}
function clean_cluster() {
  local topology=${1}
  if [[ "${topology}" =~ ([0-9]+)([a-za-Z]+) ]];then
    local store="${BASH_REMATCH[2]}${BASH_REMATCH[1]}"
    if [ -d "${DEPLOY_ROOT}/${store}" ];then
      if [ -f "${DEPLOY_ROOT}/${store}/kwbase.listen-addr" ];then
        ${KWBIN} quit --insecure --host=$(get_listen_addr ${store}) \
          --drain-wait ${DRAIN_WAIT} 2>&1 | tee -a $LOG_FILE > /dev/null
      fi
      rm -fr "${DEPLOY_ROOT}/${store}"
    fi
  fi
}

function cluster_exec_v2() {
  local sql_file=${1}
  local output_file=${2}

  local node_annotation_regex='^\s*--\s*node:(.+)'

  declare -a stmts
  local last_stmt=""
  while read -r line || [ -n "${line}" ];do
    if [ -n "${last_stmt}" ];then
      if [[ "${line}" =~ ${node_annotation_regex} ]];then
        stmts+=("${last_stmt}")
        last_stmt=""
      fi
    fi
    last_stmt+="${line}\n"
  done < ${sql_file}

  [ -n "${last_stmt}" ] && stmts+=("${last_stmt}")

  local exec_node_sed_regex='^\s*--[-[[:space:]]]*node:\s*(.+)'

  for idx in ${!stmts[@]};do
    if [ -n "${stmts[${idx}]}" ];then
      nodes=$(echo -e "${stmts[${idx}]}" | sed -n -re 's//\1/p')
      for node in ${nodes};do
        exesql="$(echo -e "${stmts[${idx}]}" | grep -v -P '^\s*--.*')"
        ${KWBIN} sql --host=$(get_listen_addr ${node}) --insecure \
          --format=table --set "errexit=false" --echo-sql -e "${exesql}" \
          2>&1 | tee -a ${output_file} > /dev/null
      done
    fi
  done
}