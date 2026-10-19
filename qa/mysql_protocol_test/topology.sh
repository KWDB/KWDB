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
#
# 按 qa 的拓扑（1n / 5c / 5cr）起带 MySQL 监听的节点，供本目录的测试脚本使用。
#
# 节点命名、目录、端口与启停方式都与 util/utils.sh 一致：store 是
# ${DEPLOY_ROOT}/<n1|c1..c5|cr1..cr5>，监听 127.0.102.145:26257 起递增，
# --background + --pid-file，跑完只停进程、不删目录，现场留给排查。
# 不直接复用 utils.sh 的启动函数，是因为它们固定 --insecure、也没有给
# --mysql-addr 留参数位置：协议与 JDBC 测试要测口令认证和 TLS，必须用安全节点。
#
# 用法（先 source env.sh）：
#   source "$QA_DIR/mysql_protocol_test/topology.sh"
#   mysql_topo_start 5c secure      # secure 或 insecure
#   ... "$(mysql_topo_mysql_port 1)" ...
#   mysql_topo_stop 5c

MYSQL_TOPO_HOST=${MYSQL_TOPO_HOST:-127.0.102.145}
MYSQL_TOPO_PG_PORT_START=${MYSQL_TOPO_PG_PORT_START:-26257}
MYSQL_TOPO_HTTP_PORT_START=${MYSQL_TOPO_HTTP_PORT_START:-8080}
MYSQL_TOPO_BRPC_PORT_START=${MYSQL_TOPO_BRPC_PORT_START:-28257}
MYSQL_TOPO_MYSQL_PORT_START=${MYSQL_TOPO_MYSQL_PORT_START:-3307}
MYSQL_TOPO_CERTS="${DEPLOY_ROOT}/mysql-certs"

# 当前拓扑：模式（n / c / cr）与节点数，由 mysql_topo_start 设置。
MYSQL_TOPO_MODE=""
MYSQL_TOPO_COUNT=0
MYSQL_TOPO_SECURE=""

# mysql_topo_parse <topology>：拆成模式与节点数，写入 MYSQL_TOPO_MODE/COUNT。
mysql_topo_parse() {
  if [[ ! "$1" =~ ^([0-9]+)(n|c|cr)$ ]]; then
    echo "mysql_topology: unrecognizable topology: $1 (want 1n, 5c or 5cr)" >&2
    return 1
  fi
  MYSQL_TOPO_COUNT=${BASH_REMATCH[1]}
  MYSQL_TOPO_MODE=${BASH_REMATCH[2]}
  if [ "$MYSQL_TOPO_MODE" = n ] && [ "$MYSQL_TOPO_COUNT" != 1 ]; then
    echo "mysql_topology: a single node topology is 1n, got $1" >&2
    return 1
  fi
}

mysql_topo_node()       { echo "${MYSQL_TOPO_MODE}$1"; }
mysql_topo_store()      { echo "${DEPLOY_ROOT}/$(mysql_topo_node "$1")"; }
mysql_topo_pg_addr()    { echo "${MYSQL_TOPO_HOST}:$((MYSQL_TOPO_PG_PORT_START + $1 - 1))"; }
mysql_topo_mysql_port() { echo "$((MYSQL_TOPO_MYSQL_PORT_START + $1 - 1))"; }
mysql_topo_log()        { echo "$(mysql_topo_store "$1")/logs/kwbase.log"; }

# mysql_topo_security_flags：给 kwbase 客户端命令用的安全参数。
mysql_topo_security_flags() {
  if [ "$MYSQL_TOPO_SECURE" = secure ]; then
    echo "--certs-dir=${MYSQL_TOPO_CERTS}"
  else
    echo "--insecure"
  fi
}

# mysql_topo_sql <idx> [kwbase sql 参数...]：经 pgwire 连第 idx 个节点执行 SQL。
mysql_topo_sql() {
  local idx=$1; shift
  "$KWBIN" sql $(mysql_topo_security_flags) --host="$(mysql_topo_pg_addr "$idx")" "$@"
}

mysql_topo_pid() {
  local f
  f="$(mysql_topo_store "$1")/kwbase.pid"
  [ -f "$f" ] && cat "$f"
}

# mysql_topo_node_alive <idx>：进程是否还在。
mysql_topo_node_alive() {
  local pid
  pid=$(mysql_topo_pid "$1")
  [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null
}

# mysql_topo_start_node <idx>：按当前拓扑启动（或重启）第 idx 个节点，store 保留。
mysql_topo_start_node() {
  local idx=$1 cmd
  case $MYSQL_TOPO_MODE in
    n)  cmd=start-single-node ;;
    c)  cmd=start ;;
    cr) cmd=start-single-replica ;;
  esac
  local args=(
    "$cmd" $(mysql_topo_security_flags)
    --listen-addr="$(mysql_topo_pg_addr "$idx")"
    --http-addr="${MYSQL_TOPO_HOST}:$((MYSQL_TOPO_HTTP_PORT_START + idx - 1))"
    --brpc-addr="${MYSQL_TOPO_HOST}:$((MYSQL_TOPO_BRPC_PORT_START + idx - 1))"
    --mysql-addr="${MYSQL_TOPO_HOST}:$(mysql_topo_mysql_port "$idx")"
    --store="$(mysql_topo_store "$idx")"
    --pid-file="$(mysql_topo_store "$idx")/kwbase.pid"
    --background
  )
  if [ "$MYSQL_TOPO_MODE" != n ]; then
    args+=(
      --locality=region=CN-100000-0$(printf "%02d" "$idx")
      --join="$(mysql_topo_pg_addr 1)"
      --external-io-dir="${DEPLOY_ROOT}/extern"
    )
  fi
  ulimit -c unlimited
  GOTRACEBACK=crash "$KWBIN" "${args[@]}" >> "${DEPLOY_ROOT}/$(mysql_topo_node "$idx").start.log" 2>&1
}

# mysql_topo_wait_live [timeout 秒]：等所有节点都 live。
mysql_topo_wait_live() {
  local deadline=$(( $(date +%s) + ${1:-120} )) live
  while [ "$(date +%s)" -lt "$deadline" ]; do
    live=$("$KWBIN" node status $(mysql_topo_security_flags) --format csv \
             --host="$(mysql_topo_pg_addr 1)" 2>/dev/null | grep -cP ",true,true$")
    [ "$live" = "$MYSQL_TOPO_COUNT" ] && return 0
    sleep 1
  done
  return 1
}

# mysql_topo_create_certs：安全模式的 CA、节点证书、root 客户端证书。
# 节点证书要包含 MYSQL_TOPO_HOST：客户端按这个地址连接并校验证书。
mysql_topo_create_certs() {
  rm -rf "$MYSQL_TOPO_CERTS"
  mkdir -p "$MYSQL_TOPO_CERTS"
  "$KWBIN" cert create-ca --certs-dir="$MYSQL_TOPO_CERTS" \
    --ca-key="$MYSQL_TOPO_CERTS/ca.key" >/dev/null &&
  "$KWBIN" cert create-node localhost 127.0.0.1 "$MYSQL_TOPO_HOST" \
    --certs-dir="$MYSQL_TOPO_CERTS" --ca-key="$MYSQL_TOPO_CERTS/ca.key" >/dev/null &&
  "$KWBIN" cert create-client root --certs-dir="$MYSQL_TOPO_CERTS" \
    --ca-key="$MYSQL_TOPO_CERTS/ca.key" >/dev/null
}

# mysql_topo_stop <topology>：按 pid 停掉该拓扑的全部节点（与 stop_node_v2 一样
# SIGKILL），目录保留。SIGKILL 是异步的，等进程真正退出再返回，否则紧接着
# 起下一个拓扑时端口可能还被占着。
mysql_topo_stop() {
  mysql_topo_parse "$1" || return 1
  local idx pid pids=()
  for idx in $(seq "$MYSQL_TOPO_COUNT" -1 1); do
    pid=$(mysql_topo_pid "$idx")
    if [ -n "$pid" ] && kill -9 "$pid" 2>/dev/null; then
      pids+=("$pid")
    fi
  done
  for _ in $(seq 1 30); do
    for idx in "${!pids[@]}"; do
      kill -0 "${pids[$idx]}" 2>/dev/null || unset 'pids[idx]'
    done
    [ ${#pids[@]} -eq 0 ] && return 0
    sleep 1
  done
  echo "mysql_topology: processes still alive after SIGKILL: ${pids[*]}" >&2
  return 1
}

# mysql_topo_start <topology> <secure|insecure>：清掉上次的现场，起全部节点，
# 集群模式再 init，等到全部 live。
mysql_topo_start() {
  mysql_topo_parse "$1" || return 1
  MYSQL_TOPO_SECURE=$2
  local idx
  mysql_topo_stop "$1"
  for idx in $(seq 1 "$MYSQL_TOPO_COUNT"); do
    rm -rf "$(mysql_topo_store "$idx")" "${DEPLOY_ROOT}/$(mysql_topo_node "$idx").start.log"
  done
  # 与 utils.sh 的 _cluster_start_ 一样清空集群共用的 external-io 目录：上次 EXPORT
  # 留下的文件会让同一路径的 EXPORT 报 "export path ... is not empty"。
  rm -rf "${DEPLOY_ROOT}/extern"
  mkdir -p "$DEPLOY_ROOT" "${DEPLOY_ROOT}/extern"
  if [ "$MYSQL_TOPO_SECURE" = secure ]; then
    mysql_topo_create_certs || return 1
  fi
  for idx in $(seq 1 "$MYSQL_TOPO_COUNT"); do
    mysql_topo_start_node "$idx" || return 1
  done
  if [ "$MYSQL_TOPO_MODE" != n ]; then
    "$KWBIN" init $(mysql_topo_security_flags) --host="$(mysql_topo_pg_addr 1)" \
      >> "${DEPLOY_ROOT}/$(mysql_topo_node 1).start.log" 2>&1 || return 1
  fi
  mysql_topo_wait_live 120
}
