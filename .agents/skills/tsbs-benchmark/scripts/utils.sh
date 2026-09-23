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

function log() {
    echo "[tsbs] $*" >&2
}

function die() {
    echo "[tsbs] ERROR: $*" >&2
    exit 1
}

export host_ip=127.0.102.145
export listenport=26257
export GOPATH
GOPATH=$(go env GOPATH)

export PROJ_BASE_DIR
PROJ_BASE_DIR=$(git rev-parse --show-toplevel)

export QA_DIR
QA_DIR=$PROJ_BASE_DIR/qa

source ${QA_DIR}/tsbs_test/tsbs_env.sh
TSBS_PATH="${QA_DIR}/tsbs_test/bin"


QUERY_TIMES=30
FORMAT=kwdb
TSBS_CASE=cpu-only
LOAD_TS_START="2020-01-01T00:00:00Z"
LOAD_WORKERS=12

DEPLOY_ROOT=${PROJ_BASE_DIR}/tsbs-benchmark-data
mkdir -p $DEPLOY_ROOT

KWBIN="${PROJ_BASE_DIR}/install/bin/kwbase"
if [[ ! -x ${KWBIN} ]]; then
    die "${KWBIN} does not exist or is not executable"
fi

# Verify it's a Release build
if ! go version -m "${KWBIN}" 2>/dev/null | grep -q 'build.typ=Release'; then
    die "${KWBIN} is not a Release build. Please build a release version (make release)."
fi

function set_defaults() {
    ME_HOST_IP="${host_ip:-127.0.102.145}"
    ME_HOST_PORT="${listenport:-26257}"
    DATA_DIR="${PROJ_BASE_DIR}/tsbs-benchmark-data"
    ARCH="$(uname -m)"
    LOAD_DATA_DIR="${DATA_DIR}/load_data"
    QUERY_DATA_DIR="${DATA_DIR}/query_data"
    export KW_IO_MODE=1
}

function check_kwbase_available() {
  local store=${1}
  if [ ! -d "${DEPLOY_ROOT}/${store}" ];then
    log "${DEPLOY_ROOT}/${store} not found"
    echo 0 && return 1
  fi
  listenaddr=$host_ip:$listenport
  "${KWBIN}" node status --insecure --format csv --host=${listenaddr} | grep "${listenaddr}" | tail -n1 | grep -cP ",true,true$"
}

function start_single_node() {
  local store=${1}
  local brpc_port=28257

  # Check if kwbase is already running on the target port
  if lsof -i:${listenport} >/dev/null 2>&1; then
    die "port ${listenport} is already in use. Stop the existing kwbase instance first."
  fi

  rm -fr "${DEPLOY_ROOT:?}/${store}"
  export GOTRACEBACK=crash
  if ! ${KWBIN} start-single-node --insecure --listen-addr=${host_ip}:${listenport} \
    --brpc-addr=${host_ip}:${brpc_port} \
    --store=${DEPLOY_ROOT}/${store} \
    --pid-file=${DEPLOY_ROOT}/${store}/kwbase.pid \
    --background; then
    die "failed to start kwbase, check lesten addr ${host_ip}:${listenport}"
  fi

  for c in {1..30};do
    if [ "1" = "$(check_kwbase_available $store)" ];then
      break
    fi
    sleep 1
  done
  ${KWBIN} sql --insecure --host=${host_ip}:${listenport} -e \
  "ALTER RANGE default CONFIGURE ZONE USING gc.ttlseconds = 60;"
}

function resolve_query_ts_end() {
    local scale="$1"
    case "$scale" in
        100000)
            echo "2020-01-01T15:00:01Z"
            ;;
        1000000|10000000)
            echo "2020-01-01T12:04:01Z"
            ;;
        *)
            echo "2020-01-05T00:00:01Z"
            ;;
    esac
}

function resolve_load_ts_end() {
    local scale="$1"
    case "$scale" in
        100)
            echo "2020-02-01T00:00:00Z"
            ;;
        4000)
            echo "2020-01-05T00:00:00Z"
            ;;
        100000)
            echo "2020-01-01T03:00:00Z"
            ;;
        1000000)
            echo "2020-01-01T00:03:00Z"
            ;;
        *)
            die "unsupported scale for load window: ${scale}"
            ;;
    esac
}

function generate_load_data() {
    local scale="$1"
    local load_ts_end;
    load_ts_end="$(resolve_load_ts_end "${scale}")"
    local load_data="${LOAD_DATA_DIR}/${FORMAT}/${TSBS_CASE}_${FORMAT}_scale_${scale}_${LOAD_WORKERS}order.dat"

    mkdir -p "${LOAD_DATA_DIR}/${FORMAT}"
    if [[ -f "${load_data}" ]]; then
        log "load data already exists: ${load_data}"
        echo "${load_data}"
        return
    fi

    log "generating load data for scale ${scale}"
    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_generate_data_${ARCH}" \
        --format="${FORMAT}" \
        --use-case="${TSBS_CASE}" \
        --seed=123 \
        --scale="${scale}" \
        --log-interval=10s \
        --timestamp-start="${LOAD_TS_START}" \
        --timestamp-end="${load_ts_end}" \
        --orderquantity="${LOAD_WORKERS}" > "${load_data}"

    echo "${load_data}"
}

function generate_query_data() {
    local scale="$1"
    local query_type="$2"
    local query_ts_end
    query_ts_end="$(resolve_query_ts_end "${scale}")"
    mkdir -p ${QUERY_DATA_DIR}
    local query_data="${QUERY_DATA_DIR}/${FORMAT}_scale${scale}_${TSBS_CASE}_${query_type}_query_times${QUERY_TIMES}.dat"

    if [[ -f "${query_data}" ]]; then
        log "${query_type} query data already exists"
        echo "${query_data}"
        return
    fi

    log "generating query data for scale ${scale}, query ${query_type}"
    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_generate_queries_${ARCH}" \
        --format="${FORMAT}" \
        --use-case="${TSBS_CASE}" \
        --seed=123 \
        --scale="${scale}" \
        --query-type="${query_type}" \
        --queries="${QUERY_TIMES}" \
        --timestamp-start="2020-01-01T00:00:00Z" \
        --timestamp-end="${query_ts_end}" \
        --db-name=benchmark | gzip > "${query_data}.gz"
    gunzip "${query_data}.gz"
    echo "${query_data}"
}

function load_data_for_scale() {
    local scale="${1:?}"
    local load_data="${2:?}"

    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_load_kwdb_${ARCH}" \
        --file="${load_data}" \
        --user=root \
        --pass=1234 \
        --host="${ME_HOST_IP}" \
        --port="${ME_HOST_PORT}" \
        --insert-type=prepare \
        --db-name=benchmark \
        --partition=false \
        --batch-size=1000 \
        --case="${TSBS_CASE}" \
        --workers="${LOAD_WORKERS}"
}

function run_query() {
    local scale="$1"
    local query_type="$2"
    local query_data="$3"
    local query_worker="$4"

    log "running query ${query_type} for scale ${scale} with worker ${query_worker}"
    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_run_queries_kwdb_${ARCH}" \
        --file="${query_data}" \
        --user=root \
        --pass=1234 \
        --host="${ME_HOST_IP}" \
        --port="${ME_HOST_PORT}" \
        --query-type="${query_type}" \
        --prepare=false \
        --workers="${query_worker}"
}

set_defaults
