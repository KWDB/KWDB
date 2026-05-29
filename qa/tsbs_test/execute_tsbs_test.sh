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

QA_DIR=${QA_DIR:-"/home/inspur/src/gitee.com/kwbasedb/qa"}
source "${QA_DIR}/tsbs_test/tsbs_env.sh"

readonly SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly CLUSTER_SETTINGS_DIR="${QA_DIR}/tsbs_test/cluster_settings"
readonly TSBS_PATH="${QA_DIR}/tsbs_test/bin"

NODE_NUM=""
SCALES=""
TSBS_RESULT_DIR=""
KWDB_CT_NAME=""
ME_HOST_IP=""
ME_HOST_PORT=""
DATA_DIR=""
QUERY_WORKERS=""
PRIMARY_QUERY_WORKER=""
QUERY_TYPES_PARAM="${QUERY_TYPES_PARAM:-""}"
UPDATE_THRESHOLD="${UPDATE_THRESHOLD:-false}"
COMPARE_THRESHOLD="${COMPARE_THRESHOLD:-false}"
BIN_DIR="${BIN_DIR:-"/home/inspur/src/gitee.com/kwbasedb/install/bin"}"
KWBIN="${KWBIN:-"${BIN_DIR}/kwbase"}"
BRANCH_NAME=""
ARCH=""
RUN_TS=""

LOAD_WORKERS="${load_workers:-12}"
LOAD_BATCH_SIZES="${load_batchsizes:-1000}"
TSBS_CASE="${tsbs_case:-cpu-only}"
LOAD_INTERVAL="${load_interval:-10s}"
DB_NAME="${db_name:-benchmark}"
LOAD_TS_START="${load_ts_start:-2016-01-01T00:00:00Z}"
INSERT_TYPE="${insert_type:-prepare}"
QUERY_TIMES="${query_times:-30}"
WAL="${wal:-3}"
FORMAT="${format:-kwdb}"
PARALLEL_DEGREE="${parallel_degree:-8}"
INSERT_DIRECT="${insert_direct:-1}"
REPLICA_MODE="${replica_mode:-1}"

LOAD_DATA_DIR=""
QUERY_DATA_DIR=""
THRESHOLD_DIR=""

resolve_primary_query_worker() {
    local workers_raw="$1"
    local workers_list=()
    IFS=',' read -r -a workers_list <<< "${workers_raw}"
    [[ ${#workers_list[@]} -gt 0 ]] || die "query workers list is empty"
    [[ -n "${workers_list[0]}" ]] || die "primary query worker is empty"
    echo "${workers_list[0]}"
}

usage() {
    cat <<'EOF'
Usage:
  execute_tsbs_test.sh [legacy positional args]
  execute_tsbs_test.sh [options]

Legacy positional args:
  1  node_num
  2  scales
  3  tsbs_result_dir
  4  kwdb_ct_name
  5  host
  6  port
  7  data_dir
  8  query_workers
  9  query_types

Options:
  --node-num NUM
  --scales CSV
  --result-dir DIR
  --cluster-name NAME
  --host IP
  --port PORT
  --data-dir DIR
  --query-workers NUM
  --query-types CSV
  --insert-type TYPE
  --insert-direct NUM
  --wal NUM
  --replica-mode NUM
  --parallel-degree NUM
  --format NAME
  --query-times NUM
  --load-workers NUM
  --load-batch-sizes NUM
  --tsbs-case NAME
  --db-name NAME
  --load-interval DURATION
  --load-ts-start TIMESTAMP
  --update-threshold true|false
  --compare-threshold true|false
  --bin-dir DIR
  --kwbin PATH
  --branch-name NAME
  -h, --help
EOF
}

log() {
    echo "[tsbs] $*" >&2
}

die() {
    echo "[tsbs] ERROR: $*" >&2
    exit 1
}

get_git_branch() {
    local repo_dir="${1:-.}"
    (
        cd "$repo_dir" 2>/dev/null || exit 1
        local branch
        branch="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || true)"
        if [[ -z "$branch" ]]; then
            exit 1
        fi
        if [[ "$branch" == "HEAD" ]]; then
            branch="detached-HEAD-($(git rev-parse --short HEAD 2>/dev/null))"
        fi
        echo "$branch"
    )
}

sanitize_path_component() {
    local value="$1"
    value="${value//\//_}"
    value="${value// /_}"
    value="${value//:/_}"
    echo "${value}"
}

set_defaults() {
    NODE_NUM="${NODE_NUM:-1}"
    SCALES="${SCALES:-100,4000,100000,1000000}"
    TSBS_RESULT_DIR="${TSBS_RESULT_DIR:-"${QA_DIR}/tsbs_test/tsbs_result"}"
    KWDB_CT_NAME="${KWDB_CT_NAME:-kwdb}"
    ME_HOST_IP="${ME_HOST_IP:-127.0.102.145}"
    ME_HOST_PORT="${ME_HOST_PORT:-26257}"
    DATA_DIR="${DATA_DIR:-"${QA_DIR}/tsbs_test/data"}"
    QUERY_WORKERS="${QUERY_WORKERS:-${query_workers:-8}}"
    PRIMARY_QUERY_WORKER="${PRIMARY_QUERY_WORKER:-$(resolve_primary_query_worker "${QUERY_WORKERS}")}"
    BRANCH_NAME="${BRANCH_NAME:-$(get_git_branch "${QA_DIR}/.." || true)}"
    BRANCH_NAME="${BRANCH_NAME:-unknown-branch}"
    ARCH="$(uname -m)"
    RUN_TS="$(date +%Y_%m%d_%H%M%S)"
    LOAD_DATA_DIR="${DATA_DIR}/load_data"
    QUERY_DATA_DIR="${DATA_DIR}/query_data"
    THRESHOLD_DIR="${TSBS_RESULT_DIR}/threshold/cluster${NODE_NUM}_insertdirect${INSERT_DIRECT}_${INSERT_TYPE}_wal${WAL}_replica${REPLICA_MODE}_dop${PARALLEL_DEGREE}"
    export KW_IO_MODE=1
}

parse_legacy_args() {
    NODE_NUM="${1:-$NODE_NUM}"
    SCALES="${2:-$SCALES}"
    TSBS_RESULT_DIR="${3:-$TSBS_RESULT_DIR}"
    KWDB_CT_NAME="${4:-$KWDB_CT_NAME}"
    ME_HOST_IP="${5:-$ME_HOST_IP}"
    ME_HOST_PORT="${6:-$ME_HOST_PORT}"
    DATA_DIR="${7:-$DATA_DIR}"
    QUERY_WORKERS="${8:-$QUERY_WORKERS}"
    QUERY_TYPES_PARAM="${9:-$QUERY_TYPES_PARAM}"
}

parse_named_args() {
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --node-num) NODE_NUM="$2"; shift 2 ;;
            --scales) SCALES="$2"; shift 2 ;;
            --result-dir) TSBS_RESULT_DIR="$2"; shift 2 ;;
            --cluster-name) KWDB_CT_NAME="$2"; shift 2 ;;
            --host) ME_HOST_IP="$2"; shift 2 ;;
            --port) ME_HOST_PORT="$2"; shift 2 ;;
            --data-dir) DATA_DIR="$2"; shift 2 ;;
            --query-workers) QUERY_WORKERS="$2"; shift 2 ;;
            --query-types) QUERY_TYPES_PARAM="$2"; shift 2 ;;
            --insert-type) INSERT_TYPE="$2"; shift 2 ;;
            --insert-direct) INSERT_DIRECT="$2"; shift 2 ;;
            --wal) WAL="$2"; shift 2 ;;
            --replica-mode) REPLICA_MODE="$2"; shift 2 ;;
            --parallel-degree) PARALLEL_DEGREE="$2"; shift 2 ;;
            --format) FORMAT="$2"; shift 2 ;;
            --query-times) QUERY_TIMES="$2"; shift 2 ;;
            --load-workers) LOAD_WORKERS="$2"; shift 2 ;;
            --load-batch-sizes) LOAD_BATCH_SIZES="$2"; shift 2 ;;
            --tsbs-case) TSBS_CASE="$2"; shift 2 ;;
            --db-name) DB_NAME="$2"; shift 2 ;;
            --load-interval) LOAD_INTERVAL="$2"; shift 2 ;;
            --load-ts-start) LOAD_TS_START="$2"; shift 2 ;;
            --update-threshold) UPDATE_THRESHOLD="$2"; shift 2 ;;
            --compare-threshold) COMPARE_THRESHOLD="$2"; shift 2 ;;
            --bin-dir) BIN_DIR="$2"; shift 2 ;;
            --kwbin) KWBIN="$2"; shift 2 ;;
            --branch-name) BRANCH_NAME="$2"; shift 2 ;;
            -h|--help) usage; exit 0 ;;
            *)
                die "unknown option: $1"
                ;;
        esac
    done
}

parse_args() {
    if [[ $# -eq 0 ]]; then
        return
    fi

    if [[ "$1" == --* || "$1" == "-h" ]]; then
        parse_named_args "$@"
        return
    fi

    parse_legacy_args "$@"
}

validate_config() {
    [[ -x "$KWBIN" ]] || die "KWBIN not found or not executable: $KWBIN"
    [[ -d "$TSBS_PATH" ]] || die "TSBS_PATH not found: $TSBS_PATH"
    [[ -f "${CLUSTER_SETTINGS_DIR}/general.sql" ]] || die "general.sql not found: ${CLUSTER_SETTINGS_DIR}/general.sql"

    mkdir -p "${LOAD_DATA_DIR}" "${QUERY_DATA_DIR}" "${THRESHOLD_DIR}"

    if [[ ! -f "${THRESHOLD_DIR}/TSBS_THRESHOLD.csv" && "$UPDATE_THRESHOLD" == "false" && "$COMPARE_THRESHOLD" == "true" ]]; then
        die "threshold not found, please run with --update-threshold true first"
    fi

    local query_workers_list=()
    IFS=',' read -r -a query_workers_list <<< "${QUERY_WORKERS}"
    [[ ${#query_workers_list[@]} -gt 0 ]] || die "query workers list is empty"
    local query_worker
    for query_worker in "${query_workers_list[@]}"; do
        [[ "$query_worker" =~ ^[0-9]+$ ]] || die "invalid query worker: ${query_worker}"
    done
}

apply_sql_file() {
    local sql_file="$1"
    local optional_unknown_setting="${2:-false}"
    local statement=""
    local line=""
    local output=""

    while IFS= read -r line || [[ -n "$line" ]]; do
        if [[ -z "${line//[[:space:]]/}" || "${line}" =~ ^[[:space:]]*-- ]]; then
            continue
        fi

        statement+="${line}"$'\n'
        if [[ "$line" != *";" ]]; then
            continue
        fi

        if output="$("$KWBIN" sql --host="${ME_HOST_IP}" --port="${ME_HOST_PORT}" --insecure --execute "${statement}" 2>&1)"; then
            statement=""
            continue
        fi

        if [[ "$optional_unknown_setting" == "true" && "$output" == *"unknown cluster setting"* ]]; then
            log "skip unsupported cluster setting: ${statement//$'\n'/ }"
            statement=""
            continue
        fi

        echo "$output" >&2
        die "failed to execute sql from ${sql_file}: ${statement//$'\n'/ }"
    done < "${sql_file}"
}

wait_cluster_ready() {
    log "checking kaiwudb cluster node status"
    local all_nodes_ready node_status

    while true; do
        all_nodes_ready=true
        for ((i = 1; i <= NODE_NUM; i++)); do
            local line=$((1 + i))
            node_status="$("$KWBIN" node status --insecure --host="${ME_HOST_IP}:${ME_HOST_PORT}" | awk "NR==${line}{print \$11}")"
            log "node ${i} available status is ${node_status:-<empty>}"
            if [[ -z "$node_status" || "$node_status" != "true" ]]; then
                all_nodes_ready=false
            fi
        done

        if [[ "$all_nodes_ready" == "true" ]]; then
            return
        fi
        sleep 0.1
    done
}

resolve_load_ts_end() {
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

resolve_query_ts_end() {
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

resolve_load_partition() {
    case "$NODE_NUM" in
        1)
            echo "false"
            ;;
        *)
            echo "true"
            ;;
    esac
}

resolve_query_compress() {
    local scale="$1"
    case "$scale" in
        1000000)
            echo "snappy_compress"
            ;;
        *)
            echo "off"
            ;;
    esac
}

apply_cluster_settings() {
    local scale="$1"
    apply_sql_file "${CLUSTER_SETTINGS_DIR}/general.sql" true
    if [[ -f "${CLUSTER_SETTINGS_DIR}/scale${scale}.sql" ]]; then
        apply_sql_file "${CLUSTER_SETTINGS_DIR}/scale${scale}.sql" true
    fi
}

apply_after_load_settings() {
    local scale="$1"
    if [[ -f "${CLUSTER_SETTINGS_DIR}/after_load_scale${scale}.sql" ]]; then
        apply_sql_file "${CLUSTER_SETTINGS_DIR}/after_load_scale${scale}.sql" false
    fi
}

get_result_base_dir() {
    local scale="$1"
    local safe_branch_name
    safe_branch_name="$(sanitize_path_component "${BRANCH_NAME}")"
    local result_dir_name="${RUN_TS}_${safe_branch_name}_scale${scale}_cluster${NODE_NUM}_insertdirect${INSERT_DIRECT}_${INSERT_TYPE}_wal${WAL}_replica${REPLICA_MODE}_dop${PARALLEL_DEGREE}"
    echo "${TSBS_RESULT_DIR}/${KWDB_CT_NAME}/${result_dir_name}"
}

generate_load_data() {
    local scale="$1"
    local load_ts_end="$2"
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
        --log-interval="${LOAD_INTERVAL}" \
        --timestamp-start="${LOAD_TS_START}" \
        --timestamp-end="${load_ts_end}" \
        --orderquantity="${LOAD_WORKERS}" > "${load_data}"

    echo "${load_data}"
}

record_metric() {
    local metric_name="$1"
    local scale="$2"
    local result_file="$3"
    local output_dir="$4"
    local worker="$5"

    local cmd=(python3 "${QA_DIR}/tsbs_test/record_result.py"
        -v "${BRANCH_NAME}"
        -p "${RUN_TS}"
        -f "${FORMAT}"
        -s "${scale}"
        -n "${metric_name}"
        -w "${worker}"
        -t "${QUERY_TIMES}"
        -r "${result_file}"
        -d "${output_dir}"
        -o "${PARALLEL_DEGREE}")

    if [[ "${UPDATE_THRESHOLD}" == "true" ]]; then
        cmd=(python3 "${QA_DIR}/tsbs_test/update_threshold.py"
            -v tsbs
            -p "${RUN_TS}"
            -f "${FORMAT}"
            -s "${scale}"
            -n "${metric_name}"
            -w "${worker}"
            -t "${QUERY_TIMES}"
            -r "${result_file}"
            -d "${THRESHOLD_DIR}"
            -o "${PARALLEL_DEGREE}")
    elif [[ "${COMPARE_THRESHOLD}" == "true" ]]; then
        cmd+=(-c "${THRESHOLD_DIR}")
    fi

    "${cmd[@]}"
}

load_data_for_scale() {
    local scale="$1"
    local load_data="$2"
    local load_result_dir="$3"
    local load_result_file="${load_result_dir}/${TSBS_CASE}_${FORMAT}_scale_${scale}.log"
    local partition
    partition="$(resolve_load_partition)"

    log "loading data for scale ${scale} with partition=${partition}"
    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_load_kwdb_${ARCH}" \
        --file="${load_data}" \
        --user=root \
        --pass=1234 \
        --host="${ME_HOST_IP}" \
        --port="${ME_HOST_PORT}" \
        --insert-type="${INSERT_TYPE}" \
        --db-name="${DB_NAME}" \
        --partition="${partition}" \
        --batch-size="${LOAD_BATCH_SIZES}" \
        --case="${TSBS_CASE}" \
        --workers="${LOAD_WORKERS}" > "${load_result_file}"

    record_metric "load" "${scale}" "${load_result_file}" "${load_result_dir}" "${LOAD_WORKERS}"
}

verify_loaded_data() {
    local load_result_dir="$1"
    local query_result_dir="$2"
    local ranges_info_file="${query_result_dir}/ranges_info.log"
    local count_info_file="${load_result_dir}/count_info.log"
    local count_result
    local attempt
    local ranges_ok=false

    log "collecting range and row-count diagnostics after load"
    for attempt in 1 2 3; do
        if "$KWBIN" sql --insecure --host="${ME_HOST_IP}:${ME_HOST_PORT}" \
            --execute="select * from kwdb_internal.ranges where table_name='cpu';" > "${ranges_info_file}" 2>"${ranges_info_file}.err"; then
            rm -f "${ranges_info_file}.err"
            ranges_ok=true
            break
        fi
        log "warning: failed to collect ranges_info on attempt ${attempt}, retrying"
        sleep 1
    done
    if [[ "${ranges_ok}" != "true" ]]; then
        log "warning: failed to collect ranges_info.log after retries; continuing with count verification"
    fi

    "$KWBIN" sql --insecure --host="${ME_HOST_IP}:${ME_HOST_PORT}" \
        --execute="select count(1) from benchmark.cpu;" > "${count_info_file}"

    count_result="$(sed -n '2p' "${count_info_file}" | tr -d '[:space:]')"
    if [[ -z "${count_result}" || "${count_result}" == "0" ]]; then
        die "load verification failed: benchmark.cpu row count is ${count_result:-<empty>}"
    fi
    log "load verification succeeded: benchmark.cpu row count is ${count_result}"
}

resolve_query_types() {
    local scale="$1"

    if [[ -n "${QUERY_TYPES_PARAM}" ]]; then
        echo "${QUERY_TYPES_PARAM}"
        return
    fi

    case "$scale" in
        1)
            echo "${QUERY_TYPES_1}"
            ;;
        3)
            echo "${QUERY_TYPES_3}"
            ;;
        *)
            case "$NODE_NUM" in
                1)
                    echo "${QUERY_TYPES_1}"
                    ;;
                3)
                    echo "${QUERY_TYPES_3}"
                    ;;
                *)
                    echo "${QUERY_TYPES_ALL}"
                    ;;
            esac
            ;;
    esac
}

generate_query_data() {
    local scale="$1"
    local query_type="$2"
    local query_ts_end="$3"
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
        --db-name="${DB_NAME}" | gzip > "${query_data}.gz"

    gunzip "${query_data}.gz"
    echo "${query_data}"
}

run_query() {
    local scale="$1"
    local query_type="$2"
    local query_data="$3"
    local query_result_dir="$4"
    local query_worker="$5"
    local query_result="${query_result_dir}/${FORMAT}_scale${scale}_${TSBS_CASE}_${query_type}_worker${query_worker}.log"
    local query_compress
    query_compress="$(resolve_query_compress "${scale}")"

    log "running query ${query_type} for scale ${scale} with worker ${query_worker}, compress=${query_compress}"
    LD_LIBRARY_PATH="${TSBS_PATH}/lib" "${TSBS_PATH}/tsbs_run_queries_kwdb_${ARCH}" \
        --file="${query_data}" \
        --user=root \
        --pass=1234 \
        --host="${ME_HOST_IP}" \
        --port="${ME_HOST_PORT}" \
        --query-type="${query_type}" \
        --prepare=false \
        --compress="${query_compress}" \
        --workers="${query_worker}" > "${query_result}"

    record_metric "${query_type}" "${scale}" "${query_result}" "${query_result_dir}" "${query_worker}"
}

run_scale() {
    local scale="$1"
    local result_base_dir
    result_base_dir="$(get_result_base_dir "${scale}")"
    local load_result_dir="${result_base_dir}/load_result"
    local query_result_dir="${result_base_dir}/query_result"
    mkdir -p "${load_result_dir}" "${query_result_dir}"

    log "testing scale ${scale}"
    local load_ts_end
    load_ts_end="$(resolve_load_ts_end "${scale}")"
    local query_ts_end
    query_ts_end="$(resolve_query_ts_end "${scale}")"

    apply_cluster_settings "${scale}"

    local load_data
    load_data="$(generate_load_data "${scale}" "${load_ts_end}")"
    load_data_for_scale "${scale}" "${load_data}" "${load_result_dir}"
    verify_loaded_data "${load_result_dir}" "${query_result_dir}"

    apply_after_load_settings "${scale}"

    local query_types_raw
    query_types_raw="$(resolve_query_types "${scale}")"
    local query_types_list=()
    if [[ -n "${QUERY_TYPES_PARAM}" ]]; then
        IFS=',' read -r -a query_types_list <<< "${query_types_raw}"
    else
        read -r -a query_types_list <<< "${query_types_raw}"
    fi

    local query_type
    for query_type in "${query_types_list[@]}"; do
        local query_data
        query_data="$(generate_query_data "${scale}" "${query_type}" "${query_ts_end}")"
        local query_workers_list=()
        IFS=',' read -r -a query_workers_list <<< "${QUERY_WORKERS}"
        local query_worker
        for query_worker in "${query_workers_list[@]}"; do
            run_query "${scale}" "${query_type}" "${query_data}" "${query_result_dir}" "${query_worker}"
        done
    done
}

main() {
    set_defaults
    parse_args "$@"
    set_defaults
    validate_config

    log "configuration summary:"
    log "  node_num=${NODE_NUM}"
    log "  scales=${SCALES}"
    log "  result_dir=${TSBS_RESULT_DIR}"
    log "  host=${ME_HOST_IP}:${ME_HOST_PORT}"
    log "  query_workers=${QUERY_WORKERS}"
    log "  insert_type=${INSERT_TYPE}"
    log "  wal=${WAL}"
    log "  replica_mode=${REPLICA_MODE}"
    log "  parallel_degree=${PARALLEL_DEGREE}"

    wait_cluster_ready

    local scales_list=()
    IFS=',' read -r -a scales_list <<< "${SCALES}"
    local scale
    for scale in "${scales_list[@]}"; do
        run_scale "${scale}"
    done
}

main "$@"
