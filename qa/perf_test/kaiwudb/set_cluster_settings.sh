#!/bin/bash

PORT=${1:-26888}
HTTP_ADDR=${2:-8080}
PARALLEL_DEGREE=${3:-8}
HASH_SCAN_MODE=${4:-0}
MULTI_MODEL_ENABLED=${5:-true}
SPECIAL_OPTIMIZATION_MODE=${6:-1100}
HASH_SCAN_MODE_SETTING_NAME=${7:-ts.sql.hash_scan_mode}

CUR_DIR="$(cd "$(dirname "$0")"; pwd)"
# perf_test root dir
ROOT_DIR=${CUR_DIR}/..
GOPATH_DIR=$(realpath "${ROOT_DIR}/../../..")
KWBASE_DIR=${GOPATH_DIR}/src/gitee.com/kwbasedb
KWBASE_BIN_PATH=$(realpath "${KWBASE_DIR}/install/bin")
# Use dirname to obtain WORK_PATH (remove the last-level directory)
export WORKSPACE_DIR=$(dirname "$GOPATH_DIR")
# Use basename to obtain UUID_PATH (remove the last-level directory)
export UUID_PATH=$(basename "$GOPATH_DIR")
DATASET_DIR=${WORKSPACE_DIR}/dataset/mock_data

DB_LISTEN_ADDR="127.0.0.1:${PORT}"
DB_INSECURE="--insecure"
DB_BACKGROUND="--background"
KWBASE_BIN=./kwbase

export LD_LIBRARY_PATH=${KWBASE_DIR}/install/lib

# Define an error handling function
function handle_error {
    echo "Error occurred at line $1 while executing: $2"
    exit 1
}

# trap calls the handle_error function when any command fails
trap 'handle_error $LINENO "$BASH_COMMAND"' ERR

# alias to start kwbase
echo "go to $KWBASE_BIN_PATH and set cluster settings"
cd $KWBASE_BIN_PATH || exit 1

echo "Set cluster settings in 5 seconds..."
sleep 5
$KWBASE_BIN sql $DB_INSECURE --execute="set cluster setting ts.parallel_degree=${PARALLEL_DEGREE};" --host=$DB_LISTEN_ADDR
$KWBASE_BIN sql $DB_INSECURE --execute="set cluster setting sql.defaults.multimodel.enabled = ${MULTI_MODEL_ENABLED};" --host=$DB_LISTEN_ADDR
$KWBASE_BIN sql $DB_INSECURE --execute="set cluster setting ${HASH_SCAN_MODE_SETTING_NAME}=${HASH_SCAN_MODE};" --host=$DB_LISTEN_ADDR
$KWBASE_BIN sql $DB_INSECURE --execute="set cluster setting ts.sql.query_opt_mode = ${SPECIAL_OPTIMIZATION_MODE};" --host=$DB_LISTEN_ADDR
echo "Set cluster settings is done."

$KWBASE_BIN quit $DB_INSECURE --host=$DB_LISTEN_ADDR
echo "Shutting down kwbase, wait for 5 seconds..."
sleep 5