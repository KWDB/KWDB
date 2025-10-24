#!/bin/bash

PORT=${1:-26888}
HTTP_ADDR=${2:-8080}
DOCKER_CONTAINER_PREFIX=$3

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
echo "go to $KWBASE_BIN_PATH and start creating stats"
cd $KWBASE_BIN_PATH || exit 1

echo "Create stats for tables in 5 seconds..."
sleep 5

docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c 'mkdir -p /kaiwudb'
docker cp ${CUR_DIR}/create_and_show_stats.sql ${DOCKER_CONTAINER_PREFIX}-$PORT:/kaiwudb/
docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c '/home/inspur/install/bin/kwbase sql --insecure --host=127.0.0.1:26888 < /kaiwudb/create_and_show_stats.sql' || { echo "Failed to create stats for tables"; exit 1; }

echo "Creating stats for tables is done."
