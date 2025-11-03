#!/bin/bash

PORT=${1:-26888}
HTTP_ADDR=${2:-8080}
DOCKER_CONTAINER_PREFIX=$3

CUR_DIR="$(cd "$(dirname "$0")"; pwd)"
# perf_test root dir
ROOT_DIR=${CUR_DIR}/../..
GOPATH_DIR=$(realpath "${ROOT_DIR}/../../..")
KWBASE_DIR=${GOPATH_DIR}/src/gitee.com/kwbasedb
KWBASE_BIN_PATH=$(realpath "${KWBASE_DIR}/install/bin")
# Use dirname to obtain WORK_PATH (remove the last-level directory)
export WORKSPACE_DIR=$(dirname "$GOPATH_DIR")
# Use basename to obtain UUID_PATH (remove the last-level directory)
export UUID_PATH=$(basename "$GOPATH_DIR")
PIPE_DATASET_DIR=${WORKSPACE_DIR}/dataset/pipeline_dataset/
RBY_DATASET_DIR=${WORKSPACE_DIR}/dataset/mock_data

DB_LISTEN_ADDR="127.0.0.1:${PORT}"
DB_INSECURE="--insecure"
DB_BACKGROUND="--background"

export LD_LIBRARY_PATH=${KWBASE_DIR}/install/lib

# Define an error handling function
function handle_error {
    echo "Error occurred at line $1 while executing: $2"
    exit 1
}

# trap calls the handle_error function when any command fails
trap 'handle_error $LINENO "$BASH_COMMAND"' ERR

# alias to start kwbase
echo "go to $KWBASE_BIN_PATH and start loading data"
cd $KWBASE_BIN_PATH || exit 1

echo "Creating tables and inserting data in 5 seconds..."
sleep 5

echo "Creating databases and tables in 5 seconds..."
sleep 5

echo "Copy the sql file that needs to be imported into the container"

echo "docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c 'mkdir /prep-pipeline'"
docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c 'mkdir /prep-pipeline'

echo "docker cp ${CUR_DIR}/pipeline_create_db_table.sql ${DOCKER_CONTAINER_PREFIX}-$PORT:/prep-pipeline/"
docker cp ${CUR_DIR}/pipeline_create_db_table.sql ${DOCKER_CONTAINER_PREFIX}-$PORT:/prep-pipeline/

docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c '/home/inspur/install/bin/kwbase sql --insecure --host=127.0.0.1:26888 < /prep-pipeline/pipeline_create_db_table.sql' || { echo "Failed to create databases and tables"; exit 1; }

echo "Loading data..."

echo "docker cp ${CUR_DIR}/load.sql ${DOCKER_CONTAINER_PREFIX}-$PORT:/prep-pipeline/"
docker cp ${CUR_DIR}/load.sql ${DOCKER_CONTAINER_PREFIX}-$PORT:/prep-pipeline/

docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c '/home/inspur/install/bin/kwbase sql --insecure --host=127.0.0.1:26888 < /prep-pipeline/load.sql' || { echo "Failed to load data"; exit 1; }

echo "Complete the data writing."