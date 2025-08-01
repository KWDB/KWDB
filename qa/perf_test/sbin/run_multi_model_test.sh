#!/bin/bash
set +e
whoami
export

echo "AGILE_MODULE_NAME value is : ${AGILE_MODULE_NAME}"
echo "AGILE_COMPILE_BRANCH value is : ${AGILE_COMPILE_BRANCH}"
echo "AGILE_SOURCE_BRANCH value is : ${AGILE_SOURCE_BRANCH}"
echo "MANUAL_AGILE_BRANCH value is : ${MANUAL_AGILE_BRANCH}"
echo "PULL_REQUEST_NUMBER value is : ${GITEE_PULL_REQUEST_NUMBER}"

# Commands that return parameters must be in the execution path of the agent and need to be recorded in advance
export agentpath=`pwd`
echo $agentpath

# Set the kwbase pipeline directory
uuid=multi_model_regression_test_$AGILE_PIPELINE_BUILD_NUMBER
echo "CI_PATH=$CI_PATH"

workspace=$CI_WORKSPACE
echo "CI_WORKSPACE=$CI_WORKSPACE"

# Clear the old result folder created by CICD
echo "find ${workspace}/flask/flask_output all folders containing 'multi_model_regression_test_' for more than 48 hours"
find ${workspace}/flask/flask_output -maxdepth 1 -type d -name "multi_model_regression_test_*" -mmin +2880 -ls
echo "Delete all the above folders"
find ${workspace}/flask/flask_output -maxdepth 1 -type d -name "multi_model_regression_test_*" -mmin +2880 | xargs sudo rm -rf
echo ""

# Delete containers that have existed for more than one day
echo "rm containler with more than one day left"
for container in $(docker ps -a | grep 'kwdb-pipe-test-' | grep 'days ago' | awk '{print $1}')
do
    echo $container
    docker rm -f $container
done

set -e

# Verify the dataset
echo "Verifying dataset directories..."

DATASET_DIR="${CI_WORKSPACE}/dataset"
PIPELINE_DATASET_DIR="$DATASET_DIR/pipeline_dataset"
MOCK_DATA_DIR="$DATASET_DIR/mock_data"
if [ ! -d "$PIPELINE_DATASET_DIR" ] || [ ! -d "$MOCK_DATA_DIR" ]; then
  echo "Error: Required dataset directories are missing."
  echo "Expected directories:"
  echo "  - $PIPELINE_DATASET_DIR"
  echo "  - $MOCK_DATA_DIR"
  exit 1
else
  echo "Dataset directories are present."
fi

# find an available port
while true
do
  RANDOM_NUM=$(date +%s%N | cut -b10-19 | sed 's|^[0]\+||')
  echo "RANDOM_NUM is $RANDOM_NUM"
  port_number=$((RANDOM_NUM % 10000 + 16800))
  if ! netstat -tuln | grep ":$port_number" > /dev/null; then
    echo "Found available port: $port_number"
    break
  else
    echo "Port $port_number is in use."
  fi
done
echo "Free port $port_number is found."

set +e

# start test
cd ${CI_WORKSPACE}/${CI_PATH}/pipeline-scripts/pipelines/perf_test/sbin
./run_test.sh prep_env -N "$query_times" -p "$port_number" -F true -L false -G "$GENERATE_DATABASE" -b "$BACKUP_DATABASE" -D "$BACKUP_DATABASE_PATH" -m "$MULTI_MODEL_ENABLED" -K "$KEEP_DOCKER_CONTAINER" -s "$SPECIAL_OPTIMIZATION_MODE" -S "$TEST_RUN_TIME"

exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo "The data preparation stage failed."
  #rm -rf ${CI_WORKSPACE}/${CI_PATH}
  exit 1
fi

./run_test.sh run_kw_start -N "$query_times" -p "$port_number" -L false -G "$GENERATE_DATABASE" -b "$BACKUP_DATABASE" -D "$BACKUP_DATABASE_PATH" -m "$MULTI_MODEL_ENABLED" -K "$KEEP_DOCKER_CONTAINER" -s "$SPECIAL_OPTIMIZATION_MODE" -S "$TEST_RUN_TIME"

exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo "start kwbase failed."
  #rm -rf ${CI_WORKSPACE}/${CI_PATH}
  exit 1
fi

./run_test.sh run_parallel_regression -P ${parallel_degree} -p "$port_number" -o "${CI_WORKSPACE}/flask/flask_output/${CI_PATH}" -a $ACCESS_MODE -M $MASTER_OVERWRITE -L false -G "$GENERATE_DATABASE" -b "$BACKUP_DATABASE" -D "$BACKUP_DATABASE_PATH" -c "$COLLECT_PHYSICAL_PLAN" -m "$MULTI_MODEL_ENABLED" -K "$KEEP_DOCKER_CONTAINER" -E "$EXCLUDE_SQL_FILES" -f "$SQL_FILE_FILTER" -s "$SPECIAL_OPTIMIZATION_MODE" -S "$TEST_RUN_TIME" -C "$CORRECTNESS_CHECK"

exit_status=$?
if [ $exit_status -ne 0 ]; then
  echo "run_regression.sh failed, with one or more correctness errors"
  exit 1
else
  #rm -rf ${CI_WORKSPACE}/${CI_PATH}
  echo "The test has been successfully completed."
fi

exit 0