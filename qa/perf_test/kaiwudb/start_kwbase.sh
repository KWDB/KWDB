#!/bin/bash

# cd /data1/workspace/CI_kwdbts_pipe_test/src/gitee.com/perf-test/kaiwudb
# ./start_kwbase.sh "" "" "/data1/workspace" "CI_kwdbts_pipe_test" 26888 "src/gitee.com"

docker_repo_name=$1
docker_tag_name=$2
workspace=$3
uuid=$4
PORT=$5
CODE_PLATFORM=$6
BUFFER_POOL_SIZE=$7
MAX_SQL_MEMORY=$8
TEST_RUN_TIME=$9
DOCKER_CONTAINER_PREFIX=${10}

# When no directory is entered
if [ -z "$workspace" ] || [ -z "$uuid" ] || [ -z "$CODE_PLATFORM" ]; then
  echo "workspace or uuid or CODE_PLATFORM is not specified"
  exit 1
fi

timeout_seconds=$TEST_RUN_TIME
echo "timeout_seconds is: $timeout_seconds"

# The default image used is 22.04
# 20.04 images is：devops.inspur.com:80/isg/zdp/repo/local_repo/zdp/kwdb/pipe_net_test:20.04-20240604
if [ -z "$docker_repo_name" ] || [ -z "$docker_tag_name" ]; then
  docker_repo_name=devops.inspur.com:80/isg/zdp/repo/local_repo/quality_team/kwdb/perf_test
  docker_tag_name=22.04-20240819
fi

# Check if the Docker image exists
if ! docker image inspect $docker_repo_name:$docker_tag_name > /dev/null 2>&1; then
  echo "Docker image $docker_repo_name:$docker_tag_name not found. Trying to pull..."
  if ! docker pull $docker_repo_name:$docker_tag_name; then
    echo "Docker pull failed. Trying to build the image..."
    docker build -t $docker_repo_name:$docker_tag_name .
  fi
fi

chmod +x entrypoint.sh
cp entrypoint.sh $workspace/$uuid/src/$CODE_PLATFORM/kwbasedb/

# Run Docker containers, map ports and mount directories
echo ”Prepare to start the Docker container: ${DOCKER_CONTAINER_PREFIX}-${PORT}, This container will survive: ${timeout_seconds} second“
docker run -d \
  --privileged \
  -p $PORT:26888 \
  -v $workspace/$uuid/src/$CODE_PLATFORM/kwbasedb:/home/inspur/ \
  -v $workspace/dataset/mock_data:/mock_data \
  -v /dev:/dev \
  --cpus="16" \
  --memory="32g" \
  --name ${DOCKER_CONTAINER_PREFIX}-$PORT \
  -e QA_TIMEOUT=${timeout_seconds} \
  -e BUFFER_POOL_SIZE=${BUFFER_POOL_SIZE} \
  -e MAX_SQL_MEMORY=${MAX_SQL_MEMORY} \
  -e LD_LIBRARY_PATH=/home/inspur/install/lib \
  --entrypoint /home/inspur/entrypoint.sh \
  $docker_repo_name:$docker_tag_name 

if [ $? -ne 0 ]; then
  # Just fail the automation test if docker container cannot be created.
  echo "Failed to create the docker container named ${DOCKER_CONTAINER_PREFIX}-${PORT}."
  exit 1
fi

# Wait for the database to be ready
function wait_for_kwbase() {
  local retries=120

  echo "Waiting for database to be ready..."
  for ((i=0; i<retries; i++)); do
    docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT /home/inspur/install/bin/kwbase sql --insecure --host=127.0.0.1:26888 -e "set cluster setting cluster.license = 'jvzHX1u7QvMJY9fztVbX4URcI+KPz3s5xMG/6Rk9TWxkChKYejABFq8HO9qcTOrckppMncgG82eXgGQwAYwWGLc6wG3bqA6DnNsQ1OcV29uZs2PEkGPZiLEDzTEwPTHEGXhvjEs9wU0H/uI3Me2nDZi7bwcK3Z7uTRcVKOunVxUWLmr3j9Sm18UhjwVjdVPH05eyZyaUFk2CqUrj6hVZwPjZ8+mw8o1eFYGowxLp0p30CTys76zErvsine+BMnkN68egIyGbSYGwEBbh4is+qsIqtNYBWCUW6eIRimyGriginAKeg+fKRMj627qIuYqSADm9Peo7z/Rf8ZwnyjJWXr1oOAtdAdzOJfv8SUVISs3e/5/IoaBHBSw7dG3ogv1KcKx1+bN4ARdUSiLRuGWT3gRs3dc/9evUl44rqrN0m0czxIcl5HrbSRyWLcg9yxsY9Vo62hL8d/qT3EOf1K3tBeP1GZHAI6Cx7vsi1CxoJGavs5fCpnHQeyljAfoQgWwNAYudK7G2V0rVVX/Kwha7wuDDycLYz7A2fZhP4o1X9W+VKJfpd6PAjS20qVnXnkc6rguSorT2Bi3JQwowocsUwEtCzLjvv5piY+XYND+/mLCB07zltjFT4eQdvZQKCu+dPqenRAGyfuSvjjEyOzdSmISGaFCnNe8JJ3PSIOZiy4U=';" > /dev/null 2>&1
    docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT /home/inspur/install/bin/kwbase sql --insecure --host=127.0.0.1:26888 -e "SHOW DATABASES;" > /dev/null 2>&1
    if [ $? -eq 0 ]; then
      echo "Database is ready!"
      return 0
    fi
    sleep 2
  done

  echo "Database is not ready after ${retries} attempts."
  return 1
}

# Wait for the database to be ready
wait_for_kwbase 127.0.0.1 26888

# If the database is ready, exit normally
if [ $? -eq 0 ]; then
  echo "kwbase starts success"
  exit 0
else
  echo "Failed to connect to the database. Exiting."
  exit 1
fi
