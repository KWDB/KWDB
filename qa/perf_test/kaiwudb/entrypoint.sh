#!/bin/bash

# Export library path
export LD_LIBRARY_PATH=/home/inspur/install/lib

apt-get update && apt-get install -y protobuf-compiler libgflags-dev libprotoc-dev libprotobuf-dev

# Start Service
cd install/bin
./kwbase start-single-node --listen-addr=0.0.0.0:26888 --http-addr=0.0.0.0:9999 --insecure --buffer-pool-size=${BUFFER_POOL_SIZE:-12288} --max-sql-memory=${MAX_SQL_MEMORY:-25%} --external-io-dir=/mock_data --background

# Wait for the specified timeout period, which defaults to 288800 seconds
: "${QA_TIMEOUT:=288800}"

echo "Running with QA_TIMEOUT=$QA_TIMEOUT seconds..."
sleep $QA_TIMEOUT

echo "Timeout reached, stopping the container."
