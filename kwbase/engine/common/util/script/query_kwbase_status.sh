#!/bin/bash -e

if [ $# -ne 2 ]
then
  echo -e "Failed!\n\tusage: $0 ins_nm port"
  exit 1
fi
script_dir=$(pwd)
ins_nm=$1
port=$2

if [[ -z "${KWDB_ROOT}" ]]; then
  KWDB_ROOT=$(cd ..; pwd)
fi

KWDB_BIN_DIR=${KWDB_ROOT}/bin
KWDB_LIB_DIR=${KWDB_ROOT}/lib
KWDB_DEPLOY_DIR=${KWDB_ROOT}/deploy
KWDB_DATA_ROOT=${KWDB_DEPLOY_DIR}/$ins_nm

LD_LIBRARY_PATH=${KWDB_ROOT}/lib:${LD_LIBRARY_PATH} KWDB_ROOT=${KWDB_ROOT} KWDB_DATA_ROOT=${KWDB_DATA_ROOT} ./kwbase node status --insecure --host=127.0.0.1:$port