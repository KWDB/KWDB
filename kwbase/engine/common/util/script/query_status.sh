#!/bin/bash -e

if [ $# -ne 1 ]
then
  echo -e "Failed!\n\tusage: $0 ins_nm"
  exit 1
fi
script_dir=$(pwd)
ins_nm=$1


if [[ -z "${KWDB_ROOT}" ]]; then
  KWDB_ROOT=$(cd ..; pwd)
fi
source ${KWDB_ROOT}/deploy/${ins_nm}/setenv.sh
./kwdbAdmin status
