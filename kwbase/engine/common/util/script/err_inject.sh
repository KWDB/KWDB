#!/bin/bash

function print_help() {
  echo -e "usage: "
  echo -e "\t$0 port1,port2 trigger fault_id when [val]"
  echo -e "\t$0 port1 active fault_id when [val]"
  echo -e "\t$0 port1,port2 deactive fault_id"
  echo -e "\t$0 port1 ls"
}

function execute_inject_cmd() {
  port=$1
  echo "$2" | nc 127.0.0.1 $port -w 5
  if [[ $? -ne 0 ]]; then 
    echo "Connect failed. Is $ins_nm running?";
  fi;
  echo -e "\n"
}

if [ $# -lt 2 ]; then
  print_help
  exit 1
fi
script_dir=$(pwd)
ins_list=$1

if [[ -z "${KWDB_ROOT}" ]]; then
  KWDB_ROOT=$(cd ..; pwd)
fi

KWDB_BIN_DIR=${KWDB_ROOT}/bin
KWDB_LIB_DIR=${KWDB_ROOT}/lib
KWDB_DEPLOY_DIR=${KWDB_ROOT}/deploy
IFS=","; ins_arr=($ins_list); unset IFS;
for ins_nm in "${ins_arr[@]}"
do
  execute_inject_cmd $ins_nm "${*:2}"
done
