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
QA_DIR=${QA_DIR:-"/home/inspur/src/gitee.com/kwbasedb/qa"}
source ${QA_DIR}/tsbs_test/tsbs_env.sh

# set default values
node_num=${1:-"1"}
scales=${2:-"100,4000,100000,1000000"}
TSBS_RESULT_DIR=${3:-"${QA_DIR}/tsbs_test/tsbs_result"}
KWDB_CT_NAME=${4:-"kwdb"}
me_host_ip=${5:-"127.0.102.145"}
me_host_port=${6:-"26257"}
DATA_DIR=${7:-"${QA_DIR}/tsbs_test/data"}
# 设置默认值为false，防止变量未定义导致的错误
UPDATE_THRESHOLD=${UPDATE_THRESHOLD:-false}
IFS=',' read -ra TSBS_SCALE_LIST <<< "${scales}"
BIN_DIR=${BIN_DIR:-"/home/inspur/src/gitee.com/kwbasedb/install/bin"}
KWBIN=${KWBIN:-"${BIN_DIR}/kwbase"}
export KW_IO_MODE=1

if [ -z "$KWBIN" ]; then
    echo "KWBIN not set"
    exit 1
fi

get_git_branch() {
    local repo_dir="${1:-.}"  # optional parameter: specify repo directory (default current directory)
    cd "$repo_dir" 2>/dev/null || { echo ""; return 1; }
    local branch=$(git rev-parse --abbrev-ref HEAD 2>/dev/null)
    if [ "$branch" = "HEAD" ]; then
        branch="detached-HEAD-($(git rev-parse --short HEAD 2>/dev/null))"
    fi
    echo "$branch"
}

# auto get tsbs branch name
BRANCH_NAME=$(get_git_branch)

arch=$(uname -m)
time=`date +%Y_%m%d_%H%M%S`

loadDataDir="${DATA_DIR}/load_data"


queryDataDir="${DATA_DIR}/query_data"
thresholdDir="${TSBS_RESULT_DIR}/threshold/cluster${node_num}_insertdirect${insert_direct}_${insert_type}_wal${wal}_replica${replica_mode}_dop${parallel_degree}"
clusterSettingsDir="${QA_DIR}/tsbs_test/cluster_settings"

mkdir -p ${loadDataDir}
mkdir -p ${queryDataDir}
mkdir -p ${thresholdDir}

if [[ ! -f "$thresholdDir/TSBS_THRESHOLD.csv" && $UPDATE_THRESHOLD = false ]]; then
    echo "Threshold not found. Please run with UPDATE_THRESHOLD=true first."
    exit 1
fi

TSBS_PATH="${QA_DIR}/tsbs_test/bin"
echo "check kaiwudb cluster node status"

all_nodes_ready=false
while [[ ${all_nodes_ready} == "false" ]]; do
  all_nodes_ready=true
  for ((i=1;i<=${node_num};i++))
  do
      ((line=1+$i))
      node_status=$($KWBIN node status --insecure --host=${me_host_ip}:${me_host_port} | awk 'NR=='$line'{print $11}')
      node_ip=$($KWBIN node status --insecure --host=${me_host_ip}:${me_host_port} | awk 'NR=='$line'{split($2, ip_port, ":"); print ip_port[1]}')
      echo "node ${i} available status is ${node_status}"
      if [ $i -eq 1 ]; then
          ip1=${node_ip}
          echo "node ${i} ip is ${node_ip}"
      fi
      if [ $i -eq 2 ]; then
          ip2=${node_ip}
          echo "node ${i} ip is ${node_ip}"
      fi
      if [ $i -eq 3 ]; then
          ip3=${node_ip}
          echo "node ${i} ip is ${node_ip}"
      fi

      if [ -z "${node_status}" ]; then
          echo "node status is null, exit test"
          all_nodes_ready=false
      elif [ ${node_status} != "true" ]; then
          echo "node status is false, exit test"
          all_nodes_ready=false
      fi
  done
  if [[ ${all_nodes_ready} == "false" ]];then
    sleep 0.1
  fi
done


if [ ! -d "$TSBS_PATH" ];then
    echo "TSBS_PATH not found"
    exit 1
fi

for scale in ${TSBS_SCALE_LIST[@]}; do

    queryResultDir="${TSBS_RESULT_DIR}/${KWDB_CT_NAME}/${time}_${BRANCH_NAME}_scale${scale}_cluster${node_num}_insertdirect${insert_direct}_${insert_type}_wal${wal}_replica${replica_mode}_dop${parallel_degree}/query_result"
    loadResultDir="${TSBS_RESULT_DIR}/${KWDB_CT_NAME}/${time}_${BRANCH_NAME}_scale${scale}_cluster${node_num}_insertdirect${insert_direct}_${insert_type}_wal${wal}_replica${replica_mode}_dop${parallel_degree}/load_result"
    mkdir -p ${loadResultDir}
    mkdir -p ${queryResultDir}

    echo "testing scale ${scale}"
    if [ ${scale} -eq 100 ]; then
        load_ts_end="2016-02-01T00:00:00Z"
        echo "generate 31 days data"
    elif [ ${scale} -eq 4000 ]; then
        load_ts_end="2016-01-05T00:00:00Z"
        echo "generate 4 days data"
    elif [ ${scale} -eq 100000 ]; then
        load_ts_end="2016-01-01T03:00:00Z"
        echo "generate 3 hours data"
    elif [ ${scale} -eq 1000000 ]; then
        load_ts_end="2016-01-01T00:03:00Z"
        echo "generate 3 minutes data"
    fi
    #query test parameters
    query_ts_start="2016-01-01T00:00:00Z"
    query_ts_end="2016-01-05T00:00:01Z"

    if [ ${scale} -eq 100000 ]; then
    query_ts_end="2016-01-01T15:00:01Z"
    elif [ ${scale} -eq 1000000 ] || [ ${scale} -eq 10000000 ]; then
    query_ts_end="2016-01-01T12:04:01Z"
    else
    echo "use defalut query end time"
    fi

    if [ ! -f "${clusterSettingsDir}/general.sql" ]; then
        echo "clusterSettingsDir not found"
        exit 1
    fi
    $KWBIN sql --host=${me_host_ip} --port=${me_host_port} --insecure < ${clusterSettingsDir}/general.sql > /dev/null 2>&1

    if [ -f "${clusterSettingsDir}/scale${scale}.sql" ]; then
        $KWBIN sql --host=${me_host_ip} --port=${me_host_port} --insecure < ${clusterSettingsDir}/scale${scale}.sql > /dev/null 2>&1
    fi
    load_data=${loadDataDir}/${format}/${tsbs_case}_${format}_scale_${scale}_${load_workers}order.dat
    mkdir -p ${loadDataDir}/${format}
    if [ ! -f "${load_data}" ]; then
        echo start to generate load data
        LD_LIBRARY_PATH=${TSBS_PATH}/lib ${TSBS_PATH}/tsbs_generate_data_${arch} \
            --format=${format} \
            --use-case=${tsbs_case} \
            --seed=123 \
            --scale=${scale} \
            --log-interval=${load_interval} \
            --timestamp-start=${load_ts_start} \
            --timestamp-end=${load_ts_end} \
            --orderquantity=${load_workers} > ${load_data}
        #sleep 10
    else
        echo load data already exists
    fi

    #region load data
    echo start to load data
    LD_LIBRARY_PATH=${TSBS_PATH}/lib ${TSBS_PATH}/tsbs_load_kwdb_${arch} \
      --file=${load_data} \
      --user=root \
      --pass=1234 \
      --host=${me_host_ip} \
      --port=${me_host_port} \
      --insert-type=${insert_type} \
      --db-name=${db_name} \
      --partition=false \
      --batch-size=${load_batchsizes} \
      --case=${tsbs_case} \
      --workers=${load_workers} > ${loadResultDir}/${tsbs_case}_${format}_scale_${scale}.log

    if [ "${UPDATE_THRESHOLD}" = "true" ]; then
        python3 ${QA_DIR}/tsbs_test/update_threshold.py -v tsbs -p ${time} -f ${format} -s ${scale} -n load -w ${query_workers} -t ${query_times} -r ${loadResultDir}/${tsbs_case}_${format}_scale_${scale}.log -d ${thresholdDir} -o ${parallel_degree}
        if [ $? = 1 ]; then
            echo "update threshold failed"
            exit 1
        fi
    else
        python3 ${QA_DIR}/tsbs_test/record_result.py -v tsbs -p ${time} -f ${format} -s ${scale} -n load -w ${query_workers} -t ${query_times} -r ${loadResultDir}/${tsbs_case}_${format}_scale_${scale}.log -d ${queryResultDir} -o ${parallel_degree} -c ${thresholdDir}
        if [ $? = 1 ]; then
            echo "record load result failed"
            exit 1
        fi
    fi

    #endregion

    #region query data
    if [ -f "${clusterSettingsDir}/after_load_scale${scale}.sql" ]; then
        $KWBIN sql --host=${me_host_ip} --port=${me_host_port} --insecure < ${clusterSettingsDir}/after_load_scale${scale}.sql > /dev/null 2>&1
    fi

    for QUERY_TYPE in ${QUERY_TYPES_SIMPLE}; do
        query_data=${queryDataDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_query_times${query_times}.dat
        if [ ! -f "${query_data}" ]; then
            LD_LIBRARY_PATH=${TSBS_PATH}/lib ${TSBS_PATH}/tsbs_generate_queries_${arch} \
                --format=${format} \
                --use-case=${tsbs_case} \
                --seed=123 \
                --scale=${scale} \
                --query-type=${QUERY_TYPE} \
                --queries=${query_times} \
                --timestamp-start=${query_ts_start} \
                --timestamp-end=${query_ts_end} \
                --db-name=${db_name} | gzip > ${query_data}.gz

            gunzip ${query_data}.gz
            #sleep 5
        else
            echo ${QUERY_TYPE} query data already exists 
        fi
        query_result=${queryResultDir}/${format}_scale${scale}_${tsbs_case}_${QUERY_TYPE}_worker${query_workers}.log
        LD_LIBRARY_PATH=${TSBS_PATH}/lib ${TSBS_PATH}/tsbs_run_queries_kwdb_${arch} \
            --file=${query_data} \
            --user=root \
            --pass=1234 \
            --host=${me_host_ip} \
            --port=${me_host_port} \
            --query-type=${QUERY_TYPE} \
            --workers=${query_workers} > ${query_result}
        
        if [ "${UPDATE_THRESHOLD}" = "true" ]; then
            python3 ${QA_DIR}/tsbs_test/update_threshold.py -v tsbs -p ${time} -f ${format} -s ${scale} -n ${QUERY_TYPE} -w ${query_workers} -t ${query_times} -r ${query_result} -d ${thresholdDir} -o ${parallel_degree}
            if [ $? = 1 ]; then
                echo "update threshold failed"
                exit 1
            fi
        else
            python3 ${QA_DIR}/tsbs_test/record_result.py -v tsbs -p ${time} -f ${format} -s ${scale} -n ${QUERY_TYPE} -w ${query_workers} -t ${query_times} -r ${query_result} -d ${queryResultDir} -o ${parallel_degree} -c ${thresholdDir}
            if [ $? = 1 ]; then
                echo "record result ${QUERY_TYPE} failed"
                exit 1
            fi
        fi
    done
    #endregion
done


