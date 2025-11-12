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

source $QA_DIR/util/utils.sh

topology=${1}

sql_file=${2}
output_file=${3}
store_dir=${4}
output_cmd_file=${5}
open_source=${6}

node_annotation_regex='^\s*--\s*node:\s*(.+)'
exec_node_sed_regex='^\s*--[-[[:space:]]]*node:\s*(.+)'

declare -a stmts

python3 $QA_DIR/util/distribute_regression.py ${sql_file} ${KWBIN} ${output_file} ${store_dir} ${output_cmd_file} ${open_source}

rm -rf ${LOG_DIR}/${SQL_FILTER}.log
while read -r line || [ -n "${line}" ];do
  stmts+=(${line})
  #echo '***************************'
  echo "$line" >> ${LOG_DIR}/${SQL_FILTER}.log
  eval "$line"
done < ${output_cmd_file}

  #[ -n "${last_section[0]}" -a -n "${last_section[1]}" ] && \
  #stmts+=("--node:${last_section[0]}\n${last_section[1]}")

  #for idx in ${!stmts[@]};do
  #stmt=${stmts[${idx}]}
  # echo $stmt
  #if [ -n "${stmt}" ];then
  #  nodes=$(echo -e "${stmt}" | sed -n -re "s/${node_annotation_regex}/\1/p")
  #  clean_stmt="$(echo -e "${stmt}" | grep -v -P '^\s*--.*')"
  #  echo "=========="
  #  echo ${nodes}
  #  echo "########" ${clean_stmt}
  #  echo "=========="
  #  for node in ${nodes};do
  #    ${KWBIN} sql --host=$(get_listen_addr ${node}) --insecure \
  #      --format=table --set "errexit=false" --echo-sql -e "${clean_stmt}" \
  #      2>&1 | tee -a ${output_file} > /dev/null
  #  done
  #fi
  #done
