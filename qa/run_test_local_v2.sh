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

set -e

cur_dir="$(
  cd "$(dirname "$0")"
  pwd
)"
source $cur_dir/env.sh
source $QA_DIR/util/utils.sh
testfilter=${1}
sqlfilter=${2}
shift 2
topologies=${@}

echo "---------- Config ----------"
if [ -z "$testfilter" ]; then
  echo -e -n "${INFO}TEST_case_name(default:TEST_v2_*)${NC}: "
  read testfilter
  if [ "$testfilter" = "" ]; then testfilter="TEST_v2_*"; fi
fi
echo -e "${SUCC}TEST_case_name:${testfilter}${NC}"

if [ -z "$sqlfilter" ]; then
  echo -e -n "${INFO}SQL_case(default:*.sql)${NC}: "
  read sqlfilter
  if [ "$sqlfilter" = "" ]; then sqlfilter="*.sql"; fi
fi
echo -e "${SUCC}SQL_case:${sqlfilter}${NC}"

export SQL_FILTER=$sqlfilter

echo -e "${INFO}---------- Begin run tests ----------${NC}"

testdir=("Integration" "performance")

mkdir -p $LOG_DIR
rm -f $SUMMARY_FILE
rm -f $LOG_FILE

test_start_time=$(date +%s)
test_results=()
passed_cnt=0

while true; do
  for dir in ${testdir[@]}; do
    # echo "---------- Begin run $dir test ----------"
    for ut in $(cd $QA_DIR && find $dir -executable -type f | sort); do
      utname=$(basename $ut)
      if [[ $utname != $testfilter ]]; then
        continue
      fi
      echo -e "${INFO}$(date)---------- run $utname ----------${NC}" | tee -a $LOG_FILE
      free | tee -a $LOG_FILE
      ipcs -m | tee -a $LOG_FILE
      utlog_dir=$logdir/$utname
      [ -d $utlog_dir ] && rm -rf $utlog_dir
      mkdir -p $utlog_dir
      ut_start_time=$(date +%s)
      # exec ut and rdirect all output to utname.log
      sh -c "cd $QA_DIR && $ut ${topologies}" 2>&1 | tee $utlog_dir/$utname.log
      ut_end_time=$(date +%s)
      duration=$((ut_end_time - ut_start_time))
      if ! grep "${utname}_passed" $utlog_dir/$utname.log; then
        echo -e "${FAIL}$(date) $utname Failed, can't find ${utname}_passed on the last line${NC}" | tee -a $LOG_FILE
        echo -e "${FAIL}$(date) Please check log: $utlog_dir/$utname.log${NC}" | tee -a $LOG_FILE
        tail -2 $utlog_dir/$utname.log | head -1
        test_results+=("${FAIL}$utname \t\t FAILED ($duration)s${NC}")
        test_failed_hang
        if [[ $failed_continue != "yes" ]]; then
          break
        fi
      else
        # tail -n 2 $utlog_dir/$utname.log
        echo -e "${SUCC}$(date) $utname passed${NC}" | tee -a $LOG_FILE
        test_results+=("${SUCC}$utname \t\t PASSED ($duration)s${NC}")
        passed_cnt=$((passed_cnt + 1))
      fi
    done
    if [ $passed_cnt != ${#test_results[@]} ]; then
      test_failed_hang
      if [[ $failed_continue != "yes" ]]; then
        break
      fi
    fi
  done
  if [[ $REPEAT_REGRESSION_TEST != "yes" ]]; then
    break
  fi
done

source $BIN_DIR/utils.sh

test_end_time=$(date +%s)
echo -e "${INFO}[==========local log file==========]${NC}"
print_locallog

echo -e "${INFO}[==========QA log info==========]${NC}"
if test -f $LOG_FILE; then
  cat $LOG_FILE
fi
echo -e "${INFO}[========== QA summary info ==========]${NC}"
if test -f $SUMMARY_FILE; then
  cat $SUMMARY_FILE
fi
echo -e "${INFO}========== Summary of all regression test, all results ==========${NC}"
(
  IFS=$'\n'
  echo -e "${test_results[*]}"
)
echo -e "${INFO}PASSED $passed_cnt/${#test_results[@]} ($((test_end_time - test_start_time)))s${NC}"
if [ $passed_cnt = ${#test_results[@]} ]; then
  echo "All tests passed"
else
  echo -e "${FAIL}Test failed! Will skip the rest of tests, please check $logdir${NC}"
fi

# safe exit, otherwise can't stop ci container
exit 0
