#!/bin/bash -x
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

function execute_regression_sql_basic_v2() {
  local sqldirs=("basic_v2")

  test_start_time=$(date +%s)
  passed_cnt=0
  total_cnt=0

  for topology in ${@}; do
    echo_info "Test topology: ${topology}"

    $QA_DIR/util/setup_basic_v2.sh ${topology}
    if [ $? = 1 ]; then
      echo_err "setup ${topology} failed"
      return 1
    fi
    # rm -fr $QA_DIR/TEST_integration
    python3 $(dirname $0)/execute_regression_sql_v2.py -t ${topology} -d ${sqldirs[*]} -f $SQL_FILTER -s $SUMMARY_FILE

    read passed failed total <<<"$(tail -n1 $SUMMARY_FILE | awk '{print $2,$3,$4}')"
    passed_cnt=$((passed_cnt + passed))
    total_cnt=$((total_cnt + total))

    $QA_DIR/util/stop_basic_v2.sh ${topology}
    if [ $? = 1 ]; then
      echo_err "shutdown ${topology} failed"
      return 1
    fi
  done
  test_end_time=$(date +%s)
  echo -e "${INFO}$0 PASSED ${passed_cnt}/${total_cnt} ($((test_end_time - test_start_time)))s${NC}" | tee -a $SUMMARY_FILE
  if [ $passed_cnt = ${total_cnt} ]; then
    echo -e "${SUCC}All sql tests passed${NC}"
  else
    echo -e "${FAIL}Test failed, please check the diff files!${NC}" | tee -a $SUMMARY_FILE
    return 1
  fi
}

function execute_regression_sql_cluster_v2() {
  local sqldirs=("cluster_v2")

  test_start_time=$(date +%s)
  test_results=()
  passed_cnt=0

  for topology in ${@}; do
    echo_info "Test topology: ${topology}"
    $QA_DIR/util/setup_basic_v2.sh ${topology}
    if [ $? = 1 ]; then
      echo_err "setup ${topology} failed"
      return 1
    fi

    for dir in ${sqldirs[@]}; do
      # echo "---------- Begin execute $dir sqls ----------"
      mkdir -p $QA_DIR/TEST_integration/$dir/
      for sql_file_path in $(cd $QA_DIR && find Integration/$dir -type f -name *.sql | sort); do
        ut_start_time=$(date +%s)
        sql_file=$(basename $sql_file_path)
        if [[ $sql_file != $SQL_FILTER && $sql_file != "aa_init_replication_service.sql" ]]; then
          continue
        fi
        echo_info "execute ${topology} $sql_file_path"
        # use ksql exec sql file and write all output to utname.log
        test_file=$QA_DIR/Integration/$dir/$sql_file
        out_file=$QA_DIR/TEST_integration/$dir/${sql_file}_${topology}.out
        master_file=$QA_DIR/Integration/$dir/master/${sql_file}_${topology}.master
        diff_file=$QA_DIR/Integration/$dir/master/${sql_file}_${topology}.diff
        rm -f diff_file
        if [[ $OVERWRITE_MASTER == "yes" ]]; then
          out_file=$master_file
        fi
        rm -fr $out_file
        $QA_DIR/util/cluster_exec_v2.sh ${topology} $test_file $out_file
        ut_end_time=$(date +%s)
        duration=$((ut_end_time - ut_start_time))
        if [ ! -e $master_file ]; then
          echo "" >>$master_file
        fi
        $QA_DIR/util/master_compare_v2.sh $master_file $out_file $diff_file
        if [ 0 -ne $? ] || [ -s $diff_file ]; then
          echo_err "$(date)---------- finish ${topology} $sql_file_path FAILED"
          echo_err "$sql_file failed"
          test_results+=("${FAIL}${sql_file}_${topology} \t\t FAILED ($duration)s${NC}")
          echo "Difference between $master_file and $out_file" >>$SUMMARY_FILE
          cat $diff_file >>$SUMMARY_FILE
          # cat $diff_file
        else
          echo_succ "finish ${topology} $sql_file_path SUCCEED"
          test_results+=("${SUCC}${sql_file}_${topology} \t\t PASSED ($duration)s${NC}")
          passed_cnt=$((passed_cnt + 1))
          rm -f $diff_file
        fi
      done
    done
  done
  test_end_time=$(date +%s)
  echo -e "${INFO}$0 PASSED $passed_cnt/${#test_results[@]} ($((test_end_time - test_start_time)))s${NC}" | tee -a $SUMMARY_FILE
  (
    IFS=$'\n'
    echo -e "${test_results[*]}"
  ) | tee -a $SUMMARY_FILE
  if [ $passed_cnt = ${#test_results[@]} ]; then
    echo -e "${SUCC}All sql tests passed${NC}"
  else
    echo -e "${FAIL}Test failed, please check the diff files!${NC}" | tee -a $SUMMARY_FILE
    return 1
  fi
}

function execute_regression_sql_distribute_v2() {
  local sqldirs=("cluster_v2")

  test_start_time=$(date +%s)
  test_results=()
  passed_cnt=0

  for topology in ${@};do
    echo_info "Test topology: ${topology}"



    for dir in ${sqldirs[@]};
    do
      # echo "---------- Begin execute $dir sqls ----------"
      mkdir -p $QA_DIR/TEST_integration/$dir/
      for sql_file_path in `cd $QA_DIR && find Integration/$dir -type f -name *.sql | sort`
        do

          ut_start_time=$(date +%s)
          sql_file=$(basename $sql_file_path)
          if [[ $sql_file != $SQL_FILTER && $sql_file != "aa_init_replication_service.sql" ]];then
            continue
          fi

          echo '==============================='
          echo $sql_file_path
          echo $SQL_FILTER
          echo $dir
          echo '==============================='

          pkill -9 kwbase
          sleep 10s
          echo 'before umount'
          find "${DEPLOY_ROOT}" -type d -exec findmnt {} \;
          echo 'after umount'
          find "${DEPLOY_ROOT}" -type d -exec findmnt {} \; -exec umount -l {} \;
          find "${DEPLOY_ROOT}" -type d -exec findmnt {} \;

          rm -rf $QA_DIR/../install/deploy/

          ls -al $QA_DIR/../install/

          $QA_DIR/util/setup_basic_v2.sh ${topology}
          if [ $? = 1 ]; then
            echo_err "setup ${topology} failed"
            return 1
          fi

          tar -zxvf $QA_DIR/Integration/$dir/tsdb1.tar.gz -C  $QA_DIR/../install/deploy/extern/

          echo_info "execute ${topology} $sql_file_path"
          # use ksql exec sql file and write all output to utname.log
          test_file=$QA_DIR/Integration/$dir/$sql_file
          out_file=$QA_DIR/TEST_integration/$dir/${sql_file}_${topology}.out
          master_file=$QA_DIR/Integration/$dir/master/${sql_file}_${topology}.master
          diff_file=$QA_DIR/Integration/$dir/master/${sql_file}_${topology}.diff
          rm -f diff_file
          if [[ $OVERWRITE_MASTER == "yes" ]]; then
            out_file=$master_file
          fi
          rm -fr $out_file
          storedir=$QA_DIR/../install/deploy

          $QA_DIR/util/distribute_exec_v2.sh $topology $test_file $out_file $storedir $QA_DIR/Integration/$dir/master/${sql_file}.sh
          ut_end_time=$(date +%s)
          duration=$((ut_end_time-ut_start_time))
          if [ ! -e $master_file ]; then
            echo "" >> $master_file
          fi
          $QA_DIR/util/master_compare_v2.sh $master_file $out_file $diff_file

          current_time=$(date +"%Y-%m-%d-%H-%M-%S")

          if [ 0 -ne $? ] || [ -s $diff_file ]; then
            res='FAIL'
            mkdir -p $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}
            echo_err "$(date)---------- finish ${topology} $sql_file_path FAILED"
            echo_err "$sql_file failed"
            test_results+=("${FAIL}${sql_file}_${topology} \t\t FAILED ($duration)s${NC}")
            echo "Difference between $master_file and $out_file" >> $SUMMARY_FILE
            cat $diff_file >> $SUMMARY_FILE
            cat $out_file > $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}/${sql_file}_${topology}_$folder.out
            cat $diff_file > $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}/${sql_file}_${topology}_$folder.diff
            # cat $diff_file
#            return 1
          else
            res='SUCC'
            mkdir -p $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}
            echo_succ "finish ${topology} $sql_file_path SUCCEED"
            test_results+=("${SUCC}${sql_file}_${topology} \t\t PASSED ($duration)s${NC}")
            passed_cnt=$((passed_cnt+1))
            cat $out_file > $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}/${sql_file}_${topology}_$folder.out
            cat $diff_file > $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}/${sql_file}_${topology}_$folder.diff

            rm -f $diff_file
          fi

          for folder in `ls $storedir`; do
          	tar -cvzPf $QA_DIR/TEST_integration/$dir/${sql_file}/$current_time-${res}/${sql_file}_${topology}_$folder.logs.tar.gz -C $storedir/$folder logs
          done

          $QA_DIR/util/stop_basic_v2.sh ${topology}
          if [ $? = 1 ]; then
            echo_err "shutdown ${topology} failed"
            return 1
          fi
          if [ "$res" = "FAIL" ]; then
            return 1
          fi
        done



    done

  done
  test_end_time=$(date +%s)
  echo -e "${INFO}$0 PASSED $passed_cnt/${#test_results[@]} ($((test_end_time-test_start_time)))s${NC}" | tee -a $SUMMARY_FILE
  ( IFS=$'\n'; echo -e "${test_results[*]}" ) | tee -a $SUMMARY_FILE
  if [ $passed_cnt = ${#test_results[@]} ]; then
    echo -e "${SUCC}All sql tests passed${NC}"
  else
    echo -e "${FAIL}Test failed, please check the diff files!${NC}" | tee -a $SUMMARY_FILE
    return 1
  fi
}
