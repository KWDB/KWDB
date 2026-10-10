# Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd.  All rights reserved.
#!/bin/bash

build_kwsa_test=$2
# Memory leak check
rm -rf $1/mem-check-log.txt
cd $1/tests
memcheck_no_pass_cnt=0
for case_root in $(find $(pwd) -maxdepth 1 -type d -name "*.dir" -not -name kwdb_assert.txt); do
  case_name=$(basename ${case_root} | grep -Po ".*(?=\.dir)")
  cd $case_root
  # Perform memory leak detection for each test case, and output logs to log files
  valgrind --tool=memcheck --leak-check=full --max-threads=10000 --error-exitcode=1 --log-file=./mem-check-log-single.txt ./$case_name
  if [ $? -gt 0 ]; then
    memcheck_no_pass_cnt=$((memcheck_no_pass_cnt+1))
    echo "\nunit test: $test_file, memory leak detection failed!" >> $1/mem-check-log.txt
    # Append and save the abnormal test case log to the mem check log file
    cat ./mem-check-log-single.txt >> $1/mem-check-log.txt
    rm -rf ./mem-check-log-single.txt
  fi
  cd ..
done
cd $1

echo "\n------memcheck summary log------"
if [ $memcheck_no_pass_cnt -gt 0 ]; then
  echo "\033[31m There are $memcheck_no_pass_cnt test cases that fail the memory leak detection \033[0m"
  cat $1/mem-check-log.txt
else
  echo "memcheck pass"
fi