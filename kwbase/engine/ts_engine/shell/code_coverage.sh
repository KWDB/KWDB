
#!/bin/bash

build_kwsa_test=$2

COVERAGE_FILE=coverage.info
REPORT_FOLDER=coverage_report

# Generate corresponding. gcda files for source files that have not been covered by testing
for gcno_file in `find $1/CMakeFiles -name '*.gcno'`
do
  gcda_file=${gcno_file%.*}.gcda
  if [ ! -f "$gcda_file" ]; then
    touch $gcda_file
  fi
done

# Collect coverage files generated after running test files and output them to$ {COVERAGE_FILE}_tmp
lcov --rc lcov_branch_coverage=1 -c -d $1 -o ${COVERAGE_FILE}_tmp
# Filter out$ {COVERAGE_FILE}_tmp Output source file paths and information that do not require attention to ${COVERAGE-FILE}
lcov --rc lcov_branch_coverage=1 -r ${COVERAGE_FILE}_tmp "*/third_party/*" "*/test/*" "*/tests/*" "*gtest*" "*8.5.0*" "*/9/*" "*v1*" "/usr/include/*" "*/kwsa/*" "*/roachpb/*" "*/cm_trace_plugin.cpp" "*/nanobench.h" "*/mmap/*" "*/impexp/*" -o ${COVERAGE_FILE}
# Generate an HTML file for coverage statistics based on the coverage information of the ${COVERAGE-FILE} file, and output it to ${REPORT-FOLEDER}
genhtml --rc genhtml_branch_coverage=1 ${COVERAGE_FILE} -o ${REPORT_FOLDER}
rm -rf ${COVERAGE_FILE}_tmp
# rm -rf ${COVERAGE_FILE}

# In a non pipeline environment, when executing make cov, the default browser will automatically open the generated code coverage report
if [ -z "$AGILE_PIPELINE_BUILD_ID" ]; then
  # Determine whether to generate an HTML file
  if [ -f ./coverage_report/index.html ]; then
    if [ "$(uname)" = "Darwin" ]; then
      # In the Darwin environment, the browser automatically opens the HTML file
      nohup open ./coverage_report/index.html &
    elif [ "$(expr substr $(uname -s) 1 5)" = "Linux" ]; then
      # In Linux environment, the browser automatically opens HTML files
      option=""
      if [ `whoami` = "root" ]; then
        option="--no-tbl_sub_path"
      fi
      if [ $KWDB_BROWSER_PATH ]; then
        echo "kwdb_path"
        nohup $KWDB_BROWSER_PATH $option ./coverage_report/index.html &
      elif [ -f /usr/bin/google-chrome-stable ]; then
        nohup /usr/bin/google-chrome-stable $option ./coverage_report/index.html &
      elif [ -f /usr/bin/firefox ]; then
        nohup /usr/bin/firefox $option ./coverage_report/index.html &
      fi
    fi
  fi
fi
