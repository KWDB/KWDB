#!/bin/bash

# File Name: run_regression.sh
# Description: Multimodal regression test
#
# Usage:
#   bash ./run_regression.sh [arg1] [arg2] [arg3] [arg4] [arg5]
#
# Arguments:
#   arg1:<PORT>                port
#   arg2:<PARALLEL_DEGREE>     PARALLEL DEGREE
#   arg3:<OUTPUT_DIR>          output dir
#   arg4:<ACCESS_MODE>         access mode: default/tag_hash/primary_tag/rel_hash
#   arg5:<MASTER_OVERWRITE>    overwrite the master file
#
# Example:
#   bash ./run_regression.sh 26267 8 /home/inspur/actual_file default false

# Convert from kwdb time format to milliseconds
function toMs(){
    total_time="0.0"
    time_str=$1
    time=($(echo "$time_str" | grep -oE '[0-9.]+'))
    unit=($(echo "$time_str" | grep -oE '[^0-9.]+'))
    for ((j=0; j<${#time[@]}; j++)); do
        t=${time[$j]}
        u=${unit[$j]}
        if [[ "$u" == "m" ]]; then
            total_time=$(echo "$total_time + $t * 60000" | bc)
        elif [[ "$u" == "s" ]]; then
            total_time=$(echo "$total_time + $t * 1000" | bc)
        elif [[ "$u" == "ms" ]]; then
            total_time=$(echo "scale=3; $total_time + $t" | bc)
        fi
    done
    # Round to the nearest whole number and convert to an integer
    rounded_time=$(printf "%.0f" "$total_time")
    echo "$rounded_time"
}

# Convert from ms to kwdb time format
function fromMs(){
    time=$1
    ret=''
    d=$(echo "scale=0;$time/1000"|bc)
    r=$(echo "$time-$d*1000"|bc)
    ret="${r}ms"
    if [ $d -gt 0 ]; then
        r=$((d%60))
        d=$((d-r))
        d=$((d/60))
        ret="${r}s$ret"
    fi
    if [ $d -gt 0 ]; then
        ret="${d}m$ret"
    fi
    echo "$ret"
}

# Define Function: Generate SQL query
generate_query() {
  local csv_name="$1"
  local multi_model_enabled="$2"

  # Check if it is an EXPLAIN query
  if [[ "$csv_name" == EXPLAIN-* ]]; then
    original_query_name="${csv_name#EXPLAIN-}"  # Remove the "EXPLAIN-" prefix
    sql_file="${SQL_DIR}/${original_query_name}.sql"
    if [ -f "$sql_file" ]; then
      if [ "$multi_model_enabled" == "true" ]; then
        query=$(cat "$sql_file")
        if [[ $query == *"set hash_scan_mode="* ]]; then
          query=$(echo "$query" | sed 's/\(set hash_scan_mode=[0-9];\)/\1\r\nEXPLAIN /g')
        else
          query="EXPLAIN $query"  # generate EXPLAIN query
        fi
      else
        query=$(cat "$sql_file" | sed 's/\(set hash_scan_mode=[0-9];\)/set enable_multimodel=false;/g')
        if [[ $query == *"set enable_multimodel=false;"* ]]; then
          query=$(echo "$query" | sed 's/\(set enable_multimodel=false;\)/\1\r\nEXPLAIN /g')
        else
          query="EXPLAIN $query"  # generate EXPLAIN query
        fi
      fi
      echo "$query"
    else
      echo "Error: SQL file $sql_file not found for EXPLAIN." >&2
      return 1
    fi
  else
    sql_file="${SQL_DIR}/${csv_name}.sql"
    if [ -f "$sql_file" ]; then
      if [ "$multi_model_enabled" == "true" ]; then
        cat "$sql_file"
      else
        cat "$sql_file" | sed 's/\(set hash_scan_mode=[0-9];\)/set enable_multimodel=false;/g'
      fi
    else
      echo "Error: SQL file $sql_file not found." >&2
      return 1
    fi
  fi
}

# Log functions with timestamps
function log_with_timestamp {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1"
}

CUR_DIR="$(cd "$(dirname "$0")"; pwd)"
source env.sh

# Parameter 1: Port number 2: Parallelism 3: Output directory
PORT=${1:-26888}
PARALLEL_DEGREE=${2:-8}
OUTPUT_DIR=${3:-$CUR_OUTPUT_DIR}
ACCESS_MODE=${4:-default}
MASTER_OVERWRITE=${5:-false}
LOCAL_RUN=${6:-true}
SQL_FILE_FILTER=${7:-.csv}
EXCLUDE_SQL_FILES=${8}
EXEC_ROUND_NO=${9:-1}
COLLECT_PHYSICAL_PLAN=${10:-false}
CORRECTNESS_CHECK=${11:-true}
MULTI_MODEL_ENABLED=${12:-true}
DOCKER_CONTAINER_PREFIX=${13:-kwdb-pipe-test}
QUERY_TIME_FILE_NAME=${14:-query_time}

EXCLUDE_SQL_FILES_ARRAY=($EXCLUDE_SQL_FILES)


SQL_DIR="$ROOT_DIR/tests/kaiwudb-regression"
EXPECTED_DIR="$ROOT_DIR/tests/expected_regression"
ACTUAL_DIR="$OUTPUT_DIR/tests/actual"
QUERY_TIME_FILE="$OUTPUT_DIR/$QUERY_TIME_FILE_NAME.csv"

LOG_DIR="$OUTPUT_DIR/kw-db-test.log"

# Make sure the PORT exists
if [ -z "$PORT" ]; then
  log_with_timestamp "PORT for kwbase is not specified"
  exit 1
fi

if [ ! -d $OUTPUT_DIR ]; then
  log_with_timestamp "mkdir -p $OUTPUT_DIR in run_regression.sh"
  mkdir -p $OUTPUT_DIR
fi

# Define a function that maps access_mode to a numeric value
case "$ACCESS_MODE" in
  default)
    HASH_SCAN_MODE=0
    ;;
  tag_hash)
    HASH_SCAN_MODE=1
    ;;
  primary_tag)
    HASH_SCAN_MODE=2
    ;;
  rel_hash)
    HASH_SCAN_MODE=3
    ;;
  *)
    echo "Unknown access_mode: $ACCESS_MODE"
    exit 1
    ;;
esac
echo "Kwbase ACCESS_MODE set to: $HASH_SCAN_MODE($ACCESS_MODE)"

# actual output dir
rm -rf "$ACTUAL_DIR"
mkdir -p "$ACTUAL_DIR"

# Obtain all the files that match ${SQL_FILE_FILTER} in the expected_regression directory
echo "SQL_FILE_FILTER is $SQL_FILE_FILTER"
sql_file_filters=($SQL_FILE_FILTER)
csv_files=()
for sql_file in $(ls $EXPECTED_DIR/* | sort); do
  sql_file_name=$(basename "$sql_file")
  found=false
  for file_filter in "${sql_file_filters[@]}"; do
    if [[ $sql_file_name =~ "$file_filter" ]]; then
      found=true
      break
    fi
  done
  if [ "$found" == "true" ]; then
    csv_files+=("$sql_file")
  fi
done
echo "csv_files is $csv_files"

# Check if the master file is overwritten
if [[ "$MASTER_OVERWRITE" == "true" ]]; then
  for csv_file in "${csv_files[@]}"; do
    csv_file_name=$(basename "$csv_file" .csv)  # Get the CSV file name (without extension)

    # generate select query
    query=$(generate_query "$csv_file_name" "$MULTI_MODEL_ENABLED")
    if [ $? -ne 0 ]; then
      log_with_timestamp "Skipping $csv_file_name due to missing SQL file."
      continue
    fi
    # Call the function to execute the query and output it to EXPECTED_DIR
    output_file="$EXPECTED_DIR/${csv_file_name}.csv"
    # Execute the query and save the result to ACTUAL_DIR
    log_with_timestamp "Executing query for ${csv_file_name}, output to $output_file"
    docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c "export LD_LIBRARY_PATH=/home/inspur/install/lib; echo \"$query\" | /home/inspur/install/bin/kwbase sql --insecure --format=csv --set 'show_times=true' --host=127.0.0.1:26888" > "$output_file"
  done
else
  header_line="Total Time,Execution Start Time"
  run_time_line="$(date +'%Y-%m-%d %H:%M:%S')"
  physical_plan_line="N/A,N/A"
  total_time=0.0
  stopped=false
  # Loop through to find the *.sql file corresponding to each *.csv file and compare the output
  for csv_file in "${csv_files[@]}"; do
    csv_file_name=$(basename "$csv_file" .csv)
    echo "csv_file_name is $csv_file_name"
    found=false
    for exclude_sql_file in "${EXCLUDE_SQL_FILES_ARRAY[@]}"
    do
      if [[ $csv_file_name =~ "$exclude_sql_file" ]]; then
        echo "Found"
        found=true
        break;
      fi
    done
    if [ "$found" == "true" ]; then
      echo "skipping $csv_file..."
      continue
    fi

    if [ -z "${CSV_LISTS}" ]; then
      CSV_LISTS="$csv_file_name"
    else
      CSV_LISTS="$CSV_LISTS,$csv_file_name"
    fi


    query=$(generate_query "$csv_file_name" "$MULTI_MODEL_ENABLED")
    if [ $? -ne 0 ]; then
      log_with_timestamp "Skipping $csv_file_name due to missing SQL file."
      continue
    fi

    output_file="$ACTUAL_DIR/${csv_file_name}.csv"

    log_with_timestamp "Executing query for ${csv_file_name}, output to $output_file"
    analyze_html="N/A"
    if ! test -f ./stop_stress_test; then
      docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c "export LD_LIBRARY_PATH=/home/inspur/install/lib; echo \"$query\" | /home/inspur/install/bin/kwbase sql --insecure --format=csv --set 'show_times=true' --host=127.0.0.1:26888" > "$output_file" 2>&1
      time_str=$(tac "$output_file" | grep -m 1 "Time:" | awk '{print $2}')
      execution_time=$(toMs "$time_str")
      total_time=$(echo "$total_time+$execution_time" | bc)

      if [[ "$COLLECT_PHYSICAL_PLAN" == "true" ]]; then
        if ! [[ "$csv_file_name" == EXPLAIN-* ]]; then
          # collect physical plan for each query
          # Use explain analysis to obtain the Physics Plan
          if [[ $query == *"set hash_scan_mode="* ]]; then
            query=$(echo "$query" | sed 's/\(set hash_scan_mode=[0-9];\)/\1\r\nEXPLAIN ANALYZE /g')
          elif [[ $query == *"set enable_multimodel=false;"* ]]; then
            query=$(echo "$query" | sed 's/\(set enable_multimodel=false;\)/\1\r\nEXPLAIN ANALYZE /g')
          else
            query="EXPLAIN ANALYZE $query"
          fi
          json_output=$( docker exec ${DOCKER_CONTAINER_PREFIX}-$PORT sh -c "export LD_LIBRARY_PATH=/home/inspur/install/lib; /home/inspur/install/bin/kwbase sql --insecure --format=table --host=127.0.0.1:26888 -e \"$query\"")
          # Extract the value of encodePlan from JSON
          encode_plan=$(echo "$json_output" | grep -oP '(?<="encodePlan": ")[^"]+')
          # Construct the final URL
          analyze_html="<a href=\"https://cockroachdb.github.io/distsqlplan/decode.html#$encode_plan\">Link</a>"
        fi
      fi
    else
      time_str="N/A"
      stopped=true
    fi
    run_time_line="$run_time_line,$time_str"
    physical_plan_line="$physical_plan_line,$analyze_html"
    header_line="$header_line,$csv_file_name"
  done
  run_time_line="$(fromMs "$total_time"),$run_time_line"

  if [ $EXEC_ROUND_NO == "1" ]; then
    echo "$header_line" > $QUERY_TIME_FILE
  fi
  echo "$run_time_line" >> $QUERY_TIME_FILE
  if [[ "$COLLECT_PHYSICAL_PLAN" == "true" ]]; then
    echo "$physical_plan_line" >> $QUERY_TIME_FILE
  fi
  if [ "$stopped" == "true" ]; then
    echo "skipping result check because regression run is stopped, exiting..."
    exit 0
  fi

  if [[ "$CORRECTNESS_CHECK" == "true" ]]; then
    # Print the CSV files to be compared (excluding those with the "EXPLAIN-" prefix)
    log_with_timestamp "Start checking results for: $CSV_LISTS"
    all_match=true
    # Check if CSV_LISTS is empty and call compare_sord.py for comparison
    if [ -n "$CSV_LISTS" ]; then
        log_with_timestamp "Running: python3 compare_sorted.py $EXPECTED_DIR $ACTUAL_DIR $CSV_LISTS"
        # call compare_sord.py for comparison
        if python3 compare_sorted.py "$EXPECTED_DIR" "$ACTUAL_DIR" "$CSV_LISTS"; then
            log_with_timestamp "All CSV files match successfully."
        else
            log_with_timestamp "Mismatch found during comparison."
            all_match=false  # If the comparison fails, set the flag to false
        fi
    else
        log_with_timestamp "No valid CSV files found for comparison."
    fi
    log_with_timestamp "Done checking CSV_FILES"

    # Check whether the csv file with only explain contains fall back
    EXPLAIN_CSV_LISTS=$(find "$EXPECTED_DIR" -name "EXPLAIN-$SQL_FILE_FILTER" -exec basename {} .csv \; | grep -v -F -f <(echo "$CSV_LISTS" | sed 's/,/\n/g' | sed 's/^/EXPLAIN-/') | sort | paste -sd "," -)
    log_with_timestamp "Start checking EXPLAIN results for: $EXPLAIN_CSV_LISTS"
    # Traverse EXPLAIN_CSV_LISTS and check if they contain "multi-model fall back"
    if [ -n "$EXPLAIN_CSV_LISTS" ]; then
      for explain_csv in $(echo "$EXPLAIN_CSV_LISTS" | tr ',' ' '); do
        explain_file="$EXPECTED_DIR/${explain_csv}.csv"
        # Check whether "multi-model fall back" is included in the file
        if ! grep -q "multi-model fall back" "$explain_file"; then
          log_with_timestamp "WARNING: $explain_csv does NOT contain 'multi-model fall back'."
        fi
      done
    else
      log_with_timestamp "No EXPLAIN CSV files found for comparison."
    fi

    log_with_timestamp "Done checking EXPLAIN_CSV_LISTS"

    # Output the information based on the comparison results and set the exit status
    if $all_match; then
        log_with_timestamp "$All CSV files match!"
        exit 0
    else
        log_with_timestamp "Some CSV files do not match! Please review above log output"
        exit 1
    fi
  else
    echo "Skipping correctness check."
  fi
fi

exit 0
