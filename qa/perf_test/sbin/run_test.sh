#!/bin/bash

# file name: run_test.sh
# Description: Automate the execution of tests,
# including environment preparation, Flask service detection,
# performance detection, final processing, etc.

# 1. export environment variables
source "$(dirname "$0")/env.sh"

# Define the log directory and file path
LOG_DIR="$WORKSPACE_DIR/log/$UUID_PATH"
LOG_FILE="$LOG_DIR/run_perf_test.log"

# Ensure that the log directory exists
mkdir -p "$LOG_DIR"


# Global variables can be set by users through command-line parameters
OUTPUT_DIR=$CUR_OUTPUT_DIR
QUERY_TIMES=""
PARALLEL_DEGREE=8
PORT=26888
FORCE_CLEAN=false
TEST_RUN_TIME="6000"  # default 6000 second
CONCURRENT_RUNS=5
REGRESSION_RUNS=5
ACCESS_MODE="default"
BUFFER_POOL_SIZE=8192
CORRECTNESS_CHECK=false
MASTER_OVERWRITE=false
LOCAL_RUN=true
SQL_FILE_FILTER=".csv"
EXCLUDE_SQL_FILES=""
GENERATE_DATABASE=false
BACKUP_DATABASE=false
BACKUP_DATABASE_PATH="/data2/workspace/kwbase-data-backup"
COLLECT_PHYSICAL_PLAN=false
MULTI_MODEL_ENABLED=true
SPECIAL_OPTIMIZATION_MODE=1100
KEEP_DOCKER_CONTAINER=false
STRESS_WITH_CHAOS=false
DOCKER_CONTAINER_PREFIX=kwdb-pipe-test
HASH_SCAN_MODE_SETTING_NAME=ts.sql.hash_scan_mode

# help message
function show_help() {
    echo "Usage: run_test.sh [command] [options]"
    echo ""
    echo "Commands:"
    echo "  help               Show this help message"
    echo "  prep_env           Prepare insert/load test environment"
    echo "  run_flask          Check or update the Flask service"
    echo "  run_kw_start       Start kwbase in docker environment"
    echo "  run_kw             Run kwbase performance test"
    echo "  finalize           Finalize the test results"
    echo "  run_regression     Run regression tests to check query correctness"
    echo "  run_stress         Run the stress test, including regression and performance tests"
    echo "  run_perf           Run all steps sequentially, including prep_env, run_flask, run_kw_start, run_kw, finalize"
    echo "  run_stress_test    Run the complete stress test, including prep_env, run_flask, run_kw_start, and stress testing"
    echo ""
    echo "Options:"
    echo "  -o <output_dir>             Set output directory"
    echo "  -N <query_times>            Set the number of times each query should run"
    echo "  -P <parallel_degree>        Set kwbase parallel degree (default: 8)"
    echo "  -p <port>                   Set kwbase port number (default: 26888)"
    echo "  -r <run_regression_counts>  Set the number of concurrent regression tests for complete stress test only (default: 5)"
    echo "  -R <run_perf_counts>        Set the number of concurrent preformance tests for complete stress test only (default: 5)"
    echo "  -S <test_run_time>        Set the total time to run the test (in seconds)"
    echo "  -a <access_mode>            Set access mode (default: default)"
    echo "  -B <buffer_pool_size>       Set Buffer pool size for kwbasedb"
    echo "  -C <true/flase>             Set if preformance tests will do correctness check"
    echo "  -F <true/false>             Force clean kwbase-data before running prep_env"
    echo "  -M <true/false>             Force overwrite regression master files"
    echo "  -L <true/false>             Run the tests locally"
    echo "  -f <sql_file_filter>        SQL file filter, the test will only run matched sqls"
    echo "  -G <generate_database>      Generate database and load data into it if true, otherwise will restore the database from backup database"
    echo "  -b <backup_database>        Backup the database generated, valid only if GENERATE_DATABASE is true"
    echo "  -D <backup_database_path>   The path to backup/restore database"
    echo "  -E <exclude_sql_files>      Exclude the files to run regression test"
    echo "  -c <collect_physical_plan>  Collect the physical plans in regression test if true"
    echo "  -m <multi_model_enabled>    Enable multi model optimization"
    echo "  -O <stress_with_chaos>      Enable chaos for stress test"
    echo ""
    echo "Example:"
    echo "  run_test.sh run_perf -o '/data1/workspace/output' -N 10 -t '700 40000 12000' -P 8 -p 26888 -C true"
    echo "  run_test.sh run_stress_test -o '/data1/workspace/output' -N 10 -t '700 40000 12000' -P 8 -p 26888 -R 5 -S 10800"
    echo "  run_test.sh run_stress -o '/data1/workspace/output' -N 10 -t '700 40000 12000' -P 8 -p 26888 -R 5 -S 10800"
    echo "  run_test.sh run_regression -p 26888 -P 8 -o '/data1/workspace/output' -a default -M true"
}

function check_required_parameters() {
    if [ -z "$QUERY_TIMES" ]; then
        echo "Error: QUERY_TIMES must be set."
        print_final_values
        show_help
        exit 1
    fi
    if { [ "$COMMAND" = "run_stress_test" ] || [ "$COMMAND" = "run_stress" ]; } && [ -z "$TEST_RUN_TIME" ]; then
        echo "Error: TEST_RUN_TIME must be set for $COMMAND."
        print_final_values
        show_help
        exit 1
    fi
}

# Print the title character drawing of the module
function print_header() {
    local header="$1"
    local border="****************************************"
    local padding=$(( (40 - ${#header}) / 2 ))
    printf "%s\n" "$border"
    printf "%*s%s%*s\n" "$padding" "" "$header" "$padding" ""
    printf "%s\n" "$border"
}

# Print the final value of the variable
function print_final_values() {
    echo "====== Final values: ======"
    echo "QUERY_TIMES: $QUERY_TIMES"
    echo "PARALLEL_DEGREE: $PARALLEL_DEGREE"
    echo "PORT: $PORT"
    echo "HTTP_PORT: $HTTP_PORT"
    echo "FORCE_CLEAN: $FORCE_CLEAN"
    echo "OUTPUT_DIR: $OUTPUT_DIR"
    echo "ACCESS_MODE: $ACCESS_MODE"
    echo "Test Docker Image: $TEST_IMAGE_NAME:$TEST_IMAGE_TAG"
    echo "CODE_PLATFORM: $CODE_PLATFORM"
    echo "BUFFER_POOL_SIZE: $BUFFER_POOL_SIZE"
    echo "TEST_RUN_TIME: $TEST_RUN_TIME"
    echo "CORRECTNESS_CHECK: $CORRECTNESS_CHECK"
    echo "MASTER_OVERWRITE: $MASTER_OVERWRITE"
    echo "LOCAL_RUN: $LOCAL_RUN"
    echo "SQL_FILE_FILTER: $SQL_FILE_FILTER"
    echo "EXCLUDE_SQL_FILES: $EXCLUDE_SQL_FILES"
    echo "GENERATE_DATABASE: $GENERATE_DATABASE"
    echo "BACKUP_DATABASE: $BACKUP_DATABASE"
    echo "BACKUP_DATABASE_PATH: $BACKUP_DATABASE_PATH"
    echo "COLLECT_PHYSICAL_PLAN: $COLLECT_PHYSICAL_PLAN"
    echo "MULTI_MODEL_ENABLED: $MULTI_MODEL_ENABLED"
    echo "SPECIAL_OPTIMIZATION_MODE: $SPECIAL_OPTIMIZATION_MODE"
    echo "KEEP_DOCKER_CONTAINER: $KEEP_DOCKER_CONTAINER"
    echo "STRESS_WITH_CHAOS: $STRESS_WITH_CHAOS"
    echo "DOCKER_CONTAINER_PREFIX: $DOCKER_CONTAINER_PREFIX"
    echo ""
    echo "==========================="
}

function print_errlog() {
    # show errlog.log which might contain crash callstack
    if [ -f "../../../../src/gitee.com/kwbasedb/install/bin/kwbase-data/logs/errlog.log" ]; then
        echo "errlog.log: "
        cat ../../../../src/gitee.com/kwbasedb/install/bin/kwbase-data/logs/errlog.log
    else
        echo "errlog.log doesn't exist."
    fi
}

# Parse command-line parameters
while [[ $# -gt 0 ]]; do
    key="$1"

    case $key in
        -o|--output_dir)
        OUTPUT_DIR="$2"
        shift # past argument
        shift # past value
        ;;
        -O|--stress_with_chaos)
        if [ "$2" = "true" ]; then
            STRESS_WITH_CHAOS=true
            DOCKER_CONTAINER_PREFIX=kwbase-chaos-test
        fi
        shift
        shift
        ;;
        -N|--query_times)
        QUERY_TIMES="$2"
        shift
        shift
        ;;
        -H|--hash_scan_mode_setting_name)
        HASH_SCAN_MODE_SETTING_NAME="$2"
        shift
        shift
        ;;
        -P|--parallel_degree)
        PARALLEL_DEGREE="$2"
        shift
        shift
        ;;
        -p|--port)
        PORT="$2"
        shift
        shift
        ;;
        -a|--access_mode)
        ACCESS_MODE="$2"
        shift
        shift
        ;;
        -r|--run_regression_counts)
        REGRESSION_RUNS="$2"
        shift
        shift
        ;;
        -R|--run_perf_counts)
        CONCURRENT_RUNS="$2"
        shift
        shift
        ;;
        -C|--correctness_check)
        if [ "$2" = "true" ]; then
            CORRECTNESS_CHECK=true
        fi
        shift
        shift
        ;;
        -c|--collect_physical_plan)
        if [ "$2" = "true" ]; then
            COLLECT_PHYSICAL_PLAN=true
        fi
        shift
        shift
        ;;
        -M|--master_overwrite)
        if [ "$2" = "true" ]; then
            MASTER_OVERWRITE=true
        fi
        shift
        shift
        ;;
        -m|--multi_model_enabled)
        if [ "$2" = "false" ]; then
            MULTI_MODEL_ENABLED=false
        fi
        shift
        shift
        ;;
        -s|--special_optimization_mode)
        SPECIAL_OPTIMIZATION_MODE="$2"
        shift
        shift
        ;;
        -L|--local_run)
        if [ "$2" = "false" ]; then
            LOCAL_RUN=false
        fi
        shift
        shift
        ;;
        -K|--keep_docker_container)
        if [ "$2" = "true" ]; then
            KEEP_DOCKER_CONTAINER=true
        fi
        shift
        shift
        ;;
        -E|--exclude_sql_files)
        EXCLUDE_SQL_FILES="$2"
        shift
        shift
        ;;
        -f|--sql_file_filter)
        SQL_FILE_FILTER="$2"
        shift
        shift
        ;;
        -F|--force_clean)
        if [ "$2" = "true" ]; then
            FORCE_CLEAN=true
        fi
        shift
        shift
        ;;
        -G|--generate_database)
        if [ "$2" = "true" ]; then
            GENERATE_DATABASE=true
        fi
        shift
        shift
        ;;
        -b|--backup_database)
        if [ "$2" = "true" ]; then
            BACKUP_DATABASE=true
        fi
        shift
        shift
        ;;
        -D|--backup_database_path)
        BACKUP_DATABASE_PATH="$2"
        shift
        shift
        ;;
        -B|--buffer-pool-size)
        BUFFER_POOL_SIZE="$2"
        shift
        shift
        ;;
        -S|--test_run_time)
        TEST_RUN_TIME="$2"
        shift
        shift
        ;;
        help)
        echo "print help"
        show_help
        exit 0
        ;;
        prep_env|run_flask|run_kw|finalize|run_perf|run_stress_test|run_kw_start|run_regression|run_parallel_regression|run_stress)
        COMMAND="$1"
        shift
        ;;
        *)
        echo "Unknown option $1"
        show_help
        exit 1
        ;;
    esac
done

# Check if KWBASE_BIN exists
if [ ! -f "$KWBASE_BIN" ]; then
  echo "Error: KWBASE_BIN not found at $KWBASE_BIN. Please ensure kwbase is installed at the specified path." >&2
  exit 1
fi

# Handling Commands
case $COMMAND in
    prep_env)
        print_header "Start Preparing Environment"
        print_final_values
        # Run the insertion/loading test environment preparation function
        echo "Preparing insert/load test environment..."
        if [ "$FORCE_CLEAN" = true ]; then
            echo "FORCE_CLEAN is $FORCE_CLEAN, will check if $PORT is in use and clean $BIN_DIR/kwbase-data"
            # Check if there is any process occupying $PORT
            PORT_OCCUPIED=$(lsof -i :$PORT | grep LISTEN | awk '{print $2}')
            if [ -n "$PORT_OCCUPIED" ]; then
                echo "Port $PORT is currently in use by process $PORT_OCCUPIED, will exit now."
                exit 1
            else
                echo "Port $PORT is not in use."
            fi

            # After confirming that the port is released, delete the kwbase-data directory
            if [ -d "$BIN_DIR/kwbase-data" ]; then
                echo "Removing existing kwbase-data directory..."
                rm -rf "$BIN_DIR/kwbase-data"
            fi
        else
            echo "FORCE_CLEAN is not set. Skipping cleaning of kwbase-data."
            # After confirming that the port is released, delete the kwbase-data directory
            if [ -d "$BIN_DIR/kwbase-data" ]; then
                echo "$BIN_DIR/kwbase-data directory exists, skipping prep_env.sh..."
                exit 0
            fi
        fi
        CUR_DIR="$(cd "$(dirname "$0")"; pwd)"
        # perf_test root dir
        ROOT_DIR=${CUR_DIR}/..
        GOPATH_DIR=$(realpath "${ROOT_DIR}/../../..")
        KWBASE_DIR=${GOPATH_DIR}/src/gitee.com/kwbasedb
        KWBASE_BIN_PATH=$(realpath "${KWBASE_DIR}/install/bin")

        if [ "$GENERATE_DATABASE" != true ]; then
            # restore database from backup database path
            rm -rf $KWBASE_BIN_PATH/kwbase-data
            cp -r $BACKUP_DATABASE_PATH/kwbase-data $KWBASE_BIN_PATH
        fi

        if [ "$LOCAL_RUN" = false ]; then
            # start kwbase
            $0 run_kw_start -N "$QUERY_TIMES" -p "$PORT" -B "$BUFFER_POOL_SIZE" -S "$TEST_RUN_TIME" -L "$LOCAL_RUN" -K "$KEEP_DOCKER_CONTAINER" -O "$STRESS_WITH_CHAOS" || { print_header "KWBASE service start failed"; print_errlog; exit 1; }
        fi

        if [ "$GENERATE_DATABASE" = true ]; then
            for dir in "$KWQUERY_DIR/pipeline" "$KWQUERY_DIR/runbayun"; do
                if [ -d "$dir" ] && [ -f "$dir/prep_env.sh" ]; then
                    echo "Running prep_env.sh in $dir..."
                    # TEST_IMAGE_NAME, TEST_IMAGE_TAG, WORKSPACE_DIR, UUID_PATH, CODE_PLATFORM, HTTP_PORT set in env.sh
                    bash "$dir/prep_env.sh" "$PORT" "$HTTP_PORT" || { print_header "Failed to run prep_env.sh in $dir"; print_errlog; exit 1; }
                else
                    echo "Directory $dir or prep_env.sh not found, skipping."
                fi
            done
            # insert extra needed data for test
            $KWBASE_BIN sql --insecure --host=127.0.0.1:${PORT} < ${SBIN_DIR}/../tests/kaiwudb-regression/prep.sql
            # create stats to avoid inconsistent query results
            bash "$KWQUERY_DIR/create_stats_after_loading_data.sh" "$PORT" "$HTTP_PORT" || { print_header "Failed to run create_stats_after_loading_data.sh in $dir"; print_errlog; exit 1; }
        fi
        # set cluster settings and shutdown kwbase
        bash "$KWQUERY_DIR/set_cluster_settings.sh" "$PORT" "$HTTP_PORT" "$PARALLEL_DEGREE" "$ACCESS_MODE" "$MULTI_MODEL_ENABLED" "$SPECIAL_OPTIMIZATION_MODE" "$HASH_SCAN_MODE_SETTING_NAME" || { print_header "Failed to run set_cluster_settings.sh in $dir"; print_errlog; exit 1; }

        if [ "$LOCAL_RUN" = false ]; then
            echo "Stopping and removing Docker container ${DOCKER_CONTAINER_PREFIX}-$PORT..."
            docker stop ${DOCKER_CONTAINER_PREFIX}-$PORT || true
            docker rm -f ${DOCKER_CONTAINER_PREFIX}-$PORT || true
        fi

        if [ "$GENERATE_DATABASE" = true ]; then
            if [ "$BACKUP_DATABASE" = true ]; then
                # copy database into backup database path
                rm -rf $BACKUP_DATABASE_PATH/kwbase-data
                mkdir -p $BACKUP_DATABASE_PATH
                cp -r $KWBASE_BIN_PATH/kwbase-data $BACKUP_DATABASE_PATH
            fi
        fi

        print_header "Finish Preparing Environment"
        ;;
    run_kw_start)
        print_header "Start KWBASE Service"
        print_final_values

        # start service
        echo "Starting KWBASE service in 10 seconds..."
        sleep 10
        cd $KWQUERY_DIR
        # TEST_IMAGE_NAME, TEST_IMAGE_TAG, WORKSPACE_DIR, UUID_PATH, CODE_PLATFORM, HTTP_PORT set in env.sh
        bash "start_kwbase.sh" "$TEST_IMAGE_NAME" "$TEST_IMAGE_TAG" "$WORKSPACE_DIR" "$UUID_PATH" "$PORT" "$CODE_PLATFORM" "$BUFFER_POOL_SIZE" "$MAX_SQL_MEMORY" "$TEST_RUN_TIME" "$DOCKER_CONTAINER_PREFIX" || { print_header "Failed to start KWBASE service"; print_errlog; exit 1; }
        print_header "Finish KWBASE Service"
        ;;
    run_regression)
        print_header "Start Running Regression Test"
        print_final_values

        # Execute the regression test
        cd $SBIN_DIR
        TEST_PASSED=true
        bash "$(dirname "$0")/run_regression.sh" "$PORT" "$PARALLEL_DEGREE" "$OUTPUT_DIR" "$ACCESS_MODE" "$MASTER_OVERWRITE" "$LOCAL_RUN" "$SQL_FILE_FILTER" "$EXCLUDE_SQL_FILES" "1" "$COLLECT_PHYSICAL_PLAN" "$CORRECTNESS_CHECK" "$MULTI_MODEL_ENABLED" || { print_header "Regression test failed"; TEST_PASSED=false; }
        print_errlog
        if [[ "$LOCAL_RUN" = "false" && "$KEEP_DOCKER_CONTAINER" = "false" && "$TEST_PASSED" = "true" ]]; then
            # First, stop and remove the Docker container
            echo "Stopping and removing Docker container ${DOCKER_CONTAINER_PREFIX}-$PORT..."
            docker stop ${DOCKER_CONTAINER_PREFIX}-$PORT || true
            docker rm -f ${DOCKER_CONTAINER_PREFIX}-$PORT || true
        fi
        print_header "Finish Running Regression Test"
        if [[ "$TEST_PASSED" = "false" ]]; then
            exit 1
        fi
        ;;
    run_parallel_regression)
        print_header "Start Running Regression Test"
        print_final_values

        # Multi-threaded concurrent execution of regression testing
        cd $SBIN_DIR

        PARALLEL_SQL_FILE_FILTER=("EXPLAIN fallback multi q1.csv q2.csv q4.csv q5.csv q6.csv q7.csv q8.csv loosenlimit_sql_15 loosenlimit_sql_23"
                                    "insideout"
                                    "insideout_sql_8"
                                    "loosenlimit"
                                    "materialization"
                                    "outsidein"
                                    "outsidein_new2_HashTagScan outsidein_new12_HashTagScan"
                                    "outsidein_new8_HashTagScan outsidein_new10_HashTagScan outsidein_new19_HashTagScan"
                                    "primary outsidein_new10_HashRelScan outsidein_new20_HashTagScan"
                                    "qa_sql")
        PARALLEL_EXCLUDE_SQL_FILES=("$EXCLUDE_SQL_FILES"
                                    "$EXCLUDE_SQL_FILES EXPLAIN insideout_sql_8"
                                    "$EXCLUDE_SQL_FILES EXPLAIN"
                                    "$EXCLUDE_SQL_FILES EXPLAIN loosenlimit_sql_15 loosenlimit_sql_23"
                                    "$EXCLUDE_SQL_FILES EXPLAIN"
                                    "$EXCLUDE_SQL_FILES EXPLAIN qa_sql outsidein_new2_HashTagScan outsidein_new8_HashTagScan outsidein_new10_HashTagScan outsidein_new19_HashTagScan outsidein_new12_HashTagScan outsidein_new10_HashRelScan outsidein_new20_HashTagScan"
                                    "$EXCLUDE_SQL_FILES EXPLAIN"
                                    "$EXCLUDE_SQL_FILES EXPLAIN"
                                    "$EXCLUDE_SQL_FILES EXPLAIN"
                                    "$EXCLUDE_SQL_FILES EXPLAIN")
        PARALLEL_NUM=${#PARALLEL_SQL_FILE_FILTER[@]}

        declare -a REGRESSION_PIDS
        for (( i = 0; i < ${PARALLEL_NUM}; i++ ));
        do
            (
                echo "Regression $i, filter: ${PARALLEL_SQL_FILE_FILTER[$i]}"
                if [ -f "$OUTPUT_DIR/regression-$i/failed" ]; then
                    rm $OUTPUT_DIR/regression-$i/failed
                fi
                if [ -f "$OUTPUT_DIR/regression-$i/passed" ]; then
                    rm $OUTPUT_DIR/regression-$i/passed
                fi
                mkdir -p $OUTPUT_DIR/regression-$i/
                bash "$(dirname "$0")/run_regression.sh" "$PORT" "$PARALLEL_DEGREE" "$OUTPUT_DIR" "$ACCESS_MODE" "$MASTER_OVERWRITE" "$LOCAL_RUN" "${PARALLEL_SQL_FILE_FILTER[$i]}" "${PARALLEL_EXCLUDE_SQL_FILES[$i]}" "1" "$COLLECT_PHYSICAL_PLAN" "$CORRECTNESS_CHECK" "$MULTI_MODEL_ENABLED" "query_time_${i}"
                if [ "$?" != "0" ]; then
                    echo "#$i regression run failed." > $OUTPUT_DIR/regression-$i/failed
                    echo "#$i regression run exits."
                    exit 1
                fi
                echo "#$i regression run passed." > $OUTPUT_DIR/regression-$i/passed
            ) &
            # Obtain the PID of the background process and store it in an array
            REGRESSION_PIDS[$i]=$!
        done
        echo "Waiting for all threads to complete regression tests..."
        wait "${REGRESSION_PIDS[@]}"

        print_errlog
        TEST_PASSED=true
        for i in $(seq 0 "$PARALLEL_NUM"); do
            if [ -f $OUTPUT_DIR/regression-$i/failed ]; then
                echo $OUTPUT_DIR/regression-$i/failed
                TEST_PASSED=false
            else
                echo $OUTPUT_DIR/regression-$i/passed
            fi
        done

        if [[ "$LOCAL_RUN" = "false" && "$KEEP_DOCKER_CONTAINER" = "false" ]]; then
            # First, stop and remove the Docker container
            echo "Stopping and removing Docker container ${DOCKER_CONTAINER_PREFIX}-$PORT..."
            docker stop ${DOCKER_CONTAINER_PREFIX}-$PORT || true
            docker rm -f ${DOCKER_CONTAINER_PREFIX}-$PORT || true
        fi
        print_header "Finish Running Regression Test"

        if [ "$TEST_PASSED" == "true" ]; then
            print_header "Regression test passed."
        else
            print_header "Regression test failed."
            exit 1
        fi
        ;;
    *)
        echo "Invalid command. Type 'help' for instructions."
        show_help
        exit 1
        ;;
esac
exit 0
