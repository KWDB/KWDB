# README

## 1. Introduction

perf_test is an automated script designed to handle various testing procedures including environment setup, regression testing, and final result processing. It streamlines the testing process and ensures consistency and reliability across different environments.

## 2. Prerequisites

Before running the script, ensure the following conditions are met:
- **Operating System**: Linux environment, Ubuntu 20.04 or 22.04 is recommended.
- **Shell Environment**: bash shell.
- **Dependencies**:
  - **Docker**: Used to run KWBASE services.
  - **Python 3**: Necessary for running Python scripts like `compare_sorted.py`.

Ensure `kwbasedb` is installed and compiled, and that required datasets are placed under `<WORKSPACE_DIR>/dataset`.

## 3. Environment Variables

   Refer to the pipeline configuration for the necessary environment parameters. Example:

   ```bash
   export BUFFER_POOL_SIZE=16384
   export WORKSPACE_DIR=/data1/workspace
   export CODE_PLATFORM=gitee.com
   export PORT=26888
   export QUERY_TIMES=10
   ```

## 4. Usage

### Syntax

```bash
./run_test.sh [command] [options]
```

### Commands

- `help`: Display help information.
- `prep_env`: Prepare the insert/load test environment.
- `run_kw_start`: Start KWBASE in the Docker environment.
- `run_regression`: Run regression tests to check query correctness.

### Options

- `-o <output_dir>`: Set the output directory.
- `-N <query_times>`: Set the number of times each query should run.
- `-P <parallel_degree>`: Set KWBASE parallel degree (default: 8).
- `-p <port>`: Set KWBASE port number (default: 26888).
- `-S <test_run_time>`: Set the total time to run the test (in seconds).
- `-F <true/false>`: Force clean `kwbase-data` before running `prep_env`.

### Example Commands

1. **Run regression test only**:

   ```bash
   ./run_test.sh run_regression -p 26888 -P 8 -o '/data1/workspace/output'
   ```

## 5. Logging and Output

- **Log Files**: Logs are stored under `$WORKSPACE_DIR/log/$UUID_PATH`.
- **Output Files**: regression results are stored in the specified output directory.

## 7. Troubleshooting

- **Permission Denied**: Ensure that the script has execution permissions (`chmod +x run_test.sh`).
- **Missing Dependencies**: Verify that all required software, including Docker and Python packages, is installed correctly.

---