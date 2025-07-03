// Copyright (c) 2022-present, Shanghai Yunxi Technology Co, Ltd. All rights reserved.
//
// This software (KWDB) is licensed under Mulan PSL v2.
// You can use this software according to the terms and conditions of the Mulan PSL v2.
// You may obtain a copy of Mulan PSL v2 at:
//          http://license.coscl.org.cn/MulanPSL2
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PSL v2 for more details.

#pragma once

#include <getopt.h>
#include <cstdio>
#include <iostream>
#include <vector>

namespace kwdbts {

struct MetaParams {
  kwdbts::KSchemaKey database_id = 1;
  std::string database_name = "ts_db";
  int TABLE_NUM = 1;
  int COLUMN_NUM = 2;
  int ENTITY_NUM = 100;
  uint64_t PARTITION_INTERVAL = 86400;
  uint64_t RETENTIONS_TIME = 0;
  uint64_t COMPRESS_TIME = 0;
};

// Parameters for performance testing
struct BenchParams {
  // meta parameters
  MetaParams meta_param;
  // chaos parameters
  std::string chaos_param = "";
  // number of data per batch
  int BATCH_NUM = 1;
  // bench runtime
  int RUN_TIME = 20;
  // number of executions per thread: -1 indicates no limit
  int64_t RUN_COUNT = -1;
  // exit the thread due to an error
  bool ERROR_EXIT = true;
  // delete existing tables
  bool DROP_EXIST = true;
  // statistics are displayed every SHOW_INTERVAL seconds
  int SHOW_INTERVAL = 60;
  // the interval between each insert
  int wait_ms = 0;
  // use an existing database
  bool exist_db = false;
  // clean up data when exiting
  bool CLEAN_DATA = false;
  // scan time interval (ms)
  int scan_range = 100;
  int init_data = 0;
  int read_wait = 0;
  int null_data_ratio_percent = 30;
  // data types used
  std::vector<int> data_types;
  // starting value of timestamp, default to current system time (ms)
  int64_t time_start = 0;
  // data ts add mode. 0: ordered. 1: using current max ts.
  int ts_add_mode = 1;
  // timestamp increment, default to 10 ms
  int time_inc = 10;
  // the proportion of unordered data written
  int dis_int = 0;
  int dis_ext = 0;
  std::vector<int> tag_types;
  std::string engine_params;
  std::string engine_version = "1";
};
}

struct MainParams {
  //  Number of writing threads
  int WORK_THREAD = 1;
  std::vector<std::string> benchmarks;
  //  independent read thread
  bool single_read = false;
};

struct ParseUtil {
  static void PrintUsage(const char* cmd) {
    fprintf(stdout, "%s"
                    "\t--table_num \tnumber of tables, default: 1\n"
                    "\t--column_num \tnumber of columns per table, default: 2\n"
                    "\t--entity_num \tnumber of entities in each table, default: 100\n"
                    "\t--thread_num \tnumber of concurrent threads\n"
                    "\t--duration \trun duration in seconds\n"
                    "\t--run_count \truns per thread. default: -1\n"
                    "\t--batch_num \tnumber of data per batch, default: 1\n"
                    "\t--scan_ms_intv \tscan time interval (ms), default: 100 ms\n"
                    "\t--wait_ms \tthe interval between each insert, in milliseconds, default: 0\n"
                    "\t--show_intv \tstatistics are displayed every SHOW_INTERVAL seconds\n"
                    "\t--drop_exist \tdelete existing tables, default: true\n"
                    "\t--benchmarks \tthe type of worker that each thread executes in serial: \"savedata scan\"\n"
                    "\t--single_read \tindependent read thread\n"
                    "\t--read_wait \tthe interval between each query, default: 0\n"
                    "\t--error_exit \texit with an error: 0-not exit 1-exit. default: 1\n"
                    "\t--data_type \tdata types used, default: int\n"
                    "\t--disorder \tdisorder data write ratio, format: m, n\n"
                    "\t--retentions_time \tretentions time for table, in seconds, default: 0\n"
                    "\t--compress_time \tcompress time for table, in seconds, default: 0\n"
                    "\t--partition_interval \tpartition interval for table, in seconds, default: 86400\n"
                    "\t--engine_paras \t storage engine running parameters.\n",
            cmd);
  }

  static std::vector<std::string> ParseBench(std::string& benchmark) {
    std::vector<std::string> bench_vec;
    int pos = 0;
    int len = benchmark.size();
    while (pos < len) {
      int pos_sec = benchmark.find(",", pos);
      if (pos_sec == benchmark.npos) {
        pos_sec = len;
      }
      bench_vec.emplace_back(benchmark.substr(pos, pos_sec - pos));
      pos = pos_sec + 1;
    }
    return bench_vec;
  }

  static std::vector<int> ParseDataType(std::string& data_types) {
    std::vector<std::string> type_strings;
    int pos = 0;
    int len = data_types.size();
    while (pos < len) {
      int pos_sec = data_types.find(",", pos);
      if (pos_sec == data_types.npos) {
        pos_sec = len;
      }
      type_strings.emplace_back(data_types.substr(pos, pos_sec - pos));
      pos = pos_sec + 1;
    }
    std::vector<int> data_type;
    if (type_strings[0] == "id") {
      for (size_t i = 1; i < type_strings.size(); i++) {
        data_type.push_back(atoi(type_strings[i].c_str()));
      }
      return data_type;
    }
    for (int i = 0 ; i < type_strings.size() ; i++) {
      if (type_strings[i] == "timestamp") {
        data_type.emplace_back(0);
      } else if (type_strings[i] == "smallint") {
        data_type.emplace_back(1);
      } else if (type_strings[i] == "int") {
        data_type.emplace_back(2);
      } else if (type_strings[i] == "bigint") {
        data_type.emplace_back(3);
      } else if (type_strings[i] == "float") {
        data_type.emplace_back(4);
      } else if (type_strings[i] == "double") {
        data_type.emplace_back(5);
      } else if (type_strings[i] == "bool") {
        data_type.emplace_back(6);
      } else if (type_strings[i] == "char") {
        data_type.emplace_back(7);
      } else if (type_strings[i] == "binary") {
        data_type.emplace_back(8);
      } else if (type_strings[i] == "varchar") {
        data_type.emplace_back(9);
      }
    }
    return data_type;
  }

  // parsing parameters
  static void ParseOpts(int argc, char* argv[], kwdbts::BenchParams* params, MainParams* main_params) {
    int opt;
    // index value of long_ops
    int option_index = 0;
    const char* optstring = "a:b:c:d:e:f:g:h:i:j:k:l:m:n:o:p:q:r:s:t:u:v";
    static struct option long_options[] = {
        {"table_num",    required_argument, NULL, 'a'},  // number of tables
        {"column_num",  required_argument, NULL, 'b'},  // number of columns per table
        {"thread_num",    required_argument, NULL, 'c'},  // number of concurrent threads
        {"duration",      required_argument, NULL, 'd'},  // run duration in seconds
        {"batch_num",     required_argument, NULL, 'e'},  // number of data per batch
        {"scan_ms_intv",  required_argument, NULL, 'f'},  // scan time interval (ms)
        {"wait_ms",       required_argument, NULL, 'g'},  // the interval between each insert
        {"show_intv",     required_argument, NULL, 'h'},  // statistics are displayed every SHOW_INTERVAL seconds
        {"exist_db",      required_argument, NULL, 'i'},  // use an existing database
        {"benchmarks",    required_argument, NULL, 'j'},  // the type of worker that each thread executes in serial
        {"single_read",   required_argument, NULL, 'k'},  // independent read thread
        {"read_wait",     required_argument, NULL, 'l'},  // the interval between each query
        {"data_type",     required_argument, NULL, 'm'},  // data types used
        {"error_exit",    required_argument, NULL, 'n'},  // exit with an error
        {"run_count",     required_argument, NULL, 'o'},  // runs per thread
        {"drop_exist",    required_argument, NULL, 'p'},  // delete existing tables
        {"time_inc",    required_argument, NULL, 'q'},  // incremental timestamp for data writing
        {"time_start",     required_argument, NULL, 'r'},  // start timestamp for data writing
        {"disorder",     required_argument, NULL, 's'},  // disorder data write ratio
        {"entity_num",  required_argument, NULL, 't'},  // number of entities in each table
        {"partition_interval",  required_argument, NULL, 'u'},  // partition interval for table, in seconds
        {"retentions_time",  required_argument, NULL, 'v'},  // retentions time for table, in seconds
        {"compress_time",  required_argument, NULL, 'w'},  // compress time for table, in seconds
        {"engine_params",  required_argument, NULL, 'x'},  // sotrage engine parameters.
        {"engine_version",  required_argument, NULL, 'y'},  // sotrage engine version.
        {0, 0, 0, 0}  // to prevent inputting null values
    };

    while ((opt = getopt_long(argc, argv, optstring, long_options, &option_index)) != -1) {
      switch (opt) {
        case 'a':
          params->meta_param.TABLE_NUM = std::stoi(argv[optind - 1]);
          break;
        case 'b':
          params->meta_param.COLUMN_NUM = std::stoi(argv[optind - 1]);
          break;
        case 'c':
          main_params->WORK_THREAD = std::stoi(argv[optind - 1]);
          break;
        case 'd':
          params->RUN_TIME = std::stoi(argv[optind - 1]);
          break;
        case 'e':
          params->BATCH_NUM = std::stoi(argv[optind - 1]);
          break;
        case 'f':
          params->scan_range = std::stoi(argv[optind - 1]);
          break;
        case 'g':
          params->wait_ms = std::stoi(argv[optind - 1]);
          break;
        case 'h':
          params->SHOW_INTERVAL = std::stoi(argv[optind - 1]);
          break;
        case 'i': {
          int exist_db = std::stoi(argv[optind - 1]);
          if (exist_db == 0) {
            params->exist_db = false;
          } else {
            params->exist_db = true;
          }
        }
          break;
        case 'j': {
          std::string benchmarks = argv[optind - 1];
          main_params->benchmarks = ParseBench(benchmarks);
        }
          break;
        case 'k': {
          int single_read = std::stoi(argv[optind - 1]);
          if (single_read == 0) {
            main_params->single_read = false;
          } else {
            main_params->single_read = true;
          }
        }
          break;
        case 'l':
          params->read_wait = std::stoi(argv[optind - 1]);
          break;
        case 'm': {
          std::string data_types = argv[optind - 1];
          params->data_types = ParseDataType(data_types);
        }
          break;
        case 'n':
          params->ERROR_EXIT = std::stoi(argv[optind - 1]);
          break;
        case 'o':
          params->RUN_COUNT = std::stol(argv[optind - 1]);
          break;
        case 'p':
          params->DROP_EXIST = (bool) (std::stoi(argv[optind - 1]));
          break;
        case 'q': {
          params->time_inc = std::stoi(argv[optind - 1]);
          break;
        }
        case 'r': {
          params->time_start = std::stol(argv[optind - 1]);
          break;
        }
        case 's': {
          std::string dis_str = std::string(argv[optind - 1]);
          std::vector<std::string> d_args = ParseBench(dis_str);
          if (d_args.size() > 0) {
            params->dis_int = std::stoi(d_args[0]);
          }
          if (d_args.size() > 1) {
            params->dis_ext = std::stoi(d_args[1]);
          }
          break;
        }
        case 't':
          params->meta_param.ENTITY_NUM = std::stoi(argv[optind - 1]);
          break;
        case 'u':
          params->meta_param.PARTITION_INTERVAL = std::stoull(argv[optind - 1]);
          break;
        case 'v':
          params->meta_param.RETENTIONS_TIME = std::stoull(argv[optind - 1]);
          break;
        case 'w':
          params->meta_param.COMPRESS_TIME = std::stoull(argv[optind - 1]);
          break;
        case 'x': {
          params->engine_params = std::string(argv[optind - 1]);
          break;
        }
        case 'y': {
          params->engine_version = std::string(argv[optind - 1]);
          break;
        }

        default:
          std::cout << "wrong param input!!!" << std::endl;
          break;
      }
    }
  }

#define Name(X) #X
#define Out(X) { \
  std::string xname = Name(X); \
  std::cout << "\t" << xname.substr(2, xname.size() - 1) << ":\t" << X << std::endl; \
}

  static void PrintMainParams(const MainParams& a) {
    std::cout << "========================  Main Params =================================" << std::endl;
    Out(a.WORK_THREAD);
    Out(a.single_read);
  }

  static void PrintBenchParams(const kwdbts::BenchParams& a) {
    std::cout << "======================== Bench Params =================================" << std::endl;
    Out(a.BATCH_NUM);
    Out(a.CLEAN_DATA);
    Out(a.DROP_EXIST);
    Out(a.ERROR_EXIT);
    Out(a.RUN_TIME);
    Out(a.RUN_COUNT);
    Out(a.scan_range);
    Out(a.SHOW_INTERVAL);
    Out(a.exist_db);
    Out(a.wait_ms);
    Out(a.init_data);
    Out(a.read_wait);
    Out(a.time_inc);
    Out(a.time_start);
    Out(a.chaos_param);
    Out(a.DROP_EXIST);
    Out(a.engine_params);
    std::cout << "=======================================================================" << std::endl;
  }
};
