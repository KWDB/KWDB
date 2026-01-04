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

import os
import json
import argparse

class ResultChecker:
    def __init__(self, json_file):
        self.json_file = json_file
    
    def check(self, base_dir, scale, node_num, insert_type, wal, query_workers):
        if not os.path.isfile(self.json_file):
            print(f"json_file: {self.json_file} not exist")
            return False
        
        thresholds = self._load_threshold_from_json(self.json_file)
        if thresholds is None:
            print(f"json_file: {self.json_file} load thresholds failed")
            return False
        
        threshold_key = f"{scale}_{node_num}_{insert_type}_{query_workers}"
        if threshold_key not in thresholds.keys():
            print(f"threshold_key: {threshold_key} not in thresholds.keys()")
            return False
        
        load_result_path, query_csv_path = self._get_latest_result_dir(base_dir, scale, node_num, insert_type, wal, query_workers)
        if load_result_path is None or query_csv_path is None:
            print(f"get_latest_result_dir failed: load_result_path: {load_result_path} query_csv_path: {query_csv_path}")
            return False
    
        return self._get_results_from_file(load_result_path, query_csv_path, thresholds[threshold_key])


    def _get_latest_result_dir(self, base_dir, scale, node_num, insert_type, wal, query_workers):
        # dirs_name: 2025_1202_150408_master_scale4000_cluster1_insertdirect1_prepare_wal2_replica1_dop8
        latest_dir = None
        for dir in os.listdir(base_dir):
            if dir.endswith(f"scale{scale}_cluster{node_num}_insertdirect1_{insert_type}_wal{wal}_replica{node_num}_dop{query_workers}"):
                if latest_dir is None or dir > latest_dir:
                    latest_dir = dir
                    
        if latest_dir is None:
            print(f"latest_dir: {latest_dir} not exist")
            return None, None
        
        result_dir = f"{base_dir}/{latest_dir}"
        if not os.path.isdir(result_dir):
            print(f"result_dir: {result_dir} not exist")
            return None, None
        print(f"latest_dir: {latest_dir}")
        # find only one txt file in result_dir/load_result/
        load_result_dir = f"{result_dir}/load_result"
        if not os.path.isdir(load_result_dir):
            print(f"load_result_dir: {load_result_dir} not exist")
            return None, None
        load_result_files = list(filter(lambda x: x.endswith('.log') and x != 'count_info.log', os.listdir(load_result_dir)))
        if len(load_result_files) != 1:
            print(f"load_result_files: {load_result_files} not unique")
            return None, None
        load_result_path = f"{result_dir}/load_result/{load_result_files[0]}"
        print(f"load_result_path: {load_result_path}")
        # query TSBS_TEST_RESULT.csv
        query_result_path = f"{result_dir}/query_result/TSBS_TEST_RESULT.csv"
        if not os.path.isfile(query_result_path):
            return None, None
        print(f"query_result_dir: {query_result_path}")
        return [load_result_path, query_result_path]

        return load_result_path, query_result_path
    def _get_results_from_file(self, load_result_path, query_csv_path, thresholds):
        if not os.path.isfile(load_result_path) or not os.path.isfile(query_csv_path):
            print(f"load_result_path: {load_result_path} or query_csv_path: {query_csv_path} not exist")
            return False
        # Lastrow in file: loaded 26784100 rows in 79.686sec with 8 workers (mean rate 336119.29 rows/sec, actually rate 336547.84 rows/sec without ddl time)
        load_result = 0
        with open(load_result_path, 'r') as f:
            lines = f.readlines()
            last_line = lines[-1]
            mean_rate = last_line.split('mean rate ')[1].split(' rows/sec')[0].strip()
            load_result =  float(mean_rate)
            if load_result < thresholds['import_speed']:
                print(f"load_result: {load_result} < thresholds: {thresholds['import_speed']}")
                return False
        
        with open(query_csv_path, 'r') as f:
            lines = f.readlines()
            for line in lines[1:]:
                cols = line.split(',')
                if len(cols) < 11:
                    continue
                query_type = cols[3]
                if query_type not in thresholds.keys():
                    print(f"query_type: {query_type} not in thresholds.keys()")
                    continue
                
                query_result = float(cols[8])
                if query_result > thresholds[query_type]:
                    print(f"query_type: {query_type} query_result: {query_result} > thresholds: {thresholds[query_type]}")
                    return False
        
        return True
    
    def _load_threshold_from_json(self, json_file):
        if os.path.isfile(json_file):
            with open(json_file, 'r') as f:
                data = json.load(f)
                return data


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Check TSBS test result')
    parser.add_argument('--thresholds_json', type=str, default='thresholds.json', help='JSON file of the thresholds')
    parser.add_argument('--base_dir', type=str, default='./tsbs_result', help='Base directory of the test results')
    parser.add_argument('--scale', type=str, default='1000000', help='Scale of the test')
    parser.add_argument('--node_num', type=str, default='3', help='Node number of the test')
    parser.add_argument('--insert_type', type=str, default='prepare', help='Insert type of the test')
    parser.add_argument('--query_workers', type=str, default='8', help='Query workers of the test')
    parser.add_argument('--wal', type=str, default='2', help='Wal of the test')
    args = parser.parse_args()
    checker = ResultChecker(args.thresholds_json)
    if not checker.check(args.base_dir, args.scale, args.node_num, args.insert_type, args.wal, args.query_workers):
        print("Check failed")
        exit(1)
    print("Check passed")
    exit(0)
