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
import utils
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--kwdb_version','-v',type=str, default="develop",help='kwdb_version')
parser.add_argument('--pipe_name','-p',type=str, default="kwdb_tsbs_dev_1_xx",help='pipe_name')
parser.add_argument('--tsbs_format','-f',type=str, default="kaiwudb1.1",help='tsbs_format')
parser.add_argument('--scale','-s',type=int, default=100,help='scale')
parser.add_argument('--case_name','-n',type=str, default="none",help='case_name')
parser.add_argument('--workers','-w',type=int, default=1,help='workers')
parser.add_argument('--query_times','-t',type=int, default=10,help='query_times')
parser.add_argument('--result_file','-r',type=str, default="none",help='tsbs query result file')
parser.add_argument('--threshold_dir','-d',type=str, default="none",help='threshold dir')
parser.add_argument('--dop','-o',type=str, default="8",help='degree of parallel')


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    args = parser.parse_args()
    kwdb_version = args.kwdb_version
    pipe_name = args.pipe_name
    tsbs_format = args.tsbs_format
    scale = args.scale
    case_name = args.case_name
    workers = args.workers
    query_times = args.query_times
    result_file = args.result_file
    threshold_dir = args.threshold_dir
    dop = args.dop
    if case_name == "load":
        load_rate=utils.get_load_result_from_file(result_file)
        if load_rate == -1:
            exit(1)
        threshold = float(load_rate)*0.9
        resStr = kwdb_version+","+tsbs_format+","+pipe_name+","+case_name+","+str(scale)+","+str(workers)+","+str(query_times)+","+","+load_rate+","+","+","+","+dop+","+str(threshold)
        utils.write_lines_to_file(os.path.join(threshold_dir, 'TSBS_THRESHOLD.csv'), resStr)
    else:
        #/home/inspur/src/reports/kwdb_tsbs_dev_1_2023_0710_105208/release-1.1.0_query_result/double-groupby-1_devops_kaiwudb1.1_scale_100.log
        min_ms,med_ms,mean_ms,max_ms,stddev_ms,sum_s,count=utils.get_query_result_from_file(result_file)
        
        # print('min_ms', min_ms)
        if min_ms == -1 or int(count) != query_times:
            exit(1)
        
        # Update threshold file, threshold formula=mean+2*min(stddev, 5%mean) + 1ms(prevent fast case from flapping)
        threshold=float(mean_ms)+2*min(float(stddev_ms), 0.05 * float(mean_ms)) + 1
        resStr=kwdb_version+","+tsbs_format+","+pipe_name+","+case_name+","+str(scale)+","+str(workers)+","+str(query_times)+","+min_ms+","+mean_ms+","+max_ms+","+med_ms+","+stddev_ms+","+dop+","+str(threshold)
        utils.write_lines_to_file(os.path.join(threshold_dir, 'TSBS_THRESHOLD.csv'), resStr)
