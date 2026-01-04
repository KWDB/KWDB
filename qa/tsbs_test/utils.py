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

import re

def get_file_contents(file):
    f = open(file, 'r')
    file_contents = f.read()
    f.close()
    return file_contents


def write_lines_to_file(file, lines):
    f = open(file, 'a')
    f.write(lines)
    f.write("\n")
    f.close()
    
def get_threshold_from_file(file):
    f = open(file, 'r')
    lines = f.readlines()
    f.close()
    return lines

def get_query_result_from_file(file):
    outp = get_file_contents(file);
    lines = outp.split('\n')
    for i in range(len(lines)):
        line=lines[i]#min:  2432.64ms, med:  2498.69ms, mean:  2510.03ms, max: 2616.83ms, stddev:    52.64ms, sum:  25.1sec, count: 10
        if re.match(r'min:.*', line):
            strip_line = line.replace(' ','')#min:2432.64ms,med:2498.69ms,mean:2510.03ms,max:2616.83ms,stddev:52.64ms,sum:25.1sec,count:10
            print(strip_line)
            m=re.search(r'min:(?P<min_ms>\d+(\.\d+)?)ms,med:(?P<med_ms>\d+(\.\d+)?)ms,mean:(?P<mean_ms>\d+(\.\d+)?)ms,max:(?P<max_ms>\d+(\.\d+)?)ms,stddev:(?P<stddev_ms>\d+(\.\d+)?)ms,sum:(?P<sum_s>\d+(\.\d+)?)sec,count:(?P<count>\d+)', strip_line)
            if m:
                min_ms=m.group('min_ms')
                print(min_ms)
                med_ms=m.group('med_ms')
                print(med_ms)
                mean_ms=m.group('mean_ms')
                print(mean_ms)
                max_ms=m.group('max_ms')
                print(max_ms)
                stddev_ms=m.group('stddev_ms')
                print(stddev_ms)
                sum_s=m.group('sum_s')
                print(sum_s)
                count=m.group('count')
                print(count)
                return min_ms,med_ms,mean_ms,max_ms,stddev_ms,sum_s,count
    return -1,-1,-1,-1,-1,-1,-1


def get_load_result_from_file(file):
    outp = get_file_contents(file)
    # last row: loaded 26784100 rows in 75.862sec with 8 workers (mean rate 353065.14 rows/sec)
    # get actually rate number after "actually rate"
    m=re.search(r'mean rate (?P<mean_rate>\d+(\.\d+)?) rows/sec', outp)
    if m:
        mean_rate=m.group('mean_rate')
        return mean_rate
    return -1

def get_threshold_from_csv(file, kwdb_version, tsbs_format, pipe_name, case_name, scale, workers, query_times):
    outp = get_file_contents(file)
    lines = outp.split('\n')
    for line in lines:
        values = line.split(',')
        if len(values) != 14:
            continue
        t_kwdb_version=values[0]
        t_tsbs_format=values[1]
        t_pipe_name=values[2]
        t_case_name=values[3]
        t_scale=values[4]
        t_workers=values[5]
        t_query_times=values[6]
        threshold=values[13]
        if t_kwdb_version == kwdb_version and t_tsbs_format == tsbs_format and t_case_name == case_name and t_scale == scale and t_workers == workers and t_query_times == query_times:
            return threshold
    return None
