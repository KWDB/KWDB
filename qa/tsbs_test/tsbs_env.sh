#!bin/bash
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

load_workers=12
load_batchsizes="1000"
tsbs_case="cpu-only"
load_interval="10s"
db_name="benchmark"
load_ts_start="2016-01-01T00:00:00Z"
load_ts_end="2016-02-01T00:00:00Z"
insert_type="prepare"
query_times=30
wal=3
query_workers=8
format="kwdb"
parallel_degree=8
insert_direct=1
replica_mode=1

QUERY_TYPES_ALL="\
single-groupby-1-1-1 \
single-groupby-1-1-12 \
single-groupby-1-8-1 \
single-groupby-5-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1 \
cpu-max-all-1 \
cpu-max-all-8 \
double-groupby-1 \
double-groupby-5 \
double-groupby-all \
high-cpu-all \
high-cpu-1 \
lastpoint \
groupby-orderby-limit"

QUERY_TYPES_SIMPLE="\
single-groupby-1-1-1 \
single-groupby-5-1-12 \
single-groupby-5-8-1 \
cpu-max-all-8 \
double-groupby-all \
high-cpu-all \
lastpoint \
groupby-orderby-limit"