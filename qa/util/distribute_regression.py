#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
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
import sys
import re

node_id_map = {}
http_ip_map = {}
join_node_id_map = {}
join_http_ip_map = {}


def get_nodes(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    nodes = strs[1]
    nodes = re.sub(' ', '', nodes)
    nodes = nodes.split(',')
    ids = []
    for node in nodes:
        if re.match('c\d+', node):
            ids.append(int(re.sub('c', '', node)))
    return ids


def get_args(args_list: str):
    args = args_list.split(':')
    if len(args) < 2:
        return None
    args = args[1]
    args = args.split(' ')
    args = [re.sub(' ', '', arg) for arg in args]
    args = [re.sub('\n', '', arg) for arg in args]
    l = []
    for i in range(len(args)):
        arg = args[i]
        if arg != '':
            l.append(arg)
    return l


def get_create_table_arg(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    args = strs[1]
    args = re.sub(' ', '', args)
    args = args.split(',')
    ids = []
    ts = False
    rel = False
    num = 0
    for arg in args:
        if re.match('\d+', arg):
            num = int(re.sub('c', '', arg))
        elif re.match('rel', arg):
            rel = True
        elif re.match('ts', arg):
            ts = True

    if ts == False and rel == False:
        ts = True

    return num, rel, ts


def get_nums(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    num = strs[1]
    num = re.sub(' ', '', num)
    return int(num)


def get_create_table_arg(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    args = strs[1]
    args = re.sub(' ', '', args)
    args = args.split(',')
    ids = []
    ts = False
    rel = False
    num = 0
    for arg in args:
        if re.match('\d+', arg):
            num = int(re.sub('c', '', arg))
        elif re.match('rel', arg):
            rel = True
        elif re.match('ts', arg):
            ts = True

    if ts == False and rel == False:
        ts = True

    return num, rel, ts


def get_nums(node_str: str):
    strs = node_str.split(':')
    if len(strs) < 2:
        return None
    num = strs[1]
    num = re.sub(' ', '', num)
    return int(num)


def get_url_from_node_id(node_id: int):
    return node_id_map[str(node_id)]


def get_http_url_from_node_id(node_id: int):
    return http_ip_map[str(node_id)]


def get_url_for_join(node_id: int):
    return join_node_id_map[str(node_id)]


def get_http_url_fot_join(node_id: int):
    return join_http_ip_map[str(node_id)]


def get_sleep_cmd(sql: str):
    strs = sql.split(':')
    if len(strs) < 2:
        return None
    time = strs[1]
    return "sleep " + time


def is_blank(sql):
    sql = re.sub('\n', '', sql)
    sql = re.sub(' ', '', sql)
    if sql == "":
        return True
    return False


def get_node_id_map(kwbin: str):
    command = '{} node status --insecure --host=127.0.102.145:26257'.format(kwbin)
    res = os.popen(command)
    res = res.read()
    res = res.split('\n')
    res = res[1:]
    res = res[:-1]
    global node_id_map
    for i in res:
        node_id = i.split('\t')[0]
        ip = i.split('\t')[1]
        node_id_map[node_id] = ip
        port = ip.split(':')[1]
        http_port = int(port) - 26257 + 8080
        http_addr = ip.split(':')[0] + ':' + str(http_port)
        http_ip_map[node_id] = http_addr


def get_join_node_id_map(kwbin: str):
    command = '{} node status --insecure --host=127.0.102.145:26257'.format(kwbin)
    res = os.popen(command)
    res = res.read()
    res = res.split('\n')
    res = res[1:]
    res = res[:-1]
    global node_id_map
    global join_node_id_map
    global join_http_ip_map
    for i in range(1, 6):
        node_id = str(5 + i)
        listen_port = 26261 + i
        join_node_id_map[node_id] = '127.0.102.145' + ':' + str(listen_port)
        http_port = 8084 + i
        http_addr = '127.0.102.145' + ':' + str(http_port)
        join_http_ip_map[node_id] = http_addr

    '''for i in res:
        node_id = str(int(i.split('\t')[0])+5)
        ip = i.split('\t')[1]
        listen_port = int(ip.split(':')[1])+5
        join_node_id_map[node_id] = ip.split(':')[0]+':'+str(listen_port)
        port = ip.split(':')[1]
        http_port = int(port)-26257+8080+5
        http_addr = ip.split(':')[0]+':'+str(http_port)
        join_http_ip_map[node_id] = http_addr'''


def init_load():
    cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" 2>&1 | tee -a {} > /dev/null".format(
        kwbin_path, get_url_from_node_id(last_exec_node[0]), 'import database csv data (\\"nodelocal://1/tsdb1\\");',
        output_file_path)
    cmds.append(cmd)

    pass


if __name__ == "__main__":
    args = sys.argv[1:]
    # for i in range(len(args)):
    #     print(i,'    ',args[i])
    sql_path = args[0]
    kwbin_path = args[1]
    output_file_path = args[2]

    store_dir = args[3]

    output_cmd_file = args[4]
    #
    # print("=============================================")
    # print("sql_path ", sql_path)
    # print("kwbin_path ", kwbin_path)
    # print("output_file_path ", output_file_path)
    # print("store_dir ", store_dir)
    # print("output_cmd_file ", output_cmd_file)
    # print("=============================================")

    with open(sql_path, 'r') as f:
        sqls = f.readlines()
        sqls = [re.sub('\n', ' ', sql) for sql in sqls]
    cmds = []
    stmt = ''
    last_exec_node = [1]

    get_node_id_map(kwbin_path)
    get_join_node_id_map(kwbin_path)
    # for i in node_id_map:
    #     print(i,"    ",node_id_map[i])
    #     print(i,"    ",http_ip_map[i])

    for sql in sqls:
        if re.match('-- node:', sql):
            # reset last_exec_node
            node_ids = get_nodes(sql)
            last_exec_node = node_ids
            pass
        elif re.match('-- init', sql):
            init_load()
            pass
        elif re.match('-- kill:', sql):
            # kill node
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = 'listen-addr=' + get_url_for_join(node_id)
                else:
                    url = 'listen-addr=' + get_url_from_node_id(node_id)
                cmd = 'kill -9 `ps -ef | grep kwbase | grep \'{}\''.format(url) + ' | awk \'{print $2}\'`'
                cmds.append(cmd)

            pass
        elif re.match('-- sleep', sql):
            # sleep
            cmd = get_sleep_cmd(sql)
            cmds.append(cmd)
            pass
        elif re.match('-- restart-with-ts-store', sql):
            # print(sql)
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                    store_id = int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --ts-store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          ' --external-io-dir={}/extern' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_fot_join(node_id), store_dir, 'c' + str(store_id), store_dir,
                                                                                             'c-ts' + str(store_id),
                                                                                             "%02d" % node_id,
                                                                                             store_dir + '/c' + str(
                                                                                                 store_id), store_dir,
                        get_url_from_node_id(1))
                    cmds.append(cmd)
                else:
                    url = get_url_from_node_id(node_id)
                    store_id = int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --ts-store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          '--external-io-dir={}/extern' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_from_node_id(node_id), store_dir, 'c' + str(store_id),
                        store_dir, 'c-ts' + str(store_id),
                                                                                                 "%02d" % node_id,
                                                                                                 store_dir + '/c' + str(
                                                                                                     store_id),
                        store_dir, get_url_from_node_id(1))
                    cmds.append(cmd)
        elif re.match('-- restart', sql):
            # print(sql)
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                    store_id = int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          ' --external-io-dir={}/extern' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_fot_join(node_id), store_dir, 'c' + str(store_id),
                                                                                             "%02d" % node_id,
                                                                                             store_dir + '/c' + str(
                                                                                                 store_id), store_dir,
                        get_url_from_node_id(1))
                    cmds.append(cmd)
                else:
                    url = get_url_from_node_id(node_id)
                    store_id = int(url.split(':')[-1]) - 26256
                    cmd = ' {} {}' \
                          ' --insecure --listen-addr={}' \
                          ' --http-addr={}' \
                          ' --store={}/{}' \
                          ' --locality=region=CN-100000-0{}' \
                          ' --pid-file={}/kwbase.pid ' \
                          '--external-io-dir={}/extern' \
                          ' --join={} --background'.format(
                        kwbin_path, 'start', url, get_http_url_from_node_id(node_id), store_dir, 'c' + str(store_id),
                                                                                                 "%02d" % node_id,
                                                                                                 store_dir + '/c' + str(
                                                                                                     store_id),
                        store_dir, get_url_from_node_id(1))
                    cmds.append(cmd)
        elif re.match('-- join', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_for_join(node_id)
                cmd = ' {} {}' \
                      ' --insecure --listen-addr={}' \
                      ' --http-addr={}' \
                      ' --store={}/{}' \
                      ' --pid-file={}/kwbase.pid ' \
                      ' --locality=region=CN-100000-0{}' \
                      ' --external-io-dir={}/extern' \
                      ' --join={} --background'.format(
                    kwbin_path, 'start', url, get_http_url_fot_join(node_id), store_dir,
                    'c' + str(node_id),
                    store_dir + '/c' + str(node_id),
                    "%02d" % node_id, store_dir,
                    get_url_from_node_id(1)
                )
                cmds.append(cmd)
        elif re.match('-- decommission', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                else:
                    url = get_url_from_node_id(node_id)
                cmd = ' {} {}' \
                      ' --decommission --insecure ' \
                      '--host={} --drain-wait=8s'.format(kwbin_path, 'quit', url)
                cmds.append(cmd)
        elif re.match('-- background-decommission', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                if node_id > 5:
                    url = get_url_for_join(node_id)
                else:
                    url = get_url_from_node_id(node_id)
                cmd = ' {} {}' \
                      ' decommission {} --insecure ' \
                      '--host={} --wait=none'.format(kwbin_path, 'node', node_id, url)
                cmds.append(cmd)
        elif re.match('-- wait-running', sql):
            node_ids = get_nodes(sql)
            url = get_url_from_node_id(node_ids[0])
            cmd = "count=0;" \
                  "while [ $({} sql --insecure --host={} " \
                  "-e \"SELECT sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable FROM kwdb_internal.kv_store_status;\" --format=raw | grep -v '#' | sort | tail -n 1 ) -ne 0 ] && [ $count -lt 180 ];" \
                  "do " \
                  " sleep 1s;  count=$((count+1)) ;" \
                  "done\nif [ $count -ge 180 ]; then echo \"wait-running timeout after 180s\" ;fi".format(kwbin_path,
                                                                                                          url)
            cmds.append(cmd)
        elif re.match('-- wait-nonzero-replica', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;" \
                      "while [ $({} sql --insecure --host={} " \
                      "-e \"SELECT COUNT(*) FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND {}=ANY(replicas);\" --format=raw | grep -v '#' ) -eq 0 ] && [ $count -lt 180 ]; do" \
                      "    count=$((count+1));" \
                      "    sleep 1;" \
                      "done\n" \
                      "if [ $count -ge 180 ]; then" \
                      "    echo \"wait-nonzero-replica timeout after 180s\";" \
                      "fi".format(kwbin_path, url, node_id)
                cmds.append(cmd)
        elif re.match('-- wait-zero-replica', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(1)
                cmd = "count=0;" \
                      "while [ $({} sql --insecure --host={} " \
                      "-e \"SELECT COUNT(*) FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND {}=ANY(replicas);\" --format=raw | grep -v '#' ) -ne 0 ] && [ $count -lt 180 ]; do" \
                      "    count=$((count+1));" \
                      "    sleep 1;" \
                      "done\n" \
                      "if [ $count -ge 180 ]; then" \
                      "    {} sql --insecure --host={} " \
                      "     -e \"SELECT * FROM kwdb_internal.ranges WHERE database_name = 'tsdb1' AND {}=ANY(replicas);\";" \
                      "    echo \"wait-zero-replica timeout after 180s\";" \
                      "fi".format(kwbin_path, url, node_id, kwbin_path, url, node_id)
                cmds.append(cmd)

        elif re.match('-- wait-all-replica-health', sql):
            strs = sql.split(':')
            if len(strs) < 2:
                continue
            ts = strs[1]
            ts = re.sub('s', '', ts)
            # node_ids = get_nodes(sql)
            for node_id in last_exec_node:
                url = get_url_from_node_id(1)
                cmd = "count=0;" \
                      "while [ $({} sql --insecure --host={} " \
                      "-e \"WITH liveness_and_nodes AS (SELECT node_id AS id, CASE WHEN split_part(expiration, ',', 1)::decimal > now()::decimal AND NOT upgrading THEN true ELSE false END AS is_available, COALESCE(is_live, false) AS is_live, ranges AS gossiped_replicas, decommissioning AS is_decommissioning, draining AS is_draining FROM kwdb_internal.gossip_liveness LEFT JOIN kwdb_internal.gossip_nodes USING (node_id) ), kv_store_metrics AS (SELECT node_id AS id, sum((metrics->>'replicas.leaders')::DECIMAL)::INT AS replicas_leaders, sum((metrics->>'replicas.leaseholders')::DECIMAL)::INT AS replicas_leaseholders, sum((metrics->>'replicas')::DECIMAL)::INT AS ranges, sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable, sum((metrics->>'ranges.underreplicated')::DECIMAL)::INT AS ranges_underreplicated FROM kwdb_internal.kv_store_status GROUP BY node_id ) select sum(ranges_underreplicated) from kv_store_metrics ;\" --format=raw | grep -v '#' ) -ne 0 ] && [ $count -lt {} ]; do" \
                      "    count=$((count+1));" \
                      "    sleep 1;" \
                      "done\n" \
                      "if [ $count -ge {} ]; then" \
                      "    {} sql --insecure --host={} " \
                      "     -e \"WITH liveness_and_nodes AS (SELECT node_id AS id, CASE WHEN split_part(expiration, ',', 1)::decimal > now()::decimal AND NOT upgrading THEN true ELSE false END AS is_available, COALESCE(is_live, false) AS is_live, ranges AS gossiped_replicas, decommissioning AS is_decommissioning, draining AS is_draining FROM kwdb_internal.gossip_liveness LEFT JOIN kwdb_internal.gossip_nodes USING (node_id) ), kv_store_metrics AS (SELECT node_id AS id, sum((metrics->>'replicas.leaders')::DECIMAL)::INT AS replicas_leaders, sum((metrics->>'replicas.leaseholders')::DECIMAL)::INT AS replicas_leaseholders, sum((metrics->>'replicas')::DECIMAL)::INT AS ranges, sum((metrics->>'ranges.unavailable')::DECIMAL)::INT AS ranges_unavailable, sum((metrics->>'ranges.underreplicated')::DECIMAL)::INT AS ranges_underreplicated FROM kwdb_internal.kv_store_status GROUP BY node_id ) select sum(ranges_underreplicated) from kv_store_metrics ;;\";" \
                      "    echo \"wait-all-replica-health timeout after {}s\";" \
                      "fi;" \
                      "echo cost $count s".format(kwbin_path, url, ts, ts, kwbin_path, url, ts)
                cmds.append(cmd)

        elif re.match('-- wait-clear', sql):
            strs = sql.split(':')
            if len(strs) < 2:
                continue
            ts = strs[1]
            ts = re.sub('s','',ts)
            for node_id in last_exec_node:
                url = get_url_from_node_id(1)
                cmd = "count=0;" \
                  "while [ $(ls $store_dir\/c1\/tsdb | wc -l ) -ne 2 ] && [ $count -lt {} ]; do" \
                  "    count=$((count+1));" \
                  "    sleep 1;" \
                  "done\n" \
                    "echo cost $count s".format(ts)
                cmds.append(cmd)

        elif re.match('-- upgrade-complete', sql):
            node_ids = get_nodes(sql)
            for node_id in node_ids:
                url = get_url_from_node_id(node_id)
                store_id = int(url.split(':')[-1]) - 26256
                cmd = ' {} {}' \
                      ' --insecure --listen-addr={}' \
                      ' --http-addr={}' \
                      ' --store={}/{}' \
                      ' --locality=region=CN-100000-0{}' \
                      ' --pid-file={}/kwbase.pid ' \
                      ' --external-io-dir={}/extern' \
                      ' --join={} --background'.format(
                    kwbin_path, 'start --upgrade-complete', url, get_http_url_from_node_id(node_id), store_dir,
                    'c' + str(store_id), "%02d" % node_id, store_dir + '/c' + str(store_id), store_dir,
                    get_url_from_node_id(1))
            cmds.append(cmd)
        elif re.match('-- upgrade', sql):

            node_ids = get_nodes(sql)
            for node_id in node_ids:
                cmd = '{}  node upgrade {} --insecure --host={}'.format(
                    kwbin_path, node_id, get_url_from_node_id(node_id)
                )
            cmds.append(cmd)
        elif re.match('-- create-test', sql):
            create_stmts = []
            create_stmts.append('create ts database test_create_ts_table;')
            create_stmts.append('create database test_create_rel_table;')
            table_num, create_rel, create_ts = get_create_table_arg(sql)
            for i in range(table_num):
                ts_table = 'create table test_create_ts_table.tab{}(' \
                           'k_timestamp timestamp not null,e1 float not null, e2 float4 not null, e3 float8 not null,' \
                           ' e4 double precision not null,e5 real not null,e6 int not null, e7 int2 not null, ' \
                           'e8 int4 not null, e9 int8 not null, e10 int64 not null, e11 bigint not null, ' \
                           'e12 smallint not null,e13 integer not null,e14 bool not null,e15 char not null, ' \
                           'e16 char(100) not null, e17 varbytes(100) not null,e18 timestamp not null ,' \
                           'e19 nchar(100) not null,e20 varchar(100) not null,e21 nvarchar(100) not null,' \
                           'e22 varbytes(100) not null, e23 varbytes not null,e24 nchar not null,e25 varchar not null,' \
                           'e26 nvarchar not null,e27 varbytes not null) tags (tag1 int not null)primary tags(tag1);'.format(
                    i)
                rel_table = 'CREATE TABLE test_create_rel_table.t{} (k_timestamp TIMESTAMPTZ NOT NULL,id INT4 NOT NULL,e1 INT2 ,' \
                            'e2 INT4 ,e3 INT8 ,e4 FLOAT4 ,e5 FLOAT8 ,e6 BOOL ,e7 TIMESTAMPTZ ,' \
                            'e8 char(1023) ,e9 char(255) ,e10 char(4096),e11 char ,e12 char(255) ,' \
                            'e13 char ,e14 char(4096) ,e15 char(1023) ,e16 char(200) ,e17 char(255) ,' \
                            'e18 char(200) ,e19 char(254) ,e20 char(60) ,e21 char(254) ,e22 char(63) ,' \
                            'code1 INT2 NOT NULL,code2 INT4,code3 INT8,code4 FLOAT4,code5 FLOAT8,code6 BOOL,code7 char(254),' \
                            'code8 char(128) NOT NULL,code9 char(254),code10 char(60),code11 char(254),code12 char(60),' \
                            'code13 char(2),code14 char(1023) NOT NULL,code15 char(1),code16 char(254) NOT NULL);'.format(
                    i)
                if create_ts == True and create_rel == False:
                    create_stmts.append(ts_table)
                elif create_rel == True and create_ts == False:
                    create_stmts.append(rel_table)

                elif create_ts == True and create_rel == True:
                    if i % 2 == 0:
                        create_stmts.append(ts_table)
                    else:
                        create_stmts.append(rel_table)

            for create_stmt in create_stmts:
                # don't need to record results.
                for node in last_exec_node:
                    if node > 5:

                        cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" ".format(
                            kwbin_path, get_url_for_join(node), create_stmt, )
                    else:
                        cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" ".format(
                            kwbin_path, get_url_from_node_id(node), create_stmt, )
                    cmds.append(cmd)

        # elif re.match('-- return', sql):
        #    cmds.append('return ')

        elif re.match('-- ', sql):
            # other comments
            pass
        else:
            # normal sql stmt
            if is_blank(sql):
                pass
            if len(sql) == 0:
                pass
            temp_sql = re.sub(' ', '', sql)
            if len(temp_sql) > 0 and temp_sql[-1] == ';':
                stmt += ' ' + sql
                stmt = re.sub('^ *', '', stmt)

                for node in last_exec_node:
                    if node > 5:
                        cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" 2>&1 | tee -a {} > /dev/null".format(
                            kwbin_path, get_url_for_join(node), stmt, output_file_path)
                    else:
                        cmd = "{} sql --host={} --insecure --format=table --set \"errexit=false\" --echo-sql -e \"{}\" 2>&1 | tee -a {} > /dev/null".format(
                            kwbin_path, get_url_from_node_id(node), stmt, output_file_path)
                    cmds.append(cmd)
                stmt = ''
            else:
                stmt += ' ' + sql
            pass
    with open(output_cmd_file, 'w') as f:
        for cmd in cmds:
            f.write(cmd)
            f.write('\n')
