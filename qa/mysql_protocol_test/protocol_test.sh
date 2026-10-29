#!/bin/bash
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
#
# MySQL 线协议回归测试。
#
# 覆盖范围与 logictest 互补，不重叠：logictest 的 runner 只连 pgwire，
# 结构上测不到 MySQL 协议，本脚本只测协议层行为——认证矩阵、结果集回程、
# MySQL 特有语句——不重复验证 SQL 语义。
#
# 需要 mysql 客户端（apt install mysql-client）。缺失时跳过并以 0 退出，
# 以免在没有该依赖的 CI 节点上把整条流水线判红。
#
# 用法: qa/run_mysql_protocol_test.sh protocol [拓扑...]      拓扑取 1n / 5c / 5cr，默认 1n
#
# 与其他 qa 一样，store 在 install/deploy/<n1|c1..|cr1..>，跑完只停进程不删目录，
# 失败现场（节点日志在 <store>/logs/）留给排查；下次运行开始时清掉。

SCRIPT_NAME=$(basename "${0}")
# env.sh 按 $0 推算 QA_DIR，只能由 qa/ 顶层的入口 source；与 Integration/ 下的
# 脚本一样，这里直接用入口导出的变量。
if [ -z "$QA_DIR" ] || [ -z "$DEPLOY_ROOT" ]; then
  echo "${SCRIPT_NAME}: run it via qa/run_mysql_protocol_test.sh" >&2
  exit 2
fi

source "$QA_DIR/mysql_protocol_test/topology.sh"

TOPOLOGIES=${*:-1n}
TOPO=""        # 正在测的拓扑，结果行以它开头
MYSQL_PORT=""  # MY() 连接的端口，默认第 1 个节点

PASS=0; FAIL=0

# --- 输出 -------------------------------------------------------------------

ok()   { PASS=$((PASS+1)); echo -e "${SUCC}  PASS${NC} [$TOPO] $1"; }
bad()  { FAIL=$((FAIL+1)); echo -e "${FAIL_COLOR:-\033[0;31m}  FAIL${NC} [$TOPO] $1"; [ -n "$2" ] && echo "        $2"; }
info() { echo -e "${INFO}$1${NC}"; }

cleanup() { [ -n "$TOPO" ] && mysql_topo_stop "$TOPO"; }
trap cleanup EXIT

# --- 前置检查 ---------------------------------------------------------------

if ! command -v mysql >/dev/null 2>&1; then
  echo "${SCRIPT_NAME}: mysql client not found, skipping (install mysql-client to run)"
  echo "${SCRIPT_NAME}_skipped"
  exit 0
fi

KWBIN="${KWBIN:-$QA_DIR/../install/bin/kwbase}"
LIB_DIR="${LIB_DIR:-$QA_DIR/../install/lib}"
if [ ! -x "$KWBIN" ]; then
  echo "${SCRIPT_NAME}: kwbase not found at $KWBIN (run 'make install' first)"
  echo "${SCRIPT_NAME}_failed"
  exit 1
fi
export LD_LIBRARY_PATH="$LIB_DIR:$LD_LIBRARY_PATH"

# --- 起节点 -----------------------------------------------------------------
#
# 用带证书的安全节点，不能用 --insecure：insecure 模式根本不校验口令，
# 认证部分等于没测。TLS 路径也需要证书。节点由 topology.sh 按拓扑
# 起，PG 侧的夹具与断言都连第 1 个节点。

PGSQL() { mysql_topo_sql 1 "$@"; }

# --- 断言辅助 ---------------------------------------------------------------

# MY 把 stderr 并进 stdout 并滤掉客户端那句口令警告，调用方拿到的就是全部输出。
# 额外的 mysql 参数经 $EXTRA 传入（故意不加引号，需要词分割）。
MY() { mysql -h "$MYSQL_TOPO_HOST" -P "$MYSQL_PORT" --protocol=TCP "$@" 2>&1 \
        | grep -v "Using a password on the command line"; }

# 期望连接成功且输出匹配
expect_value() { # <label> <expected> <user> <pass> <sql>
  local label="$1" want="$2" user="$3" pass="$4" sql="$5"
  local got
  got=$(MY -u "$user" -p"$pass" -N $EXTRA -e "$sql" | tr -d '[:space:]')
  if [ "$got" = "$want" ]; then ok "$label"
  else bad "$label" "expected '$want', got '$got'"; fi
}

# 期望被拒绝，且错误里含指定片段
expect_reject() { # <label> <user> <pass> <fragment>
  local label="$1" user="$2" pass="$3" frag="$4"
  local out
  out=$(MY -u "$user" -p"$pass" $EXTRA -e "SELECT 1;")
  if echo "$out" | grep -q "$frag"; then ok "$label"
  else bad "$label" "expected error containing '$frag', got: $(echo "$out" | tail -1)"; fi
}

# --- 每个拓扑跑一遍的断言 ---------------------------------------------------

run_suite() {
# --- 测试夹具 ---------------------------------------------------------------
#
# 三个账号覆盖三种凭据状态，它们在 MySQL 认证下行为不同：
#   alice  —— 正常账号，两份 MySQL 摘要齐全
#   legacy —— 摘要被置 NULL，模拟迁移前创建的老账号
#   nopw   —— 无口令账号，CREATE USER 存的是空字节串而非 NULL，是第三种状态

info "creating fixtures"
PGSQL >/dev/null 2>&1 <<'SQL'
CREATE USER alice WITH PASSWORD 'secret123';
CREATE USER legacy WITH PASSWORD 'legacy123';
CREATE USER nopw;
UPDATE system.users SET "mysqlNativeDigest"=NULL, "mysqlSHA2Digest"=NULL WHERE username='legacy';

CREATE DATABASE reldb;
CREATE TABLE reldb.t (id INT PRIMARY KEY, name STRING);
INSERT INTO reldb.t VALUES (1,'alpha'),(2,'beta');

CREATE TS DATABASE tsdb;
CREATE TABLE tsdb.sensor (k_timestamp TIMESTAMPTZ NOT NULL, temp FLOAT)
  TAGS (device_id INT NOT NULL) PRIMARY TAGS(device_id);
INSERT INTO tsdb.sensor VALUES ('2026-01-01 08:00:00', 21.5, 1);
INSERT INTO tsdb.sensor VALUES ('2026-01-01 08:00:10', 22.1, 2);
INSERT INTO tsdb.sensor VALUES ('2026-01-01 08:00:20', 23.7, 3);

GRANT ALL ON DATABASE reldb TO alice;
GRANT ALL ON DATABASE tsdb TO alice;
GRANT ALL ON TABLE reldb.t TO alice;
GRANT ALL ON TABLE tsdb.sensor TO alice;
SQL

# 每行用不同的 device_id：时序表按 (PRIMARY TAGS, k_timestamp) 去重，
# 复用同一个 tag 是覆盖而非新增，行数会对不上。

# ============================================================================
info "[1] 认证矩阵 —— 8 条插件/传输路径"
# ============================================================================
# pgwire 只有一种明文口令方法；MySQL 的三个插件各有不同的传输前提，
# 每条都要单独走一遍，因为它们在服务端是不同的代码分支。

EXTRA="--default-auth=mysql_native_password --ssl-mode=DISABLED" expect_value "native_password, plaintext" 1 alice secret123 "SELECT 1;"
EXTRA="--default-auth=caching_sha2_password --ssl-mode=DISABLED" expect_value "caching_sha2, fast path" 1 alice secret123 "SELECT 1;"
EXTRA="--default-auth=caching_sha2_password --ssl-mode=DISABLED --get-server-public-key" expect_value "caching_sha2, with RSA key" 1 alice secret123 "SELECT 1;"
EXTRA="--default-auth=caching_sha2_password --ssl-mode=REQUIRED" expect_value "caching_sha2, over TLS" 1 alice secret123 "SELECT 1;"
EXTRA="--default-auth=sha256_password --ssl-mode=DISABLED --get-server-public-key" expect_value "sha256, with RSA key" 1 alice secret123 "SELECT 1;"
EXTRA="--default-auth=sha256_password --ssl-mode=REQUIRED" expect_value "sha256, over TLS" 1 alice secret123 "SELECT 1;"
# sha256 客户端插件总是自动索取服务端公钥，--get-server-public-key 只对
# caching_sha2 生效，所以下面这条不加该标志也应通过，且抓包确认无明文口令。
EXTRA="--default-auth=sha256_password --ssl-mode=DISABLED" expect_value "sha256, plaintext without flag" 1 alice secret123 "SELECT 1;"
EXTRA="" expect_value "default negotiation" 1 alice secret123 "SELECT 1;"

# ============================================================================
info "[2] 拒绝路径 —— 比成功路径更值得测"
# ============================================================================

for p in mysql_native_password caching_sha2_password sha256_password; do
  EXTRA="--default-auth=$p --ssl-mode=REQUIRED" expect_reject "wrong password rejected ($p)" alice WRONGPASS "Access denied"
done

# 「口令错」与「账号不存在」必须不可区分，否则构成账号枚举。
WRONG_ERR=$(MY -u alice -pWRONGPASS --ssl-mode=REQUIRED -e "SELECT 1;" 2>&1 | grep -o "Access denied.*" | sed "s/'alice'/'USER'/")
GHOST_ERR=$(MY -u ghost -psecret123 --ssl-mode=REQUIRED -e "SELECT 1;" 2>&1 | grep -o "Access denied.*" | sed "s/'ghost'/'USER'/")
if [ -n "$WRONG_ERR" ] && [ "$WRONG_ERR" = "$GHOST_ERR" ]; then
  ok "nonexistent user indistinguishable from wrong password"
else
  bad "nonexistent user indistinguishable from wrong password" "wrong='$WRONG_ERR' ghost='$GHOST_ERR'"
fi

# ============================================================================
info "[3] 凭据状态 —— 迁移前的老账号"
# ============================================================================
# 摘要为 NULL 时，native_password 无从校验且没有明文回落，应当拒绝；
# 另两个插件会落到完整认证，用 bcrypt 校验，应当通过。

EXTRA="--default-auth=mysql_native_password --ssl-mode=REQUIRED" expect_reject "legacy account rejected by native_password" legacy legacy123 "Access denied"
EXTRA="--default-auth=caching_sha2_password --ssl-mode=REQUIRED" expect_value "legacy account accepted by caching_sha2" 1 legacy legacy123 "SELECT 1;"
EXTRA="--default-auth=sha256_password --ssl-mode=REQUIRED" expect_value "legacy account accepted by sha256" 1 legacy legacy123 "SELECT 1;"

PGSQL -e "ALTER USER legacy WITH PASSWORD 'legacy123';" >/dev/null 2>&1
EXTRA="--default-auth=mysql_native_password --ssl-mode=REQUIRED" expect_value "native_password works after password reset" 1 legacy legacy123 "SELECT 1;"

# ============================================================================
info "[4] 改口令后三份凭据同步"
# ============================================================================
# 任何一个插件仍接受旧口令，说明那份摘要没跟着更新——这是最危险的一类
# 失败，因为表面看起来一切正常。

PGSQL -e "ALTER USER alice WITH PASSWORD 'newpass456';" >/dev/null 2>&1
for p in mysql_native_password caching_sha2_password sha256_password; do
  EXTRA="--default-auth=$p --ssl-mode=REQUIRED" expect_reject "old password rejected ($p)" alice secret123 "Access denied"
  EXTRA="--default-auth=$p --ssl-mode=REQUIRED" expect_value "new password accepted ($p)" 1 alice newpass456 "SELECT 1;"
done
PGSQL -e "ALTER USER alice WITH PASSWORD 'secret123';" >/dev/null 2>&1

# ============================================================================
info "[5] 时序表读取 —— 本次修复的核心"
# ============================================================================
# 修复前这些查询返回列元数据齐全、零行、且不报错。必须在短路优化开启
# （默认 true）的前提下测，否则等于没测。

SC=$(PGSQL -e "SHOW CLUSTER SETTING sql.pg_encode_short_circuit.enabled;" 2>/dev/null | tail -1 | tr -d '[:space:]')
if [ "$SC" = "true" ]; then ok "short circuit is enabled (precondition)"
else bad "short circuit is enabled (precondition)" "got '$SC'; TS results below would not exercise the guard"; fi

EXTRA="" expect_value "TS table count over MySQL" 3 alice secret123 "SELECT count(*) FROM tsdb.sensor;"
EXTRA="" expect_value "TS table rows over MySQL" 123 alice secret123 "SELECT device_id FROM tsdb.sensor ORDER BY device_id;"
EXTRA="" expect_value "TS builtin last() over MySQL" 23.7 alice secret123 "SELECT last(temp) FROM tsdb.sensor;"
EXTRA="" expect_value "relational table (control)" 2 alice secret123 "SELECT count(*) FROM reldb.t;"

# ============================================================================
info "[6] MySQL 特有语句"
# ============================================================================

EXTRA="" expect_value "SET NAMES with quoted COLLATE" 1 alice secret123 "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_0900_ai_ci'; SELECT 1;"
EXTRA="" expect_value "SET NAMES with bare COLLATE" 1 alice secret123 "SET NAMES utf8mb4 COLLATE utf8mb4_general_ci; SELECT 1;"
EXTRA="" expect_value "charset round-trip after COLLATE" 中文 alice secret123 "SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'; SELECT '中文';"

# 悬空的 COLLATE 必须仍然报错：它证明改写没有变成
# 「凡是长得像 SET NAMES 就照单全收」。
DANGLING=$(MY -u alice -psecret123 -e "SET NAMES utf8mb4 COLLATE;" 2>&1 | grep -v 'command line')
if echo "$DANGLING" | grep -q "ERROR"; then ok "dangling COLLATE still rejected"
else bad "dangling COLLATE still rejected" "got: $DANGLING"; fi

# autocommit=off 被拒是有意的设计：KWDB 对显式事务外的语句总是提交，
# 兑现不了手动事务控制，所以明确告知而非默许。但提示必须可操作。
AC=$(MY -u alice -psecret123 -e "SET autocommit = 0;" 2>&1 | grep -v 'command line')
if echo "$AC" | grep -q "autocommit=True"; then ok "autocommit=0 rejection carries actionable hint"
else bad "autocommit=0 rejection carries actionable hint" "got: $(echo "$AC" | tail -1)"; fi
if echo "$AC" | grep -qE "CockroachDB|no effect"; then
  bad "autocommit hint free of stale text" "message still mentions CockroachDB or 'no effect'"
else ok "autocommit hint free of stale text"; fi
EXTRA="" expect_value "autocommit=1 accepted" 1 alice secret123 "SET autocommit = 1; SELECT 1;"

# 内部错误不应把 errors 库的联系我们样板送到客户端。
BOILER=$(MY -u alice -psecret123 -e "SELECT kwdb_internal.force_assertion_error('boom');" 2>&1)
if echo "$BOILER" | grep -q "We appreciate your feedback"; then
  bad "assertion boilerplate not leaked" "contact-us block reached the client"
else ok "assertion boilerplate not leaked"; fi

# ============================================================================
info "[7] DDL 与基本 SQL"
# ============================================================================

EXTRA="" expect_value "CREATE/INSERT/SELECT relational" ab alice secret123 \
  "CREATE TABLE reldb.demo (id INT PRIMARY KEY, name STRING); INSERT INTO reldb.demo VALUES (1,'a'),(2,'b'); SELECT name FROM reldb.demo ORDER BY id;"
EXTRA="" expect_value "DROP relational table" 0 alice secret123 \
  "DROP TABLE reldb.demo; SELECT count(*) FROM information_schema.tables WHERE table_catalog='reldb' AND table_name='demo';"
EXTRA="" expect_value "CREATE/INSERT/SELECT time-series" 21.5 alice secret123 \
  "CREATE TABLE tsdb.ts_demo (k_timestamp TIMESTAMPTZ NOT NULL, temp FLOAT) TAGS (device_id INT NOT NULL) PRIMARY TAGS(device_id); INSERT INTO tsdb.ts_demo VALUES ('2026-01-01 10:00:00', 21.5, 1); SELECT temp FROM tsdb.ts_demo;"
EXTRA="" expect_value "DROP time-series table" 0 alice secret123 \
  "DROP TABLE tsdb.ts_demo; SELECT count(*) FROM information_schema.tables WHERE table_catalog='tsdb' AND table_name='ts_demo';"

# ============================================================================
info "[8] 已知未支持 —— 变绿即说明有人补上了，届时应把断言反转"
# ============================================================================
# 这一条目前必然失败，钉住它是为了记录现状：一旦有人实现了，
# 测试会失败并提醒把期望改成通过，而不是让缺口悄悄消失。

for stmt in "DESC reldb.t"; do
  OUT=$(MY -u alice -psecret123 -e "$stmt;" 2>&1 | grep -v 'command line')
  if echo "$OUT" | grep -q "ERROR 1064"; then
    ok "known unsupported, still unsupported: $stmt"
  else
    bad "known unsupported, still unsupported: $stmt" "no longer fails — implement support and flip this assertion"
  fi
done

# ============================================================================
info "[9] 连接期与运行期会话变量 —— Connector/J loadServerVariables"
# ============================================================================
# 驱动握手后立刻发一条 19 列的 SELECT @@a AS a, ...；这一条里任何一列出错，
# 结果都是连接根本建不起来，而不是功能降级。这里钉住的是那几个「答错了驱动
# 也不报错、只是悄悄按错值行事」的变量，逐列覆盖交给 Go 端到端测试。

EXTRA="" expect_value "transaction_isolation uses MySQL's spelling" SERIALIZABLE \
  alice secret123 "SELECT @@transaction_isolation;"
EXTRA="" expect_value "autocommit is 1/0, not on/off" 1 \
  alice secret123 "SELECT @@session.autocommit;"
EXTRA="" expect_value "transaction_read_only parses as an int" 0 \
  alice secret123 "SELECT @@session.transaction_read_only;"
EXTRA="" expect_value "max_allowed_packet is a bare decimal" 67108864 \
  alice secret123 "SELECT @@max_allowed_packet;"
EXTRA="" expect_value "sql_mode declares KWDB's fixed quoting rules" \
  STRICT_TRANS_TABLES,ANSI_QUOTES,NO_BACKSLASH_ESCAPES \
  alice secret123 "SELECT @@sql_mode;"
EXTRA="" expect_value "@@version matches the handshake" 8.0.11-KWDB \
  alice secret123 "SELECT @@version;"
EXTRA="" expect_value "version_comment now answers" KaiwuDB \
  alice secret123 "SELECT @@version_comment;"
EXTRA="" expect_value "USER() resolves" alice \
  alice secret123 "SELECT USER();"

# 改写不能吃掉字面量里的 @@。
EXTRA="" expect_value "a literal @@ survives untouched" @@sql_mode \
  alice secret123 "SELECT '@@sql_mode';"

# 连接期那条 SET，MySQL 里的含义是「服务端不转码结果集」——KWDB 本来就只产
# UTF-8，所以这是被兑现了的空操作，而不是被假装的。空操作还是重置，看的是
# 随后读回来的值。
EXTRA="" expect_value "SET character_set_results = NULL is a no-op" utf8mb4 \
  alice secret123 "SET character_set_results = NULL; SELECT @@character_set_results;"

# 兼容性护栏：这两条今天有效，改写器绝不能碰它们。SET SESSION TRANSACTION
# 与 SET SESSION CHARACTERISTICS AS TRANSACTION 共用 transaction_mode_list，
# 但后者拒绝 PRIORITY、静默丢弃 AS OF SYSTEM TIME。
EXTRA="" expect_value "transaction priority still settable" high \
  alice secret123 "BEGIN; SET SESSION TRANSACTION PRIORITY HIGH; SHOW TRANSACTION PRIORITY; COMMIT;"

# Connection.setTransactionIsolation() / setReadOnly() 发的就是这两条，连接池
# 在拿到连接的第一时间就会调。
EXTRA="" expect_value "SET SESSION TRANSACTION ISOLATION LEVEL takes effect" readcommitted \
  alice secret123 "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED; SHOW transaction_isolation;"
EXTRA="" expect_value "SET SESSION TRANSACTION READ ONLY takes effect" on \
  alice secret123 "SET SESSION TRANSACTION READ ONLY; SHOW transaction_read_only;"

# 已知未支持：变绿即说明有人补上了，届时把断言反转
for stmt in "SHOW VARIABLES" "SET @@sql_mode='x'" "SELECT @@global.time_zone"; do
  OUT=$(MY -u alice -psecret123 -e "$stmt;" 2>&1 | grep -v 'command line')
  if echo "$OUT" | grep -q "ERROR"; then
    ok "known unsupported, still unsupported: $stmt"
  else
    bad "known unsupported, still unsupported: $stmt" "no longer fails — implement support and flip this assertion"
  fi
done

# ============================================================================
info "[10] 一次回复多个结果集 —— SERVER_MORE_RESULTS_EXISTS"
# ============================================================================
# 这类问题出错时客户端不报错，而是把没读完的结果当成下一条命令的回复，所以
# 每条都在同一个会话里接一句 SELECT 'in-sync'，拿它的值判断会话是否错位。
# delimiter 让 mysql 客户端把几条语句作为一个 COM_QUERY 整体发出；不用它的话
# 客户端会自己按 ';' 拆开逐条发，测不到服务端的多结果集。

# expect_lines <label> <期望输出，行之间用 | 连接>，SQL 从 stdin 读
expect_lines() {
  local label="$1" want="$2" got
  got=$(MY -u alice -psecret123 -N -B --force reldb | paste -sd '|')
  if [ "$got" = "$want" ]; then ok "$label"
  else bad "$label" "expected '$want', got '$got'"; fi
}

expect_lines "multi-statement query returns every result set" "1|second|in-sync" <<'SQL'
delimiter $$
SELECT 1; SELECT 'second'$$
delimiter ;
SELECT 'in-sync';
SQL

# MySQL 在第一条失败处停下：报错送达、其后的 SELECT 3 不执行、会话仍可用
expect_lines "error stops a multi-statement query, session stays usable" \
  "1|ERROR 1054 (42703) at line 2: column \"nosuchcol\" does not exist|in-sync" <<'SQL'
delimiter $$
SELECT 1; SELECT nosuchcol FROM t; SELECT 3$$
delimiter ;
SELECT 'in-sync';
SQL

# 两个结果集列数不同：修复前第二个结果集的行被塞进第一个结果集的列定义里，
# 多出来的列被悄悄丢掉
expect_lines "CALL returns each result set with its own columns" $'1|1\talpha|in-sync' <<'SQL'
delimiter $$
CREATE PROCEDURE two_sets() BEGIN SELECT id FROM t WHERE id = 1; SELECT id, name FROM t WHERE id = 1; END$$
delimiter ;
CALL two_sets();
SELECT 'in-sync';
SQL

# MySQL 的结果集以列数开头，列数 0 就是 OK 包的首字节，服务端须改回 OK 包
expect_lines "zero-column result set leaves the session usable" "in-sync" <<'SQL'
CREATE TABLE nocols ();
INSERT INTO nocols DEFAULT VALUES;
SELECT * FROM nocols;
SELECT 'in-sync';
SQL

}

# --- 集群专属 -----------------------------------------------------------------
#
# 每个节点都开着自己的 MySQL 监听。只连第 1 个节点测不出某个节点没起监听、或
# 监听起了但认证读不到复制过来的用户；这里连最后一个节点，并确认经它写入的
# 数据从第 1 个节点读得到。

run_cluster_checks() {
  info "[11] 集群：经最后一个节点（node $MYSQL_TOPO_COUNT）的 MySQL 端口"
  MYSQL_PORT=$(mysql_topo_mysql_port "$MYSQL_TOPO_COUNT")
  EXTRA="--ssl-mode=REQUIRED" expect_value "authenticate on node $MYSQL_TOPO_COUNT" 1 alice secret123 "SELECT 1;"
  EXTRA="" expect_value "relational read on node $MYSQL_TOPO_COUNT" 2 alice secret123 "SELECT count(*) FROM reldb.t;"
  EXTRA="" expect_value "TS read on node $MYSQL_TOPO_COUNT" 3 alice secret123 "SELECT count(*) FROM tsdb.sensor;"
  EXTRA="" expect_value "write on node $MYSQL_TOPO_COUNT" 1 alice secret123 \
    "INSERT INTO reldb.t VALUES (100,'from-last'); SELECT count(*) FROM reldb.t WHERE id = 100;"
  MYSQL_PORT=$(mysql_topo_mysql_port 1)
  EXTRA="" expect_value "that write is visible on node 1" from-last alice secret123 \
    "SELECT name FROM reldb.t WHERE id = 100;"
}

# --- 主流程 -------------------------------------------------------------------

FAILED_LOGS=""
for TOPO in $TOPOLOGIES; do
  info "========== topology $TOPO =========="
  if ! mysql_topo_start "$TOPO" secure; then
    bad "start $TOPO" "nodes did not become live; see ${DEPLOY_ROOT}/*.start.log"
    FAILED_LOGS+=" $(mysql_topo_log 1)"
    mysql_topo_stop "$TOPO"
    continue
  fi
  MYSQL_PORT=$(mysql_topo_mysql_port 1)
  fail_before=$FAIL
  run_suite
  [ "$MYSQL_TOPO_MODE" != n ] && run_cluster_checks
  [ "$FAIL" -gt "$fail_before" ] && FAILED_LOGS+=" $(mysql_topo_log 1)"
  mysql_topo_stop "$TOPO"
done
TOPO=""

# ============================================================================

echo
echo "----------------------------------------"
echo "  passed: $PASS   failed: $FAIL"
echo "----------------------------------------"
if [ "$FAIL" -gt 0 ]; then
  echo "node logs:${FAILED_LOGS} (stores kept under ${DEPLOY_ROOT})"
  echo "${SCRIPT_NAME}_failed"
  exit 1
fi
echo "${SCRIPT_NAME}_passed"
exit 0
