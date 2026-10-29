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
# MySQL Connector/J 冒烟测试。
#
# 覆盖 mysql 命令行客户端结构上测不到的东西：JDBC 驱动在连接期自己发的那批
# 语句（loadServerVariables 的 19 列 SELECT @@...、SET character_set_results
# = NULL、SET NAMES ...）。这些语句里任何一条失败，结果都是连接根本建不起来，
# 而不是某个功能降级——所以「能不能连上」本身就是本脚本的第一条断言，且与
# 「连上之后能不能用」分开断言，否则两种失败在输出里无法区分。
#
# 需要 JDK 11+（用单文件源码启动器，不需要预先 javac）与 Connector/J jar。
# 任一缺失即跳过并以 0 退出，以免在没有 Java 的 CI 节点上把整条流水线判红。
# 该脚本有意不放进 qa/Integration/：run_test_local_v2.sh 只在
# Integration/performance 下收集可执行文件，而它按 <name>_passed 判定结果，
# 会把这里的 _skipped 结尾判成失败。
#
# 用法: qa/run_mysql_protocol_test.sh jdbc [拓扑...]      拓扑取 1n / 5c / 5cr，默认 1n
#
# 节点由 topology.sh 起，与其他 qa 一样 store 在 install/deploy/ 下，
# 跑完只停进程不删目录；编译用的 Java 源文件与 truststore 在
# install/deploy/mysql-jdbc/。

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
WORK_DIR="${DEPLOY_ROOT}/mysql-jdbc"
TRUSTSTORE="$WORK_DIR/truststore.jks"
TRUSTPASS=changeit

PASS=0; FAIL=0

# --- 输出 -------------------------------------------------------------------

ok()   { PASS=$((PASS+1)); echo -e "${SUCC}  PASS${NC} [$TOPO] $1"; }
bad()  { FAIL=$((FAIL+1)); echo -e "${FAIL_COLOR:-\033[0;31m}  FAIL${NC} [$TOPO] $1"; [ -n "$2" ] && echo "        $2"; }
info() { echo -e "${INFO}$1${NC}"; }

cleanup() { [ -n "$TOPO" ] && mysql_topo_stop "$TOPO"; }
trap cleanup EXIT

# --- 前置检查 ---------------------------------------------------------------

if ! command -v java >/dev/null 2>&1; then
  echo "${SCRIPT_NAME}: java not found, skipping (install a JDK 11+ to run)"
  echo "${SCRIPT_NAME}_skipped"
  exit 0
fi

JAR="${MYSQL_CONNECTOR_JAR:-$(ls -1 "$HOME"/jdbc/mysql-connector-j-*.jar 2>/dev/null | head -1)}"
if [ -z "$JAR" ] || [ ! -r "$JAR" ]; then
  echo "${SCRIPT_NAME}: Connector/J jar not found, skipping (set MYSQL_CONNECTOR_JAR)"
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

# --- 夹具 -------------------------------------------------------------------
#
# 与 protocol_test.sh 同样有意不用 --insecure：insecure 模式根本不
# 校验口令，认证配置那一维等于没测；TLS 那一档也需要真证书。证书随拓扑重建，
# 所以 truststore 也每个拓扑重导一次。

mkdir -p "$WORK_DIR"

setup_fixtures() {
  # JDK 只认 keystore，不认 PEM，所以把自建 CA 导成 truststore。
  rm -f "$TRUSTSTORE"
  keytool -importcert -noprompt -alias kwdbca -file "$MYSQL_TOPO_CERTS/ca.crt" \
    -keystore "$TRUSTSTORE" -storepass "$TRUSTPASS" >/dev/null 2>&1
  mysql_topo_sql 1 >/dev/null 2>&1 <<'SQL'
CREATE USER alice WITH PASSWORD 'secret123';
GRANT ALL ON DATABASE defaultdb TO alice;
SQL
}

# --- 被测程序 ---------------------------------------------------------------
#
# 有意不复用 ~/jdbc 下手工调试用的那份：那一份在 catch 里正常返回，做成测试
# 会把「驱动连不上」判成通过。这里必须以非零退出。

cat > "$WORK_DIR/JdbcSmoke.java" <<'JAVA'
import java.sql.*;

public class JdbcSmoke {
  public static void main(String[] a) throws Exception {
    try (Connection c = DriverManager.getConnection(a[0], a[1], a[2])) {
      DatabaseMetaData md = c.getMetaData();
      System.out.println("CONNECTED productVersion=" + md.getDatabaseProductVersion());
      System.out.println("identifierQuote=[" + md.getIdentifierQuoteString() + "]");
      // getTransactionIsolation() is what a connection pool calls on startup.
      // The driver looks the server's spelling up in a case-sensitive, hyphenated
      // map, so an unrecognised one throws SQLState S1000 here — long after the
      // connection itself succeeded, which is why this is its own assertion.
      System.out.println("iso=" + c.getTransactionIsolation());
      try (Statement s = c.createStatement(); ResultSet rs = s.executeQuery("SELECT 1")) {
        while (rs.next()) System.out.println("SELECT 1 -> " + rs.getInt(1));
      }
      c.createStatement().execute("DROP TABLE IF EXISTS defaultdb.jdbcsmoke");
      c.createStatement().execute("CREATE TABLE defaultdb.jdbcsmoke (id INT PRIMARY KEY, v STRING)");
      // useServerPrepStmts defaults to false, so these values are interpolated
      // client-side: how the driver escapes them depends entirely on the status
      // flags and sql_mode the server declared during the handshake.
      try (PreparedStatement p = c.prepareStatement(
             "INSERT INTO defaultdb.jdbcsmoke VALUES (?,?),(?,?)")) {
        p.setInt(1,1); p.setString(2,"a\\b");
        p.setInt(3,2); p.setString(4,"quote'here");
        p.executeUpdate();
      }
      try (ResultSet rs = c.createStatement().executeQuery(
             "SELECT v, length(v) FROM defaultdb.jdbcsmoke ORDER BY id")) {
        while (rs.next()) System.out.println("row=[" + rs.getString(1) + "] len=" + rs.getInt(2));
      }
      c.createStatement().execute("DROP TABLE defaultdb.jdbcsmoke");
    } catch (SQLException e) {
      System.out.println("FAILED " + e.getClass().getName()
        + " SQLState=" + e.getSQLState() + " code=" + e.getErrorCode());
      System.out.println("msg: " + e.getMessage());
      System.exit(1);
    }
  }
}
JAVA

jdbc() { java -cp "$JAR" "$WORK_DIR/JdbcSmoke.java" "$1" "$2" "$3" 2>&1; }

# --- 断言 -------------------------------------------------------------------
#
# 同一组断言跑三种认证配置：驱动的握手分支各不相同，而连接期语句是在认证
# 之后才发的，所以「换个插件就连不上」与「插件没问题但连接期语句挂了」必须
# 各自能被看见。

run_case() { # <label> <jdbc url>
  local label="$1" url="$2" out
  out=$(jdbc "$url" alice secret123)

  echo "$out" | grep -q "CONNECTED" \
    && ok  "$label: connects" \
    || bad "$label: connects" "$(echo "$out" | tail -2)"
  echo "$out" | grep -q "productVersion=8.0.11-KWDB" \
    && ok  "$label: product version matches the handshake" \
    || bad "$label: product version matches the handshake" "$(echo "$out" | grep -m1 productVersion)"
  echo "$out" | grep -q 'identifierQuote=\["\]' \
    && ok  "$label: ANSI_QUOTES took effect" \
    || bad "$label: ANSI_QUOTES took effect" "$(echo "$out" | grep -m1 identifierQuote)"
  echo "$out" | grep -qE 'iso=(8|2|4)' \
    && ok  "$label: getTransactionIsolation does not throw" \
    || bad "$label: getTransactionIsolation does not throw" "$(echo "$out" | tail -2)"
  echo "$out" | grep -q "SELECT 1 -> 1" \
    && ok  "$label: executes a query" \
    || bad "$label: executes a query" "$(echo "$out" | tail -2)"
  # NO_BACKSLASH_ESCAPES 没被宣称的话，驱动会把这两个值编成 'a\\b' 与
  # 'quote\'here'：前者写进去多一个字节，后者让字面量提前闭合。
  echo "$out" | grep -q 'row=\[a\\b\] len=3' \
    && ok  "$label: backslash round trip" \
    || bad "$label: backslash round trip" "$(echo "$out" | grep row= | tr '\n' ' ')"
  echo "$out" | grep -q "row=\[quote'here\] len=10" \
    && ok  "$label: embedded quote round trip" \
    || bad "$label: embedded quote round trip" "$(echo "$out" | grep row= | tr '\n' ' ')"
}

run_suite() { # <mysql 端口>
  local base="jdbc:mysql://${MYSQL_TOPO_HOST}:$1/defaultdb"

  info "[1] caching_sha2_password + RSA 公钥（驱动默认插件）"
  run_case "caching_sha2 + RSA" "$base?sslMode=DISABLED&allowPublicKeyRetrieval=true"

  info "[2] mysql_native_password（老服务端兼容路径）"
  run_case "mysql_native_password" "$base?sslMode=DISABLED&defaultAuthenticationPlugin=mysql_native_password"

  info "[3] TLS"
  run_case "TLS" "$base?sslMode=REQUIRED&trustCertificateKeyStoreUrl=file://$TRUSTSTORE&trustCertificateKeyStorePassword=$TRUSTPASS"
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
  fail_before=$FAIL
  setup_fixtures
  run_suite "$(mysql_topo_mysql_port 1)"
  if [ "$MYSQL_TOPO_MODE" != n ]; then
    # 每个节点都开着自己的 MySQL 监听，最后一个节点也要能连上、能用。
    info "[4] 集群：经最后一个节点（node $MYSQL_TOPO_COUNT）连接"
    run_case "node $MYSQL_TOPO_COUNT, caching_sha2 + RSA" \
      "jdbc:mysql://${MYSQL_TOPO_HOST}:$(mysql_topo_mysql_port "$MYSQL_TOPO_COUNT")/defaultdb?sslMode=DISABLED&allowPublicKeyRetrieval=true"
  fi
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
