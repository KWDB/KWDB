import java.sql.*;

public class DatabaseService {

    // 数据库配置
    private static final String JDBC_URL = "jdbc:postgresql://127.0.0.1:26257/defaultdb";
    private static final String USERNAME = "u1";
    private static final String PASSWORD = "abc";

    public static void main(String[] args) {

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        System.out.println("输入sql语句: " + args[0]);

        try {
            // 按分号分割SQL语句，注意处理分号周围可能存在的空格
            String[] sqls = args[0].split(";");

            // 2. 建立连接
            conn = DriverManager.getConnection(JDBC_URL, USERNAME, PASSWORD);

            // 3. 创建 Statement
            stmt = conn.createStatement();
            for (int j=0; j<sqls.length; j++) {
            // 4. 执行查询
            if (sqls[j].trim().toUpperCase().startsWith("SELECT")) {
                rs = stmt.executeQuery(sqls[j]);

                // 5. 处理结果集
                ResultSetMetaData metaData = rs.getMetaData();
                int columnCount = metaData.getColumnCount();

                // 打印表头
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print(metaData.getColumnName(i));
                    if (i < columnCount) System.out.print(" | ");
                }
                System.out.println();

                // 打印分隔线
                for (int i = 1; i <= columnCount; i++) {
                    System.out.print("----------------");
                    if (i < columnCount) System.out.print("+");
                }
                System.out.println();

                // 打印数据行
                while (rs.next()) {
                    for (int i = 1; i <= columnCount; i++) {
                        System.out.print(rs.getString(i));
                        if (i < columnCount) System.out.print(" | ");
                    }
                    System.out.println();
                }
            } else {
                // 执行 UPDATE, INSERT, DELETE 等操作
                int affectedRows = stmt.executeUpdate(sqls[j]);
                System.out.println("执行成功，受影响的行数: " + affectedRows);
            }
            }

        } catch (SQLException e) {
            System.err.println("数据库错误: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // 6. 关闭资源
            try {
                if (rs != null) rs.close();
                if (stmt != null) stmt.close();
                if (conn != null) conn.close();
            } catch (SQLException e) {
                System.err.println("关闭连接时出错: " + e.getMessage());
            }
        }
    }
}