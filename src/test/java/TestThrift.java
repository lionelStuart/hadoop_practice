import java.sql.*;


/**
 * 测试thrift-hiveserver2-jdbc-api
 */
public class TestThrift {
    private static String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException {
        //查找驱动
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }
        //创建连接
        Connection connection = DriverManager.getConnection("jdbc:hive2://had001:10000/default",
                "lee", "123");
        Statement stmt = connection.createStatement();

        //建表
        String tableName = "students";
        stmt.execute("drop table if exists " + tableName);
        stmt.execute("create table " + tableName + " (key int,value string)");
        System.out.println("crate table success");

        //显示表
        String sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        ResultSet res = stmt.executeQuery(sql);
        if (res.next()) {
            System.out.println(res.getString(1));
        }

        //描述表
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        //查询操作
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }

        //查询计数
        sql = "select count(*) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        while (res.next()) {
            System.out.println(res.getString(1));
        }

    }

}

