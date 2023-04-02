package com.imap.utils;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/10 17:49
 * @Description:
 */
public class MySQLUtil {

    private volatile static Connection conn;

    public static  Connection getConnection(JdbcConnectionOptions jdbcConnectionOptions) throws ClassNotFoundException, SQLException {
        if(conn == null) {
            synchronized(MySQLUtil.class){
                if(conn == null){
//                    注册驱动器
                    Class.forName(jdbcConnectionOptions.getDriverName());
//                    获取连接
                    conn = DriverManager.getConnection(
                            jdbcConnectionOptions.getDbURL(),
                            jdbcConnectionOptions.getUsername().get(),
                            jdbcConnectionOptions.getPassword().get());
                }
            }
        }
        return conn;
    }

    public static JdbcConnectionOptions getOptions() {
        return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://localhost:3306/imap?serverTimezone=UTC&useSSL=false")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("imap")
                .withPassword("imap@SSPU")
                .build();
    }

    public static JdbcConnectionOptions getOptions(String type) {
        if("remote".equals(type)){
            return new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl("jdbc:mysql://47.116.66.37:3306/imap?serverTimezone=UTC&useSSL=false")
                .withDriverName("com.mysql.jdbc.Driver")
                .withUsername("imap")
                .withPassword("imap@SSPU")
                .build();
        }
        return getOptions();
    }
}
