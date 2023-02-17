package com.imap.utils;

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

    public static  Connection getConnection() throws ClassNotFoundException, SQLException {
        if(conn == null) {
            synchronized(MySQLUtil.class){
                if(conn == null){
//                    注册驱动器
                    Class.forName("com.mysql.jdbc.Driver");
//                    获取连接
                    conn = DriverManager.getConnection("jdbc:mysql:///imap", "root", "");
                }
            }
        }
        return conn;
    }

}
