package com.imap.utils;

import com.imap.pojo.MonitorConfig;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MonitorConfigSource implements SourceFunction<MonitorConfig> {
    private boolean running = true;
    private long interval = 1000;

    private JdbcConnectionOptions jdbcConnectionOptions ;

    // 提醒应用就绪
    private boolean flag = true;

    public MonitorConfigSource() {
        this(1000);
    }

    public MonitorConfigSource(long interval) {
        this(MySQLUtil.getOptions(),interval);
    }

    public MonitorConfigSource(JdbcConnectionOptions jdbcConnectionOptions, long interval) {
        this.jdbcConnectionOptions = jdbcConnectionOptions;
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<MonitorConfig> sourceContext) throws Exception {
        if (flag){
            System.out.println("监控配置就绪");
            flag = false;
        }
        Connection connection = MySQLUtil.getConnection(jdbcConnectionOptions);
        String sql = "select `site_id`,`timestamp`,`is_delete`,`version`,`interval`,`monitor_items` from dev_monitor_config";
        while (running) {
            Statement stat = connection.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);
            while (resultSet.next()) {
                MonitorConfig monitorConfig = MonitorConfig.fromResultSet(resultSet);
                sourceContext.collect(monitorConfig);
            }
            Thread.sleep(interval);
        }
    }

//    private static MonitorConfig getConfigFromResultSet(ResultSet resultSet) throws SQLException, IOException {
//        site_id,timestamp,status,version,interval,monitor_items
//        int siteId = resultSet.getInt("site_id");
//        long timestamp = resultSet.getTimestamp("timestamp").getTime();
//        int status = resultSet.getInt("status");
//        int version = resultSet.getInt("version");
//        int interval = resultSet.getInt("interval");
//
//        String monitorItems = resultSet.getString("monitor_items");
//        Map<String, MonitorItem> items = (Map<String, MonitorItem>)
//                MapperUtil.str2Object(monitorItems,
//                        new TypeReference<Map<String, MonitorItem>>() {
//                        });
//
//        MonitorConfig monitorConfig = new MonitorConfig(
//                siteId,
//                timestamp,
//                status,
//                version,
//                interval,
//                items
//        );
//        return monitorConfig;
//    }

    @Override
    public void cancel() {
        running = false;
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        Connection connection = MySQLUtil.getConnection(MySQLUtil.getOptions());
        String sql = "select `site_id`,`timestamp`,`status`,`version`,`interval`,`monitor_items` from dev_monitor_config";
        Statement stat = connection.createStatement();
        ResultSet resultSet = stat.executeQuery(sql);
        while (resultSet.next()) {
            MonitorConfig monitorConfig = MonitorConfig.fromResultSet(resultSet);
            System.out.println(monitorConfig);
        }
    }
}