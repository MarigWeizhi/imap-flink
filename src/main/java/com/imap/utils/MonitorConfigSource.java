package com.imap.utils;

import com.fasterxml.jackson.core.type.TypeReference;
import com.imap.pojo.MonitorConfig;
import com.imap.pojo.MonitorItem;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

public class MonitorConfigSource implements SourceFunction<MonitorConfig> {
    private boolean running = true;
    private long interval;

    public MonitorConfigSource() {
        this(1000);
    }

    public MonitorConfigSource(long interval) {
        this.interval = interval;
    }

    @Override
    public void run(SourceContext<MonitorConfig> sourceContext) throws Exception {
        Connection connection = MySQLUtil.getConnection();
        String sql = "select `site_id`,`timestamp`,`status`,`version`,`interval`,`monitor_items` from monitor_config";
        while (running) {
            Statement stat = connection.createStatement();
            ResultSet resultSet = stat.executeQuery(sql);
            while (resultSet.next()) {
                MonitorConfig monitorConfig = getConfigFromResultSet(resultSet);
                sourceContext.collect(monitorConfig);
            }
            Thread.sleep(interval);
        }
    }

    private MonitorConfig getConfigFromResultSet(ResultSet resultSet) throws SQLException, IOException {
//        site_id,timestamp,status,version,interval,monitor_items
        String siteId = resultSet.getString("site_id");
        long timestamp = resultSet.getLong("timestamp");
        int status = resultSet.getInt("status");
        int version = resultSet.getInt("version");
        int interval = resultSet.getInt("interval");
        String monitorItems = resultSet.getString("monitor_items");
        Map<String, MonitorItem> items = (Map<String, MonitorItem>)
                MapperUtil.str2Object(monitorItems,
                        new TypeReference<Map<String, MonitorItem>>() {
                        });

        MonitorConfig monitorConfig = new MonitorConfig(
                siteId,
                timestamp,
                status,
                version,
                interval,
                items
        );
        return monitorConfig;
    }

    @Override
    public void cancel() {
        running = false;
    }
}