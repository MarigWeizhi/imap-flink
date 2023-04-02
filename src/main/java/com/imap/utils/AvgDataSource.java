package com.imap.utils;

import com.imap.pojo.AvgDataItem;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.IOException;
import java.sql.*;

public class AvgDataSource implements SourceFunction<AvgDataItem> {

    // 提醒应用就绪
    private Boolean flag = true;
    private Integer siteId;
    private String startTime;
    private String endTime;
    private Integer type;


    public AvgDataSource(int siteId,int type) {
        this(siteId,"2023-01-01 00:00:00",DateTimeUtil.getDateTimeStr(),type);
    }

    public AvgDataSource(int siteId,String startTime,String endTime,int type) {
        this.siteId = siteId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.type = type;
    }

    @Override
    public void run(SourceContext<AvgDataItem> sourceContext) throws Exception {
        if(flag){
            System.out.println("监控配置就绪");
            flag = false;
        }

        Connection connection = MySQLUtil.getConnection(MySQLUtil.getOptions("remote"));
        String sql = "select `site_id`,`end_time`,`type`,`avg_tmp`,`avg_hmt`,`avg_lx` from dev_avg_data" +
                " where site_id = ? and end_time between ? and ? and type = ?";

        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1,siteId);
        preparedStatement.setString(2,startTime);
        preparedStatement.setString(3,endTime);
        preparedStatement.setInt(4,type);
        ResultSet resultSet = preparedStatement.executeQuery();

        while (resultSet.next()) {
            AvgDataItem monitorConfig = AvgDataItem.fromResultSet(resultSet);
            sourceContext.collect(monitorConfig);
        }

    }

    @Override
    public void cancel() {
    }

    public static void main(String[] args) throws SQLException, IOException, ClassNotFoundException {
        Connection connection = MySQLUtil.getConnection(MySQLUtil.getOptions());
        String sql = "select `site_id`,`end_time`,`type`,`avg_tmp`,`avg_hmt`,`avg_lx` from dev_avg_data" +
                " where site_id = ? and end_time between ? and ? and type = ?";
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setInt(1,2);
        preparedStatement.setString(2,"2023-02-01 00:00:00");
        preparedStatement.setString(3,"2023-03-30 00:00:00");
        preparedStatement.setInt(4,5);
        ResultSet resultSet = preparedStatement.executeQuery();

//        Statement stat = connection.createStatement();
//        ResultSet resultSet = stat.executeQuery(sql);
        while (resultSet.next()) {
            AvgDataItem monitorConfig = AvgDataItem.fromResultSet(resultSet);
            System.out.println(monitorConfig);
        }
    }
}