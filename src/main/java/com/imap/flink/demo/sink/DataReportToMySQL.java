package com.imap.flink.demo.sink;

import com.imap.pojo.DataReport;
import com.imap.utils.DataReportSource;
import com.imap.utils.MapperUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/18 13:56
 * @Description:
 */
public class DataReportToMySQL {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<DataReport> dataReportStream = env.addSource(new DataReportSource(1000))
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DataReport>() {
                            @Override
                            public long extractTimestamp(DataReport dataReport, long l) {
                                return dataReport.getTimestamp();
                            }
                        }));

        dataReportStream.print("out");
        String sql = "insert into dev_data_report (report_id, site_id,timestamp, type, version, status, data) values (null, ?, ?, ?, ?, ?, ?)";
        dataReportStream.addSink(JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<DataReport>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, DataReport dataReport) throws SQLException {
                        preparedStatement.setInt(1,dataReport.getSiteId());
                        preparedStatement.setTimestamp(2,new Timestamp(dataReport.getTimestamp()));
                        preparedStatement.setString(3,dataReport.getType());
                        preparedStatement.setInt(4,dataReport.getVersion());
                        preparedStatement.setInt(5,dataReport.getStatus());
                        preparedStatement.setString(6, MapperUtil.obj2Str(dataReport.getData()));
                    }
                },
                // 配置流式数据的攒批逻辑（不是来一条sink一条）
                JdbcExecutionOptions
                        .builder()
                        .withBatchIntervalMs(5000)
                        .build()
                ,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/imap")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("")
                        .build()
        ));

        env.execute();
    }
}
