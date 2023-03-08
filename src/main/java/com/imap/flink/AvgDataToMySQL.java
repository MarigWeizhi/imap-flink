package com.imap.flink;

import com.imap.flink.demo.DataReportMapper;
import com.imap.pojo.DataReport;
import com.imap.pojo.DataReportTableRow;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/27 20:19
 * @Description:
 */
public class AvgDataToMySQL {

    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        SingleOutputStreamOperator<DataReport> dataReportStream = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<DataReport>) (dataReport, l) -> dataReport.getTimestamp()));

        // 聚合dataReport 并保存至MySQL数据库
        AggDataReport(tableEnv,AvgDataEnum.MINUTE ,dataReportStream);

        dataReportStream.print("input");
        env.execute();
    }

    public static void AggDataReport(StreamTableEnvironment tableEnv, AvgDataEnum minutes, SingleOutputStreamOperator<DataReport> dataReportStream) {
        // 数据格式转换
        SingleOutputStreamOperator<DataReportTableRow> dataReportRowStream = dataReportStream.map(new DataReportMapper());

        // 创建表 并将timestamp 指定为事件时间
        Table dataReportTable = tableEnv.fromDataStream(dataReportRowStream,
                $("siteId"),
                $("timestamp").rowtime().as("ts"),
                $("type"),
                $("version"),
                $("status"),
                $("tmp"),
                $("hmt"),
                $("lx")
        );

        // 表名，加上minutes防止重名
        String tableName = "dataReportTableWith" + minutes;

        // 注册表
        tableEnv.createTemporaryView(tableName, dataReportTable);

        // 聚合查询
        Table avgDataTable = tableEnv
                .sqlQuery("SELECT " +
                        "siteId, " +
                        "window_end AS endT, " + // 窗口结束时间
                        "AVG(tmp) AS avg_tmp, " +
                        "AVG(hmt) AS avg_hmt, " +
                        "AVG(lx) AS avg_lx " +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE " + tableName + ", " +
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '" + minutes.getMinutes() + "' MINUTE)) " + // minute分钟窗口
                        "GROUP BY siteId, window_start, window_end ");

        DataStream<Row> rowDataStream = tableEnv.toDataStream(avgDataTable);
        String sql = "insert into dev_avg_data (site_id, end_time, type, avg_tmp, avg_hmt, avg_lx) values (?, ?, ?, ?, ?, ?)";
        rowDataStream.addSink(JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                        System.out.println(row);
                        preparedStatement.setInt(1, (Integer) row.getField(0));
                        preparedStatement.setTimestamp(2, Timestamp.valueOf((LocalDateTime) row.getField(1)));
                        preparedStatement.setInt(3, minutes.getType());
                        preparedStatement.setDouble(4, (Double) row.getField(2));
                        preparedStatement.setDouble(5, (Double) row.getField(3));
                        preparedStatement.setDouble(6, (Double) row.getField(4));
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

    }
}
