package com.imap.flink;

import com.imap.pojo.AvgDataItem;
import com.imap.utils.AvgDataSource;
import com.imap.utils.DateTimeUtil;
import com.imap.utils.MySQLUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
 * @Date: create in 2023/3/30 11:03
 * @Description:
 */
public class MainBatch {

    private static int siteId = 1;
    private static String start = "2023-03-01 00:00:00";
    private static String end = DateTimeUtil.getDateTimeStr();
    private static int type = 2;

    private static void init(String[] args) {
/**
 /usr/local/flink/bin/flink run -m localhost:8081 -c com.imap.flink.MainBatch ./IMAP-Flink-1.0-SNAPSHOT.jar --siteId 1 --start "2023-03-01 00:00:00" --end "2023-04-05 00:00:00" --type 3
 /usr/local/flink/bin/flink run -m localhost:8081 -c com.imap.flink.MainBatch ./IMAP-Flink-1.0-SNAPSHOT-jar-with-dependencies.jar --siteId 1 --start "2023-03-01 00:00:00" --end "2023-04-05 00:00:00" --type 3
 * */
        try{
            // 从参数中获取 --siteId 1 --start 2023-03-01 00:00:00 --end 2023-04-05 00:00:00 --type 2
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            Integer aggSiteId = parameterTool.getInt("siteId");
            String aggStart = parameterTool.get("start");
            String aggEnd = parameterTool.get("end");
            Integer aggType = parameterTool.getInt("type");

            if (aggSiteId != null) {
                siteId = aggSiteId;
            }
            if (aggStart != null) {
                start = aggStart;
            }
            if (aggEnd != null) {
                end = aggEnd;
            }
            if (aggType != null) {
                type = aggType;
            }
        }catch (Exception e){
            System.out.println("参数解析异常" + args);
            e.printStackTrace();
            System.exit(1);
        }

    }
    public static void main(String[] args) throws Exception {
        init(args);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        AvgDataEnum avgDataEnum = AvgDataEnum.from(type);
        AvgDataEnum newAvgDataEnum = AvgDataEnum.from(type+1);

        // 设置数据源，设置水位线
        SingleOutputStreamOperator<AvgDataItem> streamSource = env.addSource(new AvgDataSource(siteId, start, end, type))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<AvgDataItem>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<AvgDataItem>() {
                    @Override
                    public long extractTimestamp(AvgDataItem element, long recordTimestamp) {
                        return element.getEndTime();
                    }
                }));

        Table avgDataTable = tableEnv.fromDataStream(streamSource,
                $("siteId"),
                $("endTime").rowtime().as("ts"),
                $("type"),
                $("avgTmp"),
                $("avgHmt"),
                $("avgLx")
        );

        // 表名
        String tableName = "avgData_" + siteId + "_" + avgDataEnum.getType();
        System.out.println("注册表:" + tableName);
        // 注册表
        tableEnv.createTemporaryView(tableName, avgDataTable);
        // 聚合查询
        Table newAvgDataTable = tableEnv
                .sqlQuery("SELECT " +
                        "siteId, " +
                        "window_end AS endT, " + // 窗口结束时间
                        "AVG(avgTmp) AS avg_tmp, " +
                        "AVG(avgHmt) AS avg_hmt, " +
                        "AVG(avgHmt) AS avg_lx " +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE " + tableName + ", " +
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '" + newAvgDataEnum.getMinutes() + "' MINUTE(5))) " + // MINUTE(5) 5位数精度，分钟窗口
                        "GROUP BY siteId, window_start, window_end ");

        DataStream<Row> rowDataStream = tableEnv.toDataStream(newAvgDataTable);
        String sql = "insert into dev_avg_data (site_id, end_time, type, avg_tmp, avg_hmt, avg_lx) values (?, ?, ?, ?, ?, ?) on duplicate key update avg_tmp = values(avg_tmp), avg_hmt = values(avg_hmt), avg_lx = values(avg_lx)";

        rowDataStream.print("aggStream");
        rowDataStream.addSink(JdbcSink.sink(
                sql,
                new JdbcStatementBuilder<Row>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Row row) throws SQLException {
                        LocalDateTime localDateTime = (LocalDateTime) row.getField(1);
//                        ZoneId asiaShanghai = ZoneId.of("Asia/Shanghai");
//                        System.out.println(row);
                        preparedStatement.setInt(1, (Integer) row.getField(0));
                        preparedStatement.setTimestamp(2, Timestamp.valueOf(localDateTime.plusHours(8)));
                        preparedStatement.setInt(3, newAvgDataEnum.getType());
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
                MySQLUtil.getOptions("remote")
        ));

        env.execute();
    }


}
