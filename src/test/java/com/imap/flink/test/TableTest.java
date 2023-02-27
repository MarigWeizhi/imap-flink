package com.imap.flink.test;

import com.imap.flink.demo.DataReportMapper;
import com.imap.flink.demo.sink.Event;
import com.imap.pojo.DataReport;
import com.imap.pojo.DataReportTableRow;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
//import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/27 9:58
 * @Description:
 */
public class TableTest {
    public static void main(String[] args) throws Exception {
//        tableAPIDemo();
//        kafkaDemo1();
//        tableGroup();
        dataReportTableGroup();

    }

    private static void tableGroup() throws Exception {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner((SerializableTimestampAssigner<Event>) (element, recordTimestamp) -> element.timestamp)
                );
        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts")
                // 将 timestamp 指定为事件时间，并命名为 ts
        );
        // 为方便在 SQL 中引用，在环境中注册表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);
        // 设置 1 小时滚动窗口，执行 SQL 统计查询
        Table result = tableEnv
                .sqlQuery(
                        "SELECT " +
                                "user, " +
                                "window_end AS endT, " + // 窗口结束时间
                                "COUNT(url) AS cnt " + // 统计 url 访问次数
                                "FROM TABLE( " +
                                "TUMBLE( TABLE EventTable, " + // 1 小时滚动窗口
                                "DESCRIPTOR(ts), " +
                                "INTERVAL '1' HOUR)) " +
                                "GROUP BY user, window_start, window_end "
                );
        tableEnv.toDataStream(result).print();
        env.execute();
    }

    private static void dataReportTableGroup() throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<DataReport> dataReportStream = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<DataReport>) (dataReport, l) -> dataReport.getTimestamp()));

        SingleOutputStreamOperator<DataReportTableRow> dataReportRowStream = dataReportStream.map(new DataReportMapper());

        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

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

        // 注册表
        tableEnv.createTemporaryView("dataReportTable", dataReportTable);

        // 聚合查询
        Table avgDataTable = tableEnv
                .sqlQuery("SELECT " +
                        "siteId, " +
                        "window_end AS endT, " + // 窗口结束时间
                        "AVG(tmp) AS avg_tmp, " +
                        "AVG(hmt) AS avg_hmt, " +
                        "AVG(lx) AS avg_lx " +
                        "FROM TABLE( " +
                        "TUMBLE( TABLE dataReportTable, " +
                        "DESCRIPTOR(ts), " +
                        "INTERVAL '60' SECOND)) " + // 60 秒滚动窗口
                        "GROUP BY siteId, window_start, window_end ");

        tableEnv.toDataStream(avgDataTable).print("avg:");
        env.execute();
    }

    private static void tableAPIDemo() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SingleOutputStreamOperator<DataReport> stream = env.addSource(new DataReportSource()).assignTimestampsAndWatermarks(WatermarkStrategy
                .<DataReport>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<DataReport>() {
                    @Override
                    public long extractTimestamp(DataReport dataReport, long l) {
                        return dataReport.getTimestamp();
                    }
                }));

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        Table dataReportTable = tableEnv.fromDataStream(stream);
//        纯sql API
        Table resultTable = tableEnv.sqlQuery("select `siteId`,`timestamp`,`type`,`version` from " + dataReportTable);
        DataStream<Row> rowDataStream = tableEnv.toDataStream(resultTable);
        rowDataStream.print("res1");

        // Table API
        Table resultTable2 = dataReportTable.select($("siteId"), $("timestamp"), $("type"), $("version"))
                .where($("siteId").isLessOrEqual(3));

        tableEnv.toDataStream(resultTable2).print("res2");
        env.execute();
    }

    public static void kafkaDemo1() throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // 定义Kafka连接属性
        tableEnv.connect(new Kafka()
                        .version("universal")
                        .topic("test")
                        .startFromLatest()
//                        .property("zookeeper.connect", "localhost:2181")
                        .property("bootstrap.servers", "47.116.66.37:9092"))
                .withFormat(new Json().failOnMissingField(false))
                .withSchema(new Schema()
                                .field("siteId", DataTypes.INT())
//                        .field("timestamp", DataTypes.TIMESTAMP(3))
                                .field("timestamp", DataTypes.BIGINT())
                                .field("type", DataTypes.STRING())
                                .field("version", DataTypes.INT())
                                .field("status", DataTypes.INT())
                                .field("data", DataTypes.STRING())
                )
                .createTemporaryTable("inputTable");

        // 定义聚合查询
        Table resultTable = tableEnv.sqlQuery(
                "SELECT * FROM inputTable");

        // 打印表结构
        resultTable.printSchema();

        // 内容输出
        tableEnv.toAppendStream(resultTable, Row.class).print();

        // 执行Flink任务
        env.execute("Flink Table API Kafka Demo");
    }
}
