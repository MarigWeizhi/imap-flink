package com.imap.flink.test;

import com.imap.pojo.DataReport;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
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
        kafkaDemo1();
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
