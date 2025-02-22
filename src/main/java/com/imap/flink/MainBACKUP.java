package com.imap.flink;

import com.imap.pojo.AlarmItem;
import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorConfig;
import com.imap.utils.MapperUtil;
import com.imap.utils.MonitorConfigSource;
import com.imap.utils.MySQLUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/17 21:02
 * @Description:
 */
public class MainBACKUP {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);
        // Kafka 配置
        Properties earliestProp = new Properties();
        earliestProp.setProperty("bootstrap.servers", "47.116.66.37:9092");
        earliestProp.setProperty(KafkaOptions.SCAN_STARTUP_MODE.key(), KafkaOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST);
        Properties latestProp = new Properties();
        latestProp.setProperty("bootstrap.servers", "47.116.66.37:9092");
        latestProp.setProperty(KafkaOptions.SCAN_STARTUP_MODE.key(), KafkaOptions.SCAN_STARTUP_MODE_VALUE_LATEST);
        // 数据源
        SingleOutputStreamOperator<DataReport> dataReportStream =
                env.addSource(new FlinkKafkaConsumer<String>("report",
                        new SimpleStringSchema(),
                                latestProp))
                    .map(json -> MapperUtil.jsonToObj(json, DataReport.class))
                    // 设置水位线
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<DataReport>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((SerializableTimestampAssigner<DataReport>) (data, l) -> data.getTimestamp()));
        // 监控配置 从数据库轮询获取
        DataStreamSource<MonitorConfig> configDataStreamSource = env.addSource(new MonitorConfigSource(5000));
        // 监控广播流 <siteId,monitorConfig>
        BroadcastStream<MonitorConfig> configBroadcastStream = configDataStreamSource
                .broadcast(new MapStateDescriptor<Integer, MonitorConfig>("matcher", Integer.class, MonitorConfig.class));
        // 异常数据分流标签
        OutputTag<AlarmItem> abnormalDataTag = new OutputTag<AlarmItem>("abnormalData") {};
        // 数据异常匹配
        SingleOutputStreamOperator<DataReport> processedStream = dataReportStream.keyBy(data -> data.getSiteId())
                .connect(configBroadcastStream)
                .process(new MonitorMatcher(abnormalDataTag));
        // 提取异常数据流
        DataStream<AlarmItem> abnormalDataStream = processedStream.getSideOutput(abnormalDataTag);
        abnormalDataStream.print("异常数据");
        // 异常数据发送给Kafka alarm
        abnormalDataStream
                .map(item -> item.toString())
                .addSink(new FlinkKafkaProducer<String>("alarm",new SimpleStringSchema(),earliestProp));
        // 聚合
        AvgDataToMySQL.AggDataReport(tableEnv, MySQLUtil.getOptions(), AvgDataEnum.MINUTE, processedStream);
        AvgDataToMySQL.AggDataReport(tableEnv, MySQLUtil.getOptions(), AvgDataEnum.HOUR, processedStream);
        // TODO 输出到HDFS
        processedStream.print("输出");
        System.out.println("主进程就绪");
        env.execute();
    }
}
