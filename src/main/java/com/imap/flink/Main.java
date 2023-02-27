package com.imap.flink;

import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorConfig;
import com.imap.pojo.MonitorItem;
import com.imap.utils.MapperUtil;
import com.imap.utils.MonitorConfigSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

import java.util.Properties;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/17 21:02
 * @Description:
 */
public class Main {
    public static void main(String[] args) throws Exception {
        // 创建Flink执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        // Kafka 配置
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "47.116.66.37:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<DataReport> dataReportStream = source.map((MapFunction<String, DataReport>) json -> {
            DataReport dataReport = (DataReport) MapperUtil.str2Object(json, DataReport.class);
            return dataReport;
        });

//        DataStreamSource<MonitorConfig> configDataStreamSource = env.fromCollection(MonitorConfig.getDefaultConfigList());
//        监控广播流
        DataStreamSource<MonitorConfig> configDataStreamSource = env.addSource(new MonitorConfigSource());
        BroadcastStream<MonitorConfig> configBroadcastStream = configDataStreamSource
                .broadcast(new MapStateDescriptor<Void, MonitorConfig>("matcher", Void.class, MonitorConfig.class));

        // 异常数据分流标签
        // 最右边的大括号不能少，不然会有泛型擦除的问题
        OutputTag<Tuple2<DataReport, MonitorItem>> abnormalDataTag = new OutputTag<Tuple2<DataReport, MonitorItem>>("abnormalData") {
        };

        SingleOutputStreamOperator<DataReport> processedStream = dataReportStream.keyBy(data -> data.getSiteId())
                .connect(configBroadcastStream)
                .process(new MonitorMatcher(abnormalDataTag));

//        提取异常数据流
        DataStream<Tuple2<DataReport, MonitorItem>> abnormalDataStream = processedStream.getSideOutput(abnormalDataTag);
        abnormalDataStream.print("异常数据");
        // TODO 异常数据通知SpringBoot
        
        AvgDataToMySQL.AggDataReport(tableEnv, AvgDataEnum.MINUTE, processedStream);
        AvgDataToMySQL.AggDataReport(tableEnv, AvgDataEnum.HOUR, processedStream);

        // TODO 输出到HDFS

        processedStream.print("输出");
        env.execute();
    }
}
