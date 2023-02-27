package com.imap.flink.demo;

import com.imap.pojo.MonitorConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/8 22:48
 * @Description:
 */
public class MonitorConfigKafkaSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "47.116.66.37:9092");

        prop.setProperty("group.id", "consumer-group");
        prop.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        prop.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("config", new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<MonitorConfig> map = source.map((MapFunction<String, MonitorConfig>) json -> MonitorConfig.getConfig(json));

        map.keyBy(item -> true)
                .process(new CurrentConfigProcessFunction())
                .print("out");

        env.execute();
    }

    /**
     * @Description:获取每个站点最新的监控配置，如果有一个新数据到来，则会覆盖旧配置
     */

    private static class CurrentConfigProcessFunction extends KeyedProcessFunction<Boolean, MonitorConfig, MonitorConfig> {

        private MapState<Integer, MonitorConfig> currentConfigs;

        @Override
        public void open(Configuration parameters) throws Exception {
            currentConfigs = getRuntimeContext()
                    .getMapState(new MapStateDescriptor<Integer, MonitorConfig>("currentConfigs", Integer.class, MonitorConfig.class));
        }

        @Override
        public void processElement(MonitorConfig value,
                                   KeyedProcessFunction<Boolean, MonitorConfig, MonitorConfig>.Context ctx,
                                   Collector<MonitorConfig> out) throws Exception {
            currentConfigs.put(value.getSiteId(), value);
            for (MonitorConfig config : currentConfigs.values()) {
                out.collect(config);
            }
        }
    }
}
