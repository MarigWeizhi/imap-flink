package com.imap.flink.demo;

import com.imap.pojo.DataReport;
import com.imap.utils.MapperUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/8 22:48
 * @Description:
 */
public class KafkaSourceDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "47.116.66.37:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), prop));
        source.map((MapFunction<String, DataReport>) json -> {
            DataReport dataReport = MapperUtil.jsonToObj(json, DataReport.class);
            return dataReport;
        }).print();
        env.execute();
    }
}
