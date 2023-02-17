package com.imap.flink.demo.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/10 16:28
 * @Description:
 */
public class SinkToKafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","47.116.66.37:9092");
        DataStreamSource<String> source = env.addSource(new FlinkKafkaConsumer<String>("test", new SimpleStringSchema(), prop));
        SingleOutputStreamOperator<String> mapResult = source.map(line -> {
            List<String> stringList = Arrays.stream(line.split(",")).map(item -> item.trim()).collect(Collectors.toList());
            Event event = new Event(stringList.get(0), stringList.get(1), Long.parseLong(stringList.get(2)));
            return event.toString();
        });
        mapResult.addSink(new FlinkKafkaProducer<String>("flink",new SimpleStringSchema(),prop));

        env.execute();

    }
}
