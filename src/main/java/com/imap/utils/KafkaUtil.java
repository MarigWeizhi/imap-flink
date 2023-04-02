package com.imap.utils;

import com.imap.flink.demo.sink.Event;
import com.imap.pojo.AlarmItem;
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
 * @Date: create in 2023/3/22 23:52
 * @Description:
 */
public class KafkaUtil {

    public static void sendMsg(String topic,String msg){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","47.116.66.37:9092");
        DataStreamSource<String> source = env.fromElements(msg);
        source.addSink(new FlinkKafkaProducer<String>(topic,new SimpleStringSchema(),prop));
        try {
            env.execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        AlarmItem alarmItem = AlarmItem.from("{\"dataReport\":{\"siteId\":2,\"timestamp\":1679501328000,\"type\":\"report\",\"version\":1,\"status\":0,\"data\":{\"tmp\":88.8,\"lx\":656.21,\"hmt\":0.84}},\"monitorItem\":{\"type\":\"tmp\",\"open\":1,\"max\":22.2,\"min\":0.0}}");
        sendMsg("alarm",alarmItem.toString());
    }
}
