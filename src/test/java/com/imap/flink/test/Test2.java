package com.imap.flink.test;

import com.imap.pojo.MonitorConfig;
import com.imap.utils.MonitorConfigSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/10 18:11
 * @Description:
 */
public class Test2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<MonitorConfig> source = env.addSource(new MonitorConfigSource());
        source.print();
        env.execute();
    }
}
