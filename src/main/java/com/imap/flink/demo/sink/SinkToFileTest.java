package com.imap.flink.demo.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/10 16:00
 * @Description:
 */
public class SinkToFileTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        DataStreamSource<Event> source = env.fromElements(
                new Event("Winsa", "/user", 1000),
                new Event("Bob", "/cart", 1000),
                new Event("Bob", "/cart", 2000),
                new Event("Winsa", "/cart", 3000),
                new Event("Winsa", "/cart", 2000),
                new Event("Weizhi", "/inner", 2000),
                new Event("Weizhi", "/goods?id=1", 5000),
                new Event("Weizhi", "/goods?id=1", 2000),
                new Event("Weizhi", "/goods?id=1", 9000),
                new Event("Winsa", "/goods?id=1", 11000),
                new Event("Bob", "/cart", 3000)
        );


        StreamingFileSink<String> sink = StreamingFileSink
                .<String>forRowFormat(new Path("./out"),
                        new SimpleStringEncoder<>("UTF-8"))
                .withRollingPolicy(DefaultRollingPolicy  // 数据滚动策略
                        .builder()
                        .withMaxPartSize(1024*1024)  // 1MB 文件保存一次
                        .withRolloverInterval(TimeUnit.MINUTES.toMillis(15)) // 15分钟 文件滚动一次
                        .withInactivityInterval(TimeUnit.MINUTES.toMillis(5)) // 5分钟没有消息 就滚动一次
                        .build())
                .build();
        source.map(item -> item.toString())
                .addSink(sink);

        env.execute();
    }
}
