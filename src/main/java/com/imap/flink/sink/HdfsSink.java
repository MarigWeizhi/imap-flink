package com.imap.flink.sink;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

import static com.imap.flink.Main.REPORT_HDFS_URL;

/**
 * @Author: Weizhi
 * @Date: create in 2023/4/3 10:09
 * @Description:
 */


public class HdfsSink {
    public static FileSink<String> getHdfsSink() {
        OutputFileConfig config = OutputFileConfig
                .builder()
                .withPartPrefix("report")
                .withPartSuffix(".txt")
                .build();
        FileSink<String> finkSink = FileSink.forRowFormat(new Path(REPORT_HDFS_URL), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        // 满足以下任意一个条件 触发sink
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5)) // 距上次保存超过5分钟
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) // 已有1分钟没有新的数据
                                .withMaxPartSize(1024 * 1024) // 未保存数据已经有1MB
                                .build())
                .withOutputFileConfig(config)
                .withBucketAssigner(new DateTimeBucketAssigner("yyyy-MM-dd"))
                .build();

        return finkSink;
    }

    public static StreamingFileSink<String> getStreamFileSink() {
        return StreamingFileSink
                .forRowFormat(new Path(REPORT_HDFS_URL), new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        // 满足以下任意一个条件 触发sink
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(5)) // 距上次保存超过5分钟
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1)) // 已有1分钟没有新的数据
                                .withMaxPartSize(1024 * 1024) // 未保存数据已经有1MB
                                .build())
                .build();
    }
}