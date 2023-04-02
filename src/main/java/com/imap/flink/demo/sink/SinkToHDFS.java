package com.imap.flink.demo.sink;

import com.imap.pojo.DataReport;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.util.concurrent.TimeUnit;

/**
 * @Author: Weizhi
 * @Date: create in 2023/4/1 11:41
 * @Description:
 */
public class SinkToHDFS {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 修改用户名，解决权限问题
        System.setProperty("HADOOP_USER_NAME", "root");
        SingleOutputStreamOperator<DataReport> dataReportSingleOutputStreamOperator = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<DataReport>) (element, recordTimestamp) -> element.getTimestamp()));
        //存储设置,检查点10秒
//        env.enableCheckpointing(10000);

        String path = "hdfs://47.116.66.37:8020//imap//report";
        Path outputPath = new Path(path);
        final StreamingFileSink hdfsSink = StreamingFileSink
                .forRowFormat(outputPath, new SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(2))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(1))
                                .withMaxPartSize(1024 * 1024)
                                .build())
                .build();

        SingleOutputStreamOperator<String> map = dataReportSingleOutputStreamOperator.map(item -> item.toString());
        map.addSink(hdfsSink);
        map.print("out");
        env.execute();
    }
}
