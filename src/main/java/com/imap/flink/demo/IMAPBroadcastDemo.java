package com.imap.flink.demo;

import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorConfig;
import com.imap.pojo.MonitorItem;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/8 23:22
 * @Description: IMAP的广播Demo，根据 MonitorConfig 监控 DataReport 过滤出一条异常
 */
public class IMAPBroadcastDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<DataReport> dataStream = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forMonotonousTimestamps()
                        .withTimestampAssigner(new SerializableTimestampAssigner<DataReport>() {
                            @Override
                            public long extractTimestamp(DataReport dataReport, long l) {
                                return dataReport.getTimestamp();
                            }
                        }));
        dataStream.print("input");

        DataStreamSource<MonitorConfig> configDataStreamSource = env.fromCollection(MonitorConfig.getDefaultConfigList());
        BroadcastStream<MonitorConfig> configBroadcastStream = configDataStreamSource
                .broadcast(new MapStateDescriptor<Void, MonitorConfig>("matcher", Void.class, MonitorConfig.class));


        dataStream.keyBy(item -> item.getSiteId())
                .connect(configBroadcastStream)
                .process(new MonitorMatcher())
                .print("out");
        env.execute();
    }

    private static class MonitorMatcher extends KeyedBroadcastProcessFunction<String, DataReport, MonitorConfig, Tuple2<DataReport, MonitorItem>> {

        @Override
        public void processElement(DataReport dataReport,
                                   KeyedBroadcastProcessFunction<String, DataReport,
                                           MonitorConfig, Tuple2<DataReport, MonitorItem>>.
                                           ReadOnlyContext readOnlyContext,
                                   Collector<Tuple2<DataReport, MonitorItem>> collector) throws Exception {
            MapStateDescriptor<Void, MonitorConfig> configs = new MapStateDescriptor<>("matcher", Void.class, MonitorConfig.class);
            ReadOnlyBroadcastState<Void, MonitorConfig> broadcastState = readOnlyContext.getBroadcastState(configs);

            // 获取广播状态
            MonitorConfig monitorConfig = broadcastState.get(null);
            // 匹配站点
            if (monitorConfig.getSiteId().equals(dataReport.getSiteId())) {
                Map<String, MonitorItem> monitorItems = monitorConfig.getMonitorItems();
                // 遍历数据项
                for (Map.Entry<String, Double> entry : dataReport.getData().entrySet()) {
                    String entryKey = entry.getKey();
                    Double entryValue = entry.getValue();
                    if (monitorItems.containsKey(entryKey)) {
                        MonitorItem monitorItem = monitorItems.get(entryKey);
                        // 数据检查
                        if (monitorItem.getMax() < entryValue || monitorItem.getMin() > entryValue) {
                            System.out.println("数据异常" + entryKey + ":" + entryValue + "监控项" + monitorItem);
                            dataReport.setStatus(1);
                            collector.collect(Tuple2.of(dataReport, monitorItem));
                        }
                    }
                }
            }

        }

        @Override
        public void processBroadcastElement(MonitorConfig monitorConfig,
                                            KeyedBroadcastProcessFunction<String, DataReport,
                                                    MonitorConfig, Tuple2<DataReport, MonitorItem>>.
                                                    Context context,
                                            Collector<Tuple2<DataReport, MonitorItem>> collector) throws Exception {
            MapStateDescriptor<Void, MonitorConfig> configs = new MapStateDescriptor<>("matcher", Void.class, MonitorConfig.class);
            BroadcastState<Void, MonitorConfig> broadcastState = context.getBroadcastState(configs);
            broadcastState.put(null, monitorConfig);
        }
    }
}
