package com.imap.flink;

import com.imap.pojo.AlarmItem;
import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorConfig;
import com.imap.pojo.MonitorItem;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Map;

/**
 * @Date： 2023年2月17日21:58:16
 * @Description: 所有数据在主流输出，其中异常数据分流用abnormalDataTag获取，
 */
public class MonitorMatcher extends KeyedBroadcastProcessFunction<Integer, DataReport, MonitorConfig, DataReport> {

    OutputTag<AlarmItem> abnormalDataTag;
    MapStateDescriptor<Integer, MonitorConfig> mapStateDescriptor;

    public MonitorMatcher(OutputTag<AlarmItem> abnormalDataTag) {
        this.abnormalDataTag = abnormalDataTag;
        mapStateDescriptor = new MapStateDescriptor<>("matcher", Integer.class, MonitorConfig.class);
    }

    @Override
    public void processElement(DataReport dataReport,
                               KeyedBroadcastProcessFunction<Integer, DataReport,
                                       MonitorConfig, DataReport>.
                                       ReadOnlyContext readOnlyContext,
                               Collector<DataReport> collector) throws Exception {
        ReadOnlyBroadcastState<Integer, MonitorConfig> broadcastState = readOnlyContext.getBroadcastState(mapStateDescriptor);
        // 从广播状态中 获取指定站点的监控配置
        MonitorConfig monitorConfig = broadcastState.get(readOnlyContext.getCurrentKey());
        if (monitorConfig == null) {
            collector.collect(dataReport);
            return;
        }

        //  设备不需要监控
        if (monitorConfig.getIsDelete() == 1 || monitorConfig.getInterval() <= 0) {
            // 主数据流输出
            collector.collect(dataReport);
            return;
        }

        Map<String, MonitorItem> monitorItems = monitorConfig.getMonitorItems();
        for (Map.Entry<String, MonitorItem> monitorItemEntry : monitorItems.entrySet()) {
            String type = monitorItemEntry.getKey();
            MonitorItem monitorItem = monitorItemEntry.getValue();
            // 该数据项无需监控
            if (monitorItem.getOpen() <= 0) {
                continue;
            }
            if (dataReport.getData().containsKey(type)) {
                Double dataValue = dataReport.getData().get(type);
                // 数据检查
                if (monitorItem.getMax() < dataValue || monitorItem.getMin() > dataValue) {
                    dataReport.setStatus(1);
                    // 异常数据输出
                    readOnlyContext.output(this.abnormalDataTag, AlarmItem.of(dataReport, monitorItem));
                }
            }
        }
        // 主数据流输出
        collector.collect(dataReport);
    }

    @Override
    // 更新 MonitorConfig
    public void processBroadcastElement(MonitorConfig monitorConfig,
                                        KeyedBroadcastProcessFunction<Integer, DataReport,
                                                MonitorConfig, DataReport>.
                                                Context context,
                                        Collector<DataReport> collector) throws Exception {
        BroadcastState<Integer, MonitorConfig> broadcastState = context.getBroadcastState(mapStateDescriptor);
        broadcastState.put(monitorConfig.getSiteId(), monitorConfig);
    }
}