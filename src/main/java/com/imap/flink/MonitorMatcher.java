package com.imap.flink;

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
 *
 * @Date： 2023年2月17日21:58:16
 * @Description: 所有数据在主流输出，其中异常数据分流用abnormalDataTag获取，
 *
 * */
public class MonitorMatcher extends KeyedBroadcastProcessFunction<String, DataReport, MonitorConfig, DataReport> {

    OutputTag<Tuple2<DataReport, MonitorItem>> abnormalDataTag;
    public MonitorMatcher(OutputTag<Tuple2<DataReport, MonitorItem>> abnormalDataTag) {
        this.abnormalDataTag = abnormalDataTag;
    }

    @Override
        public void processElement(DataReport dataReport,
                                   KeyedBroadcastProcessFunction<String, DataReport,
                                           MonitorConfig, DataReport>.
                                           ReadOnlyContext readOnlyContext,
                                   Collector<DataReport> collector) throws Exception {
            MapStateDescriptor<Void, MonitorConfig> configs = new MapStateDescriptor<>("matcher", Void.class, MonitorConfig.class);
            ReadOnlyBroadcastState<Void, MonitorConfig> broadcastState = readOnlyContext.getBroadcastState(configs);
            // 获取广播状态
            MonitorConfig monitorConfig = broadcastState.get(null);
            if(monitorConfig == null){
                collector.collect(dataReport);
                return;
            }

            //  设备不需要监控
            if(monitorConfig.getInterval() <= 0){
//                主数据流输出
                collector.collect(dataReport);
                return;
            }

            // 匹配站点
            if (monitorConfig.getSiteId().equals(dataReport.getSiteId())) {
                Map<String, MonitorItem> monitorItems = monitorConfig.getMonitorItems();
                for (Map.Entry<String, MonitorItem> monitorItemEntry : monitorItems.entrySet()) {
                    String dataKey = monitorItemEntry.getKey();
                    MonitorItem monitorItem = monitorItemEntry.getValue();
                    // 该数据项无需监控
                    if(!monitorItem.getMonitor() || monitorItem.getInterval() <= 0){
                        continue;
                    }
                    if(dataReport.getData().containsKey(dataKey)){
                        Double dataValue = dataReport.getData().get(dataKey);
                        // 数据检查
                        if (monitorItem.getMax() < dataValue || monitorItem.getMin() > dataValue) {
                            dataReport.setStatus(1);
                            readOnlyContext.output(this.abnormalDataTag,Tuple2.of(dataReport, monitorItem));
                        }
                    }

                }
            }
//            主数据流输出
            collector.collect(dataReport);
        }

    @Override
        public void processBroadcastElement(MonitorConfig monitorConfig,
                                            KeyedBroadcastProcessFunction<String, DataReport,
                                                    MonitorConfig, DataReport>.
                                                    Context context,
                                            Collector<DataReport> collector) throws Exception {
            MapStateDescriptor<Void, MonitorConfig> configs = new MapStateDescriptor<>("matcher", Void.class, MonitorConfig.class);
            BroadcastState<Void, MonitorConfig> broadcastState = context.getBroadcastState(configs);
            broadcastState.put(null, monitorConfig);
        }
    }