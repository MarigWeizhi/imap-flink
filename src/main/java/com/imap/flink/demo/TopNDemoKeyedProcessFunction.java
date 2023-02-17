package com.imap.flink.demo;

import com.imap.pojo.DataReport;
import com.imap.pojo.ReportCountView;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/16 16:16
 * @Description:
 */
public class TopNDemoKeyedProcessFunction {
    public static void main(String[] args) throws Exception {
//      各站点数据数据量Top3

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<DataReport> stream = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(
                                (dataReport, l) -> dataReport.getTimestamp()));

        SingleOutputStreamOperator<ReportCountView> countStream = stream.keyBy(item -> item.getSiteId())
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new CountReportAggFunction(), new CountResultWindowFunction());

        stream.print("input");
        countStream.keyBy(item -> item.getEnd())
                .process(new TopNPRocessFunction(3)).print("top3");

        env.execute();
    }

    private static class CountReportAggFunction implements AggregateFunction<DataReport, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(DataReport dataReport, Long acc) {
            return acc + 1;
        }

        @Override
        public Long getResult(Long acc) {
            return acc;
        }

        @Override
        public Long merge(Long acc, Long acc1) {
            return acc + acc1;
        }
    }

    private static class CountResultWindowFunction extends ProcessWindowFunction<Long, ReportCountView, Integer, TimeWindow> {
        @Override
        public void process(Integer key, Context context, Iterable<Long> iterable, Collector<ReportCountView> collector) throws Exception {
            Long count = iterable.iterator().next();

            long currentWatermark = context.currentWatermark();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            ReportCountView countView = new ReportCountView(key, count, start, end);
            System.out.println("窗口时间：" + start + " ~ " + end + " 当前水位线：" + currentWatermark);
            collector.collect(countView);
        }
    }

    private static class TopNPRocessFunction extends KeyedProcessFunction<Long, ReportCountView, ReportCountView> {

        private int N;
        private ListState<ReportCountView> listState;

        public TopNPRocessFunction(int i) {
            this.N = i;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            listState = getRuntimeContext().getListState(
                    new ListStateDescriptor<ReportCountView>("siteCount", Types.POJO(ReportCountView.class))
            );
        }

        @Override
        public void processElement(ReportCountView reportCountView,Context context, Collector<ReportCountView> collector) throws Exception {
//            用listState来收集数据，而不用collector（会直接输出导致无法排序）
//            listState暂存数据，设置定时器保证一个窗口所有数据都添加进
            listState.add(reportCountView);
            // 注册1毫秒的定时器（同一窗口的end时间都一样，如果超过1毫秒，说明前一窗口数据全到齐了）

            context.timestamp();//这是窗口结束时间-1
            System.out.println("这是窗口里最后一个事件时间-1：" + new Timestamp(context.timerService().currentWatermark()));
            context.timerService().registerEventTimeTimer(context.getCurrentKey()+1);
        }

        @Override
        public void onTimer(long timestamp,OnTimerContext ctx, Collector<ReportCountView> out) throws Exception {
            ArrayList<ReportCountView> reportCountViews = new ArrayList<>();
            for (ReportCountView reportCountView : listState.get()) {
                reportCountViews.add(reportCountView);
            }
            reportCountViews.sort(Comparator.comparing(ReportCountView::getCount).reversed());
            int len = reportCountViews.size() >= N ? N : reportCountViews.size();
            for (int i = 0; i < len; i++) {
                out.collect(reportCountViews.get(i));
            }
        }
    }
}
