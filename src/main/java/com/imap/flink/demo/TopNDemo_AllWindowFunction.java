package com.imap.flink.demo;

import com.imap.pojo.DataReport;
import com.imap.pojo.ReportCountView;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/16 17:15
 * @Description:
 */
public class TopNDemo_AllWindowFunction {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<DataReport> stream = env.addSource(new DataReportSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .<DataReport>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<DataReport>)
                                (dataReport, l) -> dataReport.getTimestamp())
                );
        stream.print("input");
        stream
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new CountReportAggFunction(3), new ResultWindowFunction())
                .print("result");

        env.execute();
    }

    private static class CountReportAggFunction implements AggregateFunction<DataReport, HashMap<String,Long>, ArrayList<Tuple2<String,Long>>> {
        private int N;

        public CountReportAggFunction() {
            this(3);
        }

        public CountReportAggFunction(int n) {
            this.N = n;
        }

        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Long> add(DataReport dataReport, HashMap<String, Long> map) {
            Long count = map.getOrDefault(dataReport.getSiteId(), 0L);
            map.put(dataReport.getSiteId(),count+1);
            return map;
        }

        @Override
        public ArrayList<Tuple2<String, Long>> getResult(HashMap<String, Long> stringLongHashMap) {
            ArrayList<Tuple2<String, Long>> list = new ArrayList<>();
            ArrayList<Tuple2<String, Long>> topList = new ArrayList<>();
            for (Map.Entry<String, Long> item : stringLongHashMap.entrySet()) {
                list.add(Tuple2.of(item.getKey(),item.getValue()));
            }
//            list.sort(Comparator.comparing(i -> i.f1));
            // 升序排，取topN
            list.sort((t1,t2) -> (int) (t2.f1-t1.f1));
            int len = list.size() >= N ? N : list.size();
            for (int i = 0; i < len; i++) {
                topList.add(list.get(i));
            }
            return topList;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }

    private static class ResultWindowFunction extends ProcessAllWindowFunction<ArrayList<Tuple2<String,Long>>,ReportCountView, TimeWindow> {


        @Override
        public void process(Context context, Iterable<ArrayList<Tuple2<String, Long>>> iterable, Collector<ReportCountView> collector) throws Exception {
            ArrayList<Tuple2<String, Long>> list = iterable.iterator().next();
            long start = context.window().getStart();
            long end = context.window().getEnd();
            for (int i = 0; i < list.size(); i++) {
                Tuple2<String, Long> tuple2 = list.get(i);
                collector.collect(new ReportCountView(tuple2.f0,tuple2.f1,start,end));
            }
        }
    }
}
