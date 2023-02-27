package com.imap.utils;

import com.imap.pojo.DataReport;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Random;

import static java.lang.Thread.sleep;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/16 16:23
 * @Description:
 */
public class DataReportSource implements SourceFunction<DataReport> {
    private  boolean running = true;

//    public static final String[] SITE_ID = {"IMAP_001","IMAP_002","IMAP_003","IMAP_004","IMAP_005"};
    public static final int[] SITE_ID = {1,2,3,4,5};
    public static final String TYPE = "report";

    public static int version = 1;
    public static int status = 0;

    public Random random = new Random();

    private long interval;


    public DataReportSource() {
        this(1000);
    }

    public DataReportSource(long interval) {
        this.interval = interval;
    }


    @Override
    public void run(SourceContext<DataReport> sourceContext) throws Exception {
        String[] names = {"tmp","hmt","lx"};

        while (running){
            sleep(interval);
            HashMap<String, Double> map = new HashMap<>();
            DataReport dataReport = new DataReport();

            map.put(names[0],getRandomData(2,2));
            map.put(names[1],getRandomData(0,2));
            map.put(names[2],getRandomData(3,2));

            dataReport.setTimestamp(System.currentTimeMillis());
            dataReport.setSiteId(SITE_ID[random.nextInt(SITE_ID.length)]);
            dataReport.setType(TYPE);
            dataReport.setVersion(version);
            dataReport.setStatus(status);
            dataReport.setData(map);

            sourceContext.collect(dataReport);
        }
    }

    public static Double getRandomData(int n, int scale) {
        Random random = new Random();
        double base = Math.pow(10,n);
        BigDecimal bigDecimal = new BigDecimal(random.nextDouble() * base)
                .setScale(scale, BigDecimal.ROUND_HALF_UP);
        return bigDecimal.doubleValue();
    }

    @Override
    public void cancel() {
        running = false;
    }
}
