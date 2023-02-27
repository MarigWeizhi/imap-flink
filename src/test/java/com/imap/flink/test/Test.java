package com.imap.flink.test;

import com.imap.pojo.DataReport;
import com.imap.utils.DataReportSource;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connector.jdbc.JdbcInputFormat;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/10 16:07
 * @Description:
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<DataReport> dataReportDataStreamSource = env.addSource(new DataReportSource());


        DataStreamSource<Row> input = env.createInput(
                JdbcInputFormat
                        .buildJdbcInputFormat()
                        .setDBUrl("jdbc:mysql://localhost:3306/db1")
                        .setDrivername("com.mysql.jdbc.Driver")
                        .setUsername("root")
                        .setPassword("")
                        .setQuery("select name,age from redisuser")
                        .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
                        .finish());
        input.print();
        env.execute();
    }
}
