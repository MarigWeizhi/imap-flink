package com.imap.flink.demo.sink;

import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/10 22:44
 * @Description:
 */
public class SinkToMysql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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

        source.addSink(JdbcSink.sink("insert into test (name, url) values (?, ?)",
                new JdbcStatementBuilder<Event>() {
                    @Override
                    public void accept(PreparedStatement preparedStatement, Event event) throws SQLException {
                        preparedStatement.setString(1,event.user);
                        preparedStatement.setString(2,event.url);
                    }
                }, new JdbcConnectionOptions
                        .JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/study")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("")
                        .build()));

        env.execute();
    }
}
