package com.imap.flink.demo.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/10 16:58
 * @Description:
 */
public class SinkToRedis {
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


        FlinkJedisPoolConfig build = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .build();
        source.addSink(new RedisSink<>(build,new MyRedisMapper())).setParallelism(1);

        env.execute();
    }

    private static class MyRedisMapper implements RedisMapper<Event> {
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET,"flink");
        }

        @Override
        public String getKeyFromData(Event o) {
            return o.user;
        }

        @Override
        public String getValueFromData(Event o) {
            return o.url;
        }
    }
}
