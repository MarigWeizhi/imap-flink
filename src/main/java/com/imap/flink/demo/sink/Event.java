package com.imap.flink.demo.sink;

import java.sql.Timestamp;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/8 22:27
 * @Description:
 */
public class Event {
    public String user;
    public String url;
    public long timestamp;

    public Event() {
    }

    public Event(String user, String usr, long timestamp) {
        this.user = user;
        this.url = usr;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Event{" +
                "user='" + user + '\'' +
                ", usr='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
