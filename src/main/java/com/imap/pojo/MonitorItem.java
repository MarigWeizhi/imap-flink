package com.imap.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/14 21:46
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class MonitorItem {
    private String type;
    private Integer open;
    private Double max;
    private Double min;

    @Override
    public String toString() {
        return "MonitorItem{" +
                "type='" + type + '\'' +
                ", open=" + open +
                ", max=" + max +
                ", min=" + min +
                '}';
    }
}
