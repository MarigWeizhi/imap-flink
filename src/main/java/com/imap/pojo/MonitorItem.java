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
    private Boolean monitor;
    private Integer interval;
    private Double max;
    private Double min;

    @Override
    public String toString() {
        return "MonitorItem{" +
                "type='" + type + '\'' +
                ", monitor=" + monitor +
                ", interval=" + interval +
                ", max=" + max +
                ", min=" + min +
                '}';
    }
}
