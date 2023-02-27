package com.imap.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;
import java.util.Map;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/16 16:17
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataReport {
    private Integer siteId;  // 站点id
    private Long timestamp; // 创建时间
    private String type;  // 数据类型
    private Integer version;  // 监控版本
    private Integer status; // 数据状态 0为正常，1为异常
    private Map<String,Double> data;  // 数据

    @Override
    public String toString() {
        return "DataReport{" +
                "siteId=" + siteId +
                ", timestamp=" + new Timestamp(timestamp) +
                ", type='" + type + '\'' +
                ", version=" + version +
                ", status=" + status +
                ", data=" + data +
                '}';
    }
}
