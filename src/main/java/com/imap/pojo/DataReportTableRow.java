package com.imap.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/27 20:30
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class DataReportTableRow {
    private Integer siteId;  // 站点id
    private Long timestamp; // 创建时间
    private String type;  // 数据类型
    private Integer version; // 监控版本
    private Integer status; // 数据状态 0为正常，1为异常
    private Double tmp;
    private Double hmt;
    private Double lx;

    @Override
    public String toString() {
        return "DataReportTableRow{" +
                "siteId=" + siteId +
                ", timestamp=" + new Timestamp(timestamp) +
                ", type='" + type + '\'' +
                ", version=" + version +
                ", status=" + status +
                ", tmp=" + tmp +
                ", hmt=" + hmt +
                ", lx=" + lx +
                '}';
    }
}
