package com.imap.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.sql.Timestamp;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/16 16:50
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReportCountView {
    private String siteId;
    private Long count;
    private Long start;
    private  Long end;

    @Override
    public String toString() {
        return "ReportCountView{" +
                "siteId='" + siteId + '\'' +
                ", count=" + count +
                ", start=" + new Timestamp(start) +
                ", end=" + new Timestamp(end) +
                '}';
    }
}
