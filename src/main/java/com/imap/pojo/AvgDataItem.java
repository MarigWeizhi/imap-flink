package com.imap.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Author: Weizhi
 * @Date: create in 2023/3/30 12:26
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AvgDataItem {

    private Integer siteId;
    private Long endTime;
    private Integer type;
    private Double avgTmp;
    private Double avgHmt;
    private Double avgLx;

    public static AvgDataItem fromResultSet(ResultSet resultSet) throws SQLException {
        int siteId = resultSet.getInt("site_id");
        long endTime = resultSet.getTimestamp("end_time").getTime();
        int type = resultSet.getInt("type");
        double avgTmp = resultSet.getDouble("avg_tmp");
        double avgHmt = resultSet.getDouble("avg_hmt");
        double avgLx = resultSet.getDouble("avg_lx");
        return new AvgDataItem(siteId,endTime,type,avgTmp,avgHmt,avgLx);
    }
}
