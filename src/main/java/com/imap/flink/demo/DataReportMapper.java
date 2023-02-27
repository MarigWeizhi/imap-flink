package com.imap.flink.demo;

import com.imap.pojo.DataReport;
import com.imap.pojo.DataReportTableRow;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/27 20:26
 * @Description:
 */
public class DataReportMapper implements MapFunction<DataReport, DataReportTableRow> {
    @Override
    public DataReportTableRow map(DataReport data) throws Exception {
        DataReportTableRow res = new DataReportTableRow(
                data.getSiteId(),
                data.getTimestamp(),
                data.getType(),
                data.getVersion(),
                data.getStatus(),
                data.getData().get("tmp"),
                data.getData().get("hmt"),
                data.getData().get("lx")
        );
        return res;
    }
}
