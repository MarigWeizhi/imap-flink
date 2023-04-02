package com.imap.pojo;

import com.imap.utils.DataReportSource;
import com.imap.utils.MapperUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

/**
 * @Author: Weizhi
 * @Date: create in 2023/3/8 22:43
 * @Description:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AlarmItem implements Serializable {
    DataReport dataReport;
    MonitorItem monitorItem;

    public static AlarmItem of(DataReport dataReport, MonitorItem monitorItem) {
        return new AlarmItem(dataReport,monitorItem);
    }

    public static AlarmItem from(String json) {
        return MapperUtil.jsonToObj(json,AlarmItem.class);
    }

    @Override
    public String toString() {
        return MapperUtil.obj2Str(this);
    }

    public static void main(String[] args) {
        DataReport dataReport = DataReportSource.getRandomDataReport();
        dataReport.getData().put("tmp",88.8);
        MonitorConfig defaultConfig = MonitorConfig.getDefaultConfig(1);
        MonitorItem tmp = defaultConfig.getMonitorItems().get("tmp");
        tmp.setMax(22.2);
        AlarmItem alarmItem = AlarmItem.of(dataReport, tmp);
        System.out.println(alarmItem);
    }

}
