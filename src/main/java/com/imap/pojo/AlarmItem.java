package com.imap.pojo;

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

    @Override
    public String toString() {
        return MapperUtil.obj2Str(this);
    }
}
