package com.imap.pojo;

import com.imap.utils.DataReportSource;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/14 21:50
 * @Description:
 */


@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MonitorConfig {
    private String siteId;  // 监控站点
    private long timestamp; // 创建时间
    private int status; // 状态
    private int version;    // 监控配置版本，设备端上报时候版本过低，则会返回最新版
    private int interval;   // 设备数据上报时间间隔
    private Map<String,MonitorItem> monitorItems;  // 保存各个监控传感器配置项

    public static MonitorConfig getDefaultConfig(String siteId){
        MonitorConfig monitorConfig = new MonitorConfig();
        monitorConfig.setSiteId(siteId);
        MonitorItem tmp = new MonitorItem("tmp", true, 30,
                DataReportSource.getRandomData(2,2), 0);
        MonitorItem hmt = new MonitorItem("hmt", true, 30,
                DataReportSource.getRandomData(0,2), 0.00);
        ConcurrentHashMap<String, MonitorItem> monitorItemConcurrentHashMap = new ConcurrentHashMap<>();
        monitorItemConcurrentHashMap.put("tmp",tmp);
        monitorItemConcurrentHashMap.put("hmt",hmt);
        monitorConfig.setMonitorItems(monitorItemConcurrentHashMap);
        return monitorConfig;
    }

    public static List<MonitorConfig> getDefaultConfigList(){
        MonitorConfig config1 = getDefaultConfig("IMAP_001");
        MonitorConfig config2 = getDefaultConfig("IMAP_002");
        MonitorConfig config3 = getDefaultConfig("IMAP_003");
        MonitorConfig config4 = getDefaultConfig("IMAP_004");
        MonitorConfig config5 = getDefaultConfig("IMAP_005");

        List<MonitorConfig> monitorConfigs = Arrays.asList(config1, config2, config3, config4, config5);
        return monitorConfigs;
    }

}
