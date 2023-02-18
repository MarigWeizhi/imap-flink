package com.imap.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.imap.pojo.DataReport;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/20 22:17
 * @Description:
 */
public class MapperUtil {

    private volatile static  ObjectMapper mapper;

    private static ObjectMapper getMapper(){
        if(mapper == null){
            synchronized (MapperUtil.class){
                if (mapper == null){
                    mapper = new ObjectMapper();
                }
            }
        }
        return mapper;
    }
    public static Object str2Object(String str,Object cls) throws IOException {
        Object obj = null;
        ObjectMapper objectMapper = getMapper();
        if(cls instanceof TypeReference){
//            自定义引用类型 new TypeReference<Map<String, MonitorItem>>() {}
            obj = objectMapper.readValue(str, (TypeReference)cls);
        }

        if(cls instanceof Class){
            obj = objectMapper.readValue(str, (Class)cls);
        }
        return obj;
    }

    public static DataReport str2DataReport(String str) throws IOException {
        DataReport o = (DataReport)str2Object(str, DataReport.class);
        return o;
    }

    public static String obj2Str(Object obj) {
        ObjectMapper objectMapper = getMapper();
        String jsonString = null;
        try {
            jsonString = objectMapper.writeValueAsString(obj);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        return jsonString;
    }

    public static void main(String[] args) throws IOException {
        HashMap<String, Double> map = new HashMap<>();
        map.put("test",1.2);
        map.put("java",8.8);
        DataReport dataReport = new DataReport(1, 111L, "type", 1,0, map);
        String str = obj2Str(dataReport);
        System.out.println(str);
        DataReport dataReport1 = (DataReport)str2DataReport(str);
        System.out.println(dataReport1);
    }
}
