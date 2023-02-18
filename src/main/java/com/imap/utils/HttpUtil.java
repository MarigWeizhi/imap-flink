package com.imap.utils;

import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorItem;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/18 22:28
 * @Description:
 */
public class HttpUtil {

    private static final String BASE_URL = "http://localhost:8080/api";
    private static final String ABNORAL_REPORT = "/abnormal";

    private static volatile CloseableHttpClient httpClient;

    private static CloseableHttpClient getHttpClient() {
        if(httpClient == null){
            synchronized (HttpUtil.class){
                if (httpClient == null){
                    httpClient = HttpClients.createDefault();
                }
            }
        }
        return httpClient;
    }


    public void sendAbnormalData(Tuple2<DataReport, MonitorItem> data){
        String jsonStr = MapperUtil.obj2Str(data);
        String url = BASE_URL+ABNORAL_REPORT;
        HttpPost httpPost = new HttpPost(url);

        CloseableHttpClient client = getHttpClient();

        //请求参数转JOSN字符串
        StringEntity entity = new StringEntity(jsonStr, "UTF-8");
        entity.setContentEncoding("UTF-8");
        entity.setContentType("application/json");
        httpPost.setEntity(entity);
        try {
            HttpResponse response = client.execute(httpPost);
            if (response.getStatusLine().getStatusCode() == 200) {
                System.out.println("post成功");
//                EntityUtils.toString(response.getEntity(), "UTF-8")
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("post异常");
        }

    }

}
