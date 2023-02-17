package com.imap.flink.demo.sink;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.client.RestClientBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * @Author: Weizhi
 * @Date: create in 2023/1/13 23:17
 * @Description:
 */
public class SinkToES {
    public static void main(String[] args){
//        elastic:QB0JcxwIZYw0PySbKUDl
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<Event> source = env.fromElements(
                new Event("Winsa", "/user", 1000),
                new Event("Bob", "/cart", 1000),
                new Event("Bob", "/cart", 2000),
                new Event("Winsa", "/cart", 3000),
                new Event("Winsa", "/cart", 2000),
                new Event("Weizhi", "/inner", 2000),
                new Event("Weizhi", "/goods?id=1", 5000),
                new Event("Weizhi", "/goods?id=1", 2000),
                new Event("Weizhi", "/goods?id=1", 9000),
                new Event("Winsa", "/goods?id=1", 15000),
                new Event("Bob", "/cart", 3000)
        );

        List<HttpHost> httpHosts = Arrays.asList(new HttpHost("47.116.66.37",9200,"http"));

        ElasticsearchSinkFunction<Event> sinkFunction = new ElasticsearchSinkFunction<Event>() {
            @Override
            public void process(Event event, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
                HashMap<String, Long> data = new HashMap<>();
                data.put(event.user, event.timestamp);
                IndexRequest request = Requests.indexRequest()
                        .index("test")
                        .source(data);
                requestIndexer.add(request);
            }
        };

        ElasticsearchSink.Builder<Event> elasticsearchSinkBuder = new ElasticsearchSink.Builder<>(httpHosts, sinkFunction);
//        设置密码验证
        elasticsearchSinkBuder.setRestClientFactory(
                restClientBuilder -> {
                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                        @Override
                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                            CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
                            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials("elastic","QB0JcxwIZYw0PySbKUDl"));
                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                        }
                    });
                }
        );
        source.addSink(elasticsearchSinkBuder.build());

        try {
            env.execute();
        }catch (IOException e) {
            if (!(e.getMessage().contains("200 OK"))) {
                new Exception(e.getMessage());
            }
        }catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
