package com.imap.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
/**
 * @Author: Weizhi
 * @Date: create in 2023/4/2 9:57
 * @Description:
 */
public class HDFSTest {
    public static void main(String[] args) throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration=new Configuration();
//        configuration.set("dfs.client.use.datanode.hostname","true");
        //2.根据configuration获取Filesystem对象
        FileSystem fs=FileSystem.get(new URI("hdfs://47.116.66.37:8020"),configuration,"root");
        fs.copyFromLocalFile(new Path("A:\\local\\BigData\\IMAP-Flink\\src\\main\\resources\\log4j.properties"),new Path("/test.txt"));
        //4.释放FileSystem对象（类似数据库连接）
        fs.close();
    }
}
