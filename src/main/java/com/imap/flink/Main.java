package com.imap.flink;

import com.imap.flink.sink.HdfsSink;
import com.imap.pojo.AlarmItem;
import com.imap.pojo.DataReport;
import com.imap.pojo.MonitorConfig;
import com.imap.utils.MapperUtil;
import com.imap.utils.MonitorConfigSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.table.KafkaOptions;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * @Author: Weizhi
 * @Date: create in 2023/2/17 21:02
 * @Description:
 */
public class Main {
    public static final String REMOTE_KAFKA_URL = "weizhi:9092";
    public static final String LOCAL_KAFKA_URL = "localhost:9092";
    public static final String CKPT_HDFS_URL = "hdfs://weizhi:8020/imap/checkpoint";
    public static final String REPORT_HDFS_URL = "hdfs://weizhi:8020/imap/report";
    public static final String REMOTE_MYSQL_URL = "jdbc:mysql://weizhi:3306/imap?serverTimezone=UTC&useSSL=false";
    public static final String LOCAL_MYSQL_URL = "jdbc:mysql://localhost:3306/imap?serverTimezone=UTC&useSSL=false";
    public static final String REPORT_TOPIC = "report";
    public static final String ALARM_TOPIC = "alarm";
    public static final String MYSQL_DRIVER = "com.mysql.jdbc.Driver";
    public static final String MYSQL_USER = "imap";
    public static final String MYSQL_PW = "imap@SSPU";

    public static Properties earliestProp =  new Properties();;
    public static Properties latestProp =  new Properties();;
    public static JdbcConnectionOptions jdbcConnectionOptions;
    public static  StreamingFileSink streamingFileSink;
    public static  FileSink hdfsFileSink;
    public static boolean saveHDFS;

    private static StreamExecutionEnvironment env;
    private static StreamTableEnvironment tableEnv;

/**
 *
 /usr/local/flink/bin/flink run -m localhost:8081 -c com.imap.flink.Main ./IMAP-Flink-1.0-SNAPSHOT.jar --mysql local --kafka local --hdfs local
 /usr/local/flink/bin/flink run -m localhost:8081 -c com.imap.flink.Main ./IMAP-Flink-1.0-SNAPSHOT-jar-with-dependencies.jar --mysql local --kafka local --hdfs local
 * */
    private static void parseArgs(String[] args){
        try{// 从参数中获取 --mysql local --kafka local --hdfs local
            ParameterTool parameterTool = ParameterTool.fromArgs(args);
            String mysql = parameterTool.get("mysql");
            String kafka = parameterTool.get("kafka");
            String hdfs = parameterTool.get("hdfs");
            String mysqlUrl = REMOTE_MYSQL_URL;
            String kafkaUrl = REMOTE_KAFKA_URL;
            if (mysql != null && "local".equals(mysql)) {
                System.out.println("切换本地MYSQL");
                mysqlUrl = LOCAL_MYSQL_URL;
            }
            if (kafka != null && "local".equals(kafka)) {
                System.out.println("切换本地Kafka");
                kafkaUrl = LOCAL_KAFKA_URL;
            }
            // 默认不保存至HDFS
            if (kafka != null && "local".equals(hdfs)) {
                System.out.println("切换本地HDFS");
                saveHDFS = true;
            }
            // Mysql 配置
            jdbcConnectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                    .withUrl(mysqlUrl)
                    .withDriverName(MYSQL_DRIVER)
                    .withUsername(MYSQL_USER)
                    .withPassword(MYSQL_PW)
                    .build();

            // Kafka 配置
            earliestProp.setProperty("bootstrap.servers", kafkaUrl);
            earliestProp.setProperty(KafkaOptions.SCAN_STARTUP_MODE.key(), KafkaOptions.SCAN_STARTUP_MODE_VALUE_EARLIEST);

            latestProp.setProperty("bootstrap.servers", kafkaUrl);
            latestProp.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"report-group");
            latestProp.setProperty(KafkaOptions.SCAN_STARTUP_MODE.key(), KafkaOptions.SCAN_STARTUP_MODE_VALUE_LATEST);

            // hdfs sink 配置
            // 修改用户名，解决权限问题
            System.setProperty("HADOOP_USER_NAME", "root");
            Path outputPath = new Path(REPORT_HDFS_URL);
            hdfsFileSink  = HdfsSink.getHdfsSink();
        }catch (Exception e){
            System.out.println("初始化异常：" + args);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        // 解析参数
        parseArgs(args);
        // 初始化环境
        init();
        // 数据源
        SingleOutputStreamOperator<DataReport> dataReportStream =
                env.addSource(new FlinkKafkaConsumer<String>(REPORT_TOPIC,
                        new SimpleStringSchema(),
                                latestProp))
                    .map(json -> MapperUtil.jsonToObj(json, DataReport.class))
                    // 过滤无法解析的数据
                    .filter(item -> item!=null)
                    // 设置水位线
                    .assignTimestampsAndWatermarks(WatermarkStrategy
                    .<DataReport>forBoundedOutOfOrderness(Duration.ZERO)
                    .withTimestampAssigner((SerializableTimestampAssigner<DataReport>) (data, l) -> data.getTimestamp()));
        // 监控配置 从数据库轮询获取
        DataStreamSource<MonitorConfig> configDataStreamSource = env.addSource(
                new MonitorConfigSource(jdbcConnectionOptions,5000));
        // 监控广播流 <siteId,monitorConfig>
        BroadcastStream<MonitorConfig> configBroadcastStream = configDataStreamSource
                .broadcast(new MapStateDescriptor<Integer, MonitorConfig>(
                        "matcher",
                        Integer.class,
                        MonitorConfig.class));
        //  异常数据分流标签，最右边的大括号不能少，不然会有泛型擦除的问题
        OutputTag<AlarmItem> abnormalDataTag = new OutputTag<AlarmItem>("abnormalData") {};
        // 数据异常匹配
        SingleOutputStreamOperator<DataReport> processedStream = dataReportStream.keyBy(data -> data.getSiteId())
                .connect(configBroadcastStream)
                .process(new MonitorMatcher(abnormalDataTag));
        // 提取异常数据流
        DataStream<AlarmItem> abnormalDataStream = processedStream.getSideOutput(abnormalDataTag);
        abnormalDataStream.print("异常数据");
        // 异常数据发送给Kafka alarm
        abnormalDataStream
                .map(item -> item.toString())
                .addSink(new FlinkKafkaProducer<String>(ALARM_TOPIC,new SimpleStringSchema(),earliestProp));
        // 默认聚合分钟和小时粒度的数据
        AvgDataToMySQL.AggDataReport(tableEnv,jdbcConnectionOptions, AvgDataEnum.MINUTE, processedStream);
        AvgDataToMySQL.AggDataReport(tableEnv,jdbcConnectionOptions, AvgDataEnum.HOUR, processedStream);
        // 保存到HDFS
        if (saveHDFS){
            processedStream.sinkTo(hdfsFileSink);
        }
        processedStream.print("输出");
        System.out.println("主进程就绪");
        env.execute();
    }


    private static void init() {
        // 创建Flink执行环境
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 并行度
        env.setParallelism(1);

        if(saveHDFS){
            System.out.println("开启Checkpoint");
            //设置Checkpoint的时间间隔为5000ms
            env.enableCheckpointing(5000,CheckpointingMode.EXACTLY_ONCE);
            // 设置Checkpoint存储路径
            env.getCheckpointConfig().setCheckpointStorage(CKPT_HDFS_URL);
            //设置两个Checkpoint 之间最少等待时间,如设置Checkpoint之间最少是要等 500ms(为了避免每隔5000ms做一次Checkpoint的时候,前一次太慢和后一次重叠到一起去了  --//默认是0
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
            //设置checkpoint的超时时间,如果 Checkpoint在 10s内尚未完成说明该次Checkpoint失败,则丢弃。
            env.getCheckpointConfig().setCheckpointTimeout(10000L);
            //设置同一时间有多少个checkpoint可以同时执行
            env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
            // 设置重启策略
            // 一个时间段内的最大失败次数
            env.setRestartStrategy(RestartStrategies.failureRateRestart(3,
                    // 衡量失败次数的是时间段
                    Time.of(5, TimeUnit.MINUTES),
                    // 间隔
                    Time.of(10, TimeUnit.SECONDS)
            ));
        }

        // 创建Table执行环境
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(env, settings);
    }
}
