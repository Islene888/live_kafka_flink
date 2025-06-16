//package data;
//
//
//// 引入所有需要的类
//import data.Extract_Product.LiveEventProducer;
//        import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Objects;
//
//// 引入自定义算子
//import data.Transformation_Sink.EventTypeAggOperator;
//import data.Transformation_Sink.UserEventAggOperator;
//
//public class Main {
//    public static void main(String[] args) throws Exception {
//        // AWS/MinIO 相关设置可以都不写
//        // System.setProperty("AWS_ACCESS_KEY_ID", "minioadmin");
//        // System.setProperty("AWS_SECRET_ACCESS_KEY", "minioadmin");
//
//        // 启动 Producer 线程
//        Thread producerThread = new Thread(() -> {
//            LiveEventProducer.main(new String[0]);
//        });
//        producerThread.setDaemon(true);
//        producerThread.start();
//
//        // Flink 环境配置（S3 相关都可以不写了）
//        Configuration conf = new Configuration();
//        // 其它 Hadoop/Flink 配置也可以简化
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//
//        // Kafka Source
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("live-events")
//                .setGroupId("flink-agg-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        DataStream<String> rawStream = env.fromSource(
//                        kafkaSource,
//                        WatermarkStrategy.noWatermarks(),
//                        "Kafka Source"
//                )
//                .filter(Objects::nonNull)
//                .filter(s -> !s.trim().isEmpty());
//
//        // 打印从Kafka读到的原始消息
//        rawStream.print("1-Raw-From-Kafka");
//
//        // 聚合
//        DataStream<Tuple2<String, Integer>> aggStream = rawStream
//                .map(EventTypeAggOperator.getEventTypeMapper())
//                .filter(x -> x != null);
//
//        aggStream.print("2-After-EventType-Mapper");
//
//        DataStream<Tuple2<String, Integer>> aggStream2 = rawStream
//                .map(UserEventAggOperator.getUserEventMapper())
//                .filter(x -> x != null);
//
//        // 最终字符串流
//        DataStream<String> aggStringStream = aggStream.map(tuple -> tuple.f0 + "," + tuple.f1);
//
//        aggStringStream.print("3-Final-Data");
//
////        // 不再添加任何 Sink（无 S3 Sink）
////        aggStream.addSink(
////                PostgresSinkUtil.createPostgresSink("jdbc:postgresql://localhost:5432/postgres?user=postgres&password=")
////        ).name("JDBC Sink");
//
//        // 执行任务
//        env.execute("Flink Multi-Sink Demo (No S3)");
//    }
//}
