//package data.run;
//
//import data.Extract_Product.LiveEventProducer;
//import data.Load_Util.PostgresSinkUtil;
//import data.Transformation_Sink.EventTypeAggOperator;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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
//public class postgreSQL {
//
//    public static void main(String[] args) throws Exception {
//        // 启动模拟的 Kafka Producer，实际部署时可注释掉
//        Thread producerThread = new Thread(() -> LiveEventProducer.main(new String[0]));
//        producerThread.setDaemon(true);
//        producerThread.start();
//
//        Configuration conf = new Configuration();
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
//
//        // 1. 创建 Kafka Source
//        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
//                .setBootstrapServers("localhost:9092")
//                .setTopics("live-events")
//                .setGroupId("flink-agg-group")
//                .setStartingOffsets(OffsetsInitializer.earliest())
//                .setValueOnlyDeserializer(new SimpleStringSchema())
//                .build();
//
//        // 2. 读取 Kafka 消息流，过滤空字符串
//        DataStream<String> rawStream = env.fromSource(
//                kafkaSource,
//                WatermarkStrategy.noWatermarks(),
//                "Kafka Source"
//        ).filter(Objects::nonNull).filter(s -> !s.trim().isEmpty());
//
//        // 3. 通过 map 转换成 Tuple2<eventType, 1> 用于统计
//        DataStream<Tuple2<String, Integer>> aggStream = rawStream
//                .map(EventTypeAggOperator.getEventTypeMapper())
//                .filter(x -> x != null);
//
//        // 4. 配置 PostgreSQL JDBC URL
//        String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres?user=postgres&password=postgres";
//
//        // 5. 添加 JDBC Sink 写入 PostgreSQL
//        aggStream.addSink(PostgresSinkUtil.createPostgresSink(jdbcUrl)).name("PostgreSQL Sink");
//
//        // 6. 启动 Flink 流处理程序
//        env.execute("Flink Kafka to PostgreSQL");
//    }
//}
