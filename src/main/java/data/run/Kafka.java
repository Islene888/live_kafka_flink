//package data.run;
//
//import data.Extract_Product.LiveEventProducer;
//import data.Load_Util.KafkaSinkUtil;
//import data.Transformation_Sink.EventTypeAggOperator;
//
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple2;
//
//import org.apache.flink.connector.kafka.source.KafkaSource;
//import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
//
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Objects;
//
//public class Kafka {
//    public static void main(String[] args) throws Exception {
//        // 启动本地模拟 Producer（可选，调试时用）
//        Thread producerThread = new Thread(() -> LiveEventProducer.main(new String[0]));
//        producerThread.setDaemon(true);
//        producerThread.start();
//
//        // Flink 环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // Kafka Source - 读原始事件
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
//        // 使用算子 map，处理成 Tuple2<String, Integer> 形式
//        DataStream<Tuple2<String, Integer>> aggStream = rawStream
//                .map(EventTypeAggOperator.getEventTypeMapper())
//                .filter(x -> x != null);
//
//        // 转成字符串，方便写入 Kafka
//        DataStream<String> aggStringStream = aggStream
//                .map(tuple -> tuple.f0 + "," + tuple.f1);
//
//        // 创建 Kafka Sink，写入另一个 topic
//        String bootstrapServers = "localhost:9092";
//        String outputTopic = "live-events-agg";
//
//        aggStringStream.sinkTo(KafkaSinkUtil.createKafkaSink(bootstrapServers, outputTopic));
//
//        env.execute("Flink Kafka Process and Sink");
//    }
//}
