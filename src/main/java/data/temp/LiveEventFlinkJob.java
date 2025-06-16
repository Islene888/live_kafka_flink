package data.temp;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class LiveEventFlinkJob {
    public static void main(String[] args) throws Exception {
        // 创建 Flink 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 配置 Kafka 地址，推荐本地开发写死 localhost:9092，上生产可用环境变量
        String bootstrapServers = "localhost:9092";
        System.out.println("=== BOOTSTRAP SERVERS: " + bootstrapServers + " ===");

        // 构建 Flink Kafka Source
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setTopics("live-events") // 你的 Kafka topic
                .setGroupId("flink-test-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 构建 DataStream
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        // 输出到控制台
        stream.print();

        // 启动作业
        env.execute("Live Event Kafka -> Print");
    }
}
