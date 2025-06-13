package ella.diktok_globle_live;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        // 创建流处理执行环境
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka 配置
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // 改成你自己的
        properties.setProperty("group.id", "flink-test-group");

        // 创建 Kafka Source
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "demo-topic", // topic
                new SimpleStringSchema(),
                properties
        );

        // 从最早开始消费
        consumer.setStartFromEarliest();

        // 加入数据流
        env.addSource(consumer)
                .print(); // 直接打印消息

        // 执行 Flink 任务
        env.execute("Flink Kafka Consumer Demo");
    }
}
