package data.temp;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaMainJob {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092"); // 修改为你的Kafka地址
        properties.setProperty("group.id", "flink-demo");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "your-topic",   // 修改为你的topic
                new SimpleStringSchema(),
                properties
        );

        env.addSource(consumer).print();

        env.execute("Flink Kafka Main Demo");
    }
}
