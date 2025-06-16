package data.Transformation_Sink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

class LiveEventFlinkAggJob {

    public static void main(String[] args) throws Exception {
        // 1) 获取 env 并禁用 ClosureCleaner
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableClosureCleaner();

        // 2) 构造 KafkaSource
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("live-events")
                .setGroupId("flink-agg-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3) 从 Kafka 读 String 流
        DataStream<String> stream = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "Kafka Source"
        );

        ObjectMapper objectMapper = new ObjectMapper();

        // 4) Map 并做简单聚合：遇到非 JSON 消息就跳过
        DataStream<Tuple2<String, Integer>> aggStream = stream
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        try {
                            JsonNode node = objectMapper.readTree(value);
                            String eventType = node.get("event_type").asText();
                            return Tuple2.of(eventType, 1);
                        } catch (Exception e) {
                            // 非法 JSON 或缺字段，直接返回 null
                            return null;
                        }
                    }
                })
                // 5) 过滤掉 null
                .filter(tuple -> tuple != null)
                // 6) 分组、滚动窗口、sum
                .keyBy(tuple -> tuple.f0)
                .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
                .sum(1);

        aggStream.print();

        env.execute("Live Event Kafka -> Aggregation");
    }
}
