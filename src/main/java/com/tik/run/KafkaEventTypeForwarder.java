package com.tik.run;

import com.tik.handler.LiveEventProcessor;
import com.tik.config.KafkaSinkUtil;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.ReduceFunction;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class KafkaEventTypeForwarder {
    public static void main(String[] args) throws Exception {
        // 1. 创建 Flink 流执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. 配置 Kafka Source（消费原始事件 topic）
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setTopics("live-events")
                .setGroupId("flink-eventtype-forwarder-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        // 3. 从 Kafka Source 读入原始 JSON 字符串
        DataStream<String> rawStream = env.fromSource(
                        kafkaSource,
                        WatermarkStrategy.noWatermarks(),
                        "Kafka Source"
                ).filter(Objects::nonNull)
                .filter(s -> !s.trim().isEmpty());

        // 4. 提取 eventType 并计数
        DataStream<Tuple2<String, Integer>> aggStream = rawStream
                .map(LiveEventProcessor.eventTypeMapper())
                .filter(x -> x != null);

        // 5. 按 eventType 分组，每 5 秒聚合，并把每个窗口内所有类型统计组合成一个 Map，转 json 后写入 sink
        String bootstrapServers = "localhost:9092";
        String outputTopic = "live-events-agg";

        aggStream
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(1)))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2) {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                })
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        // 补全所有你需要前端展示的统计字段，确保始终输出
                        Map<String, Integer> stats = new HashMap<>();
                        stats.put("like", 0);
                        stats.put("comment", 0);
                        stats.put("user_join", 0);
                        stats.put("send_gift", 0);
                        // ...如有更多类型，继续加

                        // 聚合覆盖已有类型
                        for (Tuple2<String, Integer> t : elements) {
                            if (t != null && t.f0 != null && stats.containsKey(t.f0)) {
                                stats.put(t.f0, t.f1);
                            }
                        }
                        ObjectMapper om = new ObjectMapper();
                        out.collect(om.writeValueAsString(stats));
                    }
                })
                .sinkTo(KafkaSinkUtil.createKafkaSink(bootstrapServers, outputTopic));

        env.execute("Flink Kafka EventType Forwarder");
    }
}
