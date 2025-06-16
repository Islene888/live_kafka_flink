package data.Transformation_Sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


public class EventTypeAggOperator {


    public static RichMapFunction<String, Tuple2<String, Integer>> getEventTypeMapper() {
        // 使用 RichMapFunction 来处理需要初始化的资源
        return new RichMapFunction<String, Tuple2<String, Integer>>() {

            // 1. 将 ObjectMapper 声明为 transient，告诉 Flink 不要序列化它
            private transient ObjectMapper objectMapper;

            /**
             * 2. 实现 open() 方法。这个方法在每个任务实例开始处理数据前，会且仅会执行一次。
             * 这是初始化非序列化资源（如数据库连接、ObjectMapper等）的最佳位置。
             */
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                // 在这里初始化 ObjectMapper
                this.objectMapper = new ObjectMapper();
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                try {
                    JsonNode node = objectMapper.readTree(value);
                    JsonNode typeNode = node.get("event_type");
                    if (typeNode == null || typeNode.isNull()) {
                        return null;
                    }
                    String eventType = typeNode.asText();
                    return Tuple2.of(eventType, 1);
                } catch (Exception e) {
                    // JSON 解析失败时跳过该条，并可以考虑打印日志
                    // e.printStackTrace();
                    return null;
                }
            }
        };
    }
}