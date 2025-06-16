package com.tik.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * 提供用于提取事件类型并计数的 Flink 算子（RichMapFunction）。
 * 输入为 JSON 字符串，输出为 (eventType, 1) 这样的二元组，可直接用于 KeyBy、Window 等后续聚合。
 */
public class LiveEventProcessor {

    /**
     * 获取事件类型计数算子。
     * 用法：dataStream.map(EventTypeAggOperator.eventTypeMapper())
     */
    public static RichMapFunction<String, Tuple2<String, Integer>> eventTypeMapper() {
        return new RichMapFunction<String, Tuple2<String, Integer>>() {
            // Flink 推荐不可序列化对象（如 ObjectMapper）用 transient 修饰
            private transient ObjectMapper objectMapper;

            @Override
            public void open(Configuration parameters) throws Exception {
                this.objectMapper = new ObjectMapper();
            }

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                try {
                    JsonNode root = objectMapper.readTree(value);
                    // 支持不同 key 命名，比如 event_type 或 eventType
                    // 支持 eventType 优先（小驼峰，和你前端一致），其次兼容 event_type（下划线，万一有别的后端推数据）
                    JsonNode typeNode = root.has("eventType") ? root.get("eventType") : root.get("event_type");
                    if (typeNode == null || typeNode.isNull()) {
                        return null; // 或者返回 Tuple2.of("unknown", 1)
                    }
                    return Tuple2.of(typeNode.asText(), 1);
                } catch (Exception e) {
                    // 可以考虑加日志
                    // log.warn("JSON解析失败: {}", value, e);
                    return null;
                }
            }
        };
    }
}
