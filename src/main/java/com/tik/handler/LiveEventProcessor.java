package com.tik.handler;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;


public class LiveEventProcessor {


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
                    JsonNode typeNode = root.has("eventType") ? root.get("eventType") : root.get("event_type");
                    if (typeNode == null || typeNode.isNull()) {
                        System.out.println("❌ 没有找到 eventType, 原文: " + value);
                        return null;
                    }
                    String eventType = typeNode.asText();
                    System.out.println("🔥 Flink收到事件: " + eventType + " | " + value);
                    return Tuple2.of(eventType, 1);
                } catch (Exception e) {
                    System.out.println("❌ 解析 JSON 失败: " + value);
                    return null;
                }
            }

        };
    }
}
