package data.Transformation_Sink;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;

/**
 * 【已修正】用户事件聚合算子工具类
 * 解析 JSON，提取 user_id 做 group/count
 */
public class UserEventAggOperator {

    /**
     * 【已修正】返回用于提取 user_id 并聚合的 RichMapFunction
     */
    public static RichMapFunction<String, Tuple2<String, Integer>> getUserEventMapper() {
        // 使用 RichMapFunction 来处理需要初始化的资源
        return new RichMapFunction<String, Tuple2<String, Integer>>() {

            // 手动指定一个版本号，避免 InvalidClassException
            private static final long serialVersionUID = 1L;

            // 1. 将 ObjectMapper 声明为 transient，告诉 Flink 不要序列化它
            private transient ObjectMapper objectMapper;

            /**
             * 2. 实现 open() 方法，在任务开始前执行一次，用于初始化资源
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
                    // 防止 user_id 字段不存在
                    JsonNode userIdNode = node.get("user_id");
                    if (userIdNode == null || userIdNode.isNull()) {
                        return null;
                    }
                    // 将user_id统一作为字符串处理
                    String userId = userIdNode.asText();
                    return Tuple2.of(userId, 1);
                } catch (Exception e) {
                    // JSON 解析失败，丢弃
                    return null;
                }
            }
        };
    }
}