package com.tik.service;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class KafkaService {

    private static final Logger log = LoggerFactory.getLogger(KafkaService.class);
    private final String KAFKA_TOPIC = "live-events"; // 定义要发送到的Topic

    // Spring会自动将我们在上一步创建的Producer Bean注入到这里
    @Autowired
    private Producer<String, String> kafkaProducer;

    public void sendEvent(String eventMessage) {
        try {
            // 将从WebSocket收到的消息，原封不动地发送到Kafka
            kafkaProducer.send(new ProducerRecord<>(KAFKA_TOPIC, eventMessage));
            log.info("🚀 成功发送事件到Kafka Topic [{}]: {}", KAFKA_TOPIC, eventMessage);
        } catch (Exception e) {
            log.error("❌ 发送事件到Kafka失败: {}", eventMessage, e);
        }
    }
}