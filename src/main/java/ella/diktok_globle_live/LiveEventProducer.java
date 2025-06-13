package ella.diktok_globle_live;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class LiveEventProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(props);

        String[] users = {"u1", "u2", "u3", "u4"};
        String[] events = {"enter_room", "send_gift", "comment", "like"};
        Random random = new Random();

        while (true) {
            String msg = String.format("{\"user_id\":\"%s\",\"event_type\":\"%s\",\"value\":%d,\"ts\":%d}",
                    users[random.nextInt(users.length)],
                    events[random.nextInt(events.length)],
                    random.nextInt(100),
                    System.currentTimeMillis()/1000
            );
            producer.send(new ProducerRecord<>("live-events", msg));
            System.out.println("Sent: " + msg);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException ignored) {}
        }
        // producer.close(); // 你可根据需求加关闭
    }
}
