package data.Load;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class LoadResult {

    // S3 文件输出（CSV 格式）DataStream<String>
    public static StreamingFileSink<String> createS3Sink(String s3BucketPath) {
        return StreamingFileSink
                .forRowFormat(new Path(s3BucketPath), new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
                                .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
                                .withMaxPartSize(128 * 1024 * 1024)
                                .build())
                .build();
    }



    // Kafka 输出（DataStream<String>）
    public static KafkaSink<String> createKafkaSink(String bootstrapServers, String topic) {
        return KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .build();
    }

    // PostgreSQL 输出（DataStream<Tuple2<String, Integer>>）
    public static org.apache.flink.streaming.api.functions.sink.SinkFunction<Tuple2<String, Integer>> createPostgresSink(String jdbcUrl) {
        return JdbcSink.sink(
                "INSERT INTO test_event_aggregation (event_type, count) VALUES (?, ?)",
                new JdbcStatementBuilder<Tuple2<String, Integer>>() {
                    @Override
                    public void accept(PreparedStatement ps, Tuple2<String, Integer> value) throws SQLException {
                        ps.setString(1, value.f0);
                        ps.setInt(2, value.f1);
                    }
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(1000)
                        .build(),
                new org.apache.flink.connector.jdbc.JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .build()
        );
    }

}
