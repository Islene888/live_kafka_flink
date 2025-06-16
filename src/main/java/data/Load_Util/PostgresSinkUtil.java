package data.Load_Util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresSinkUtil {

    public static SinkFunction<Tuple2<String, Integer>> createPostgresSink(String jdbcUrl) {
        String insertSql = "INSERT INTO event_counts (event_type, count) VALUES (?, ?) " +
                "ON CONFLICT (event_type) DO UPDATE SET count = event_counts.count + EXCLUDED.count";

        JdbcStatementBuilder<Tuple2<String, Integer>> statementBuilder = new JdbcStatementBuilder<Tuple2<String, Integer>>() {
            @Override
            public void accept(PreparedStatement ps, Tuple2<String, Integer> record) throws SQLException {
                ps.setString(1, record.f0);
                ps.setInt(2, record.f1);
            }
        };

        return JdbcSink.sink(
                insertSql,
                statementBuilder,
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(jdbcUrl)
                        .withDriverName("org.postgresql.Driver")
                        .build()
        );
    }
}
