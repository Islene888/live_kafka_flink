package data.Load_Util;

import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.core.fs.Path;

import java.util.concurrent.TimeUnit;

public class S3SinkUtil {
    public static StreamingFileSink<String> createS3Sink(String s3BucketPath) {
        return StreamingFileSink
                .forRowFormat(new Path(s3BucketPath), new org.apache.flink.api.common.serialization.SimpleStringEncoder<String>("UTF-8"))
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(TimeUnit.SECONDS.toMillis(1))  // 15 分钟生成新文件
                                .withInactivityInterval(TimeUnit.SECONDS.toMillis(1)) // 5分钟无写入生成新文件
                                .withMaxPartSize(128 * 1024 * 1024)                  // 128MB 文件切片
                                .build())
                .build();
    }
}
