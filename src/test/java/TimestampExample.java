import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class TimestampExample {
    public static void main(String[] args) {
        long timestamp = 1760496940186L; // 毫秒级时间戳

        // 转换为 LocalDateTime（按系统默认时区）
        LocalDateTime dateTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(timestamp),
                ZoneId.systemDefault()
        );

        // 格式化输出
        String formatted = dateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        System.out.println(formatted);
    }
}
