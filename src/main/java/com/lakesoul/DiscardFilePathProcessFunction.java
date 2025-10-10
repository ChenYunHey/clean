package com.lakesoul;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * KeyedProcessFunction:
 * - 输入: (filePath, timestamp)
 * - 按 filePath 分 key
 * - 逻辑:
 *   - 如果文件已过期，立即清理；
 *   - 如果未过期，为该文件注册定时器；
 *   - 当定时器触发时，执行文件与数据库的清理操作。
 */
public class DiscardFilePathProcessFunction
        extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

    private static final Logger log = LoggerFactory.getLogger(DiscardFilePathProcessFunction.class);

    String pgUrl;
    String pgUserName;
    String pgPasswd;
    Connection connection;

    private final long expiredTimestamp;

    // 用于保存每个文件的最后更新时间戳
    private transient ValueState<Long> lastUpdateTimestampState;

    public DiscardFilePathProcessFunction(String pgUrl, String pgUserName, String pgPasswd, long expiredTimestamp) throws SQLException {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
        this.expiredTimestamp = expiredTimestamp;


    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<Long> desc =
                new ValueStateDescriptor<>("lastUpdateTimestampState", Long.class);
        lastUpdateTimestampState = getRuntimeContext().getState(desc);

        connection = DriverManager.getConnection(pgUrl, pgUserName, pgPasswd);
    }

    @Override
    public void processElement(
            Tuple2<String, Long> value,
            Context ctx,
            Collector<String> out) throws Exception {

        String filePath = value.f0;
        long fileTimestamp = value.f1;
        long currentProcessingTime = ctx.timerService().currentProcessingTime();

        lastUpdateTimestampState.update(fileTimestamp);

        if (currentProcessingTime - fileTimestamp > expiredTimestamp) {
            // 已过期：立即清理
            log.info("文件 [{}] 已过期，立即清理。", filePath);
            cleanFileAndRecord(filePath);
            lastUpdateTimestampState.clear();
        } else {
            // 未过期：注册定时器
            long triggerTime = fileTimestamp + expiredTimestamp;
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
            log.info("文件 [{}] 未过期，注册定时器将在 {} 触发清理。", filePath, triggerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String filePath = ctx.getCurrentKey();
        Long fileTimestamp = lastUpdateTimestampState.value();

        if (fileTimestamp == null) {
            return; // 已被清理或状态丢失
        }

        long currentProcessingTime = ctx.timerService().currentProcessingTime();

        if (currentProcessingTime - fileTimestamp >= expiredTimestamp) {
            log.info("定时器触发，文件 [{}] 已过期，开始清理。", filePath);
            cleanFileAndRecord(filePath);
            lastUpdateTimestampState.clear();
        } else {
            log.debug("定时器触发时文件 [{}] 仍未过期，跳过清理。", filePath);
        }
    }

    /** 实际清理逻辑：删除文件 + 删除数据库记录 */
    private void cleanFileAndRecord(String filePath) {
        CleanUtils cleanUtils = new CleanUtils();
        try {
            cleanUtils.deleteFile(filePath);

            String sql = "DELETE FROM discard_compressed_file_info WHERE file_path = ?";
            try (PreparedStatement ps = connection.prepareStatement(sql)) {
                ps.setString(1, filePath);
                int rowsDeleted = ps.executeUpdate();
                log.info("清理文件 [{}] 成功，删除数据库记录 {} 行。", filePath, rowsDeleted);

            }
        } catch (SQLException e) {
            log.error("删除数据库记录失败，文件 [{}]", filePath, e);
        } catch (Exception e) {
            log.error("删除文件失败，文件 [{}]", filePath, e);
        }
    }
}
