package com.lakesoul;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 *使用 HikariCP 连接池，解决 Connection 被关闭的问题
 */
public class DiscardFilePathProcessFunction
        extends KeyedProcessFunction<String, Tuple2<String, Long>, String> {

    private static final Logger log = LoggerFactory.getLogger(DiscardFilePathProcessFunction.class);

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;
    private final long expiredTimestamp;

    private transient ValueState<Long> lastUpdateTimestampState;
    private transient DataSource dataSource; 

    public DiscardFilePathProcessFunction(String pgUrl, String pgUserName, String pgPasswd, long expiredTimestamp) {
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
        this.expiredTimestamp = expiredTimestamp;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        // 初始化状态
        ValueStateDescriptor<Long> desc =
                new ValueStateDescriptor<>("lastUpdateTimestampState", Long.class);
        lastUpdateTimestampState = getRuntimeContext().getState(desc);

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(pgUrl);
        config.setUsername(pgUserName);
        config.setPassword(pgPasswd);
        config.setDriverClassName("org.postgresql.Driver");

        config.setMaximumPoolSize(5);            // 每个 TM 的最大连接数
        config.setMinimumIdle(1);
        config.setConnectionTimeout(10000);      // 10 秒超时
        config.setIdleTimeout(60000);            // 1 分钟空闲回收
        config.setMaxLifetime(300000);           // 5 分钟重建连接
        config.setAutoCommit(true);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        dataSource = new HikariDataSource(config);
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
            log.info("文件 [{}] 已过期，立即清理。", filePath);
            cleanFileAndRecord(filePath);
            lastUpdateTimestampState.clear();
        } else {
            long triggerTime = fileTimestamp + expiredTimestamp;
            ctx.timerService().registerProcessingTimeTimer(triggerTime);
            log.info("文件 [{}] 未过期，注册定时器将在 {} 触发清理。", filePath, triggerTime);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        String filePath = ctx.getCurrentKey();
        Long fileTimestamp = lastUpdateTimestampState.value();
        if (fileTimestamp == null) return;

        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        if (currentProcessingTime - fileTimestamp >= expiredTimestamp) {
            log.info("定时器触发，文件 [{}] 已过期，开始清理。", filePath);
            cleanFileAndRecord(filePath);
            lastUpdateTimestampState.clear();
        } else {
            log.debug("定时器触发时文件 [{}] 仍未过期，跳过清理。", filePath);
        }
    }

    private void cleanFileAndRecord(String filePath) {
        CleanUtils cleanUtils = new CleanUtils();
        try {
            cleanUtils.deleteFile(filePath);

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement ps = connection.prepareStatement("DELETE FROM discard_compressed_file_info WHERE file_path = ?")) {
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

    @Override
    public void close() throws Exception {
        super.close();
        if (dataSource instanceof HikariDataSource) {
            ((HikariDataSource) dataSource).close();
            log.info("HikariCP 连接池已关闭。");
        }
    }
}
