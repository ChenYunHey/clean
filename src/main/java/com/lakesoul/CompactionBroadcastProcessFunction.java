package com.lakesoul;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;

public class CompactionBroadcastProcessFunction extends KeyedBroadcastProcessFunction<
        String,  // 主流 key 类型
        PartitionInfoRecordGets.PartitionInfo,  // 主流元素类型
        CompactProcessFunction.CompactionOut, // 广播流元素类型
        PartitionInfoRecordGets.PartitionInfo> {
    private static final Logger log = LoggerFactory.getLogger(CompactionBroadcastProcessFunction.class); // 输出类型

    private final MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc;
    private transient ValueState<PartitionInfoRecordGets.PartitionInfo> elementState;

    private final String pgUrl;
    private final String pgUserName;
    private final String pgPasswd;
    private final int expiredTime;
    private final long ontimerInterval;
    //private transient Connection pgConnection;
    private static CleanUtils cleanUtils;
    private transient DataSource dataSource;


    public CompactionBroadcastProcessFunction(MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc, String pgUrl, String pgUserName, String pgPasswd, int expiredTime, long ontimerInterval) {
        this.broadcastStateDesc = broadcastStateDesc;
        this.pgUrl = pgUrl;
        this.pgUserName = pgUserName;
        this.pgPasswd = pgPasswd;
        this.expiredTime = expiredTime;
        this.ontimerInterval = ontimerInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        ValueStateDescriptor<PartitionInfoRecordGets.PartitionInfo> desc =
                new ValueStateDescriptor<>(
                        "elementState",
                        TypeInformation.of(new TypeHint<PartitionInfoRecordGets.PartitionInfo>() {}));
        elementState = getRuntimeContext().getState(desc);
        cleanUtils = new CleanUtils();
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
    public void processBroadcastElement(
            CompactProcessFunction.CompactionOut value,
            Context ctx,
            Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {

        BroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);

        String key = value.getTableId() + "/" + value.getPartitionDesc();

        CompactProcessFunction.CompactionOut current = state.get(key);

        if (current == null) {
            // 首次写入
            state.put(key, value);
        } else {
            // 计算最大的 switchVersion
            long maxSwitchVersion = Math.max(current.switchVersion, value.switchVersion);

            if (value.getTimestamp() > current.getTimestamp()) {
                // 如果新来的 timestamp 更大 → 用新值覆盖，但 switchVersion 保留最大
                state.put(key, new CompactProcessFunction.CompactionOut(
                        value.getTableId(),
                        value.getPartitionDesc(),
                        value.getVersion(),
                        value.getTimestamp(),
                        value.isOldCompaction(),
                        maxSwitchVersion
                ));
            } else {
                // timestamp 旧 → 保留 current，但更新 switchVersion 为最大
                if (maxSwitchVersion > current.switchVersion) {
                    state.put(key, new CompactProcessFunction.CompactionOut(
                            current.getTableId(),
                            current.getPartitionDesc(),
                            current.getVersion(),
                            current.getTimestamp(),
                            current.isOldCompaction(),
                            maxSwitchVersion
                    ));
                }
            }
        }
    }


    @Override
    public void processElement(
            PartitionInfoRecordGets.PartitionInfo value,
            ReadOnlyContext ctx,
            Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {

        elementState.update(value);
        ReadOnlyBroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);
        String key = value.table_id + "/" + value.partition_desc;
        long valueTimestamp = value.timestamp;
        CompactProcessFunction.CompactionOut compaction = state.get(key);
        if (compaction != null) {
            // enrich 主流数据
            long compactTimstamp = compaction.timestamp;
            log.info("当前时间差为：" + (valueTimestamp - compactTimstamp));
            long currTimestamp = System.currentTimeMillis();
            if (valueTimestamp < compactTimstamp && currTimestamp - valueTimestamp > expiredTime){
                log.info("执行version为"+ value.version +"的删除操作");
                CleanUtils cleanUtils = new CleanUtils();
                boolean latestCompactVersionIsOld = state.get(key).isOldCompaction();
                boolean belongOldCompaction = value.version < state.get(key).switchVersion || latestCompactVersionIsOld;
                try (Connection connection = dataSource.getConnection()) {
                    cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.table_id, value.partition_desc, connection, belongOldCompaction);
                    cleanUtils.cleanPartitionInfo(value.table_id, value.partition_desc, value.version, connection);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                log.info("version: " + value.version + " 执行旧版清理： " + belongOldCompaction);
                elementState.clear();
            } else {
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                log.info("version: " + value.version + "注册定时器，等待执行");
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
            }
        } else {
            log.info("version :" + value.version +"没有过期，再次注册定时器");
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx,
                        Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {

        String currentKey = ctx.getCurrentKey();
        String[] split = currentKey.split("/");
        String tableId = split[0];
        String partitionDesc = split[1];
        ReadOnlyBroadcastState<String, CompactProcessFunction.CompactionOut> state =
                ctx.getBroadcastState(broadcastStateDesc);
        String key = tableId + "/" + partitionDesc;
        CompactProcessFunction.CompactionOut compactionOut = state.get(key);
        PartitionInfoRecordGets.PartitionInfo value = elementState.value();

        if (compactionOut != null) {
            log.info("当前新旧压缩切换版本为：" + state.get(key).switchVersion);
            long currTimestamp = value.timestamp;
            long compactTimestamp = compactionOut.timestamp;
            if (currTimestamp < compactTimestamp && timestamp - currTimestamp > expiredTime){
                //TODO
                boolean latestCompactVersionIsOld = state.get(key).isOldCompaction();
                boolean belongOldCompaction = value.version < state.get(key).switchVersion || latestCompactVersionIsOld;
                log.info("version: " + value.version + " 执行旧版清理： " + belongOldCompaction);
                try (Connection connection = dataSource.getConnection()) {
                    cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.table_id, value.partition_desc, connection, belongOldCompaction);
                    cleanUtils.cleanPartitionInfo(value.table_id, value.partition_desc, value.version, connection);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                elementState.clear();
            } else {
                log.info("version: " + value.version + "再次注册定时器，等待执行");
                ctx.timerService().registerProcessingTimeTimer(timestamp + ontimerInterval);
            }
        } else {
            log.info("version: " + value.version + "再次注册定时器，等待执行");
            ctx.timerService().registerProcessingTimeTimer(timestamp + ontimerInterval);
        }

    }
}

