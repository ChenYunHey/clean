package com.lakesoul;

import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;

public class CompactionBroadcastProcessFunction extends KeyedBroadcastProcessFunction<
        String,  // 主流 key 类型
        PartitionInfoRecordGets.PartitionInfo,  // 主流元素类型
        CompactProcessFunction.CompactionOut, // 广播流元素类型
        PartitionInfoRecordGets.PartitionInfo> { // 输出类型

    private final MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc;
    private transient ValueState<PartitionInfoRecordGets.PartitionInfo> elementState;

    private final String pgUrl;
    private final String userName;
    private final String password;
    private final int expiredTime;
    private final long ontimerInterval;
    private transient Connection pgConnection;
    private static CleanUtils cleanUtils;

    public CompactionBroadcastProcessFunction(MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc, String pgUrl, String userName, String password, int expiredTime, long ontimerInterval) {
        this.broadcastStateDesc = broadcastStateDesc;
        this.pgUrl = pgUrl;
        this.userName = userName;
        this.password = password;
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

        pgConnection = DriverManager.getConnection(pgUrl,userName,password);
        cleanUtils = new CleanUtils();
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
        //out.collect(new CompactionOut(tableId, partitionDesc, version, timestamp, isOldCompaction, switchCompactionVersionState.value().version));
        if (compaction != null) {
            // enrich 主流数据
            long compactTimstamp = compaction.timestamp;
            System.out.println("当前时间差为：" + (valueTimestamp - compactTimstamp));
            long currTimestamp = System.currentTimeMillis();
            if (valueTimestamp < compactTimstamp && currTimestamp - valueTimestamp > expiredTime){
                //TODO
                System.out.println("执行删除操作");
                CleanUtils cleanUtils = new CleanUtils();
                boolean latestCompactVersionIsOld = state.get(key).isOldCompaction();
                boolean belongOldCompaction = value.version < state.get(key).switchVersion || latestCompactVersionIsOld;
                cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.table_id, value.partition_desc, pgConnection, belongOldCompaction);
                cleanUtils.cleanPartitionInfo(value.table_id, value.partition_desc, value.version, pgConnection);
                System.out.println("version: " + value.version + " 执行旧版清理： " + belongOldCompaction);
                if (value.version < 4 && !belongOldCompaction) {
                    System.out.println(state.get(key).switchVersion + "qqqqqqqqqqqqqqqqqq");
                }
                elementState.clear();
            } else {
                System.out.println("2：触发定时器");
                long currentProcessingTime = ctx.timerService().currentProcessingTime();
                System.out.println("version: " + value.version + "再次注册定时器，等待执行");
                ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
            }
        } else {
            System.out.println("1：注册定时器");
            long currentProcessingTime = ctx.timerService().currentProcessingTime();
            System.out.println(currentProcessingTime +  ontimerInterval);
            //ctx.timerService().registerEventTimeTimer(currentProcessingTime +  ontimerInterval);
            ctx.timerService().registerProcessingTimeTimer(currentProcessingTime + ontimerInterval);
        }
        //out.collect(value);

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
        System.out.println(compactionOut+"-----------------");
        PartitionInfoRecordGets.PartitionInfo value = elementState.value();

        if (compactionOut != null) {
            System.out.println("当前新旧压缩切换版本为：" + state.get(key).switchVersion);
            long currTimestamp = value.timestamp;
            long compactTimestamp = compactionOut.timestamp;
            if (currTimestamp < compactTimestamp && timestamp - currTimestamp > expiredTime){
                //TODO
                boolean latestCompactVersionIsOld = state.get(key).isOldCompaction();
                boolean belongOldCompaction = value.version < state.get(key).switchVersion || latestCompactVersionIsOld;
                System.out.println("version: " + value.version + " 执行旧版清理： " + belongOldCompaction);
                cleanUtils.deleteFileAndDataCommitInfo(value.snapshot, value.table_id, value.partition_desc, pgConnection, belongOldCompaction);
                cleanUtils.cleanPartitionInfo(value.table_id, value.partition_desc, value.version, pgConnection);
                System.out.println("执行清理工作");
                elementState.clear();
            } else {
                System.out.println("2：触发定时器");
                System.out.println("version: " + value.version + "再次注册定时器，等待执行");
                ctx.timerService().registerProcessingTimeTimer(timestamp + ontimerInterval);
            }
        } else {
            System.out.println("3：触发定时器");
            ctx.timerService().registerProcessingTimeTimer(timestamp + ontimerInterval);
        }

    }
}

