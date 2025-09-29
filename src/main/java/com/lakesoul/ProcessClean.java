package com.lakesoul;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import com.lakesoul.PartitionInfoRecordGets.PartitionInfo;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public  class ProcessClean extends KeyedProcessFunction<String, PartitionInfo, String> {
    private transient MapState<String, PartitionInfo.WillStateValue> willState;
    private transient ValueState<Boolean> timerInitializedState;
    private transient MapState<String, Long> compactNewState;
    private transient ValueState<Boolean> compactionVersionState;

    private final String pgUrl;
    private final String userName;
    private final String password;
    private final int expiredTime;
    private final int ontimerInterval;

    private transient Connection pgConnection;

    public ProcessClean(String pgUrl, String userName, String password, int expiredTime, int ontimerInterval) {
        this.pgUrl = pgUrl;
        this.userName = userName;
        this.password = password;
        this.expiredTime = expiredTime;
        this.ontimerInterval = ontimerInterval;
    }

    @Override
    public void open(Configuration parameters) throws SQLException, ClassNotFoundException {
        // 初始化状态变量
        MapStateDescriptor<String, PartitionInfo.WillStateValue> willStateDesc =
                new MapStateDescriptor<>("willStateDesc", String.class, PartitionInfo.WillStateValue.class);
        willState = getRuntimeContext().getMapState(willStateDesc);

        MapStateDescriptor<String, Long> compactNewStateDesc =
                new MapStateDescriptor<>("newCompactDesc", String.class, Long.class);
        compactNewState = getRuntimeContext().getMapState(compactNewStateDesc);

        ValueStateDescriptor<Boolean> initDesc =
                new ValueStateDescriptor<>("timerInit", Boolean.class, false);
        timerInitializedState = getRuntimeContext().getState(initDesc);

        ValueStateDescriptor<Boolean> compactVersionDesc =
                new ValueStateDescriptor<>("compactVersion", Boolean.class, true);
        compactionVersionState = getRuntimeContext().getState(compactVersionDesc);

        //PGConnectionPool.init(pgUrl, userName, password);
        //pgConnection = PGConnectionPool.getConnection();
        Class.forName("org.postgresql.Driver");
        pgConnection = DriverManager.getConnection(pgUrl,userName,password);


    }

    @Override
    public void processElement(PartitionInfo value, KeyedProcessFunction<String, PartitionInfo, String>.Context ctx, Collector<String> out) throws Exception {
        String tableId = value.table_id;
        String partitionDesc = value.partition_desc;
        String commitOp = value.commit_op;
        long timestamp = value.timestamp;
        int version = value.version;
        List<String> snapshot = value.snapshot;


    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        long recordTimestamp = timestamp - ontimerInterval;
        String currentKey = ctx.getCurrentKey();
        if (compactNewState.contains(currentKey)){
            Long compactionTimestamp = compactNewState.get(currentKey);
            if (recordTimestamp - compactionTimestamp > expiredTime){
                String tableId = currentKey.split("/")[0];
                String partionDesc = currentKey.split("/")[1];
                //doclean(tableId,partionDesc,recordTimestamp);
            }
        } else {
            //ctx.timerService().registerProcessingTimeTimer();
        }
    }

    @Override
    public void close() throws Exception {
        if (pgConnection != null && !pgConnection.isClosed()) {
            pgConnection.close();
            System.out.println("✅ PostgreSQL connection returned to pool.");
        }
    }
}
