package com.lakesoul;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDeclaration;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import com.lakesoul.PartitionInfoRecordGets.PartitionInfo;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

public class CompactProcessFunction extends KeyedProcessFunction<String, PartitionInfo, CompactProcessFunction.CompactionOut> {

    private transient Connection pgConnection;
    private final String pgUrl;
    private final String userName;
    private final String password;
    private long switchVersion = 0;
    private transient ValueState<Long> switchCompactionVersionState;

    public CompactProcessFunction(String pgUrl, String userName, String password) {
        this.pgUrl = pgUrl;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        Class.forName("org.postgresql.Driver");
        pgConnection = DriverManager.getConnection(pgUrl,userName,password);
        ValueStateDescriptor<Long> switchCompactionVersionDesc = new ValueStateDescriptor<>("switchCompactionVersion",
                Long.class,
                -1L);

        switchCompactionVersionState = getRuntimeContext().getState(switchCompactionVersionDesc);

    }

    @Override
    public void processElement(PartitionInfo value, KeyedProcessFunction<String, PartitionInfo, CompactionOut>.Context ctx, Collector<CompactionOut> out) throws Exception {
        String partitionDesc = value.partition_desc;
        String tableId = value.table_id;
        long timestamp = value.timestamp;
        long version = value.version;
        String commitOp = value.commit_op;
        List<String> snapshot = value.snapshot;
        CleanUtils cleanUtils = new CleanUtils();
        if (snapshot.size() == 1) {

            boolean isOldCompaction = commitOp.equals("UpdateCommit")
                    || cleanUtils.getCompactVersion(tableId, partitionDesc, version, pgConnection);
            Long current = switchCompactionVersionState.value();
            if (current == null) {
                current = -1L;
            }
            if (isOldCompaction && version > current) {
                switchCompactionVersionState.update(version);
                current = version;

            }
            out.collect(new CompactionOut(tableId, partitionDesc, version, timestamp, isOldCompaction, switchCompactionVersionState.value()));

            System.out.println("current version:" + switchCompactionVersionState.value());

        }
    }

    public static class CompactionOut{
        String tableId;
        String partitionDesc;
        long version;
        long timestamp;
        boolean isOldCompaction;
        long switchVersion;

        public CompactionOut(String tableId, String partitionDesc, long version, long timestamp, boolean isOldCompaction, long switchTime) {
            this.tableId = tableId;
            this.partitionDesc = partitionDesc;
            this.version = version;
            this.timestamp = timestamp;
            this.isOldCompaction = isOldCompaction;
            this.switchVersion = switchTime;
        }

        public String getTableId() {
            return tableId;
        }

        public String getPartitionDesc() {
            return partitionDesc;
        }

        public long getVersion() {
            return version;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public boolean isOldCompaction() {
            return isOldCompaction;
        }

        public void setSwitchVersion(long switchVersion) {
            this.switchVersion = switchVersion;
        }

        @Override
        public String toString() {
            return "CompactionOut{" +
                    "tableId='" + tableId + '\'' +
                    ", partitionDesc='" + partitionDesc + '\'' +
                    ", version=" + version +
                    ", timestamp=" + timestamp +
                    ", isOldCompaction=" + isOldCompaction +
                    ", switchVersion=" + switchVersion +
                    '}';
        }
    }

}