package com.lakesoul;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lakesoul.utils.SourceOptions;
import com.ververica.cdc.connectors.base.options.StartupOptions;
import com.ververica.cdc.connectors.base.source.jdbc.JdbcIncrementalSource;
import com.ververica.cdc.connectors.postgres.source.PostgresSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

public class Clean {
    private static final Logger log = LoggerFactory.getLogger(Clean.class);
    public static int expiredTime;
    public static String host;
    private static String dbName;
    public static String userName;
    public static String passWord;
    public static int port;
    private static int splitSize;
    private static String slotName;
    private static String pluginName;
    private static String schemaList;
    public static String pgUrl;
    private static int sourceParallelism;
    private static CleanUtils cleanUtils;
    private static String targetTables;
    private static String startMode;

    public static void main(String[] args) throws Exception {
        ParameterTool parameter = ParameterTool.fromArgs(args);
        PgDeserialization deserialization = new PgDeserialization();
        Properties debeziumProperties = new Properties();
        debeziumProperties.setProperty("include.unknown.datatypes", "true");
        String[] tableList = new String[]{"public.partition_info", "public.discard_compressed_file_info"};
        //String[] tableList = new String[]{ "public.discard_compressed_file_info"};

        userName = parameter.get(SourceOptions.SOURCE_DB_USER.key());
        dbName = parameter.get(SourceOptions.SOURCE_DB_DB_NAME.key());
        passWord = parameter.get(SourceOptions.SOURCE_DB_PASSWORD.key());
        host = parameter.get(SourceOptions.SOURCE_DB_HOST.key());
        port = parameter.getInt(SourceOptions.SOURCE_DB_PORT.key(), SourceOptions.SOURCE_DB_PORT.defaultValue());
        slotName = parameter.get(SourceOptions.SLOT_NAME.key());
        pluginName = parameter.get(SourceOptions.PLUG_NAME.key());
        splitSize = parameter.getInt(SourceOptions.SPLIT_SIZE.key(), SourceOptions.SPLIT_SIZE.defaultValue());
        schemaList = parameter.get(SourceOptions.SCHEMA_LIST.key());
        startMode = parameter.get(SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.key(), (String)SourceOptions.STARTUP_OPTIONS_CONFIG_OPTION.defaultValue());
        pgUrl = parameter.get(SourceOptions.PG_URL.key());
        sourceParallelism = parameter.getInt(SourceOptions.SOURCE_PARALLELISM.key(), SourceOptions.SOURCE_PARALLELISM.defaultValue());
        targetTables = parameter.get(SourceOptions.TARGET_TABLES.key(),null);
        StartupOptions startupOptions = StartupOptions.initial();
        if (startMode.equals("latest")) {
            startupOptions = StartupOptions.latest();
        } else if (startMode.equals("earliest")) {
            startupOptions = StartupOptions.earliest();
        }

        //int ontimerInterval = 60000;
        int ontimerInterval = parameter.getInt(SourceOptions.ONTIMER_INTERVAL.key(), 10) * 60000;
        //expiredTime = 60000;
        expiredTime = parameter.getInt(SourceOptions.DATA_EXPIRED_TIME.key(),120000) ;
        if (expiredTime < 10){
            expiredTime = expiredTime * 86400000;
        }
        JdbcIncrementalSource<String> postgresIncrementalSource =
                PostgresSourceBuilder.PostgresIncrementalSource.<String>builder()
                        .hostname(host)
                        .port(port)
                        .database(dbName)
                        .schemaList(schemaList)
                        .tableList(tableList)
                        .startupOptions(startupOptions)
                        .username(userName)
                        .password(passWord)
                        .slotName(slotName)
                        .decodingPluginName(pluginName) // use pgoutput for PostgreSQL 10+
                        .deserializer(deserialization)
                        .splitSize(splitSize) // the split size of each snapshot split
                        .debeziumProperties(debeziumProperties)
                        .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> postgresParallelSource = env.fromSource(
                        postgresIncrementalSource,
                        WatermarkStrategy.noWatermarks(),
                        "PostgresParallelSource")
                .setParallelism(sourceParallelism);
        final OutputTag<String> partitionInfoTag = new OutputTag<String>("partition_info") {};
        final OutputTag<String> discardFileInfoTag = new OutputTag<String>("discard_compressed_file_info") {};
        SingleOutputStreamOperator<String> mainStream = postgresParallelSource.process(
                new ProcessFunction<String, String>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<String> out) throws Exception {
                        try {
                            JSONObject json = JSON.parseObject(value);
                            String tableName = json.getString("tableName");
                            if ("partition_info".equals(tableName)) {
                                ctx.output(partitionInfoTag, value);
                            } else if ("discard_compressed_file_info".equals(tableName)) {
                                ctx.output(discardFileInfoTag, value);
                            }
                        } catch (Exception e) {
                            System.err.println("JSON parse error: " + e.getMessage());
                        }
                    }
                }
        );
        SideOutputDataStream<String> partitionInfoStream = mainStream.getSideOutput(partitionInfoTag);
        SideOutputDataStream<String> discardFileInfoStream = mainStream.getSideOutput(discardFileInfoTag);

        //discardFileInfoStream.map(new DiscardPathMapFunction()).filter(Objects::nonNull).keyBy(value -> value.f0).process(new DiscardFilePathProcessFunction(pgUrl,userName,passWord,expiredTime));

        CleanUtils utils = new CleanUtils();
        final OutputTag<PartitionInfoRecordGets.PartitionInfo> compactionCommitTag =
                new OutputTag<>("compactionCommit"){};
        Connection connection = DriverManager.getConnection(pgUrl, userName, passWord);
        List<String> tableIdList = utils.getTableIdByTableName(targetTables, connection);
        SingleOutputStreamOperator<PartitionInfoRecordGets.PartitionInfo> mainStreaming = partitionInfoStream.map(new PartitionInfoRecordGets.metaMapper(tableIdList))
                .filter(Objects::nonNull).process(new ProcessFunction<PartitionInfoRecordGets.PartitionInfo, PartitionInfoRecordGets.PartitionInfo>() {
                    @Override
                    public void processElement(PartitionInfoRecordGets.PartitionInfo value,
                                               ProcessFunction<PartitionInfoRecordGets.PartitionInfo,
                                                       PartitionInfoRecordGets.PartitionInfo>.Context ctx,
                                               Collector<PartitionInfoRecordGets.PartitionInfo> out) throws Exception {
                        if (value.commit_op.equals("CompactionCommit") || value.commit_op.equals("UpdateCommit")){
                            ctx.output(compactionCommitTag,value);
                        }
                        out.collect(value);

                    }
                });
        SideOutputDataStream<PartitionInfoRecordGets.PartitionInfo> compactStreaming = mainStreaming.getSideOutput(compactionCommitTag);
        KeyedStream<PartitionInfoRecordGets.PartitionInfo, String> partitionInfoStringKeyedStream = mainStreaming.keyBy(value -> value.table_id + "/" + value.partition_desc + "/" + value.version);
        SingleOutputStreamOperator<CompactProcessFunction.CompactionOut> compactiomStreaming = compactStreaming
                .keyBy(value -> value.table_id + "/" + value.partition_desc)
                .process(new CompactProcessFunction(pgUrl, userName, passWord));


        MapStateDescriptor<String, CompactProcessFunction.CompactionOut> broadcastStateDesc =
                new MapStateDescriptor<>(
                        "compactionBroadcastState",
                        BasicTypeInfo.STRING_TYPE_INFO,
                        TypeInformation.of(new TypeHint<CompactProcessFunction.CompactionOut>() {}));

        BroadcastStream<CompactProcessFunction.CompactionOut> broadcastStream =
                compactiomStreaming.broadcast(broadcastStateDesc);

        BroadcastConnectedStream<PartitionInfoRecordGets.PartitionInfo, CompactProcessFunction.CompactionOut> connectedStream =
                partitionInfoStringKeyedStream.connect(broadcastStream);

        connectedStream.process(new CompactionBroadcastProcessFunction(broadcastStateDesc, pgUrl, userName, passWord, expiredTime, ontimerInterval));

        discardFileInfoStream
                .map(new DiscardPathMapFunction())
                .filter(value -> !value.f0.equals("delete"))
                .keyBy(value -> value.f0)
                .process(new DiscardFilePathProcessFunction(pgUrl,userName,passWord,expiredTime))
                .name("处理新版过期数据");
        //discardFileInfoStream.map(new DiscardPathMapFunction()).filter(value -> !value.f0.equals("delete")).print();
        env.execute();


    }
}
