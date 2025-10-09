package com.lakesoul;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class DiscardFilePathProcessFunction extends ProcessFunction<Tuple2<String, Long>,String> {

    private static final Logger log = LoggerFactory.getLogger(DiscardFilePathProcessFunction.class);
    Connection connection;
    Long expiredTimestamp;

    public DiscardFilePathProcessFunction(Connection connection, Long expiredTimestamp) {
        this.connection = connection;
        this.expiredTimestamp = expiredTimestamp;
    }

    @Override
    public void processElement(Tuple2<String, Long> value, ProcessFunction<Tuple2<String, Long>, String>.Context ctx, Collector<String> out) throws Exception {
        CleanUtils cleanUtils = new CleanUtils();
        long currentProcessingTime = ctx.timerService().currentProcessingTime();
        if (currentProcessingTime - value.f1 > expiredTimestamp){
            String filePath = value.f0;
            cleanUtils.deleteFile(filePath);
            String cleanDiscardSql = "DELETE FROM discard_compressed_file_info WHERE table_path = " + filePath;
            try (PreparedStatement preparedStatement = connection.prepareStatement(cleanDiscardSql)) {
                log.info("删除discard_compressed_file_info 表数据");
                int rowsDeleted = preparedStatement.executeUpdate();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public void open(OpenContext openContext) throws Exception {
        super.open(openContext);
    }
}
