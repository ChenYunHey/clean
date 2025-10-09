package com.lakesoul;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;

import java.util.HashMap;

public class DiscardPathMapFunction implements MapFunction<String, HashMap<String, Long>> {
    @Override
    public HashMap<String, Long> map(String value) throws Exception {
        JSONObject parse = (JSONObject) JSONObject.parse(value);
        String PGtableName = parse.get("tableName").toString();
        JSONObject commitJson = null;
        HashMap<String, Long> discardFileMap = new HashMap<>();

        if (PGtableName.equals("partition_info")){
            if (!parse.getJSONObject("after").isEmpty()){
                commitJson = (JSONObject) parse.get("after");
            } else {
                commitJson = (JSONObject) parse.get("before");
            }
        }
        String eventOp = parse.getString("commitOp");
        if (!eventOp.equals("delete")){
            String filePath = commitJson.getString("file_path");
            long timestamp = commitJson.getLong("timestamp");
            discardFileMap.put(filePath, timestamp);
            return discardFileMap;
        }
        return null;
    }
}
