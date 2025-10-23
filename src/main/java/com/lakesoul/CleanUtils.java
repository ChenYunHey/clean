package com.lakesoul;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.sql.*;
import java.util.*;

public class CleanUtils {

    private static final Logger logger = LoggerFactory.getLogger(CleanUtils.class);

    public void cleanPartitionInfo(String table_id, String partition_desc, int version, Connection connection) throws SQLException {
        String sql = "DELETE FROM partition_info where table_id= '" + table_id +
                "' and partition_desc ='" + partition_desc + "' and version = '" + version + "'";
        try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            preparedStatement.executeUpdate();
            logger.info(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            logger.info("删除partition_info数据异常");
        }
    }

    private static final String HDFS_URI_PREFIX = "hdfs:/";
    private static final String S3_URI_PREFIX = "S3:/";

    public void deleteFile(List<String> filePathList) throws SQLException {
        Configuration hdfsConfig = new Configuration();
        for (String filePath : filePathList) {
            if (filePath.startsWith(HDFS_URI_PREFIX)) {
                deleteHdfsFile(filePath, hdfsConfig);
            } else if (filePath.startsWith("file:/")) {
                try {
                    URI uri = new URI(filePath);
                    String actualPath = new File(uri.getPath()).getAbsolutePath();
                    deleteLocalFile(actualPath);
                } catch (URISyntaxException e) {
                    e.printStackTrace();
                    logger.info("无法解析文件URI: " + filePath);
                }
            } else {
                deleteLocalFile(filePath);
            }
        }
    }

    public void deleteFile(String filePath) throws SQLException {
        Configuration hdfsConfig = new Configuration();
        if (filePath.startsWith(HDFS_URI_PREFIX)) {
            deleteHdfsFile(filePath, hdfsConfig);
        } else if (filePath.startsWith("file:/")) {
            try {
                URI uri = new URI(filePath);
                String actualPath = new File(uri.getPath()).getAbsolutePath();
                deleteLocalFile(actualPath);
            } catch (URISyntaxException e) {
                e.printStackTrace();
                logger.info("无法解析文件URI: " + filePath);
            }
        } else {
            deleteLocalFile(filePath);
        }
    }

    private void deleteHdfsFile(String filePath, Configuration hdfsConfig) {
        try {
            FileSystem fs = FileSystem.get(URI.create(filePath), hdfsConfig);
            Path path = new Path(filePath);
            if (fs.exists(path)) {
                fs.delete(path, false); // false 表示不递归删除
                logger.info("=============================HDFS 文件已删除: " + filePath);
                deleteEmptyParentDirectories(fs, path.getParent());
                fs.close();
            } else {
                logger.info("=============================HDFS 文件不存在: " + filePath);
            }
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("=============================删除 HDFS 文件失败: " + filePath);
        }
    }

    private void deleteLocalFile(String filePath) {
        File file = new File(filePath);
        if (file.exists()) {
            if (file.delete()) {
                logger.info("本地文件已删除：" + filePath);
                deleteEmptyParentDirectories(file.getParentFile());
            } else {
                logger.info("本地文件删除失败: " + filePath);
            }
        } else {
            logger.info("=============================本地文件不存在: " + filePath);
        }
    }

    public boolean getCompactVersion(String tableId, String partitionDesc, long version, Connection connection) throws SQLException {

        String snapshotSql = "SELECT snapshot FROM partition_info " +
                "WHERE table_id = ? AND partition_desc = ? AND version = ?";

        List<UUID> snapshotCommitIds = new ArrayList<>();

        try (PreparedStatement ps = connection.prepareStatement(snapshotSql)) {
            ps.setString(1, tableId);
            ps.setString(2, partitionDesc);
            ps.setLong(3, version);

            try (ResultSet rs = ps.executeQuery()) {
                if (rs.next()) {
                    Array snapshotArray = rs.getArray("snapshot");
                    if (snapshotArray != null) {
                        UUID[] snapshot = (UUID[]) snapshotArray.getArray();
                        snapshotCommitIds.addAll(Arrays.asList(snapshot));
                    }
                }
            }
        }

        String fileSql = "SELECT unnest(file_ops) AS op FROM data_commit_info WHERE commit_id = ANY(?)";

        try (PreparedStatement ps = connection.prepareStatement(fileSql)) {
            Array uuidArray = connection.createArrayOf("uuid", snapshotCommitIds.toArray());
            ps.setArray(1, uuidArray);

            try (ResultSet rs = ps.executeQuery()) {
                boolean allCompact = true; // 假设全部都包含 "compact_"
                while (rs.next()){
                    Object op = rs.getObject("op");
                    String path = op.toString();
                    logger.info("当前压缩的文件目录：" + path);
                    logger.info("oldCompaction: " + path.contains("compact_"));
                    if (!path.contains("compact_")) {
                        allCompact = false;
                        break;
                    }
                }
                return allCompact;
            }
        }
    }

    public void deleteFileAndDataCommitInfo(List<String> snapshot, String tableId, String partitionDesc, Connection connection, Boolean oldCompaction) {
        snapshot.forEach(commitId -> {
            if (oldCompaction) {
                String sql = "SELECT \n" +
                        "    dci.table_id, \n" +
                        "    dci.partition_desc, \n" +
                        "    dci.commit_id, \n" +
                        "    file_op.path \n" +
                        "FROM \n" +
                        "    data_commit_info dci, \n" +
                        "    unnest(dci.file_ops) AS file_op \n" +
                        "WHERE \n" +
                        "    dci.table_id = '" + tableId + "' \n" +
                        "    AND dci.partition_desc = '" + partitionDesc + "' \n" +
                        "    AND dci.commit_id = '" + commitId + "'";
                try (PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
                    // 执行删除操作
                    ResultSet pathSet = preparedStatement.executeQuery();
                    logger.info(sql);
                    List<String> oldCompactionFileList = new ArrayList<>();
                    while (pathSet.next()) {
                        String path = pathSet.getString("path");
                        oldCompactionFileList.add(path);
                    }
                    if (!oldCompactionFileList.isEmpty()){
                        deleteFile(oldCompactionFileList);
                    }
                } catch (SQLException e) {
                    // 处理SQL异常
                    e.printStackTrace();

                }
            }
                    String deleteDataCommitInfoSql = "DELETE FROM data_commit_info \n" +
                            "WHERE table_id = '" + tableId + "' \n" +
                            "AND commit_id = '" + commitId + "' \n" +
                            "AND partition_desc = '" + partitionDesc + "'";
                    try (PreparedStatement preparedStatement = connection.prepareStatement(deleteDataCommitInfoSql)) {
                        logger.info(deleteDataCommitInfoSql);
                        preparedStatement.executeUpdate();
                    } catch (SQLException e) {
                        throw new RuntimeException(e);
                    }
                }
        );
    }

    private void deleteEmptyParentDirectories(FileSystem fs, Path directory) throws IOException {
        if (directory == null) {
            return;
        }
        // 检查目录是否为空
        if (fs.listStatus(directory).length == 0) {
            fs.delete(directory, false); // 删除空目录
            deleteEmptyParentDirectories(fs, directory.getParent());
        }
    }

    private void deleteEmptyParentDirectories(File directory) {
        if (directory == null) {
            return; // 根目录不需要处理
        }
        // 检查目录是否为空
        if (Objects.requireNonNull(directory.list()).length == 0) {
            if (directory.delete()) {
                deleteEmptyParentDirectories(directory.getParentFile());
            }
        }
    }

    public List<String> getTableIdByTableName(String tableNames, Connection connection) throws SQLException {
        if (tableNames == null) {
            return null;
        }
        List<String> tableList = new ArrayList<>();
        for (String table : tableNames.split(",")) {
            String dbName = table.split("\\.")[0];
            String tableName = table.split("\\.")[1];
            String sql = "select table_id from table_name_id where table_name = ? and table_namespace = ?";
            PreparedStatement preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setString(1,tableName);
            preparedStatement.setString(2, dbName);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()){
                String tableId = resultSet.getString("table_id");
                tableList.add(tableId);
            }

        }
        connection.close();
        return tableList;

    }

}
