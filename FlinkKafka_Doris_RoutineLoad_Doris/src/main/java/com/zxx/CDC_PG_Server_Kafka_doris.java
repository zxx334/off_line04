package com.zxx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.sql.*;
import java.util.concurrent.TimeUnit;

public class CDC_PG_Server_Kafka_doris {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 使用触发器方案的 CDC 源
        DataStreamSource<String> source = env.addSource(new PGTriggerCDCSource());

        // 添加数据格式转换和监控
        SingleOutputStreamOperator<String> transformedStream = source
                .map(new MapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        System.out.println("=== Flink CDC 数据流监控 ===");
                        System.out.println("原始变更数据: " + value);
                        System.out.println("处理时间: " + new java.util.Date());

                        // 数据格式转换逻辑
                        ObjectMapper mapper = new ObjectMapper();
                        JsonNode original = mapper.readTree(value);
                        JsonNode newData = original.get("new_data");

                        String result;
                        if (newData != null && !newData.isNull() && newData.isObject()) {
                            // 提取 new_data 字段并添加元数据
                            ObjectNode dorisJson = mapper.createObjectNode();
                            dorisJson.setAll((ObjectNode) newData);
                            dorisJson.put("source_db", "postgresql");
                            dorisJson.put("cdc_time", new java.util.Date().toString());
                            dorisJson.put("original_change_id", original.get("change_id").asLong());
                            dorisJson.put("original_operation", original.get("operation").asText());
                            dorisJson.put("original_table_name", original.get("table_name").asText());

                            result = mapper.writeValueAsString(dorisJson);
                            System.out.println("转换后数据: " + result);
                        } else {
                            // 如果没有 new_data，保持原格式但添加标记
                            ObjectNode modifiedOriginal = (ObjectNode) original.deepCopy();
                            modifiedOriginal.put("source_db", "postgresql");
                            modifiedOriginal.put("cdc_time", new java.util.Date().toString());
                            modifiedOriginal.put("data_format", "original");

                            result = mapper.writeValueAsString(modifiedOriginal);
                            System.out.println("无new_data，使用原格式: " + result);
                        }

                        System.out.println("=== 监控结束 ===\n");
                        return result;
                    }
                })
                .name("data-transformer"); // 给算子命名便于在Flink UI中识别

        // 发送到 Kafka
        transformedStream.addSink(new FlinkKafkaProducer<>(
                "cdh03:9092",
                "cdc-data-topic",
                new SimpleStringSchema()
        )).name("kafka-sink");

        env.execute("Flink PostgreSQL Trigger CDC to Kafka");
        System.out.println("作业启动成功！");
    }

    /**
     * PostgreSQL 触发器 CDC 源
     */
    public static class PGTriggerCDCSource extends RichSourceFunction<String> {

        private volatile boolean isRunning = true;
        private Connection connection;
        private PreparedStatement statement;
        private long pollInterval = 3000; // 3秒轮询间隔

        // PostgreSQL 连接配置
        private final String url = "jdbc:postgresql://cdh03:5432/test_db";
        private final String user = "postgres";
        private final String password = "123456";

        private long lastProcessedId = 0;
        private int recordCount = 0;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册驱动
            Class.forName("org.postgresql.Driver");
            // 建立连接
            connection = DriverManager.getConnection(url, user, password);
            // 准备查询语句 - 查询变更记录表
            statement = connection.prepareStatement(
                    "SELECT change_id, table_name, record_id, operation, old_data, new_data, change_time " +
                            "FROM users_changelog " +
                            "WHERE change_id > ? " +
                            "ORDER BY change_id"
            );

            // 初始化时输出连接信息
            System.out.println("=== PostgreSQL CDC 源初始化 ===");
            System.out.println("数据库连接: " + url);
            System.out.println("轮询间隔: " + pollInterval + "ms");
            System.out.println("初始最后处理ID: " + getLastProcessedId());
            System.out.println("=== 初始化完成 ===\n");
        }

        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            lastProcessedId = getLastProcessedId();

            System.out.println("开始轮询变更数据，起始ID: " + lastProcessedId);

            while (isRunning) {
                try {
                    statement.setLong(1, lastProcessedId);
                    ResultSet rs = statement.executeQuery();

                    boolean hasData = false;
                    int batchCount = 0;

                    while (rs.next()) {
                        String changeData = buildChangeJson(rs);

                        // 输出源端数据信息
                        long currentChangeId = rs.getLong("change_id");
                        String tableName = rs.getString("table_name");
                        String operation = rs.getString("operation");

                        System.out.println(">>> 源端捕获变更 <<<");
                        System.out.println("变更ID: " + currentChangeId);
                        System.out.println("表名: " + tableName);
                        System.out.println("操作类型: " + operation);
                        System.out.println("记录ID: " + rs.getInt("record_id"));
                        System.out.println("发送到下游处理...");

                        ctx.collect(changeData);

                        lastProcessedId = currentChangeId;
                        hasData = true;
                        batchCount++;
                        recordCount++;
                    }

                    if (hasData) {
                        System.out.println("批次处理完成: " + batchCount + " 条记录");
                        System.out.println("当前最后处理ID: " + lastProcessedId);
                        System.out.println("累计处理记录数: " + recordCount);
                    } else {
                        // 没有新数据时休眠
                        System.out.println("[" + new java.util.Date() + "] 未发现新变更数据，等待 " + pollInterval + "ms...");
                        TimeUnit.MILLISECONDS.sleep(pollInterval);
                    }

                } catch (Exception e) {
                    System.err.println("[" + new java.util.Date() + "] 查询变更记录失败: " + e.getMessage());
                    e.printStackTrace();
                    TimeUnit.MILLISECONDS.sleep(pollInterval);
                }
            }
        }

        @Override
        public void cancel() {
            System.out.println("=== 收到取消信号，停止CDC源 ===");
            isRunning = false;
            closeResources();
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("=== 关闭CDC源 ===");
            System.out.println("最终最后处理ID: " + lastProcessedId);
            System.out.println("总处理记录数: " + recordCount);
            closeResources();
        }

        private void closeResources() {
            try {
                if (statement != null) {
                    statement.close();
                    System.out.println("数据库语句已关闭");
                }
                if (connection != null) {
                    connection.close();
                    System.out.println("数据库连接已关闭");
                }
            } catch (SQLException e) {
                System.err.println("关闭数据库连接失败: " + e.getMessage());
            }
        }

        /**
         * 获取最后处理的变更ID（可以从文件或数据库读取）
         */
        private long getLastProcessedId() {
            // 这里简单返回0，实际应用中可以从文件或数据库读取
            // 在实际应用中，你可以从以下位置读取：
            // 1. 本地文件
            // 2. 数据库中的状态表
            // 3. Redis等外部存储
            return lastProcessedId;
        }

        /**
         * 构建变更数据的 JSON 字符串
         */
        private String buildChangeJson(ResultSet rs) throws Exception {
            ObjectMapper mapper = new ObjectMapper();
            ObjectNode jsonNode = mapper.createObjectNode();

            jsonNode.put("change_id", rs.getLong("change_id"));
            jsonNode.put("table_name", rs.getString("table_name"));
            jsonNode.put("record_id", rs.getInt("record_id"));
            jsonNode.put("operation", rs.getString("operation"));
            jsonNode.put("change_time", rs.getTimestamp("change_time").toString());

            // 处理旧数据
            String oldData = rs.getString("old_data");
            if (oldData != null && !oldData.isEmpty()) {
                jsonNode.set("old_data", mapper.readTree(oldData));
            }

            // 处理新数据
            String newData = rs.getString("new_data");
            if (newData != null && !newData.isEmpty()) {
                jsonNode.set("new_data", mapper.readTree(newData));
            }

            return mapper.writeValueAsString(jsonNode);
        }
    }
}