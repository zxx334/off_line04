// HiveHBaseIntegration.java
package com.zxx;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * zxx 10.4
 * Hive与HBase集成解决方案
 * 功能：
 * 1. 创建HBase分区表
 * 2. 使用Flink写入分区数据到HBase
 * 3. 支持Hive映射查询
 */
public class HiveHBaseIntegration {
    
    // HBase配置
    private static final String ZK_QUORUM = "cdh01:2181,cdh02:2181,cdh03:2181";
    private static final String NAMESPACE = "ods";
    private static final String HBASE_TABLE = "ods:order_info_partitioned";
    private static final String COLUMN_FAMILY = "cf";
    
    public static void main(String[] args) throws Exception {
        System.out.println("=== 开始Hive-HBase集成作业 ===");
        
        // 步骤1: 创建HBase表
        createHBaseTable();
        
        // 步骤2: 执行Flink作业写入数据
        runFlinkJob();
        
        System.out.println("=== Hive-HBase集成作业完成 ===");
        System.out.println("请执行以下步骤完成集成：");
        System.out.println("1. 在Hive中执行映射表创建SQL");
        System.out.println("2. 使用Hive查询数据: SELECT * FROM ods_hive.order_info_hbase;");
    }
    
    /**
     * 创建HBase分区表
     */
    public static void createHBaseTable() {
        System.out.println("开始创建HBase表...");
        
        Connection connection = null;
        Admin admin = null;
        
        try {
            // 创建HBase配置
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            hbaseConf.set("hbase.rpc.timeout", "30000");
            hbaseConf.set("hbase.client.operation.timeout", "30000");
            
            // 创建连接
            connection = ConnectionFactory.createConnection(hbaseConf);
            admin = connection.getAdmin();
            
            System.out.println("HBase连接成功，集群版本: " + admin.getClusterStatus().getHBaseVersion());
            
            // 检查并创建命名空间
            createNamespaceIfNotExists(admin);
            
            // 检查并创建表
            createTableIfNotExists(admin);
            
            System.out.println("HBase表创建完成");
            
        } catch (Exception e) {
            System.err.println("HBase表创建失败: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("HBase表创建失败", e);
        } finally {
            // 关闭资源
            closeResources(admin, connection);
        }
    }
    
    /**
     * 创建命名空间（如果不存在）
     */
    private static void createNamespaceIfNotExists(Admin admin) throws Exception {
        String namespace = NAMESPACE;
        try {
            admin.getNamespaceDescriptor(namespace);
            System.out.println("命名空间 " + namespace + " 已存在");
        } catch (Exception e) {
            System.out.println("命名空间 " + namespace + " 不存在，开始创建...");
            NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
            admin.createNamespace(namespaceDesc);
            System.out.println("命名空间 " + namespace + " 创建成功!");
        }
    }
    
    /**
     * 创建表（如果不存在）
     */
    private static void createTableIfNotExists(Admin admin) throws Exception {
        TableName tableName = TableName.valueOf(HBASE_TABLE);
        
        if (!admin.tableExists(tableName)) {
            System.out.println("表 " + HBASE_TABLE + " 不存在，开始创建...");
            
            // 创建列族
            ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder.newBuilder(COLUMN_FAMILY.getBytes())
                    .setMaxVersions(3)
                    .build();
            
            // 创建表描述符
            TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(cfDescriptor)
                    .build();
            
            admin.createTable(tableDescriptor);
            System.out.println("分区表 " + HBASE_TABLE + " 创建成功!");
        } else {
            System.out.println("表 " + HBASE_TABLE + " 已存在");
        }
    }
    
    /**
     * 运行Flink作业写入数据
     */
    public static void runFlinkJob() throws Exception {
        System.out.println("开始执行Flink数据写入作业...");
        
        // 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        // 模拟订单数据流（包含分区字段）
        DataStream<Map<String, String>> orderStream = env.fromElements(
                createOrder("ORD100001", "2025-10-01 10:00:00", "150.00", "SUCCESS", "20251001"),
                createOrder("ORD100002", "2025-10-01 11:30:00", "89.90", "PENDING", "20251001"),
                createOrder("ORD100003", "2025-10-02 09:15:00", "299.99", "SUCCESS", "20251002"),
                createOrder("ORD100004", "2025-10-02 14:20:00", "45.50", "FAILED", "20251002"),
                createOrder("ORD100005", "2025-10-03 16:45:00", "199.00", "SUCCESS", "20251003"),
                createOrder("ORD100006", "2025-10-03 18:20:00", "78.50", "SUCCESS", "20251003"),
                createOrder("ORD100007", "2025-10-04 08:45:00", "320.00", "PENDING", "20251004")
        );
        
        // 写入HBase
        orderStream.addSink(new PartitionedHBaseSink());
        
        // 执行作业
        env.execute("Hive-HBase Integration Job");
        
        System.out.println("Flink数据写入作业完成");
    }
    
    /**
     * 自定义HBase Sink（支持分区）
     */
    public static class PartitionedHBaseSink extends RichSinkFunction<Map<String, String>> {
        private Connection hbaseConn;
        private BufferedMutator mutator;
        
        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化HBase分区Sink...");
            
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            hbaseConf.set("hbase.rpc.timeout", "30000");
            hbaseConf.set("hbase.client.operation.timeout", "30000");
            
            hbaseConn = ConnectionFactory.createConnection(hbaseConf);
            
            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(HBASE_TABLE));
            params.writeBufferSize(2 * 1024 * 1024); // 2MB缓冲区
            mutator = hbaseConn.getBufferedMutator(params);
            
            System.out.println("HBase分区Sink初始化完成");
        }
        
        @Override
        public void invoke(Map<String, String> order, Context context) throws Exception {
            String orderId = order.get("order_id");
            String createTime = order.get("create_time");
            String amount = order.get("amount");
            String status = order.get("status");
            String partitionDate = order.get("partition_date");
            
            // 生成分区RowKey: 分区日期 + 订单ID（便于按分区扫描）
            String rowKey = partitionDate + "_" + orderId;
            
            System.out.println("写入分区数据: " + order + ", RowKey: " + rowKey);
            
            // 创建Put对象
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("order_id"), Bytes.toBytes(orderId));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("create_time"), Bytes.toBytes(createTime));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("amount"), Bytes.toBytes(amount));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("status"), Bytes.toBytes(status));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("partition_date"), Bytes.toBytes(partitionDate));
            
            // 写入数据
            mutator.mutate(put);
            mutator.flush(); // 立即刷新，便于调试
            
            System.out.println("✓ 分区数据写入HBase成功: " + orderId + ", 分区: " + partitionDate);
        }
        
        @Override
        public void close() throws Exception {
            System.out.println("关闭HBase连接...");
            if (mutator != null) {
                mutator.close();
            }
            if (hbaseConn != null) {
                hbaseConn.close();
            }
            System.out.println("HBase连接已关闭");
        }
    }
    
    /**
     * 生成模拟订单数据（包含分区字段）
     */
    private static Map<String, String> createOrder(String orderId, String createTime, 
                                                  String amount, String status, String partitionDate) {
        Map<String, String> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("create_time", createTime);
        order.put("amount", amount);
        order.put("status", status);
        order.put("partition_date", partitionDate);
        return order;
    }
    
    /**
     * 关闭HBase资源
     */
    private static void closeResources(Admin admin, Connection connection) {
        try {
            if (admin != null) {
                admin.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {
            System.err.println("关闭HBase资源时发生错误: " + e.getMessage());
        }
    }
    
    /**
     * 获取Hive映射表创建SQL
     * 这个方法返回需要在Hive中执行的SQL语句
     */
    public static String getHiveMappingSQL() {
        return 
            "-- Hive与HBase映射表创建SQL\n" +
            "-- 创建Hive数据库\n" +
            "CREATE DATABASE IF NOT EXISTS ods_hive;\n" +
            "USE ods_hive;\n\n" +
            
            "-- 创建Hive外部表映射HBase表\n" +
            "CREATE EXTERNAL TABLE IF NOT EXISTS order_info_hbase (\n" +
            "    rowkey STRING,\n" +
            "    order_id STRING,\n" +
            "    create_time STRING,\n" +
            "    amount STRING,\n" +
            "    status STRING,\n" +
            "    partition_date STRING\n" +
            ")\n" +
            "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "    \"hbase.columns.mapping\" = \":key,cf:order_id,cf:create_time,cf:amount,cf:status,cf:partition_date\"\n" +
            ")\n" +
            "TBLPROPERTIES (\n" +
            "    \"hbase.table.name\" = \"ods:order_info_partitioned\",\n" +
            "    \"hbase.mapred.output.outputtable\" = \"ods:order_info_partitioned\"\n" +
            ");\n\n" +
            
            "-- 创建Hive分区表（基于partition_date字段）\n" +
            "CREATE EXTERNAL TABLE IF NOT EXISTS order_info_partitioned (\n" +
            "    rowkey STRING,\n" +
            "    order_id STRING,\n" +
            "    create_time STRING,\n" +
            "    amount STRING,\n" +
            "    status STRING\n" +
            ")\n" +
            "PARTITIONED BY (partition_date STRING)\n" +
            "STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'\n" +
            "WITH SERDEPROPERTIES (\n" +
            "    \"hbase.columns.mapping\" = \":key,cf:order_id,cf:create_time,cf:amount,cf:status\"\n" +
            ")\n" +
            "TBLPROPERTIES (\n" +
            "    \"hbase.table.name\" = \"ods:order_info_partitioned\",\n" +
            "    \"hbase.mapred.output.outputtable\" = \"ods:order_info_partitioned\"\n" +
            ");\n\n" +
            
            "-- 添加分区\n" +
            "ALTER TABLE order_info_partitioned ADD PARTITION (partition_date='20251001');\n" +
            "ALTER TABLE order_info_partitioned ADD PARTITION (partition_date='20251002');\n" +
            "ALTER TABLE order_info_partitioned ADD PARTITION (partition_date='20251003');\n" +
            "ALTER TABLE order_info_partitioned ADD PARTITION (partition_date='20251004');\n\n" +
            
            "-- 查询示例\n" +
            "-- SELECT * FROM order_info_hbase LIMIT 5;\n" +
            "-- SELECT * FROM order_info_partitioned WHERE partition_date = '20251001';\n" +
            "-- SELECT status, COUNT(*) as count FROM order_info_hbase GROUP BY status;";
    }
    
    /**
     * 打印使用说明
     */
    public static void printUsage() {
        System.out.println("\n=== 使用说明 ===");
        System.out.println("1. 运行主程序: java HiveHBaseIntegration");
        System.out.println("2. 在Hive中执行以下SQL创建映射表:");
        System.out.println(getHiveMappingSQL());
        System.out.println("3. 验证数据:");
        System.out.println("   hive -e \"USE ods_hive; SELECT * FROM order_info_hbase;\"");
        System.out.println("4. 分区查询:");
        System.out.println("   hive -e \"USE ods_hive; SELECT * FROM order_info_partitioned WHERE partition_date='20251001';\"");
    }
}