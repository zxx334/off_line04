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

public class T1 {

    // HBase配置参数
    private static final String HBASE_TABLE = "ods:order_info";
    private static final String COLUMN_FAMILY = "cf";
    private static final String ZK_QUORUM = "cdh01:2181,cdh02:2181,cdh03:2181";

    public static void main(String[] args) throws Exception {
        // 1. 先测试HBase连接和创建表
        testAndCreateTable();

        // 2. 初始化Flink环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);  // 先改为1，便于调试

        // 3. 模拟订单数据流
        DataStream<Map<String, String>> orderStream = env.fromElements(
                createOrder("ORD123456", "2025-10-02 12:00:00", "100.00", "SUCCESS"),
                createOrder("ORD123457", "2025-10-02 12:05:00", "200.50", "PENDING")
        );

        // 4. 转换并写入HBase
        orderStream.addSink(new OrderHBaseSink());

        System.out.println("开始执行Flink作业...");
        env.execute("Flink Write Order to HBase");
    }

    // 测试连接并创建表
    private static void testAndCreateTable() {
        try {
            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            hbaseConf.set("hbase.rpc.timeout", "30000");
            hbaseConf.set("hbase.client.operation.timeout", "30000");

            Connection connection = ConnectionFactory.createConnection(hbaseConf);
            Admin admin = connection.getAdmin();

            System.out.println("=== HBase连接测试 ===");
            System.out.println("集群状态: " + admin.getClusterStatus().getHBaseVersion());

            // 检查并创建命名空间
            String namespace = "ods";
            try {
                admin.getNamespaceDescriptor(namespace);
                System.out.println("命名空间 " + namespace + " 已存在");
            } catch (org.apache.hadoop.hbase.NamespaceNotFoundException e) {
                System.out.println("命名空间 " + namespace + " 不存在，开始创建...");
                NamespaceDescriptor namespaceDesc = NamespaceDescriptor.create(namespace).build();
                admin.createNamespace(namespaceDesc);
                System.out.println("命名空间 " + namespace + " 创建成功!");
            }

            TableName tableName = TableName.valueOf(HBASE_TABLE);

            if (!admin.tableExists(tableName)) {
                System.out.println("表 " + HBASE_TABLE + " 不存在，开始创建...");

                // 创建表描述符
                TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
                        .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(COLUMN_FAMILY)).build())
                        .build();

                admin.createTable(tableDescriptor);
                System.out.println("表 " + HBASE_TABLE + " 创建成功!");
            } else {
                System.out.println("表 " + HBASE_TABLE + " 已存在");
            }

            admin.close();
            connection.close();
            System.out.println("=== HBase连接测试完成 ===\n");

        } catch (Exception e) {
            System.err.println("HBase连接或表创建失败: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    // 自定义HBase Sink（修改后的版本）
    public static class OrderHBaseSink extends RichSinkFunction<Map<String, String>> {
        private Connection hbaseConn;
        private BufferedMutator mutator;

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("初始化HBase Sink...");

            org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
            hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
            hbaseConf.set("hbase.rpc.timeout", "30000");
            hbaseConf.set("hbase.client.operation.timeout", "30000");

            hbaseConn = ConnectionFactory.createConnection(hbaseConf);

            BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(HBASE_TABLE));
            params.writeBufferSize(2 * 1024 * 1024);  // 2MB缓冲区
            mutator = hbaseConn.getBufferedMutator(params);

            System.out.println("HBase Sink初始化完成");
        }

        @Override
        public void invoke(Map<String, String> order, Context context) throws Exception {
            String orderId = order.get("order_id");
            String createTime = order.get("create_time");
            String amount = order.get("amount");
            String status = order.get("status");

            // 生成RowKey（简化版本）
            String rowKey = "order_" + orderId;

            System.out.println("写入数据: " + order);

            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("order_id"), Bytes.toBytes(orderId));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("create_time"), Bytes.toBytes(createTime));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("amount"), Bytes.toBytes(amount));
            put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("status"), Bytes.toBytes(status));

            mutator.mutate(put);
            mutator.flush();  // 立即刷新，便于调试

            System.out.println("数据写入HBase成功: " + orderId);
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
        }
    }

    // 生成模拟订单数据
    private static Map<String, String> createOrder(String orderId, String createTime, String amount, String status) {
        Map<String, String> order = new HashMap<>();
        order.put("order_id", orderId);
        order.put("create_time", createTime);
        order.put("amount", amount);
        order.put("status", status);
        return order;
    }
}