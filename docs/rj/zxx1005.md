##张喜翔 10.4 日志 flink 写入HBase


### Flink写入HBase使⽤ hive 和 hbase 的映射表进⾏映射, 数据需要可查询, 实现可分区为佳

遇到的核心问题

1. HBase映射表分区限制：HBase映射表是非原生表，无法使用ALTER TABLE ADD PARTITION

2. 数据组织优化：需要合理的RowKey设计来支持高效查询

3. 查询性能：如何在不支持物理分区的情况下实现分区查询

解决方案设计思路

整体架构

    Flink实时数据 → HBase存储 → Hive映射表 → SQL查询

核心设计原则

1. 逻辑分区替代物理分区：通过RowKey设计实现分区逻辑

2. 数据冗余存储：在HBase中存储分区字段，便于查询过滤

3. 视图封装：使用Hive视图简化常用查询


### Java代码详细说明
1. 主类结构设计

       public class HiveHBaseIntegration {
       // 配置参数集中管理
       private static final String ZK_QUORUM = "cdh01:2181,cdh02:2181,cdh03:2181";
       private static final String NAMESPACE = "ods";
       private static final String HBASE_TABLE = "ods:order_info_partitioned";
       private static final String COLUMN_FAMILY = "cf";
    
       public static void main(String[] args) throws Exception {
       createHBaseTable();    // 步骤1：创建表结构
       runFlinkJob();         // 步骤2：写入数据
       }
       }
设计理由：

配置参数集中管理，便于维护修改

清晰的执行流程：先建表后写数据

单一职责原则，每个方法专注一个功能


2. HBase表创建逻辑

       private static void createTableIfNotExists(Admin admin) throws Exception {
       TableName tableName = TableName.valueOf(HBASE_TABLE);
    
       if (!admin.tableExists(tableName)) {
       ColumnFamilyDescriptor cfDescriptor = ColumnFamilyDescriptorBuilder
       .newBuilder(COLUMN_FAMILY.getBytes())
       .setMaxVersions(3)  // 保留3个版本，支持数据追溯
       .build();
    
            TableDescriptor tableDescriptor = TableDescriptorBuilder
                .newBuilder(tableName)
                .setColumnFamily(cfDescriptor)
                .build();
            
            admin.createTable(tableDescriptor);
       }
       }
#### 设计理由：

setMaxVersions(3)：保留历史版本，便于数据审计和错误恢复

先检查表是否存在，避免重复创建

统一的列族管理，便于后续扩展


3. Flink数据写入优化


    @Override
    public void invoke(Map<String, String> order, Context context) throws Exception {
    String orderId = order.get("order_id");
    String partitionDate = order.get("partition_date");
    
        // RowKey设计：分区日期_订单ID
        String rowKey = partitionDate + "_" + orderId;
        
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("order_id"), Bytes.toBytes(orderId));
        put.addColumn(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes("partition_date"), Bytes.toBytes(partitionDate));
        // ... 其他字段
        
        mutator.mutate(put);
        mutator.flush();  // 立即刷新，调试阶段使用
    }

#### RowKey设计策略：


分区日期_订单ID：保证同一分区的数据物理上相邻

支持按分区前缀扫描，提高查询效率

避免热点问题，数据均匀分布


##### 性能优化：
使用BufferedMutator批量写入，提高吞吐量

writeBufferSize(2 * 1024 * 1024)：2MB缓冲区平衡性能与实时性

4. 资源管理


    @Override
    public void close() throws Exception {
    if (mutator != null) mutator.close();
    if (hbaseConn != null) hbaseConn.close();
    }



#### 设计理由：

确保连接资源正确释放

防止资源泄漏导致系统不稳定

遵循Flink生命周期管理规范


### SQL代码详细说明

1. 基础映射表设计


    CREATE EXTERNAL TABLE IF NOT EXISTS order_info_hbase (
    rowkey STRING,
    order_id STRING,
    create_time STRING,
    amount STRING,
    status STRING,
    partition_date STRING
    )
    STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
    WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,cf:order_id,cf:create_time,cf:amount,cf:status,cf:partition_date"
    )
    TBLPROPERTIES (
    "hbase.table.name" = "ods:order_info_partitioned"
    );


#### 列映射设计：


    :key → rowkey：映射HBase的RowKey

    cf:order_id → order_id：业务主键
    
    cf:partition_date → partition_date：分区字段，用于查询过滤


### 设计理由：

完整的字段映射，支持所有业务字段查询

分区字段冗余存储，便于直接使用WHERE过滤

外部表设计，数据存储在HBase，Hive只负责查询


2. 解决分区问题的创新方案

问题：HBase映射表不支持物理分区

解决方案：逻辑分区 + 视图封装


    -- 方案1：直接使用WHERE条件分区查询
    SELECT * FROM order_info_hbase WHERE partition_date = '20251001';
    
    -- 方案2：创建视图封装常用查询
    CREATE VIEW IF NOT EXISTS order_info_20251001 AS
    SELECT * FROM order_info_hbase WHERE partition_date = '20251001';
    
    -- 方案3：动态分区统计
    SELECT partition_date, COUNT(*) as daily_orders
    FROM order_info_hbase
    WHERE partition_date IN ('20251001', '20251002', '20251003')
    GROUP BY partition_date;


#### 设计理由：

WHERE条件过滤：利用Hive的查询优化器，在HBase端进行过滤

视图封装：为常用查询提供简化接口，提高易用性

动态统计：利用SQL的GROUP BY实现分区统计功能

3. 查询优化策略


    -- 高效查询：利用RowKey前缀扫描
    SELECT * FROM order_info_hbase
    WHERE rowkey LIKE '20251001%';
    
    -- 状态统计：利用Hive聚合能力
    SELECT status, COUNT(*) as order_count, SUM(CAST(amount AS DOUBLE)) as total_amount
    FROM order_info_hbase
    GROUP BY status;
    
    -- 时间范围查询：结合分区和业务时间
    SELECT * FROM order_info_hbase
    WHERE partition_date = '20251001'
    AND create_time BETWEEN '2025-10-01 00:00:00' AND '2025-10-01 23:59:59';


#### 性能考虑：

    LIKE '20251001%'：利用HBase的RowKey前缀扫描，性能最佳
    
    WHERE partition_date = '值'：字段过滤，Hive会下推到HBase
    
    避免全表扫描，充分利用分区字段


## 注意事项

    数据一致性：HBase和Hive之间的数据是实时同步的
    
    查询性能：复杂查询建议在Hive中执行，点查建议直接使用HBase API
    
    分区管理：新的分区日期会自动支持，无需修改表结构
    
    监控告警：建议监控HBase Region分布和Hive查询性能
    
    这个方案成功解决了HBase映射表不支持物理分区的技术限制，通过创新的逻辑分区设计实现了高效的数据查询和管理。








### Flink写入HBase问题解决全记录

### 问题概述

在开发Flink作业将订单数据写入HBase的过程中，遇到了两个主要问题：

1. HDFS健康状态异常 - 安全模式开启且有损坏数据块

2.  HBase命名空间不存在 - 无法在ods命名空间下创建表


问题一：HDFS健康状态异常

错误现象

HDFS处于安全模式（Safe mode is ON）

存在损坏的数据块（Blocks with corrupt replicas: 3）

大量DataNode报告运行状况不良


诊断命令及结果

    hdfs dfsadmin -report

输出显示：

        Safe mode is ON
    Configured Capacity: 136168667136 (126.82 GB)
    Present Capacity: 77021151232 (71.73 GB)
    DFS Remaining: 74028396544 (68.94 GB)
    DFS Used: 2992754688 (2.  79 GB)
    DFS Used%: 3.   89%
    Replicated Blocks:
    Under replicated blocks: 0
    Blocks with corrupt replicas: 3
    Missing blocks: 0

问题分析

1. 安全模式开启：HDFS处于只读状态，无法写入新数据

2.  损坏数据块：3个HBase WAL文件损坏，触发了安全模式

3.   节点资源不均：cdn01节点系统空间使用过多（27.76GB）


解决方案

步骤1：退出安全模式

    hdfs dfsadmin -safemode leave

步骤2：检查损坏数据块

    hdfs fsck / -list-corruptfileblocks

输出显示3个损坏的HBase WAL文件：

    /hbase/WALs/cdh01,16020,1759241036838/cdh03%2C16020%2C1759241036838.cdh03%2C16020%2C1759241036838.reglongroup-0.1759255447030
    /hbase/WALs/cdh01,16020,1759241036930/cdh03%2C16020%2C1759241036930.cdh03%2C16020%2C1759241036930.reglongroup-0.1759255446843
    /hbase/WALs/cdh01,16020,1759241036747/cdh03%2C16020%2C1759241036747.cdh03%2C16020%2C1759241036747.reglongroup-0.1759255447380


步骤3：删除损坏文件

    hdfs fsck / -delete


步骤4：验证修复结果


    hdfs fsck /


输出显示：

    Status: HEALTHY
    Number of data-nodes: 3
    Corrupt blocks: 0
    Missing blocks: 0
    The filesystem under path '/' is HEALTHY

HDFS问题解决总结

1.  安全模式已关闭

2.   损坏数据块已清理

3.    文件系统恢复健康状态

4. 所有DataNode正常运行


错误信息

    HBase连接或表创建失败: org.apache.hadoop.hbase.NamespaceNotFoundException: ods
	at org.apache.hadoop.hbase.master.ClusterSchemaServiceImpl.getNamespace(...)



问题二：HBase命名空间不存在


问题分析

根本原因

1. HBase中不存在ods命名空间
2.  代码尝试在ods命名空间下创建order_info表，但命名空间未预先创建
3.   HBase要求命名空间必须先存在才能在其中创建表


环境信息

1. HBase版本：2.  1. 0-cdh6.3.   2
2.  Flink版本：本地执行环境
3.   开发环境：Windows 11 + IntelliJ IDEA

解决方案

方案选择：代码自动创建命名空间

核心代码实现

    private static void testAndCreateTable() {
    try {
        org.apache.hadoop.conf.Configuration hbaseConf = HBaseConfiguration.create();
        hbaseConf.set("hbase.zookeeper.quorum", ZK_QUORUM);
        hbaseConf.set("hbase.rpc.timeout", "30000");
        hbase.conf.set("hbase.client.operation.timeout", "30000");

        Connection connection = ConnectionFactory.createConnection(hbaseConf);
        Admin admin = connection.getAdmin();

        System.out.println("=== HBase连接测试 ===");
        System.out.println("集群状态: " + admin.getClusterStatus().getHBaseVersion());

        // 核心修复代码：检查并创建命名空间
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


代码设计思路

1. 防御性编程：先检查命名空间是否存在，再决定是否创建

2.  异常处理：通过捕获NamespaceNotFoundException来检测命名空间状态

3.   自修复机制：自动创建缺失的命名空间，减少手动干预

4. 资源管理：确保连接正确关闭，避免资源泄漏

需要添加的依赖

import org.apache.hadoop.hbase.NamespaceDescriptor;