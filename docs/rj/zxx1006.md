### Flink CDC 数据同步项目文档
#### 📋 项目概述
本项目实现从 PostgreSQL 和 SQLServer 数据库通过 Flink CDC 实时同步数据到 Kafka，并使用 Doris Routine Load 方式加载到 Doris 数据仓库，采用动态分区和分桶优化。


🏗️ 系统架构

    PostgreSQL/SQLServer → Flink CDC → Kafka → Doris Routine Load → Doris


📁 项目结构

    stream-realtimeTT2/
    ├── src/
    │   └── main/
    │       └── java/
    │           └── com/
    │               └── zxx/
    │                   ├── CDC_PG_Server_Kafka_doris.java
    │                   └── MultiSourceCDCToKafka.java
    ├── pom.xml
    └── README.md



⚙️ 环境配置
1. PostgreSQL 配置


位置: CDC_PG_Server_Kafka_doris.java 第 47-49 行

    private final String url = "jdbc:postgresql://cdh03:5432/test_db";
    private final String user = "postgres";
    private final String password = "123456";


问题: 需要创建变更日志表和触发器


解决方案:


    -- 创建变更日志表
    CREATE TABLE users_changelog (
    change_id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR(50),
    record_id INTEGER,
    operation VARCHAR(10),
    old_data JSON,
    new_data JSON,
    change_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    -- 创建触发器函数
    CREATE OR REPLACE FUNCTION users_change_trigger()
    RETURNS TRIGGER AS $$
    BEGIN
    IF TG_OP = 'INSERT' THEN
    INSERT INTO users_changelog (table_name, record_id, operation, new_data)
    VALUES ('users', NEW.id, 'I', row_to_json(NEW));
    RETURN NEW;
    ELSIF TG_OP = 'UPDATE' THEN
    INSERT INTO users_changelog (table_name, record_id, operation, old_data, new_data)
    VALUES ('users', NEW.id, 'U', row_to_json(OLD), row_to_json(NEW));
    RETURN NEW;
    ELSIF TG_OP = 'DELETE' THEN
    INSERT INTO users_changelog (table_name, record_id, operation, old_data)
    VALUES ('users', OLD.id, 'D', row_to_json(OLD));
    RETURN OLD;
    END IF;
    RETURN NULL;
    END;
    $$ LANGUAGE plpgsql;
    
    -- 创建触发器
    CREATE TRIGGER users_cdc_trigger
    AFTER INSERT OR UPDATE OR DELETE ON users
    FOR EACH ROW EXECUTE FUNCTION users_change_trigger();


2. Kafka 配置

位置: CDC_PG_Server_Kafka_doris.java 第 24-28 行

    observedStream.addSink(new FlinkKafkaProducer<>(
    "cdh03:9092",
    "cdc-data-topic",
    new SimpleStringSchema()
    )).name("kafka-sink");


3. Doris 配置

3.1 Doris 连接问题

问题: 默认端口连接失败

错误信息:

    mysql -h cdh03 -P 9030 -u root
    ERROR 2003 (HY000): Can't connect to MySQL server on 'cdh03' (111)


解决方案: 使用修改后的端口


    # 检查实际监听端口
    netstat -tlnp | grep doris
    
    # 使用正确端口连接
    mysql -h cdh03 -P 19030 -u root

3.2 Doris 表创建问题

问题: DUPLICATE KEY 列顺序错误

错误信息:


    ERROR 1105 (HY000): errCode = 2, detailMessage = Key columns should be a ordered prefix of the schema.


解决方案: 调整列顺序，将分区键放在前面

    CREATE TABLE IF NOT EXISTS pg_users_doris (
    `id` BIGINT,
    `cdc_time` DATETIME DEFAULT CURRENT_TIMESTAMP,  -- 分区键放在前面
    `name` VARCHAR(50),
    `email` VARCHAR(100),
    `created_at` DATETIME,
    `updated_at` DATETIME,
    `source_db` VARCHAR(20) DEFAULT 'postgresql'
    ) ENGINE=OLAP
    DUPLICATE KEY(`id`, `cdc_time`)  -- 与表定义顺序一致
    PARTITION BY RANGE (`cdc_time`)()
    DISTRIBUTED BY HASH(`id`) BUCKETS 8
    PROPERTIES (
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-7",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8",
    "replication_num" = "1"
    );


3.3 Routine Load 配置问题

问题: max_batch_rows 参数值太小

错误信息:

    ERROR 1105 (HY000): errCode = 2, detailMessage = max_batch_rows should > 200000

解决方案: 调整参数值


    CREATE ROUTINE LOAD cdc_db.pg_users_load ON pg_users_doris
    PROPERTIES
    (
    "desired_concurrent_number" = "3",
    "max_batch_interval" = "20",
    "max_batch_rows" = "200000",  -- 修改为 >= 200000
    "max_batch_size" = "104857600",
    "strict_mode" = "false",
    "format" = "json"
    )
    FROM KAFKA
    (
    "kafka_broker_list" = "cdh03:9092",
    "kafka_topic" = "cdc-data-topic",
    "property.group.id" = "doris_pg_users"
    );


🔧 Maven 依赖配置

主要依赖 (pom.xml)

    <!-- Flink CDC 连接器 -->
    <dependency>
        <groupId>com.ververica</groupId>
        <artifactId>flink-connector-postgres-cdc</artifactId>
        <version>2.4.2</version>
    </dependency>
    
    <!-- SQLServer CDC 连接器 -->
    <dependency>
        <groupId>com.ververica</groupId>
        <artifactId>flink-connector-sqlserver-cdc</artifactId>
        <version>2.4.2</version>
    </dependency>
    
    <!-- Flink 核心 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-streaming-java_2.12</artifactId>
        <version>1.14.6</version>
    </dependency>
    
    <!-- Kafka 连接器 -->
    <dependency>
        <groupId>org.apache.flink</groupId>
        <artifactId>flink-connector-kafka_2.12</artifactId>
        <version>1.14.6</version>
    </dependency>


###  运行步骤

1. 启动 Flink 作业 （cdh01）里面


2. 验证数据流水线

cdh03进入doris 里查看

        -- 检查 Routine Load 状态
        SHOW ROUTINE LOAD;
        
        -- 检查数据是否加载
        SELECT * FROM pg_users_doris LIMIT 10;
        SELECT COUNT(*) FROM pg_users_doris;


3. 监控数据流

Flink 控制台: 查看 CDC 数据捕获日志

Kafka: 监控 cdc-data-topic 主题消息

Doris: 检查表数据和 Routine Load 状态



🐛 常见问题排查


### 问题1: Flink CDC 无数据输出

检查项:

PostgreSQL 触发器是否创建成功

users_changelog 表是否有数据

数据库连接配置是否正确

### 问题2: Kafka 无消息
检查项:

Kafka 服务是否运行

主题是否存在

Flink Kafka Producer 配置



### 问题3: Doris 表无数据
检查项:

Routine Load 状态是否为 RUNNING

数据格式是否匹配

Kafka 偏移量是否正确



### 问题4: 内存不足错误

错误信息: Process memory not enough

解决方案: 增加 Doris BE 内存配置或优化查询

## 📊 性能优化


1. 动态分区配置
   按天自动分区

保留最近7天数据

预创建未来3天分区

2. 分桶优化
   使用 HASH 分桶

8个分桶均匀分布数据

基于业务键值分布

3. Routine Load 调优
   合适的批处理大小

合理的并发数

适当的提交间隔

