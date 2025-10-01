
SELECT version();
-- 创建数据库
CREATE DATABASE IF NOT EXISTS behavior_analysis;

-- 使用数据库
USE behavior_analysis;

-- 创建外部表
DROP TABLE IF EXISTS behavior_analysis.user_actions;
CREATE EXTERNAL TABLE behavior_analysis.user_actions (
                                                         user_id INT,
                                                         user_name STRING,
                                                         action_type STRING,
                                                         product_id STRING,
                                                         amount DOUBLE,
                                                         action_time STRING
)
    PARTITIONED BY (ds STRING)
    ROW FORMAT DELIMITED
        FIELDS TERMINATED BY ','
    STORED AS TEXTFILE
    LOCATION 'hdfs:///user/hive/warehouse/behavior_analysis.db/user_actions/'
    TBLPROPERTIES ('skip.header.line.count'='1');

-- 添加分区
ALTER TABLE behavior_analysis.user_actions ADD PARTITION (ds='20250925');

-- 查询测试
SELECT * FROM behavior_analysis.user_actions WHERE ds='20250925' LIMIT 5;