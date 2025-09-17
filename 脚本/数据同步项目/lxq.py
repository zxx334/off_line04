#!/usr/bin/python
# coding: utf-8
import mysql.connector
from pyhive import hive
import logging

# 配置日志
logging.basicConfig(level=logging.DEBUG)

# MySQL 配置
mysql_host = 'cdh03'
mysql_user = 'root'
mysql_password = 'root'
mysql_database = 'realtime_v1'

# Hive 配置
hive_host = 'cdh03'
hive_port = 10000  # Hive 服务端口
hive_username = 'root'
hive_database = 'a'


# SeaTunnel 配置
seatunnel_version = "2.3.10"
bigdata_env = "CDH 6.3.2"

# 连接到 MySQL 数据库
def connect_to_mysql():
    try:
        conn = mysql.connector.connect(
            host=mysql_host,
            user=mysql_user,
            password=mysql_password,
            database=mysql_database
        )
        print("连接 MySQL 数据库成功")
        return conn
    except Exception as e:
        print(f"连接 MySQL 数据库失败: {e}")
        return None

# 获取 MySQL 数据库中的表名和字段
def get_table_structure(conn):
    """获取指定数据库中的所有表名和字段"""
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES")
    tables = [table[0] for table in cursor.fetchall()]
    table_structure = {}
    for table in tables:
        cursor.execute(f"SHOW COLUMNS FROM {table}")
        columns = [col[0] for col in cursor.fetchall()]
        table_structure[table] = columns
    return table_structure

# 连接到 Hive
def connect_to_hive():
    try:
        conn = hive.Connection(host=hive_host, port=hive_port, username=hive_username, database=hive_database)
        print("连接 Hive 成功")
        return conn
    except Exception as e:
        print(f"连接 Hive 失败: {e}")
        return None

# 创建表
def create_table(conn, table_name, columns):
    cursor = conn.cursor()
    try:
        # 定义建表语句
        column_definitions = ",\n    ".join([f"`{col}` STRING" for col in columns])
        column_definitions += ",\n    `ds` STRING"  # 始终添加 ds 列
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {column_definitions}
        )
        ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
        STORED AS TEXTFILE
        """
        cursor.execute(create_table_sql)
        print(f"表 {table_name} 创建成功")
    except Exception as e:
        print(f"创建表 {table_name} 失败: {e}")
    finally:
        cursor.close()

# 生成 SeaTunnel 配置文件
def generate_seatunnel_conf(table_name, columns):
    conf_file_name = f"{table_name}.conf"
    fields_str = ", ".join([f"`{col}`" for col in columns + ['ds']])
    conf_content = f"""
# seatunnel version {seatunnel_version}
# bigdata env {bigdata_env}

env {{
    parallelism = 2
    job.mode = "BATCH"
}}

source{{
    Jdbc {{
        url = "jdbc:mysql://{mysql_host}:3306/{mysql_database}?serverTimezone=GMT%2b8&useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&useSSL=false&allowPublicKeyRetrieval=true"
        driver = "com.mysql.cj.jdbc.Driver"
        connection_check_timeout_sec = 100
        user = "{mysql_user}"
        password = "{mysql_password}"
        query = "select {fields_str} from {mysql_database}.{table_name};"
    }}
}}

sink {{
    Hive {{
        table_name = "bigdata_offline_v1_ws.ods_{table_name}"
        metastore_uri = "thrift://cdh03:9083"
        hive.hadoop.conf-path = "/etc/hadoop/conf"
        save_mode = "overwrite"
        partition_by = ["ds"]
        dynamic_partition = true
        file_format = "orc"
        orc_compress = "SNAPPY"
        tbl_properties = {{
            "external.table.purge" = "true"
        }}
        fields = [{fields_str}]
    }}
}}
"""
    with open(conf_file_name, 'w') as conf_file:
        conf_file.write(conf_content)
    print(f"SeaTunnel 配置文件 {conf_file_name} 生成成功")
# 主函数
def main():
    mysql_conn = connect_to_mysql()
    if mysql_conn:
        table_structure = get_table_structure(mysql_conn)
        print("\nMySQL 数据库中的表结构:")
        for table, columns in table_structure.items():
            print(f"{table}: {columns}")

        hive_conn = connect_to_hive()
        if hive_conn:
            for table, columns in table_structure.items():
                create_table(hive_conn, table, columns)
                generate_seatunnel_conf(table, columns)
            hive_conn.close()
        mysql_conn.close()

if __name__ == "__main__":
    main()