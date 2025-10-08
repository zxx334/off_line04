### 📋 部署概述

本文档详细记录了在 cdh03 节点（IP: 192.168.200.32）上部署 Apache Doris 2.1.6 单节点集群的完整过程。

## 🏗️ 环境信息

### 服务器信息
    主机名: cdh03
    
    IP地址: 192.168.200.32
    
    操作系统: CentOS/Linux
    
    内存: 7.6GB
    
    磁盘: 46.97GB
    
    Swap: 已禁用（Doris 要求）

### 软件版本
    Apache Doris: 2.1.6
    
    Java: 1.8.0_181
    
    MySQL客户端: 5.7.99

### 安装目录
    /opt/soft/apache-doris-2.1.6/

## 📥 安装前准备

1. 禁用 Swap 内存

       # 临时禁用
        swapoff -a
        
        # 永久禁用（编辑 fstab）
        cp /etc/fstab /etc/fstab.backup
        sed -i '/swap/s/^/#/' /etc/fstab
        
        # 验证
        free -h

2. 创建安装目录


    mkdir -p /opt/soft/apache-doris-2.1.6
    cd /opt/soft/apache-doris-2.1.6

3. 下载并解压 Doris

           # 从官网下载 Doris 2.1.6
          # 解压到当前目录
        tar -zxvf apache-doris-2.1.6-bin-x64.tar.gz

## ⚙️ FE（Frontend）配置

1. 创建必要目录


    mkdir -p /opt/soft/apache-doris-2.1.6/doris-meta
    mkdir -p /opt/soft/apache-doris-2.1.6/fe/log

2. 配置 fe.conf

       vi /opt/soft/apache-doris-2.1.6/fe/conf/fe.conf


    # 元数据目录
    meta_dir = /opt/soft/apache-doris-2.1.6/doris-meta
    
    # 网络配置
    priority_networks = 192.168.200.0/24
    
    # 端口配置
    edit_log_port = 19010
    mysql_service_port = 9030
    http_port = 8030
    rpc_port = 9020
    query_port = 9030
    
    # JVM 配置
    JAVA_OPTS = -Xmx4096m -Xms4096m -XX:+UseG1GC
    
    # 日志配置
    sys_log_level = INFO
    sys_log_mode = NORMAL

3. 启动 FE

        cd /opt/soft/apache-doris-2.1.6/fe
        
        # 启动 FE
        ./bin/start_fe.sh --daemon
        
        # 检查启动状态
        sleep 10
        ps aux | grep doris
        netstat -tlnp | grep 19010


4. 验证 FE 启动

        # 查看 FE 日志
        tail -50 /opt/soft/apache-doris-2.1.6/fe/log/fe.log
        
        # 检查端口监听
        netstat -tlnp | grep -E '(19010|9030|8030|9020)'



## ⚙️ BE（Backend）配置

1. 创建必要目录


    mkdir -p /opt/soft/apache-doris-2.1.6/storage
    mkdir -p /opt/soft/apache-doris-2.1.6/be/log
    
    # 设置权限
    chmod 755 /opt/soft/apache-doris-2.1.6/storage


2. 配置 be.conf


    vi /opt/soft/apache-doris-2.1.6/be/conf/be.conf


    # 数据存储目录
    storage_root_path = /opt/soft/apache-doris-2.1.6/storage
    
    # 网络配置
    priority_networks = 192.168.200.0/24
    frontend_address = 192.168.200.32
    
    # 端口配置
    be_port = 9060
    webserver_port = 18040
    heartbeat_service_port = 9050
    brpc_port = 8060
    
    # 日志目录
    sys_log_dir = /opt/soft/apache-doris-2.1.6/be/log


3. 启动 BE


    cd /opt/soft/apache-doris-2.1.6/be
    
    # 启动 BE
    ./bin/start_be.sh --daemon
    
    # 检查启动状态
    sleep 10
    ps aux | grep doris_be
    netstat -tlnp | grep -E '(9060|18040|9050|8060)'


4. 验证 BE 启动


    # 查看 BE 日志
    tail -50 /opt/soft/apache-doris-2.1.6/be/log/be.INFO
    
    # 检查心跳连接
    tail -100 /opt/soft/apache-doris-2.1.6/be/log/be.INFO | grep -i "heartbeat"


## 🔗 集群配置

1. 连接到 Doris


    mysql -h 127.0.0.1 -P 9030 -u root  

2. 添加 BE 节点到集群


    -- 添加 BE 节点
    ALTER SYSTEM ADD BACKEND "cdh03:9050";
    
    -- 等待心跳建立
    SELECT SLEEP(10);

3. 验证集群状态


    -- 查看 BE 状态
    SHOW PROC '/backends';
    
    -- 查看 FE 状态
    SHOW PROC '/frontends';
    预期输出：BE 的 Alive 状态应该为 true


## 🧪 功能测试

1. 创建测试数据库和表


    -- 创建数据库
    CREATE DATABASE test_db;
    
    -- 使用数据库
    USE test_db;
    
    -- 创建测试表
    CREATE TABLE test_table (
    id INT,
    name VARCHAR(50),
    create_time DATETIME DEFAULT CURRENT_TIMESTAMP
    ) DISTRIBUTED BY HASH(id) BUCKETS 1
    PROPERTIES("replication_num" = "1");
    
    -- 查看表结构
    SHOW CREATE TABLE test_table;


2. 数据操作测试

    
    -- 插入数据
    INSERT INTO test_table (id, name) VALUES
    (1, 'hello'),
    (2, 'doris'),
    (3, 'test');
    
    -- 查询数据
    SELECT * FROM test_table;
    
    -- 更新数据
    UPDATE test_table SET name = 'doris_updated' WHERE id = 2;
    
    -- 删除数据
    DELETE FROM test_table WHERE id = 3;
    
    -- 验证操作结果
    SELECT * FROM test_table;

## 🛠️ 运维管理

1. 服务启停
    
       # 启动 FE
        cd /opt/soft/apache-doris-2.1.6/fe
        ./bin/start_fe.sh --daemon
        
        # 停止 FE
        ./bin/stop_fe.sh
        
        # 启动 BE
        cd /opt/soft/apache-doris-2.1.6/be
        ./bin/start_be.sh --daemon
        
        # 停止 BE
        ./bin/stop_be.sh

2. 日志查看


    # FE 日志
    tail -f /opt/soft/apache-doris-2.1.6/fe/log/fe.log
    
    # BE 日志
    tail -f /opt/soft/apache-doris-2.1.6/be/log/be.INFO
    
    # 启动日志
    tail -f /opt/soft/apache-doris-2.1.6/fe/log/fe.out
    tail -f /opt/soft/apache-doris-2.1.6/be/log/be.out



3. 数据备份


    # 备份元数据目录
    tar -czf doris-meta-backup.tar.gz /opt/soft/apache-doris-2.1.6/doris-meta
    
    # 备份数据目录
    tar -czf doris-storage-backup.tar.gz /opt/soft/apache-doris-2.1.6/storage


## ⚠️ 故障排查
# 常见问题及解决方案

1. 端口冲突


    # 检查端口占用
    netstat -tlnp | grep -E '(9010|9030|8030|9020|9060|8040|9050|8060)'
    
    # 杀死占用进程
    kill -9 <PID>

2. BE 无法连接 FE


    检查 frontend_address 配置
    
    验证网络连通性：telnet 192.168.200.32 9020
    
    查看 BE 日志中的心跳信息


3. Swap 未禁用


    # 检查 Swap
    free -h
    
    # 禁用 Swap
    swapoff -a
    sed -i '/swap/d' /etc/fstab


4. 内存不足


调整 fe.conf 中的 JVM 参数：


    JAVA_OPTS = -Xmx2048m -Xms2048m -XX:+UseG1GC


## 🔄 日常维护
1. 定期检查


    # 检查服务状态
    ps aux | grep doris
    
    # 检查磁盘空间
    df -h /opt/soft/
    
    # 检查内存使用
    free -h

2. 性能监控

         -- 查看慢查询
           SHOW PROC '/slow_queries';
        
        -- 查看表统计信息
        SHOW TABLE STATS test_db.test_table;


3. 清理维护


    -- 清理过期数据
    SET GABLE_GC_DELETE_SAFE_TIME = 3600;
    
    -- 优化表
    ALTER TABLE test_db.test_table COMPACT;



# 🎯 总结

通过以上步骤，我们成功在 cdh03 节点上部署了 Apache Doris 2.1.6 单节点集群。集群包含：

1个 FE 节点（Frontend）

1个 BE 节点（Backend）

所有必要服务正常运行

基础功能测试通过

集群现在可以用于开发和测试环境，如需生产环境使用，建议部署多节点集群以提高可用性。
