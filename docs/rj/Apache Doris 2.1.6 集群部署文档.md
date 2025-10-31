![[Pasted image 20251027202921.png]]## 集群架构

- **FE (Frontend)**: 1节点
    
- **BE (Backend)**: 1节点
    
- **部署环境**: CDH 6.3.2


## 服务配置

### FE (Frontend) 配置

**配置文件**: `/opt/soft/apache-doris-2.1.6/fe/conf/fe.conf`

	meta_dir = /opt/soft/apache-doris-2.1.6/doris-meta
	priority_networks = 192.168.200.0/24
	edit_log_port = 19010
	mysql_service_port = 9030
	http_port = 8030
	rpc_port = 9020
	query_port = 9030
	sys_log_level = INFO
	sys_log_mode = NORMAL
	JAVA_OPTS="-Xmx2048m -Xms2048m -XX:+UseMembar -XX:SurvivorRatio=8 -XX:MaxTenuringThreshold=7 -XX:+PrintGCDateStamps -XX:+PrintGCDetails -XX:+UseConcMarkSweepGC -XX:+UseParNewGC -XX:+CMSClassUnloadingEnabled -XX:-CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=80 -XX:SoftRefLRUPolicyMSPerMB=0 -Xloggc:$DORIS_HOME/log/fe.gc.log"

### BE (Backend) 配置

**配置文件**: `/opt/soft/apache-doris-2.1.6/be/conf/be.conf`

	webserver_port = 8043
	be_port = 9060
	heartbeat_service_port = 9050
	brpc_port = 8060
	priority_networks = 192.168.200.0/24
	
## 服务端口说明


| 服务           | 端口    | 用途          | 协议    |
| ------------ | ----- | ----------- | ----- |
| FE Query     | 9030  | MySQL客户端连接  | MySQL |
| FE HTTP      | 8030  | Web UI和管理接口 | HTTP  |
| FE RPC       | 9020  | 内部RPC通信     | TCP   |
| FE EditLog   | 19010 | 元数据日志同步     | TCP   |
| BE Heartbeat | 9050  | 心跳检测        | TCP   |
| BE Service   | 9060  | 后端服务        | TCP   |
| BE HTTP      | 8043  | Web UI和监控   | HTTP  |
| BE BRPC      | 8060  | 高性能RPC      | TCP   |

## 启动命令

### 启动FE

	cd /opt/soft/apache-doris-2.1.6/fe
	./bin/start_fe.sh --daemon

### 启动BE

	cd /opt/soft/apache-doris-2.1.6/be
	./bin/start_be.sh --daemon

### 停止服务

	# 停止FE
	cd /opt/soft/apache-doris-2.1.6/fe && ./bin/stop_fe.sh

	# 停止BE  
	cd /opt/soft/apache-doris-2.1.6/be && ./bin/stop_be.sh


## 连接信息

### MySQL客户端连接

	mysql -h 192.168.200.32 -P 9030 -uroot

### Web监控界面

	- **FE Web UI**: [http://192.168.200.32:8030](http://192.168.200.32:8030/)
	    
	- **BE Web UI**: [http://192.168.200.32:8043](http://192.168.200.32:8043/)


## 集群管理命令

### 查看集群状态

	-- 查看BE状态
SHOW PROC '/backends';
	
	-- 查看FE状态  
	SHOW PROC '/frontends';
	
	-- 查看集群整体状态
	SHOW PROC '/';

### 添加BE节点（如需要）

	ALTER SYSTEM ADD BACKEND "cdh03:9050";

### 创建测试数据库

	CREATE DATABASE test_db;
	USE test_db;
	
	CREATE TABLE test_table (
	    id INT,
	    name VARCHAR(50)
	) DISTRIBUTED BY HASH(id) BUCKETS 1
	PROPERTIES ("replication_num" =
	
	 "1");

## DataGrip连接配置

### 连接参数

- **Host**: 192.168.200.32
    
- **Port**: 9030
    
- **User**: root
    
- **Password**: (无密码)
    
- **Database**: (可选，可连接后选择)