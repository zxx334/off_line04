### 1. 安装 PostgreSQL 14


	# 添加 PostgreSQL 官方仓库
	sudo dnf install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-10-x86_64/pgdg-redhat-repo-latest.noarch.rpm
	
	# 安装 PostgreSQL 14
	sudo dnf install -y postgresql14-server postgresql14-contrib
	
	# 初始化数据库
	sudo /usr/pgsql-14/bin/postgresql-14-setup initdb
	
	# 启动服务
	sudo systemctl start postgresql-14
	sudo systemctl enable postgresql-14
### 2. 配置 PostgreSQL

	# 编辑主配置文件
	sudo vim /var/lib/pgsql/14/data/postgresql.conf

修改内容：

	listen_addresses = ‘*’
	port = 5432
	
	 编辑认证配置
	sudo vim /var/lib/pgsql/14/data/pg_hba.conf
添加内容：
	# 允许远程连接
	host    all             all             0.0.0.0/0               scram-sha-256
	host    all             all             ::/0                    scram-sha-256
	
	# 修改本地认证为密码认证
	local   all             all                                     md5

### 3. 设置密码和重启

	# 重启服务
	sudo systemctl restart postgresql-14
	
	# 设置 postgres 用户密码
	sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'Zxx123,./';"
	
	# 验证安装
	sudo systemctl status postgresql-14


### 4. 连接信息

- **主机**: `localhost`
    
- **端口**: `5432`
    
- **用户名**: `postgres`
    
- **密码**: `Zxx123,./`
    
- **默认数据库**: `postgres`

### 5. 测试连接

	# 本地连接测试
	psql -U postgres -h 127.0.0.1 -p 5432 -c "SELECT version();"
	
	# 创建新数据库
	createdb -U postgres -h localhost mydatabase
	
	# 进入交互模式
	psql -U postgres -h localhost
### 6. 常用 PostgreSQL 命令

	# 数据库操作
	\l                          # 列出所有数据库
	\c <database>               # 连接到数据库
	\dt                         # 列出所有表
	\du                         # 列出所有用户
	
	# 备份和恢复
	pg_dump -U postgres -h localhost dbname > backup.sql
	psql -U postgres -h localhost dbname < backup.sql
## 🔧 服务管理命令

### 查看服务状态

	
	# PostgreSQL 服务
	sudo systemctl status postgresql-14
### 启动/停止服务

	# PostgreSQL
	sudo systemctl start postgresql-14
	sudo systemctl stop postgresql-14

### 查看服务日志

	# PostgreSQL
	sudo journalctl -u postgresql-14
