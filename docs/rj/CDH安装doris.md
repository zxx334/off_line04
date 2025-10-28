
##### 下载并解压

```
wget https://apache-doris-releases.oss-accelerate.aliyuncs.com/apache-doris-2.1.6-bin-x64.tar.xz

tar -zxvf apache-doris-2.1.6-bin-x64.tar.xz  -C  /opt/soft

mv /opt/apache-doris-2.1.6-bin-x64 /opt/doris
```

##### 配置单节点（fe.conf/be.conf）

```
cd /opt/soft/apache-doris-2.1.6-bin-x64/fe/conf

echo "priority_networks=192.168.200.32/24" >> fe.conf  # 替换为服务器IP

cd /opt/soft/apache-doris-2.1.6-bin-x64/be/conf

echo "priority_networks=192.168.200.32/24" >> be.conf
```

##### 启动FE和BE

```
/opt/soft/apache-doris-2.1.6-bin-x64/fe/bin/start_fe.sh --daemon

/opt/soft/apache-doris-2.1.6-bin-x64/be/bin/start_be.sh --daemon
```

###### 登录Doris客户端（默认端口9030）

`mysql -h 192.168.200.32 -P 9030 -u root`

遇到的问题:

**FE卡在catalog初始化阶段**，无法完成启动。这是一个常见问题，需要重新初始化。

```
## 解决方案

### 1. 停止当前FE进程

# 停止FE进程
kill -9 49318

# 确认进程已停止
ps aux | grep doris | grep fe

### 2. 彻底清理并重新初始化

# 清理FE元数据目录
rm -rf /opt/soft/apache-doris-2.1.6-bin-x64/fe/doris-meta/*

# 清理BE数据目录（可选，如果是全新安装）
rm -rf /opt/soft/apache-doris-2.1.6-bin-x64/be/storage/*

# 重新创建必要的目录
mkdir -p /opt/soft/apache-doris-2.1.6-bin-x64/fe/doris-meta
mkdir -p /opt/soft/apache-doris-2.1.6-bin-x64/be/storage

### 3. 检查系统配置

# 检查系统参数（之前BE启动的警告）
sysctl -w vm.max_map_count=2000000

# 检查磁盘空间
df -h

# 检查内存
free -h

##  禁用swap内存

# 临时禁用swap
swapoff -a

# 验证swap已禁用
free -h

应该看到Swap行显示为0：

text

Swap:          2.0G          0B        2.0G

## 2. 永久禁用swap

# 注释掉/etc/fstab中的swap行
sed -i '/swap/s/^/#/' /etc/fstab

# 验证修改
cat /etc/fstab | grep swap

## 3. 重新启动BE

# 启动BE
/opt/soft/apache-doris-2.1.6-bin-x64/be/bin/start_be.sh --daemon

# 检查BE状态
ps aux | grep doris | grep be

# 查看BE日志
tail -f /opt/soft/apache-doris-2.1.6-bin-x64/be/log/be.INFO

**文件描述符限制**问题。需要调整系统限制。

## 1. 临时设置文件描述符限制

# 临时设置当前会话的文件描述符限制
ulimit -n 655350

# 验证设置
ulimit -n

## 2. 永久设置系统限制

# 编辑limits.conf文件
vi /etc/security/limits.conf

# 在文件末尾添加以下内容：
* soft nofile 655350
* hard nofile 655350
* soft nproc 655350
* hard nproc 655350

保存后退出。

## 3. 编辑系统配置文件

# 编辑sysctl.conf
vi /etc/sysctl.conf

# 添加或确保包含以下内容：
fs.file-max = 655350

保存后使配置生效：

# 重新加载sysctl配置
sysctl -p

## 4. 重新登录或重启会话

# 重新登录当前会话
su - root

# 或者新建一个ssh连接

## 5. 验证限制已生效

# 检查文件描述符限制
ulimit -n

# 检查系统全局限制
cat /proc/sys/fs/file-max

**BE的HTTP端口被占用**

# 检查BE默认端口（8040）被谁占用
netstat -tlnp | grep 8040
# 或者使用
lsof -i :8040
ss -tlnp | grep 8040

#### 修改BE端口配置（推荐）

## 全面检查端口占用

# 检查所有可能冲突的端口
netstat -tlnp | grep -E '(8030|8031|8032|8040|8041|8042|8043|8044|8045|8050|8051)'

# 或者查看所有监听端口
netstat -tlnp | grep LISTEN | head -20

## 2. 找到可用的端口

# 快速测试端口是否可用
for port in {8042..8050}; do
  netstat -tln | grep ":$port " > /dev/null || echo "Port $port is available"
done

# 备份BE配置文件
cp /opt/soft/apache-doris-2.1.6-bin-x64/be/conf/be.conf /opt/soft/apache-doris-2.1.6-bin-x64/be/conf/be.conf.bak

# 编辑BE配置文件
vi /opt/soft/apache-doris-2.1.6-bin-x64/be/conf/be.conf

在配置文件中修改端口（避免冲突）：

# 将默认端口修改为其他可用端口
webserver_port = 8043
be_port = 9061
brpc_port = 8061

保存后启动BE：

# 启动BE
/opt/soft/apache-doris-2.1.6-bin-x64/be/bin/start_be.sh --daemon

# 检查BE进程
ps aux | grep doris | grep be

# 查看BE日志
tail -f /opt/soft/apache-doris-2.1.6-bin-x64/be/log/be.INFO

# 检查FE进程
ps aux | grep doris | grep fe

# 检查9030端口是否监听
netstat -tlnp | grep 9030
```

在Doris中，**默认情况下root用户没有密码**。以下是查看和管理Doris用户的方法：

```
## 1. 查看当前所有用户

连接Doris后执行：

-- 查看所有用户
SELECT User, Host, authentication_string FROM mysql.user;

-- 或者使用更详细的查询
SHOW GRANTS;

## 2. 查看当前登录用户

-- 查看当前用户
SELECT CURRENT_USER();

-- 查看用户权限
SHOW GRANTS FOR CURRENT_USER();

## 3. 修改root密码（如果还没有设置）

-- 设置root密码
SET PASSWORD FOR 'root' = PASSWORD('your_new_password');

-- 或者使用（新版本Doris）
ALTER USER 'root' IDENTIFIED BY 'your_new_password';

## 4. 创建新用户

-- 创建新用户
CREATE USER 'username' IDENTIFIED BY 'password';

-- 授予权限
GRANT ALL PRIVILEGES ON *.* TO 'username';

-- 刷新权限
FLUSH PRIVILEGES;

## 5. 查看用户权限

-- 查看特定用户权限
SHOW GRANTS FOR 'username'@'%';

-- 查看所有用户及其权限
SELECT * FROM mysql.user;
```