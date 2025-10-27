### 1. 安装 Docker

	# 安装依赖
	sudo dnf install -y gcc gcc-c++ dnf-utils device-mapper-persistent-data lvm2
	
	# 添加阿里云 Docker 仓库
	sudo dnf config-manager --add-repo http://mirrors.aliyun.com/docker-ce/linux/centos/docker-ce.repo
	
	# 安装 Docker
	sudo dnf install -y docker-ce docker-ce-cli containerd.io docker-compose-plugin


### 2. 启动和配置 Docker

# 启动 Docker 服务
sudo systemctl start docker
sudo systemctl enable docker

# 将用户添加到 docker 组
sudo usermod -aG docker $USER
newgrp docker

# 配置国内镜像加速器

	sudo mkdir -p /etc/docker
	sudo tee /etc/docker/daemon.json <<EOF
	{
	  "registry-mirrors": [
	    "https://docker.mirrors.ustc.edu.cn",
	    "https://hub-mirror.c.163.com",
	    "https://registry.docker-cn.com"
	  ]
	}
	EOF
	
	# 重启 Docker
	sudo systemctl restart docker
	
	# 验证安装
	docker --version
	docker ps

### 3. 常用 Docker 命令

	# 服务管理
	sudo systemctl start docker    # 启动
	sudo systemctl stop docker     # 停止
	sudo systemctl restart docker  # 重启
	sudo systemctl status docker   # 状态
	
	# 镜像管理
	docker images                  # 查看镜像
	docker pull <image>           # 拉取镜像
	docker rmi <image>            # 删除镜像
	
	# 容器管理
	docker ps                     # 运行中的容器
	docker ps -a                  # 所有容器
	docker stop <container>       # 停止容器
	docker start <container>      # 启动容器
	docker rm <container>         # 删除容器
	docker logs <container>       # 查看日志

## 🔧 服务管理命令

### 查看服务状态

	# Docker 服务
	sudo systemctl status docker

### 启动/停止服务

	# Docker
	sudo systemctl start docker
	sudo systemctl stop docker
	

### 查看服务日志
	# Docker
	sudo journalctl -u docker
	