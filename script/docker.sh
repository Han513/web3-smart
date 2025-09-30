#!/bin/bash

# 定义颜色常量
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # 恢复默认颜色

# 帮助信息
usage() {
  echo -e "${YELLOW}用法: $0 [build|deploy|start|stop|restart|update] [项目名称(可选)]${NC}"
  echo -e "命令说明:"
  echo -e "  build   : 构建 Docker 镜像 (启用 BuildKit)"
  echo -e "  deploy  : 创建并启动容器（后台模式）"
  echo -e "  start   : 启动已存在的容器"
  echo -e "  stop    : 停止运行中的容器"
  echo -e "  restart : 重启容器"
  echo -e "  update  : 重新构建镜像并更新容器（等效 build + deploy）"
  echo -e "\n示例:"
  echo -e "  $0 build          # 构建所有镜像"
  echo -e "  $0 build motion   # 只构建 motion 服务"
  echo -e "  $0 deploy         # 启动所有服务"
  echo -e "  $0 deploy motion  # 只启动 motion 服务"
  exit 1
}

# 构建镜像
build() {
  echo -e "${GREEN}[+] 开始构建 Docker 镜像...${NC}"
  rm -rf ./deploy/.git-credentials
  cp ~/.git-credentials ./deploy/.git-credentials
  
  if [ -n "$1" ]; then
    echo -e "${GREEN}[+] 正在构建服务: $1${NC}"
    DOCKER_BUILDKIT=1 docker compose build "$1"
  else
    echo -e "${GREEN}[+] 正在构建所有服务${NC}"
    DOCKER_BUILDKIT=1 docker compose build
  fi

  if [ $? -eq 0 ]; then
    echo -e "${GREEN}[✓] 镜像构建成功！${NC}"
    rm -rf ./deploy/.git-credentials
  else
    echo -e "${RED}[✗] 镜像构建失败！${NC}" >&2
    exit 1
  fi
}

# 启动容器（首次部署）
deploy() {
  if [ -n "$1" ]; then
    echo -e "${GREEN}[+] 正在创建并启动服务: $1${NC}"
    docker compose up -d "$1"
  else
    echo -e "${GREEN}[+] 正在创建并启动所有服务${NC}"
    docker compose up -d
  fi
  status_check "$1"
}

# 启动已存在的容器
start() {
  if [ -n "$1" ]; then
    echo -e "${GREEN}[+] 正在启动服务: $1${NC}"
    docker compose start "$1"
  else
    echo -e "${GREEN}[+] 正在启动所有服务${NC}"
    docker compose start
  fi
  status_check "$1"
}

# 停止容器
stop() {
  if [ -n "$1" ]; then
    echo -e "${YELLOW}[-] 正在停止服务: $1${NC}"
    docker compose stop "$1"
  else
    echo -e "${YELLOW}[-] 正在停止所有服务${NC}"
    docker compose stop
  fi
  echo -e "${GREEN}[✓] 服务已停止${NC}"
}

# 重启容器
restart() {
  if [ -n "$1" ]; then
    echo -e "${YELLOW}[-] 正在重启服务: $1${NC}"
    docker compose restart "$1"
  else
    echo -e "${YELLOW}[-] 正在重启所有服务${NC}"
    docker compose restart
  fi
  status_check "$1"
}

# 更新容器（重新构建+部署）
update() {
  echo -e "${GREEN}[+] 正在拉取代码 ...${NC}"
  git pull
  if [ -n "$1" ]; then
    echo -e "${GREEN}[+] 正在更新服务: $1${NC}"
    docker compose stop "$1"
    docker compose rm -f "$1"
    build "$1"
    deploy "$1"
  else
    echo -e "${GREEN}[+] 正在更新所有服务${NC}"
    docker compose down
    build
    deploy
  fi
}

# 检查容器状态
status_check() {
  if [ $? -eq 0 ]; then
    echo -e "\n${GREEN}[✓] 容器状态:${NC}"
    if [ -n "$1" ]; then
      docker compose ps "$1"
    else
      docker compose ps
    fi
  else
    echo -e "${RED}[✗] 操作执行失败！${NC}" >&2
    exit 1
  fi
}

# 参数检查
if [ $# -eq 0 ] || [ $# -gt 2 ]; then
  usage
fi

# 命令路由
case "$1" in
  "build")
    build "$2"
    ;;
  "deploy")
    deploy "$2"
    ;;
  "start")
    start "$2"
    ;;
  "restart")
    restart "$2"
    ;;
  "stop")
    stop "$2"
    ;;
  "update")
    update "$2"
    ;;
  *)
    usage
    ;;
esac