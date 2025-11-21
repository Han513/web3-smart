# ---------- Producer 构建阶段即编译golang程序 ----------
FROM golang:1.24.3 AS builder
WORKDIR /app


# 复制依赖文件并整理依赖
COPY go.mod go.sum ./

# 这行是引用当前机器git相关配置，及下载go mod
RUN --mount=type=bind,src=deploy/.git-credentials,dst=/root/.git-credentials \
    git config --global credential.helper store && go mod download -x

# 复制全部代码
COPY . .

# 进入 worker 目录并构建二进制文件
WORKDIR /app
RUN CGO_ENABLED=0 go build -o app cmd/worker/main.go
RUN CGO_ENABLED=0 go build -o prepare_cache cmd/script/main.go





# ---------- Producer 运行阶段及部署运行程序 ----------
FROM alpine:3.18.2 AS runner

# 设置时区
RUN apk add --no-cache tzdata coreutils&& \
    cp /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /root
COPY --from=builder /app/app /root/
CMD ["/root/app"]


