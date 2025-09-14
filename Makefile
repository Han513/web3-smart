# 定义变量
BINARY=web3-smart     	   # 生成的二进制文件名
GOOS=linux                 # 目标操作系统 (Ubuntu 是 Linux)
GOARCH=amd64               # 目标架构 (64位)

# 默认目标
all: build

# 构建目标
build:
	CGO_ENABLED=0 GOOS=$(GOOS) GOARCH=$(GOARCH) go build -o build/$(BINARY) cmd/worker/main.go
.PHONY: build all clean createdir help

# 清理生成的文件
clean:
	rm -f $(BUILD_DIR)/$(BINARY)

# 创建构建目录
createdir:
	mkdir -p $(BUILD_DIR)

# 帮助信息
help:
	@echo "Usage:"
	@echo "make build    - Build the binary for Linux and output to $(BUILD_DIR)"
	@echo "make clean    - Clean up the binary"
	@echo "make test     - Run tests"
