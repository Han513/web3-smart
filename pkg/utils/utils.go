package utils

import (
	"bytes"
	"compress/gzip"
	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"io"
	"math/big"
	"math/rand"
	"strings"
	"time"
)

var seededRand = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandomChoice(slice []string) string {
	if len(slice) == 0 {
		return ""
	}
	return slice[seededRand.Intn(len(slice))]
}

// Gzip压缩
func CompressData(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)

	if _, err := gz.Write(data); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Gzip解压
func DecompressData(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()

	return io.ReadAll(r)
}

// IsUnixSeconds 检查时间戳是否为秒级
func IsUnixSeconds(ts int64) bool {
	// 定义时间戳范围：1970-01-01 到 2100-01-01
	const maxUnix = 4_102_444_800 // 2100-01-01 00:00:00 UTC
	return ts >= 0 && ts < maxUnix
}

// ChecksumAddress 将 EVM 地址转换为 EIP-55 Checksum 格式
func ChecksumAddress(addr string, network string) string {
	if addr == "" {
		return ""
	}

	network = strings.ToUpper(strings.TrimSpace(network))
	addr = strings.TrimSpace(addr)

	if network == "BSC" || network == "ETH" || network == "POLYGON" {
		// 去掉前缀，统一小写处理
		addr = strings.TrimPrefix(strings.ToLower(addr), "0x")
		return common.HexToAddress("0x" + addr).Hex()
	}

	// 非 EVM 网络，直接返回原始地址
	return addr
}

// AdjustDecimals 调整精度显示
func AdjustDecimals(value *big.Int, decimals uint8) decimal.Decimal {
	decimalValue := decimal.NewFromBigInt(value, 0)
	divisor := decimal.New(1, int32(decimals))
	return decimalValue.Div(divisor)
}

// FormatUnits 格式化单位转换
func FormatUnits(amount *big.Int, decimals uint8) string {
	decimalAmount := decimal.NewFromBigInt(amount, 0)
	divisor := decimal.New(1, int32(decimals))
	result := decimalAmount.Div(divisor)
	return result.StringFixed(int32(decimals))
}

// 生成随机字符串
func GenerateRandomString(length int) string {
	const chars = "abcdefghijklmnopqrstuvwxyz0123456789"
	result := make([]byte, length)
	for i := range result {
		result[i] = chars[time.Now().UnixNano()%int64(len(chars))]
	}
	return string(result)
}
