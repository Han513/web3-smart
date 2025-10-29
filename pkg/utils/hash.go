package utils

import (
	"fmt"
	"hash/crc32"
)

func MergeHashKey(token string, timestamp int64) string {
	timeWindow := timestamp - (timestamp % 10)
	return fmt.Sprintf("%s_%d", token, timeWindow)
}

func GetHashBucket(key string, bucketSize uint32) uint32 {
	// 后续如果交易都集中在某几个池子里，导致worker负载不一致，修改hash算法
	return crc32.ChecksumIEEE([]byte(key)) % bucketSize
}
