package redis

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"
)

// SetAccountBalance 设置账户余额
func SetAccountBalance(address string, balance string) error {
	key := "account_balance:" + address
	return client.Set(ctx, key, balance, time.Hour*24).Err()
}

// GetAccountBalance 获取账户余额
func GetAccountBalance(address string) (*big.Int, error) {
	key := "account_balance:" + address
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	balance, ok := new(big.Int).SetString(val, 10)
	if !ok {
		return nil, fmt.Errorf("failed to parse balance: %s", val)
	}
	return balance, nil
}

// SetPoolSnapshot 设置池子快照
func SetPoolSnapshot(poolAddr string, reserve0, reserve1 string) error {
	key := "pool:" + poolAddr
	if err := client.HSet(ctx, key, map[string]interface{}{
		"reserve0": reserve0,
		"reserve1": reserve1,
		"updated":  time.Now().Unix(),
	}).Err(); err != nil {
		return err
	}
	// 设置过期，防止数据长时间占用内存
	client.Expire(ctx, key, time.Hour*24)
	return nil
}

// GetPoolSnapshot 获取池子快照
func GetPoolSnapshot(poolAddr string) (reserve0 string, reserve1 string, err error) {
	key := "pool:" + poolAddr
	res, err := client.HMGet(ctx, key, "reserve0", "reserve1").Result()
	if err != nil {
		return "", "", err
	}
	if len(res) != 2 || res[0] == nil || res[1] == nil {
		return "", "", nil
	}
	reserve0 = res[0].(string)
	reserve1 = res[1].(string)
	return
}

// BatchUpdatePools 批量更新 Redis 池子快照
func BatchUpdatePools(pools map[string][2]string) {
	for poolAddr, reserves := range pools {
		SetPoolSnapshot(poolAddr, reserves[0], reserves[1])
	}
}

// 存储候选路径
func SetCandidatePaths(token string, paths []string) error {
	key := "candidate_paths:" + token
	return client.Set(ctx, key, paths, time.Hour*24).Err()
}

// 获取候选路径
func GetCandidatePaths(token string) ([]string, error) {
	key := "candidate_paths:" + token
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var paths []string
	if err := json.Unmarshal([]byte(val), &paths); err != nil {
		return nil, err
	}
	return paths, nil
}

// 根据池子地址获取相关候选路径
func GetCandidatePathsByPool(poolAddr string) ([]string, error) {
	// 这里应该实现根据池子地址查找相关路径的逻辑
	// 由于我们存储的是以token为key的路径，需要遍历所有路径查找包含该池子的路径
	// 这是一个简化的实现，实际应用中可能需要更高效的索引方式
	var result []string

	// 获取所有token的候选路径
	keys, err := client.Keys(ctx, "candidate_paths:*").Result()
	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		val, err := client.Get(ctx, key).Result()
		if err != nil {
			continue
		}

		var paths []string
		if err := json.Unmarshal([]byte(val), &paths); err != nil {
			continue
		}

		// 查找包含该池子地址的路径
		for _, path := range paths {
			if strings.Contains(path, poolAddr) {
				result = append(result, path)
			}
		}
	}

	return result, nil
}
