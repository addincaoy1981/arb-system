package redis

import (
	"encoding/json"
	"fmt"
)

// PoolMetadata 用于存储池子元数据（待审核池）
type PoolMetadata struct {
	Address      string  `json:"address"`
	Token0       string  `json:"token0"`
	Token1       string  `json:"token1"`
	Token0Symbol string  `json:"token0_symbol"`
	Token1Symbol string  `json:"token1_symbol"`
	Decimals0    uint8   `json:"decimals0"`
	Decimals1    uint8   `json:"decimals1"`
	SizeETH      float64 `json:"size_eth"`
	Approved     bool    `json:"approved"`
}

// Redis key 前缀
const (
	PendingPoolPrefix = "pending_pools:"
)

// SetPendingPool 保存待审核池子（不会覆盖已存在）
func SetPendingPool(meta *PoolMetadata) error {
	key := PendingPoolPrefix + meta.Address

	// 如果已经存在，就直接返回
	exists, _ := client.Exists(ctx, key).Result()
	if exists > 0 {
		return nil
	}

	data, _ := json.Marshal(meta)
	return client.Set(ctx, key, data, 0).Err()
}

// GetPendingPool 获取单个待审核池
func GetPendingPool(address string) (*PoolMetadata, error) {
	key := PendingPoolPrefix + address
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return nil, err
	}

	var meta PoolMetadata
	if err := json.Unmarshal([]byte(val), &meta); err != nil {
		return nil, err
	}
	return &meta, nil
}

// GetAllPendingPools 拉取所有待审核池
func GetAllPendingPools() ([]*PoolMetadata, error) {
	keys, err := client.Keys(ctx, PendingPoolPrefix+"*").Result()
	if err != nil {
		return nil, err
	}

	var pools []*PoolMetadata
	for _, key := range keys {
		val, _ := client.Get(ctx, key).Result()
		var meta PoolMetadata
		if err := json.Unmarshal([]byte(val), &meta); err == nil {
			pools = append(pools, &meta)
		}
	}
	return pools, nil
}

// GetAllApprovedPools 获取所有已审核通过的池子
func GetAllApprovedPools() ([]*PoolMetadata, error) {
	keys, err := client.Keys(ctx, PendingPoolPrefix+"*").Result()
	if err != nil {
		return nil, err
	}

	var pools []*PoolMetadata
	for _, key := range keys {
		val, _ := client.Get(ctx, key).Result()
		var meta PoolMetadata
		if err := json.Unmarshal([]byte(val), &meta); err == nil && meta.Approved {
			pools = append(pools, &meta)
		}
	}
	return pools, nil
}

// ApprovePool 审核通过池子 → 更新状态
func ApprovePool(address string) (*PoolMetadata, error) {
	key := PendingPoolPrefix + address
	val, err := client.Get(ctx, key).Result()
	if err != nil {
		return nil, fmt.Errorf("ApprovePool get failed: %v", err)
	}

	var meta PoolMetadata
	if err := json.Unmarshal([]byte(val), &meta); err != nil {
		return nil, err
	}

	// 更新状态
	meta.Approved = true
	data, _ := json.Marshal(meta)
	if err := client.Set(ctx, key, data, 0).Err(); err != nil {
		return nil, err
	}

	return &meta, nil
}

// RemovePool 从待审核列表删除池子（比如审核拒绝）
func RemovePool(address string) error {
	key := PendingPoolPrefix + address
	return client.Del(ctx, key).Err()
}
