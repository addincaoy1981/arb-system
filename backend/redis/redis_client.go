package redis

import (
	"arb-system/backend/resolver"
	"context"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

type Redis struct {
	Client *redis.Client
}

func NewRedis(addr string, db int) *Redis {
	rdb := redis.NewClient(&redis.Options{
		Addr: addr,
		DB:   db,
	})
	return &Redis{Client: rdb}
}

// 活跃池子集合
func (r *Redis) AddActivePool(ctx context.Context, pool string) error {
	return r.Client.SAdd(ctx, "active_pools", pool).Err()
}

func (r *Redis) GetActivePools(ctx context.Context) ([]string, error) {
	return r.Client.SMembers(ctx, "active_pools").Result()
}

// 更新储备快照
func (r *Redis) UpdateReserves(ctx context.Context, pool, reserve0, reserve1 string, block uint64) error {
	key := fmt.Sprintf("pool:%s:reserves", pool)
	return r.Client.HSet(ctx, key, map[string]interface{}{
		"reserve0":     reserve0,
		"reserve1":     reserve1,
		"block_number": block,
	}).Err()
}

// 获取储备
func (r *Redis) GetReserves(ctx context.Context, pool string) (map[string]string, error) {
	key := fmt.Sprintf("pool:%s:reserves", pool)
	return r.Client.HGetAll(ctx, key).Result()
}
func SetTokenMeta(addr string, meta *resolver.TokenMeta) {
	key := "tokenmeta:" + addr
	client.HSet(ctx, key, map[string]interface{}{
		"symbol":   meta.Symbol,
		"name":     meta.Name,
		"decimals": meta.Decimals,
	})
}

func GetTokenMeta(addr string) *resolver.TokenMeta {
	key := "tokenmeta:" + addr
	data, err := client.HGetAll(ctx, key).Result()
	if err != nil || len(data) == 0 {
		return nil
	}
	decimals, _ := strconv.Atoi(data["decimals"])
	return &resolver.TokenMeta{
		Address:  addr,
		Symbol:   data["symbol"],
		Name:     data["name"],
		Decimals: uint8(decimals),
	}
}
