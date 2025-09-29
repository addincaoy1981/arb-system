package redis

import (
	"context"
	"log"
	"time"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var Rdb *redis.Client

// 初始化 Redis 连接
func InitRedis(addr string) {
	Rdb = redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: "",
		DB:       0,
		PoolSize: 50, // 高并发池
	})

	if err := Rdb.Ping(ctx).Err(); err != nil {
		log.Fatal("Redis connection failed:", err)
	}
	log.Println("Redis connected successfully")
}

// 设置池子快照
func SetPoolSnapshot(poolAddr string, reserve0, reserve1 string) {
	key := "pool:" + poolAddr
	if err := Rdb.HSet(ctx, key, map[string]interface{}{
		"reserve0": reserve0,
		"reserve1": reserve1,
		"updated":  time.Now().Unix(),
	}).Err(); err != nil {
		log.Println("Redis HSet error:", err)
	}
	// 设置过期，防止数据长时间占用内存
	Rdb.Expire(ctx, key, time.Hour*24)
}

// 获取池子快照
func GetPoolSnapshot(poolAddr string) (reserve0 string, reserve1 string, err error) {
	key := "pool:" + poolAddr
	res, err := Rdb.HMGet(ctx, key, "reserve0", "reserve1").Result()
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

// 批量更新 Redis 池子快照
func BatchUpdatePools(pools map[string][2]string) {
	for poolAddr, reserves := range pools {
		SetPoolSnapshot(poolAddr, reserves[0], reserves[1])
	}
}
