package config

import (
	"log"
	"os"
	"strconv"
)

type Config struct {
	L1Rpc        string  // Layer1 Ethereum RPC
	L2Rpc        string  // Base L2 RPC
	PrivateKey   string  // 私钥，用于原子交易
	ContractAddr string  // 原子交易合约地址
	PostgresDSN  string  // Postgres 连接字符串
	RedisAddr    string  // Redis 地址
	WorkerCount  int     // 并发 Worker 数量
	MinProfit    float64 // 套利最低利润阈值
}

func LoadConfig() *Config {
	cfg := &Config{
		L1Rpc:        os.Getenv("L1_RPC"),
		L2Rpc:        os.Getenv("L2_RPC"),
		PrivateKey:   os.Getenv("PRIVATE_KEY"),
		ContractAddr: os.Getenv("CONTRACT_ADDR"),
		PostgresDSN:  os.Getenv("POSTGRES_DSN"),
		RedisAddr:    os.Getenv("REDIS_ADDR"),
	}

	// WorkerCount 默认 4
	if wc := os.Getenv("WORKER_COUNT"); wc != "" {
		if n, err := strconv.Atoi(wc); err == nil {
			cfg.WorkerCount = n
		} else {
			cfg.WorkerCount = 4
		}
	} else {
		cfg.WorkerCount = 4
	}

	// MinProfit 默认 0.01
	if mp := os.Getenv("MIN_PROFIT"); mp != "" {
		if f, err := strconv.ParseFloat(mp, 64); err == nil {
			cfg.MinProfit = f
		} else {
			cfg.MinProfit = 0.01
		}
	} else {
		cfg.MinProfit = 0.01
	}

	// 检查必要环境变量
	if cfg.L2Rpc == "" || cfg.PrivateKey == "" || cfg.ContractAddr == "" || cfg.PostgresDSN == "" || cfg.RedisAddr == "" {
		log.Fatal("必填环境变量未设置：L2_RPC / PRIVATE_KEY / CONTRACT_ADDR / POSTGRES_DSN / REDIS_ADDR")
	}

	return cfg
}
