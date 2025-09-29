package main

import (
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"arb-system/backend/db"
	"arb-system/backend/executor"
	"arb-system/backend/graph"
	"arb-system/backend/listener"
	"arb-system/backend/redis"
	"arb-system/backend/resolver"

	"arb-system/backend/contracts/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/joho/godotenv"
)

func main() {
	// -----------------------------
	// 1. 加载环境变量
	// -----------------------------
	if err := godotenv.Load(); err != nil {
		log.Println(".env file not found")
	}

	rpcURL := os.Getenv("RPC_URL")
	privateKey := os.Getenv("PRIVATE_KEY")
	chainIDInt, _ := strconv.ParseInt(os.Getenv("CHAIN_ID"), 10, 64)
	chainID := big.NewInt(chainIDInt)
	workerNum, _ := strconv.Atoi(os.Getenv("WORKER_NUM"))
	minProfitInt, _ := strconv.ParseInt(os.Getenv("MIN_PROFIT"), 10, 64)
	minProfit := big.NewInt(minProfitInt)

	postgresDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	atomicContractAddr := os.Getenv("ATOMIC_CONTRACT_ADDR")
	factoryAddrsStr := os.Getenv("FACTORY_ADDRS")
	fromBlockStr := os.Getenv("FACTORY_FROM_BLOCK")

	startToken := os.Getenv("START_TOKEN") // 稳定币起点，例如 USDC

	// -----------------------------
	// 2. 初始化客户端和数据库
	// -----------------------------
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Fatalf("RPC connect failed: %v", err)
	}

	if err := db.InitPostgres(postgresDSN); err != nil {
		log.Fatalf("Postgres init failed: %v", err)
	}

	if err := redis.InitRedis(redisAddr); err != nil {
		log.Fatalf("Redis init failed: %v", err)
	}

	// -----------------------------
	// 3. 初始化 TokenGraph
	// -----------------------------
	graphClient := graph.NewTokenGraph()

	// -----------------------------
	// 4. Factory 自动发现 Pool (多 DEX)
	// -----------------------------
	factoryAddrs := strings.Split(factoryAddrsStr, ",")
	fromBlock := uint64(0)
	if fromBlockStr != "" {
		if blk, ok := new(big.Int).SetString(fromBlockStr, 10); ok {
			fromBlock = blk.Uint64()
		}
	}

	// 初始化 ABI
	resolver.InitERC20ABI()
	listener.InitFactoryABI(os.Getenv("FACTORY_ABI"))
	listener.InitPoolABI(listener.V3PoolABI)

	for _, addr := range factoryAddrs {
		factoryAddr := common.HexToAddress(strings.TrimSpace(addr))
		// 加载历史池子
		if fromBlock > 0 {
			listener.LoadHistoricalPools(client, factoryAddr, fromBlock)
		}
		// 订阅实时 PairCreated
		listener.SubscribeFactory(client, factoryAddr)
	}

	// -----------------------------
	// 5. 动态订阅 Swap 事件
	// -----------------------------
	listener.SubscribeDynamicPoolsOptimized(client)
	resolver.StartBackgroundRefresh(client, 10*time.Minute)

	// -----------------------------
	// 6. 初始化 Executor
	// -----------------------------
	atomicContract, err := atomic.NewAtomicArb(common.HexToAddress(atomicContractAddr), client)
	if err != nil {
		log.Fatalf("Load atomic contract failed: %v", err)
	}

	exe := &executor.Executor{
		Client:        client,
		PrivateKey:    privateKey,
		Contract:      atomicContract,
		ChainID:       chainID,
		WorkerNum:     workerNum,
		PendingSwapCh: make(chan *graph.ArbitrageOpportunity, 100),
	}
	go exe.Run()

	// -----------------------------
	// 7. DFS 套利循环，从稳定币开始
	// -----------------------------
	for {
		ops := graphClient.FindArbitrageDFS(minProfit, startToken)
		for _, op := range ops {
			select {
			case exe.PendingSwapCh <- op:
			default:
				log.Println("Executor channel full, skipping")
			}
		}
	}
}
