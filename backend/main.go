package main

import (
	"crypto/ecdsa"
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
	"github.com/ethereum/go-ethereum/crypto"
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
	// 支持多个私钥，用逗号分隔
	privateKeys := strings.Split(privateKey, ",")
	// 清理私钥前后的空格
	for i, key := range privateKeys {
		privateKeys[i] = strings.TrimSpace(key)
	}

	chainIDInt, _ := strconv.ParseInt(os.Getenv("CHAIN_ID"), 10, 64)
	chainID := big.NewInt(chainIDInt)
	workerNum, _ := strconv.Atoi(os.Getenv("WORKER_NUM"))

	postgresDSN := os.Getenv("POSTGRES_DSN")
	redisAddr := os.Getenv("REDIS_ADDR")
	atomicContractAddr := os.Getenv("ATOMIC_CONTRACT_ADDR")

	// 获取允许套利的代币地址列表
	allowedTokensStr := os.Getenv("ALLOWED_TOKENS")
	var allowedTokens []string
	if allowedTokensStr != "" {
		allowedTokens = strings.Split(allowedTokensStr, ",")
		// 清理代币地址前后的空格
		for i, token := range allowedTokens {
			allowedTokens[i] = strings.TrimSpace(token)
		}
	}

	// -----------------------------
	// 2. 初始化客户端和数据库
	// -----------------------------
	client, err := ethclient.Dial(rpcURL)
	if err != nil {
		log.Fatalf("RPC connect failed: %v", err)
	}

	db.InitPostgres(postgresDSN)
	redis.InitRedis(redisAddr)

	// -----------------------------
	// 3. 初始化 TokenGraph
	// -----------------------------
	tokenGraph := listener.InitTokenGraph(allowedTokens)

	// -----------------------------
	// 4. 初始化 ABI
	// -----------------------------
	resolver.InitERC20ABI()
	listener.InitFactoryABI(os.Getenv("FACTORY_ABI"))

	// 初始化 V3 Pool ABI，使用标准的 Uniswap V3 Pool ABI
	v3PoolABI := `[{"inputs":[],"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Burn","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":false,"internalType":"address","name":"recipient","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"Collect","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint128","name":"amount0","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"amount1","type":"uint128"}],"name":"CollectProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"paid1","type":"uint256"}],"name":"Flash","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"},{"indexed":false,"internalType":"uint16","name":"observationIndex","type":"uint16"},{"indexed":false,"internalType":"uint128","name":"observationCardinality","type":"uint128"},{"indexed":false,"internalType":"uint128","name":"observationCardinalityNext","type":"uint128"},{"indexed":false,"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"indexed":false,"internalType":"bool","name":"unlocked","type":"bool"}],"name":"Initialize","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"int24","name":"tickLower","type":"int24"},{"indexed":true,"internalType":"int24","name":"tickUpper","type":"int24"},{"indexed":false,"internalType":"uint128","name":"amount","type":"uint128"},{"indexed":false,"internalType":"uint256","name":"amount0","type":"uint256"},{"indexed":false,"internalType":"uint256","name":"amount1","type":"uint256"}],"name":"Mint","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint8","name":"feeProtocol0Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1Old","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol0New","type":"uint8"},{"indexed":false,"internalType":"uint8","name":"feeProtocol1New","type":"uint8"}],"name":"SetFeeProtocol","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount","type":"uint128"}],"name":"burn","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collect","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint128","name":"amount0Requested","type":"uint128"},{"internalType":"uint128","name":"amount1Requested","type":"uint128"}],"name":"collectProtocol","outputs":[{"internalType":"uint128","name":"amount0","type":"uint128"},{"internalType":"uint128","name":"amount1","type":"uint128"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"factory","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"fee","outputs":[{"internalType":"uint24","name":"","type":"uint24"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal0X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"feeGrowthGlobal1X128","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"flash","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint16","name":"observationIndex","type":"uint16"}],"name":"getObservation","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"}],"name":"initialize","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"liquidity","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"maxLiquidityPerTick","outputs":[{"internalType":"uint128","name":"","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"},{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"mint","outputs":[{"internalType":"uint256","name":"amount0","type":"uint256"},{"internalType":"uint256","name":"amount1","type":"uint256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"observations","outputs":[{"internalType":"uint32","name":"blockTimestamp","type":"uint32"},{"internalType":"int56","name":"tickCumulative","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityCumulativeX128","type":"uint160"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint32[]","name":"secondsAgos","type":"uint32[]"}],"name":"observe","outputs":[{"internalType":"int56[]","name":"tickCumulatives","type":"int56[]"},{"internalType":"uint160[]","name":"secondsPerLiquidityCumulativeX128s","type":"uint160[]"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"bytes32","name":"","type":"bytes32"}],"name":"positions","outputs":[{"internalType":"uint128","name":"liquidity","type":"uint128"},{"internalType":"uint256","name":"feeGrowthInside0LastX128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthInside1LastX128","type":"uint256"},{"internalType":"uint128","name":"tokensOwed0","type":"uint128"},{"internalType":"uint128","name":"tokensOwed1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"protocolFees","outputs":[{"internalType":"uint128","name":"token0","type":"uint128"},{"internalType":"uint128","name":"token1","type":"uint128"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint8","name":"feeProtocol0","type":"uint8"},{"internalType":"uint8","name":"feeProtocol1","type":"uint8"}],"name":"setFeeProtocol","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"slot0","outputs":[{"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"internalType":"int24","name":"tick","type":"int24"},{"internalType":"uint16","name":"observationIndex","type":"uint16"},{"internalType":"uint128","name":"observationCardinality","type":"uint128"},{"internalType":"uint128","name":"observationCardinalityNext","type":"uint128"},{"internalType":"uint8","name":"feeProtocol","type":"uint8"},{"internalType":"bool","name":"unlocked","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"tickLower","type":"int24"},{"internalType":"int24","name":"tickUpper","type":"int24"}],"name":"snapshotCumulativesInside","outputs":[{"internalType":"int56","name":"tickCumulativeInside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityInsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsInside","type":"uint32"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"recipient","type":"address"},{"internalType":"bool","name":"zeroForOne","type":"bool"},{"internalType":"int256","name":"amountSpecified","type":"int256"},{"internalType":"uint160","name":"sqrtPriceLimitX96","type":"uint160"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"swap","outputs":[{"internalType":"int256","name":"amount0","type":"int256"},{"internalType":"int256","name":"amount1","type":"int256"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"int16","name":"","type":"int16"}],"name":"tickBitmap","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"tickSpacing","outputs":[{"internalType":"int24","name":"","type":"int24"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"int24","name":"","type":"int24"}],"name":"ticks","outputs":[{"internalType":"uint128","name":"liquidityGross","type":"uint128"},{"internalType":"int128","name":"liquidityNet","type":"int128"},{"internalType":"uint256","name":"feeGrowthOutside0X128","type":"uint256"},{"internalType":"uint256","name":"feeGrowthOutside1X128","type":"uint256"},{"internalType":"int56","name":"tickCumulativeOutside","type":"int56"},{"internalType":"uint160","name":"secondsPerLiquidityOutsideX128","type":"uint160"},{"internalType":"uint32","name":"secondsOutside","type":"uint32"},{"internalType":"bool","name":"initialized","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	listener.InitPoolABI(v3PoolABI)

	// -----------------------------
	// 5. 订阅全网 Sync 和 Swap 事件（替代原来的池子订阅）
	// -----------------------------
	listener.SubscribeToAllSyncAndSwapEvents(client)

	// -----------------------------
	// 6. 启动SPFA定期生成候选路径的任务
	// -----------------------------
	go func() {
		for {
			// 定期运行SPFA算法生成候选路径并保存到Redis
			// 获取所有已审核的池子
			approvedPools, err := redis.GetAllApprovedPools()
			if err != nil {
				log.Printf("Failed to get approved pools: %v", err)
				time.Sleep(30 * time.Second)
				continue
			}

			// 为每个已审核的池子涉及的代币运行SPFA算法
			processedTokens := make(map[string]bool)
			for _, pool := range approvedPools {
				// 处理token0
				if !processedTokens[pool.Token0] {
					// 运行SPFA算法生成候选路径
					// 注意：这里需要实现将生成的路径存储到Redis的逻辑
					tokenGraph.SPFA(pool.Token0)
					processedTokens[pool.Token0] = true
				}

				// 处理token1
				if !processedTokens[pool.Token1] {
					// 运行SPFA算法生成候选路径
					// 注意：这里需要实现将生成的路径存储到Redis的逻辑
					tokenGraph.SPFA(pool.Token1)
					processedTokens[pool.Token1] = true
				}
			}

			log.Printf("Finished generating candidate paths for %d tokens", len(processedTokens))

			// 休眠一段时间再运行下一次
			time.Sleep(30 * time.Second)
		}
	}()

	// -----------------------------
	// 7. 初始化 Executor
	// -----------------------------
	atomicContract, err := atomic.NewAtomicArb(common.HexToAddress(atomicContractAddr), client)
	if err != nil {
		log.Fatalf("Load atomic contract failed: %v", err)
	}

	// 转换私钥为地址并获取初始余额
	var accountAddresses []common.Address
	for _, pk := range privateKeys {
		privateKey, err := crypto.HexToECDSA(pk)
		if err != nil {
			log.Printf("Invalid private key: %v", err)
			continue
		}
		publicKey := privateKey.Public()
		publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
		if !ok {
			log.Println("Error casting public key to ECDSA")
			continue
		}
		address := crypto.PubkeyToAddress(*publicKeyECDSA)
		accountAddresses = append(accountAddresses, address)
	}

	// 使用 multicall 方式批量获取账户余额并存储到 Redis
	balances, err := resolver.BatchFetchAccountBalances(client, accountAddresses)
	if err != nil {
		log.Printf("Failed to fetch account balances: %v", err)
	} else {
		// 将余额存储到 Redis
		for i, address := range accountAddresses {
			balance := balances[i]
			err := redis.SetAccountBalance(address.Hex(), balance.String())
			if err != nil {
				log.Printf("Failed to store balance for account %s: %v", address.Hex(), err)
			} else {
				log.Printf("Stored balance for account %s: %s", address.Hex(), balance.String())
			}
		}
	}

	exe := &executor.Executor{
		Client:        client,
		PrivateKeys:   privateKeys,
		Contract:      atomicContract,
		ChainID:       chainID,
		WorkerNum:     workerNum,
		PendingSwapCh: make(chan *graph.ArbitrageOpportunity, 100),
		AllowedTokens: allowedTokens,
	}

	// 设置TokenGraph的套利机会通道
	tokenGraph.SetPendingSwapCh(exe.PendingSwapCh)

	go exe.Run()

	// 保持主程序运行
	select {}
}
