package listener

import (
	"context"
	"log"
	"math/big"
	"strings"
	"sync"

	"arb-system/backend/graph"
	"arb-system/backend/redis"
	"arb-system/backend/resolver"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// 工厂配置
type FactoryConfig struct {
	Name    string
	Address common.Address
}

var poolABI abi.ABI
var factoryABI abi.ABI
var poolABIMutex sync.Mutex

const batchSize = 50
const workerNum = 10

var tg *graph.TokenGraph
var tgOnce sync.Once

// DebugLog 控制是否打印详细日志
var DebugLog = true

// -------------------- 初始化 --------------------

// 初始化 TokenGraph 单例
func InitTokenGraph(gasFee *big.Int) *graph.TokenGraph {
	tgOnce.Do(func() {
		tg = graph.NewTokenGraph(gasFee)
	})
	return tg
}

// 初始化 Pool ABI (V2)
func InitPoolABI(v2ABI string) {
	var err error
	poolABI, err = abi.JSON(strings.NewReader(v2ABI))
	if err != nil {
		log.Fatalf("Load Pool ABI failed: %v", err)
	}
}

// 初始化 Factory ABI (只包含 PairCreated)
func InitFactoryABI(factoryABIJson string) {
	var err error
	factoryABI, err = abi.JSON(strings.NewReader(factoryABIJson))
	if err != nil {
		log.Fatalf("Load Factory ABI failed: %v", err)
	}
}

// -------------------- Factory 监听 --------------------

func SubscribeFactory(client *ethclient.Client, factoryAddr common.Address) {
	query := ethereum.FilterQuery{
		Addresses: []common.Address{factoryAddr},
		Topics:    [][]common.Hash{{factoryABI.Events["PairCreated"].ID}},
	}

	logsChan := make(chan types.Log, 100)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logsChan)
	if err != nil {
		log.Fatalf("SubscribeFactory failed: %v", err)
	}

	go func() {
		for {
			select {
			case err := <-sub.Err():
				log.Println("Factory subscription error:", err)
			case vLog := <-logsChan:
				handlePairCreated(client, vLog)
			}
		}
	}()
}

func handlePairCreated(client *ethclient.Client, vLog types.Log) {
	token0 := common.BytesToAddress(vLog.Topics[1].Bytes())
	token1 := common.BytesToAddress(vLog.Topics[2].Bytes())

	var data struct {
		Pair common.Address
	}
	err := factoryABI.UnpackIntoInterface(&data, "PairCreated", vLog.Data)
	if err != nil {
		log.Println("Unpack PairCreated failed:", err)
		return
	}

	poolAddr := data.Pair.Hex()

	// --- 获取代币元数据 (Symbol/Decimals) ---
	meta0 := resolver.GetTokenMeta(client, token0.Hex())
	meta1 := resolver.GetTokenMeta(client, token1.Hex())

	token0Symbol := token0.Hex()
	token1Symbol := token1.Hex()
	decimals0 := 18
	decimals1 := 18
	if meta0 != nil {
		token0Symbol = meta0.Symbol
		decimals0 = int(meta0.Decimals)
	}
	if meta1 != nil {
		token1Symbol = meta1.Symbol
		decimals1 = int(meta1.Decimals)
	}

	log.Printf("New Pool discovered: %s [%s-%s]", poolAddr, token0Symbol, token1Symbol)

	// --- 添加池子到 TokenGraph ---
	tg.AddPool(&graph.PoolInfo{
		PoolAddress: poolAddr,
		Token0:      token0.Hex(),
		Token1:      token1.Hex(),
		IsV3:        false, // 只考虑 V2
		Reserve0:    big.NewInt(0),
		Reserve1:    big.NewInt(0),
		Decimals0:   decimals0,
		Decimals1:   decimals1,
		FeeNumer:    997,  // 默认 Uniswap V2 费率 (0.3%)
		FeeDenom:    1000, // 997/1000
	})

	// Redis 初始化快照
	redis.SetPoolSnapshot(poolAddr, "0", "0")

	// 启动 Swap 订阅
	subscribeSwap(client, tg.GetPool(poolAddr))
}

// -------------------- 高性能 Swap 订阅 --------------------

func SubscribeDynamicPoolsOptimized(client *ethclient.Client) {
	allPools := tg.GetAllPools()
	total := len(allPools)
	log.Printf("Total pools to subscribe: %d", total)

	logChan := make(chan types.Log, 2000)
	var wg sync.WaitGroup

	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for vLog := range logChan {
				handleSwapLog(vLog)
			}
		}()
	}

	for i := 0; i < total; i += batchSize {
		end := i + batchSize
		if end > total {
			end = total
		}
		batch := allPools[i:end]

		addresses := make([]common.Address, 0, len(batch))
		for _, p := range batch {
			addresses = append(addresses, common.HexToAddress(p.PoolAddress))
		}

		query := ethereum.FilterQuery{
			Addresses: addresses,
		}

		poolLogsChan := make(chan types.Log, 1000)
		sub, err := client.SubscribeFilterLogs(context.Background(), query, poolLogsChan)
		if err != nil {
			log.Printf("Batch subscribe failed: %v", err)
			continue
		}

		go func() {
			for {
				select {
				case err := <-sub.Err():
					log.Println("Subscription error:", err)
				case vLog := <-poolLogsChan:
					logChan <- vLog
				}
			}
		}()
	}

	wg.Wait()
}

// -------------------- Swap 事件处理 --------------------

func subscribeSwap(client *ethclient.Client, pool *graph.PoolInfo) {
	if pool == nil {
		return
	}
	query := ethereum.FilterQuery{
		Addresses: []common.Address{common.HexToAddress(pool.PoolAddress)},
	}
	logsChan := make(chan types.Log, 100)
	sub, err := client.SubscribeFilterLogs(context.Background(), query, logsChan)
	if err != nil {
		log.Printf("SubscribeFilterLogs failed for pool %s: %v", pool.PoolAddress, err)
		return
	}

	go func() {
		for {
			select {
			case err := <-sub.Err():
				log.Printf("Swap subscription error for pool %s: %v", pool.PoolAddress, err)
			case vLog := <-logsChan:
				handleSwapLog(vLog)
			}
		}
	}()
}

func handleSwapLog(vLog types.Log) {
	poolAddr := vLog.Address.Hex()
	pool := tg.GetPool(poolAddr) // ✅ 线程安全获取
	if pool == nil {
		log.Println("Pool not found in graph:", poolAddr)
		return
	}

	var amount0, amount1 *big.Int
	var err error

	if pool.IsV3 {
		amount0, amount1, err = parseV3SwapLog(vLog, pool)
		if err != nil {
			log.Println("parseV3SwapLog error:", err)
			return
		}
	} else {
		amount0, amount1 = pool.Reserve0, pool.Reserve1
	}

	redis.SetPoolSnapshot(poolAddr, amount0.String(), amount1.String())
	tg.UpdatePairIncremental(poolAddr, amount0, amount1)

	// 自动更新 ETH->Token 汇率
	tg.UpdateEthToTokenRate(pool.Token0)
	tg.UpdateEthToTokenRate(pool.Token1)

	// 打印时用 Symbol + Decimals 转换
	meta0 := resolver.GetTokenMeta(nil, pool.Token0)
	meta1 := resolver.GetTokenMeta(nil, pool.Token1)
	log.Printf("Updated pool %s (%s-%s) reserves: %s / %s",
		poolAddr,
		safeSymbol(meta0, pool.Token0),
		safeSymbol(meta1, pool.Token1),
		formatWithDecimals(amount0, pool.Decimals0),
		formatWithDecimals(amount1, pool.Decimals1),
	)
}

func safeSymbol(meta *resolver.TokenMeta, addr string) string {
	if meta != nil {
		return meta.Symbol
	}
	return addr
}

// 格式化 reserve 数量，按照 decimals 转成带小数点的字符串
func formatWithDecimals(amount *big.Int, decimals int) string {
	if decimals == 0 {
		return amount.String()
	}
	f := new(big.Float).SetInt(amount)
	divisor := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	f.Quo(f, divisor)
	return f.Text('f', 6)
}

// 订阅多个 DEX 工厂的 PairCreated 事件
func SubscribeFactories(client *ethclient.Client, factories []FactoryConfig) {
	logsChan := make(chan types.Log, 2000)

	for _, f := range factories {
		query := ethereum.FilterQuery{
			Addresses: []common.Address{f.Address},
			Topics:    [][]common.Hash{{factoryABI.Events["PairCreated"].ID}},
		}

		sub, err := client.SubscribeFilterLogs(context.Background(), query, logsChan)
		if err != nil {
			log.Printf("SubscribeFactory failed for %s: %v", f.Name, err)
			continue
		}

		go func(name string) {
			for {
				select {
				case err := <-sub.Err():
					log.Printf("Factory %s subscription error: %v", name, err)
				}
			}
		}(f.Name)

		log.Printf("✅ Subscribed to factory: %s (%s)", f.Name, f.Address.Hex())
	}

	// 统一处理 PairCreated 日志
	go func() {
		for vLog := range logsChan {
			handlePairCreated(client, vLog)
		}
	}()
}

// LoadHistoricalPools 拉取工厂里历史创建的池子
func LoadHistoricalPools(client *ethclient.Client, factoryAddr common.Address, fromBlock uint64) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   nil, // latest
		Addresses: []common.Address{factoryAddr},
		Topics:    [][]common.Hash{{factoryABI.Events["PairCreated"].ID}},
	}

	logs, err := client.FilterLogs(context.Background(), query)
	if err != nil {
		log.Printf("LoadHistoricalPools failed for factory %s: %v", factoryAddr.Hex(), err)
		return
	}

	log.Printf("Found %d historical pools in factory %s", len(logs), factoryAddr.Hex())
	for _, vLog := range logs {
		handlePairCreated(client, vLog)
	}
}
