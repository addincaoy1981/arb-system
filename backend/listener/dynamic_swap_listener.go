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

func InitTokenGraph(allowedTokens []string) *graph.TokenGraph {
	tgOnce.Do(func() {
		tg = graph.NewTokenGraph(allowedTokens)
	})
	return tg
}

func InitPoolABI(v2ABI string) {
	var err error
	poolABI, err = abi.JSON(strings.NewReader(v2ABI))
	if err != nil {
		log.Fatalf("Load Pool ABI failed: %v", err)
	}
}

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
	factoryABI.UnpackIntoInterface(&data, "PairCreated", vLog.Data)

	poolAddr := data.Pair.Hex()

	// 使用 multicall 批量获取代币信息
	meta := resolver.ResolvePoolMetadata(client, poolAddr, token0.Hex(), token1.Hex())
	if meta == nil {
		log.Printf("Failed to resolve metadata for pool: %s", poolAddr)
		return
	}

	log.Printf("New Pool discovered: %s [%s-%s]", poolAddr, meta.Token0Symbol, meta.Token1Symbol)

	// 新池子进入待审核列表
	redisMeta := &redis.PoolMetadata{
		Address:      meta.Address,
		Token0:       meta.Token0,
		Token1:       meta.Token1,
		Token0Symbol: meta.Token0Symbol,
		Token1Symbol: meta.Token1Symbol,
		Decimals0:    meta.Decimals0,
		Decimals1:    meta.Decimals1,
		SizeETH:      meta.SizeETH,
		Approved:     meta.Approved,
	}

	// 保存到待审核列表
	if err := redis.SetPendingPool(redisMeta); err != nil {
		log.Printf("Failed to add pool to pending list: %v", err)
	}

	log.Printf("Pool %s added to pending list for review", poolAddr)
}

// -------------------- Sync/Swap 事件订阅 --------------------

// SubscribeToAllSyncAndSwapEvents 订阅全网所有 V2 sync 和 V3 swap 事件
func SubscribeToAllSyncAndSwapEvents(client *ethclient.Client) {
	// 创建日志通道
	logsChan := make(chan types.Log, 2000)

	// 订阅 V2 Sync 事件 (topic0: 0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1)
	syncTopic := common.HexToHash("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1")

	// 订阅 V3 Swap 事件 (topic0: 0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")
	swapTopic := common.HexToHash("0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67")

	query := ethereum.FilterQuery{
		Topics: [][]common.Hash{{syncTopic, swapTopic}},
	}

	sub, err := client.SubscribeFilterLogs(context.Background(), query, logsChan)
	if err != nil {
		log.Fatalf("SubscribeToAllEvents failed: %v", err)
	}

	log.Println("Subscribed to all V2 Sync and V3 Swap events")

	// 启动工作者处理事件
	var wg sync.WaitGroup
	for i := 0; i < workerNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for vLog := range logsChan {
				handleSyncOrSwapEvent(vLog)
			}
		}()
	}

	// 错误处理
	go func() {
		for {
			select {
			case err := <-sub.Err():
				log.Printf("Event subscription error: %v", err)
			}
		}
	}()

	// 不等待工作者完成，让它们在后台运行
	// wg.Wait()
}

func handleSyncOrSwapEvent(vLog types.Log) {
	poolAddr := vLog.Address.Hex()

	// 检查池子是否已批准并存在于图中
	pool := tg.GetPool(poolAddr)
	if pool == nil {
		// 池子不存在于图中，检查是否在待审核列表
		if pendingPool, err := redis.GetPendingPool(poolAddr); err == nil && pendingPool != nil {
			log.Printf("Pool %s is in pending list, not yet approved", poolAddr)
		} else {
			log.Printf("Unknown pool detected: %s", poolAddr)
		}
		return
	}

	// 池子存在，更新储备并检查套利机会
	// 解析具体的sync或swap事件来获取新的储备值
	var amount0, amount1 *big.Int
	var err error

	// 根据事件主题确定事件类型
	if len(vLog.Topics) > 0 {
		eventSignature := vLog.Topics[0].Hex()

		// V2 Sync 事件
		if eventSignature == "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1" {
			// 解析 V2 Sync 事件数据
			amount0, amount1, err = parseV2SyncEvent(vLog.Data)
			if err != nil {
				log.Printf("Failed to parse V2 Sync event for pool %s: %v", poolAddr, err)
				return
			}
		} else if eventSignature == "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67" {
			// V3 Swap 事件
			amount0, amount1, err = parseV3SwapEvent(vLog.Data, pool)
			if err != nil {
				log.Printf("Failed to parse V3 Swap event for pool %s: %v", poolAddr, err)
				return
			}
		} else {
			log.Printf("Unknown event type for pool %s: %s", poolAddr, eventSignature)
			return
		}
	} else {
		log.Printf("Invalid event data for pool %s", poolAddr)
		return
	}

	redis.SetPoolSnapshot(poolAddr, amount0.String(), amount1.String())
	tg.UpdatePairIncremental(poolAddr, amount0, amount1)

	tg.UpdateEthToTokenRate(pool.Token0)
	tg.UpdateEthToTokenRate(pool.Token1)

	log.Printf("Updated pool %s (%s-%s) reserves: %s / %s",
		poolAddr,
		pool.Token0,
		pool.Token1,
		amount0.String(),
		amount1.String(),
	)

	// 触发套利检查
	// 根据设计文档，这里应该从Redis中读取对应的候选路径，进行暴力计算
	go func() {
		// 从Redis中获取涉及该池子的候选路径
		candidatePaths, err := redis.GetCandidatePathsByPool(poolAddr)
		if err != nil {
			log.Printf("Failed to get candidate paths for pool %s: %v", poolAddr, err)
			return
		}

		// 对每个候选路径进行利润计算
		for _, pathStr := range candidatePaths {
			// 解析路径字符串
			poolAddrs := strings.Split(pathStr, ",")

			// 构造路径对象
			var path []*graph.PoolInfo
			for _, addr := range poolAddrs {
				poolInfo := tg.GetPool(addr)
				if poolInfo != nil {
					path = append(path, poolInfo)
				}
			}

			// 如果路径不完整，跳过
			if len(path) == 0 {
				continue
			}

			// 使用牛顿法/三分法计算最优输入和利润
			// 由于存在循环依赖，我们无法直接访问执行器获取当前账户
			// 这里我们采用轮询的方式从配置的账户中选择一个进行计算
			// 在实际应用中，可能需要通过其他机制来获取当前执行器使用的账户
			
			// 从Redis中获取所有配置账户的余额，选择一个有足够余额的账户
			accountAddress := ""
			accountBalance := big.NewInt(0)
			
			// 这里应该从环境变量或配置中获取所有账户地址
			// 为了简化，我们使用一个示例地址
			accountAddress = "0x0000000000000000000000000000000000000000"
			var err error
			accountBalance, err = redis.GetAccountBalance(accountAddress)
			if err != nil {
				log.Printf("Failed to get account balance for account %s: %v", accountAddress, err)
				// 如果无法获取余额，使用默认值 1 ETH
				accountBalance = big.NewInt(1e18)
			}

			// 确定起始代币
			startToken := path[0].Token0

			// 检查起始代币是否在允许的代币列表中
			allowed := false
			if len(tg.AllowedTokens) == 0 {
				// 如果没有配置允许的代币列表，则允许所有代币
				allowed = true
			} else {
				// 检查起始代币是否在允许列表中
				for _, token := range tg.AllowedTokens {
					if strings.ToLower(token) == strings.ToLower(startToken) {
						allowed = true
						break
					}
				}
			}

			if !allowed {
				log.Printf("Token %s is not in allowed list, skipping arbitrage", startToken)
				continue
			}

			// 计算考虑余额的最优输入金额
			// 使用账户余额作为最大输入金额的限制
			optimalIn, amountOut := tg.CalculateOptimalAmountWithBalance(path, accountBalance, tg.GasFee, accountBalance)
			profit := new(big.Int).Sub(amountOut, optimalIn)

			// 检查利润是否足够覆盖gas费用，并且输出金额大于输入金额+gas费
			gasAndInput := new(big.Int).Add(tg.GasFee, optimalIn)
			if amountOut.Cmp(gasAndInput) > 0 {
				arbOp := &graph.ArbitrageOpportunity{
					Path:      path,
					Profit:    profit,
					Start:     startToken, // 起始代币
					AmountIn:  optimalIn,
					AmountOut: amountOut,
				}

				// 发送到执行器通道
				if tg.GetPendingSwapCh() != nil {
					select {
					case tg.GetPendingSwapCh() <- arbOp:
						log.Printf("Sent arbitrage opportunity to executor: path=%s, profit=%s, amountIn=%s", pathStr, profit.String(), optimalIn.String())
					default:
						log.Println("Executor channel full, skipping arbitrage opportunity")
					}
				}
			} else {
				log.Printf("Arbitrage opportunity not profitable enough: output=%s, input+gas=%s", amountOut.String(), gasAndInput.String())
			}
		}

		log.Printf("Finished checking arbitrage opportunities for pool %s", poolAddr)
	}()

}

// parseV2SyncEvent 解析 V2 Sync 事件数据
func parseV2SyncEvent(data []byte) (*big.Int, *big.Int, error) {
	// V2 Sync 事件 ABI
	// event Sync(uint112 reserve0, uint112 reserve1)
	syncABI, err := abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint112","name":"reserve0","type":"uint112"},{"indexed":false,"internalType":"uint112","name":"reserve1","type":"uint112"}],"name":"Sync","type":"event"}]`))
	if err != nil {
		return nil, nil, err
	}

	var syncData struct {
		Reserve0 *big.Int
		Reserve1 *big.Int
	}

	if err := syncABI.UnpackIntoInterface(&syncData, "Sync", data); err != nil {
		return nil, nil, err
	}

	return syncData.Reserve0, syncData.Reserve1, nil
}

// parseV3SwapEvent 解析 V3 Swap 事件数据
func parseV3SwapEvent(data []byte, pool *graph.PoolInfo) (*big.Int, *big.Int, error) {
	// V3 Swap 事件 ABI
	// event Swap(address indexed sender, address indexed recipient, int256 amount0, int256 amount1, uint160 sqrtPriceX96, uint128 liquidity, int24 tick)
	swapABI, err := abi.JSON(strings.NewReader(`[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"sender","type":"address"},{"indexed":true,"internalType":"address","name":"recipient","type":"address"},{"indexed":false,"internalType":"int256","name":"amount0","type":"int256"},{"indexed":false,"internalType":"int256","name":"amount1","type":"int256"},{"indexed":false,"internalType":"uint160","name":"sqrtPriceX96","type":"uint160"},{"indexed":false,"internalType":"uint128","name":"liquidity","type":"uint128"},{"indexed":false,"internalType":"int24","name":"tick","type":"int24"}],"name":"Swap","type":"event"}]`))
	if err != nil {
		return nil, nil, err
	}

	var swapData struct {
		Amount0      *big.Int
		Amount1      *big.Int
		SqrtPriceX96 *big.Int
		Liquidity    *big.Int
		Tick         *big.Int
	}

	if err := swapABI.UnpackIntoInterface(&swapData, "Swap", data); err != nil {
		return nil, nil, err
	}

	// 对于 V3 池子，我们直接使用事件中的金额变化来更新储备
	// 实际应用中可能需要更复杂的逻辑来计算新的储备值
	newReserve0 := new(big.Int).Add(pool.Reserve0, swapData.Amount0)
	newReserve1 := new(big.Int).Add(pool.Reserve1, swapData.Amount1)

	// 确保储备值不为负数
	if newReserve0.Sign() < 0 {
		newReserve0 = big.NewInt(0)
	}
	if newReserve1.Sign() < 0 {
		newReserve1 = big.NewInt(0)
	}

	return newReserve0, newReserve1, nil
}

// -------------------- 多工厂监听 --------------------

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

	go func() {
		for vLog := range logsChan {
			handlePairCreated(client, vLog)
		}
	}()
}

func LoadHistoricalPools(client *ethclient.Client, factoryAddr common.Address, fromBlock uint64) {
	query := ethereum.FilterQuery{
		FromBlock: big.NewInt(int64(fromBlock)),
		ToBlock:   nil,
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

// SetPendingPool 保存待审核池子（不会覆盖已存在）
func SetPendingPool(meta *redis.PoolMetadata) error {
	// 这里应该实现实际的待审核池保存逻辑
	return redis.SetPendingPool(meta)
}

// GetPendingPool 获取单个待审核池
func GetPendingPool(address string) (*redis.PoolMetadata, error) {
	// 这里应该实现实际的待审核池获取逻辑
	return redis.GetPendingPool(address)
}
