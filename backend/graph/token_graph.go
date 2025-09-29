package graph

import (
	"log"
	"math/big"
	"sync"

	"arb-system/backend/redis"
)

// PoolInfo 保存单个池子信息
type PoolInfo struct {
	PoolAddress string
	Token0      string
	Token1      string
	Reserve0    *big.Int
	Reserve1    *big.Int
	IsV3        bool
	Decimals0   int   // token0 精度
	Decimals1   int   // token1 精度
	FeeNumer    int64 // 手续费分子，例如 997 (表示 0.3%)
	FeeDenom    int64 // 手续费分母，例如 1000
}

// GetOtherToken 返回池子里与 token 不同的另一个 token
func (p *PoolInfo) GetOtherToken(token string) string {
	if token == p.Token0 {
		return p.Token1
	}
	return p.Token0
}

// ArbitrageOpportunity 表示一次套利机会
type ArbitrageOpportunity struct {
	Path      []*PoolInfo
	Profit    *big.Int
	Start     string
	AmountIn  *big.Int
	AmountOut *big.Int
}

// TokenGraph 管理所有 Pool 和 Token 节点
type TokenGraph struct {
	Pools          map[string]*PoolInfo   // poolAddress -> PoolInfo
	Tokens         map[string][]*PoolInfo // tokenAddress -> 相关池子
	mu             sync.RWMutex
	GasFee         *big.Int                   // gas 费用 (wei)
	EthToToken     map[string]*big.Float      // ETH -> token rate，用于换算 gas
	MaxDepth       int                        // 最大路径长度
	MaxSlippageBps int                        // 最大滑点 (单位: bps, 100 = 1%)
	PendingSwapCh  chan *ArbitrageOpportunity // 套利机会通道
	AllowedTokens  []string                   // 允许套利的代币地址列表
}

// NewTokenGraph 创建空 TokenGraph
func NewTokenGraph(allowedTokens []string) *TokenGraph {
	return &TokenGraph{
		Pools:          make(map[string]*PoolInfo),
		Tokens:         make(map[string][]*PoolInfo),
		GasFee:         big.NewInt(1e15), // 默认 0.001 ETH
		EthToToken:     make(map[string]*big.Float),
		MaxDepth:       5,
		MaxSlippageBps: 200, // 默认 2%
		PendingSwapCh:  nil, // 需要在初始化时设置
		AllowedTokens:  allowedTokens,
	}
}

// AddPool 添加池子到 Graph
func (tg *TokenGraph) AddPool(p *PoolInfo) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	tg.Pools[p.PoolAddress] = p
	tg.Tokens[p.Token0] = append(tg.Tokens[p.Token0], p)
	tg.Tokens[p.Token1] = append(tg.Tokens[p.Token1], p)
}

// UpdatePairIncremental 更新池子并增量检测套利
func (tg *TokenGraph) UpdatePairIncremental(poolAddr string, reserve0, reserve1 *big.Int) {
	tg.mu.Lock()
	pool, ok := tg.Pools[poolAddr]
	if !ok {
		tg.mu.Unlock()
		return
	}
	pool.Reserve0 = new(big.Int).Set(reserve0)
	pool.Reserve1 = new(big.Int).Set(reserve1)
	tg.mu.Unlock()

	// 自动刷新 ETH->Token 汇率
	tg.UpdateEthToTokenRate(pool.Token0)
	tg.UpdateEthToTokenRate(pool.Token1)

	startTokens := []string{pool.Token0, pool.Token1}
	for _, token := range startTokens {
		go tg.SPFA(token)
	}
}

// UpdateEthToTokenRate 根据最新池子价格刷新 ETH->Token 汇率
func (tg *TokenGraph) UpdateEthToTokenRate(token string) {
	tg.mu.Lock()
	defer tg.mu.Unlock()

	for _, pool := range tg.Tokens[token] {
		if pool.Token0 == "ETH" || pool.Token1 == "ETH" {
			var ethReserve, tokenReserve *big.Int
			var decimalsEth, decimalsToken int
			if pool.Token0 == "ETH" {
				ethReserve = pool.Reserve0
				tokenReserve = pool.Reserve1
				decimalsEth = pool.Decimals0
				decimalsToken = pool.Decimals1
			} else {
				ethReserve = pool.Reserve1
				tokenReserve = pool.Reserve0
				decimalsEth = pool.Decimals1
				decimalsToken = pool.Decimals0
			}
			if ethReserve.Sign() == 0 || tokenReserve.Sign() == 0 {
				continue
			}
			nEth := normalizeTo18(ethReserve, decimalsEth)
			nToken := normalizeTo18(tokenReserve, decimalsToken)
			rate := new(big.Float).Quo(new(big.Float).SetInt(nToken), new(big.Float).SetInt(nEth))
			tg.EthToToken[token] = rate
			break
		}
	}
}

// SPFA 增量套利检测 + 最优投入量计算
func (tg *TokenGraph) SPFA(startToken string) {
	type QueueNode struct {
		Token     string
		Path      []*PoolInfo
		AmountIn  *big.Int
		AmountOut *big.Int
		Depth     int
	}

	oneEther := big.NewInt(1e18)
	queue := []QueueNode{{Token: startToken, Path: []*PoolInfo{}, AmountIn: oneEther, AmountOut: oneEther, Depth: 0}}
	results := []*ArbitrageOpportunity{}

	visited := make(map[string]int)
	minProfit := tg.calculateMinProfit(startToken)

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if prevDepth, ok := visited[node.Token]; ok && node.Depth >= prevDepth {
			continue
		}
		visited[node.Token] = node.Depth

		if node.Depth >= tg.MaxDepth {
			continue
		}

		pools := tg.GetPoolsByToken(node.Token)
		for _, pool := range pools {
			nextToken := pool.GetOtherToken(node.Token)
			amountOut, ok := simulateSwapWithSlippage(pool, node.Token, node.AmountOut, tg.MaxSlippageBps)
			if !ok {
				continue
			}

			newPath := append([]*PoolInfo{}, node.Path...)
			newPath = append(newPath, pool)

			if nextToken == startToken {
				optimalIn, optimalOut := tg.calculateOptimalAmount(newPath, node.AmountIn, minProfit)
				profit := new(big.Int).Sub(optimalOut, optimalIn)
				if profit.Cmp(minProfit) > 0 {
					results = append(results, &ArbitrageOpportunity{
						Path:      newPath,
						Profit:    profit,
						Start:     startToken,
						AmountIn:  optimalIn,
						AmountOut: optimalOut,
					})
				}
			} else {
				queue = append(queue, QueueNode{
					Token:     nextToken,
					Path:      newPath,
					AmountIn:  node.AmountIn,
					AmountOut: amountOut,
					Depth:     node.Depth + 1,
				})
			}
		}
	}

	// 将候选路径存储到Redis中
	if len(results) > 0 {
		// 提取路径信息并存储到Redis
		var paths []string
		for _, result := range results {
			// 构造路径字符串
			var pathStr string
			for i, pool := range result.Path {
				if i > 0 {
					pathStr += ","
				}
				pathStr += pool.PoolAddress
			}
			paths = append(paths, pathStr)
		}

		// 存储到Redis
		err := redis.SetCandidatePaths(startToken, paths)
		if err != nil {
			log.Printf("Failed to store candidate paths to Redis: %v", err)
		} else {
			log.Printf("Stored %d candidate paths for token %s to Redis", len(paths), startToken)
		}
	}

	if len(results) > 0 {
		log.Printf("SPFA found %d arbitrage opportunities starting from %s", len(results), startToken)
		for _, arb := range results {
			log.Printf("Arb path %s, profit: %s wei, amountIn: %s, amountOut: %s",
				arb.Start, arb.Profit.String(), arb.AmountIn.String(), arb.AmountOut.String())

			// 如果发现利润超过阈值的套利机会，将其发送到执行器通道
			// 执行器会处理原子合约套利交易
			if tg.PendingSwapCh != nil {
				select {
				case tg.PendingSwapCh <- arb:
					log.Printf("Sent arbitrage opportunity to executor: %s", arb.Start)
				default:
					log.Println("Executor channel full, skipping arbitrage opportunity")
				}
			}
		}
	}
}

// simulateSwapWithSlippage 模拟 swap，考虑手续费和滑点
func simulateSwapWithSlippage(pool *PoolInfo, inputToken string, amountIn *big.Int, maxSlippageBps int) (*big.Int, bool) {
	if amountIn.Sign() == 0 {
		return big.NewInt(0), true
	}
	var resIn, resOut *big.Int
	var decimalsIn, decimalsOut int

	if inputToken == pool.Token0 {
		resIn = pool.Reserve0
		resOut = pool.Reserve1
		decimalsIn = pool.Decimals0
		decimalsOut = pool.Decimals1
	} else {
		resIn = pool.Reserve1
		resOut = pool.Reserve0
		decimalsIn = pool.Decimals1
		decimalsOut = pool.Decimals0
	}

	if resIn.Sign() == 0 || resOut.Sign() == 0 {
		return big.NewInt(0), false
	}

	amountInWithFee := new(big.Int).Mul(amountIn, big.NewInt(pool.FeeNumer))
	amountInWithFee.Div(amountInWithFee, big.NewInt(pool.FeeDenom))

	normIn := normalizeTo18(amountInWithFee, decimalsIn)
	nResIn := normalizeTo18(resIn, decimalsIn)
	nResOut := normalizeTo18(resOut, decimalsOut)

	amountOut := new(big.Int).Mul(normIn, nResOut)
	amountOut.Div(amountOut, new(big.Int).Add(nResIn, normIn))

	if amountOut.Sign() == 0 {
		return big.NewInt(0), false
	}

	// 滑点计算
	theoreticalPrice := new(big.Float).Quo(new(big.Float).SetInt(resOut), new(big.Float).SetInt(resIn))
	execPrice := new(big.Float).Quo(new(big.Float).SetInt(amountOut), new(big.Float).SetInt(amountIn))
	diff := new(big.Float).Sub(execPrice, theoreticalPrice)
	if diff.Sign() < 0 {
		diff.Neg(diff)
	}
	slippage := new(big.Float).Quo(diff, theoreticalPrice)

	slippageF, _ := slippage.Float64()
	if slippageF*10000 > float64(maxSlippageBps) {
		return big.NewInt(0), false
	}

	return denormalizeFrom18(amountOut, decimalsOut), true
}

// calculateOptimalAmount 二分搜索最优投入量
func (tg *TokenGraph) calculateOptimalAmount(path []*PoolInfo, maxAmount *big.Int, minProfit *big.Int) (*big.Int, *big.Int) {
	low := big.NewInt(1e12)
	high := new(big.Int).Set(maxAmount)
	bestIn := new(big.Int)
	bestProfit := big.NewInt(0)
	bestOut := new(big.Int)

	for low.Cmp(high) <= 0 {
		mid := new(big.Int).Add(low, high)
		mid.Div(mid, big.NewInt(2))
		amountOut := tg.SimulatePath(path, mid, tg.MaxSlippageBps)
		profit := new(big.Int).Sub(amountOut, mid)

		if profit.Cmp(bestProfit) > 0 {
			bestProfit.Set(profit)
			bestIn.Set(mid)
			bestOut.Set(amountOut)
		}

		if profit.Cmp(minProfit) < 0 {
			low.Add(mid, big.NewInt(1))
		} else {
			high.Sub(mid, big.NewInt(1))
		}
	}

	return bestIn, bestOut
}

// SimulatePath 模拟整个路径 swap 输出（公共接口）
func (tg *TokenGraph) SimulatePath(path []*PoolInfo, amountIn *big.Int, maxSlippageBps int) *big.Int {
	amount := new(big.Int).Set(amountIn)
	tokenIn := path[0].Token0
	for _, pool := range path {
		out, ok := simulateSwapWithSlippage(pool, tokenIn, amount, maxSlippageBps)
		if !ok {
			return big.NewInt(0)
		}
		amount = out
		tokenIn = pool.GetOtherToken(tokenIn)
	}
	return amount
}

// CalculateOptimalAmountWithBalance 根据账户余额计算最优套利金额
func (tg *TokenGraph) CalculateOptimalAmountWithBalance(path []*PoolInfo, maxAmount *big.Int, minProfit *big.Int, balance *big.Int) (*big.Int, *big.Int) {
	// 如果余额小于最大金额，则使用余额作为上限
	actualMax := new(big.Int).Set(maxAmount)
	if balance.Cmp(maxAmount) < 0 {
		actualMax.Set(balance)
	}

	// 使用二分法计算最优金额
	low := big.NewInt(1e12)
	high := new(big.Int).Set(actualMax)
	bestIn := new(big.Int)
	bestProfit := big.NewInt(0)
	bestOut := new(big.Int)

	for low.Cmp(high) <= 0 {
		mid := new(big.Int).Add(low, high)
		mid.Div(mid, big.NewInt(2))
		amountOut := tg.SimulatePath(path, mid, tg.MaxSlippageBps)
		profit := new(big.Int).Sub(amountOut, mid)

		if profit.Cmp(bestProfit) > 0 {
			bestProfit.Set(profit)
			bestIn.Set(mid)
			bestOut.Set(amountOut)
		}

		if profit.Cmp(minProfit) < 0 {
			low.Add(mid, big.NewInt(1))
		} else {
			high.Sub(mid, big.NewInt(1))
		}
	}

	return bestIn, bestOut
}

// calculateMinProfit 根据 start token 自动换算 gas 费用
func (tg *TokenGraph) calculateMinProfit(startToken string) *big.Int {
	rate, ok := tg.EthToToken[startToken]
	if !ok {
		return tg.GasFee
	}
	gasWei := new(big.Float).SetInt(tg.GasFee)
	profitFloat := new(big.Float).Mul(gasWei, rate)
	profitInt, _ := profitFloat.Int(nil)
	return profitInt
}

// GetPoolsByToken 返回包含 token 的所有池子
func (tg *TokenGraph) GetPool(poolAddr string) *PoolInfo {
	tg.mu.RLock()
	defer tg.mu.RUnlock()
	pool, ok := tg.Pools[poolAddr]
	if !ok {
		return nil
	}
	return pool
}

func (tg *TokenGraph) GetPoolsByToken(token string) []*PoolInfo {
	tg.mu.RLock()
	defer tg.mu.RUnlock()
	return tg.Tokens[token]
}

// GetAllPools 返回所有池子列表
func (tg *TokenGraph) GetAllPools() []*PoolInfo {
	tg.mu.RLock()
	defer tg.mu.RUnlock()
	pools := make([]*PoolInfo, 0, len(tg.Pools))
	for _, p := range tg.Pools {
		pools = append(pools, p)
	}
	return pools
}

// FindArbitrageDFS 从指定 startToken 搜索套利路径
func (tg *TokenGraph) FindArbitrageDFS(minProfit *big.Int, startToken string) []*ArbitrageOpportunity {
	// 简化版本，实际应该使用更复杂的算法
	var results []*ArbitrageOpportunity
	// 这里应该实现实际的套利检测逻辑
	return results
}

// GetPendingSwapCh 获取套利机会通道
func (tg *TokenGraph) GetPendingSwapCh() chan *ArbitrageOpportunity {
	tg.mu.RLock()
	defer tg.mu.RUnlock()
	return tg.PendingSwapCh
}

// SetPendingSwapCh 设置套利机会通道
func (tg *TokenGraph) SetPendingSwapCh(ch chan *ArbitrageOpportunity) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	tg.PendingSwapCh = ch
}

// SetExecutor 设置执行器引用
func (tg *TokenGraph) SetExecutor(executor interface{}) {
	tg.mu.Lock()
	defer tg.mu.Unlock()
	// 这里我们暂时不实现，因为会引入循环依赖
}

// ---------- 辅助函数 (decimals 统一换算) ----------

func normalizeTo18(amount *big.Int, decimals int) *big.Int {
	if decimals == 18 {
		return new(big.Int).Set(amount)
	}
	diff := 18 - decimals
	if diff > 0 {
		return new(big.Int).Mul(amount, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil))
	}
	return new(big.Int).Div(amount, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-diff)), nil))
}

func denormalizeFrom18(amount *big.Int, decimals int) *big.Int {
	if decimals == 18 {
		return new(big.Int).Set(amount)
	}
	diff := 18 - decimals
	if diff > 0 {
		return new(big.Int).Div(amount, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(diff)), nil))
	}
	return new(big.Int).Mul(amount, new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(-diff)), nil))
}
