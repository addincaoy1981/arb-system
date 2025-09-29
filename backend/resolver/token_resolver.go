package resolver

import (
	"context"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"arb-system/backend/redis"
)

// TokenMeta 保存代币元信息
type TokenMeta struct {
	Address  string
	Symbol   string
	Name     string
	Decimals uint8
}

var (
	tokenCache   = make(map[string]*TokenMeta) // 内存缓存
	cacheMu      sync.RWMutex
	erc20ABI     abi.ABI
	multicallABI abi.ABI
)

// 初始化 ERC20 ABI
func InitERC20ABI() {
	abiJSON := `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"type":"function"},
		{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"type":"function"}]`

	var err error
	erc20ABI, err = abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		log.Fatalf("Init ERC20 ABI failed: %v", err)
	}

	// Multicall ABI
	multicallJSON := `[{"constant":true,"inputs":[{"components":[{"name":"target","type":"address"},{"name":"callData","type":"bytes"}],"name":"calls","type":"tuple[]"}],"name":"aggregate","outputs":[{"name":"blockNumber","type":"uint256"},{"name":"returnData","type":"bytes[]"}],"payable":false,"stateMutability":"nonpayable","type":"function"}]`
	multicallABI, err = abi.JSON(strings.NewReader(multicallJSON))
	if err != nil {
		log.Fatalf("Init Multicall ABI failed: %v", err)
	}
}

// GetTokenMeta 获取代币信息（优先内存 -> Redis -> 链上）
func GetTokenMeta(client *ethclient.Client, addr string) *TokenMeta {
	cacheMu.RLock()
	if meta, ok := tokenCache[addr]; ok {
		cacheMu.RUnlock()
		return meta
	}
	cacheMu.RUnlock()

	// 从 Redis 取
	redisMeta := redis.GetTokenMeta(addr)
	if redisMeta != nil {
		meta := &TokenMeta{
			Address:  redisMeta.Address,
			Symbol:   redisMeta.Symbol,
			Name:     redisMeta.Name,
			Decimals: redisMeta.Decimals,
		}
		cacheMu.Lock()
		tokenCache[addr] = meta
		cacheMu.Unlock()
		return meta
	}

	// 链上读取
	newMeta, err := fetchFromChain(client, addr)
	if err != nil {
		log.Printf("fetch token meta failed for %s: %v", addr, err)
		return nil
	}

	// 存缓存 & Redis
	cacheMu.Lock()
	tokenCache[addr] = newMeta
	cacheMu.Unlock()

	redisMeta = &redis.TokenMeta{
		Address:  newMeta.Address,
		Symbol:   newMeta.Symbol,
		Name:     newMeta.Name,
		Decimals: newMeta.Decimals,
	}
	redis.SetTokenMeta(addr, redisMeta)

	return newMeta
}

// fetchFromChain 从链上获取 ERC20 信息
func fetchFromChain(client *ethclient.Client, addr string) (*TokenMeta, error) {
	contract := common.HexToAddress(addr)

	// 调用 name()
	nameBytes, err := client.CallContract(context.Background(), buildCallMsg(contract, "name"), nil)
	if err != nil {
		return nil, err
	}
	name, _ := erc20ABI.Methods["name"].Outputs.Unpack(nameBytes)

	// 调用 symbol()
	symbolBytes, err := client.CallContract(context.Background(), buildCallMsg(contract, "symbol"), nil)
	if err != nil {
		return nil, err
	}
	symbol, _ := erc20ABI.Methods["symbol"].Outputs.Unpack(symbolBytes)

	// 调用 decimals()
	decBytes, err := client.CallContract(context.Background(), buildCallMsg(contract, "decimals"), nil)
	if err != nil {
		return nil, err
	}
	dec, _ := erc20ABI.Methods["decimals"].Outputs.Unpack(decBytes)

	meta := &TokenMeta{
		Address:  addr,
		Name:     name[0].(string),
		Symbol:   symbol[0].(string),
		Decimals: dec[0].(uint8),
	}
	return meta, nil
}

// buildCallMsg 构造合约调用消息
func buildCallMsg(addr common.Address, method string) (call ethereum.CallMsg) {
	data, _ := erc20ABI.Pack(method)
	call = ethereum.CallMsg{
		To:   &addr,
		Data: data,
	}
	return
}

// -------------------- 批量获取 (Multicall) --------------------

// BatchFetchTokenMeta 批量获取代币信息（symbol/decimals/name）
func BatchFetchTokenMeta(client *ethclient.Client, tokens []common.Address) ([]*TokenMeta, error) {
	// 从环境变量读取 multicall 地址
	multicallAddrStr := os.Getenv("MULTICALL_ADDRESS")
	if multicallAddrStr == "" {
		return nil, fmt.Errorf("MULTICALL_ADDRESS not set")
	}
	multicallAddr := common.HexToAddress(multicallAddrStr)

	type Call struct {
		Target   common.Address
		CallData []byte
	}
	calls := []Call{}
	for _, token := range tokens {
		dataSymbol, _ := erc20ABI.Pack("symbol")
		dataDec, _ := erc20ABI.Pack("decimals")
		dataName, _ := erc20ABI.Pack("name")
		calls = append(calls,
			Call{Target: token, CallData: dataSymbol},
			Call{Target: token, CallData: dataDec},
			Call{Target: token, CallData: dataName},
		)
	}

	input, _ := multicallABI.Pack("aggregate", calls)
	msg := ethereum.CallMsg{To: &multicallAddr, Data: input}
	output, err := client.CallContract(context.Background(), msg, nil)
	if err != nil {
		return nil, err
	}

	// 用 struct 接收返回
	var result struct {
		BlockNumber *big.Int
		ReturnData  [][]byte
	}
	if err := multicallABI.UnpackIntoInterface(&result, "aggregate", output); err != nil {
		return nil, err
	}

	metas := []*TokenMeta{}
	for i := 0; i < len(tokens); i++ {
		var symbol string
		var name string
		var decimals uint8

		// 容错解码
		if out, err := erc20ABI.Methods["symbol"].Outputs.Unpack(result.ReturnData[i*3]); err == nil && len(out) > 0 {
			symbol, _ = out[0].(string)
		}
		if out, err := erc20ABI.Methods["decimals"].Outputs.Unpack(result.ReturnData[i*3+1]); err == nil && len(out) > 0 {
			decimals, _ = out[0].(uint8)
		}
		if out, err := erc20ABI.Methods["name"].Outputs.Unpack(result.ReturnData[i*3+2]); err == nil && len(out) > 0 {
			name, _ = out[0].(string)
		}

		meta := &TokenMeta{
			Address:  tokens[i].Hex(),
			Symbol:   symbol,
			Name:     name,
			Decimals: decimals,
		}

		cacheMu.Lock()
		tokenCache[meta.Address] = meta
		cacheMu.Unlock()

		redisMeta := &redis.TokenMeta{
			Address:  meta.Address,
			Symbol:   meta.Symbol,
			Name:     meta.Name,
			Decimals: meta.Decimals,
		}
		redis.SetTokenMeta(meta.Address, redisMeta)

		metas = append(metas, meta)
	}
	return metas, nil
}

// BatchFetchAccountBalances 批量获取账户余额
func BatchFetchAccountBalances(client *ethclient.Client, accounts []common.Address) ([]*big.Int, error) {
	balances := []*big.Int{}
	for _, account := range accounts {
		balance, err := client.BalanceAt(context.Background(), account, nil)
		if err != nil {
			log.Printf("Failed to get balance for account %s: %v", account.Hex(), err)
			balances = append(balances, big.NewInt(0))
		} else {
			balances = append(balances, balance)
		}
	}

	return balances, nil
}

// -------------------- 后台刷新 --------------------

// StartBackgroundRefresh 定期刷新缓存中的代币信息（同样使用 Multicall）
func StartBackgroundRefresh(client *ethclient.Client, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for {
			<-ticker.C

			cacheMu.RLock()
			addrs := []common.Address{}
			for addr := range tokenCache {
				addrs = append(addrs, common.HexToAddress(addr))
			}
			cacheMu.RUnlock()

			if len(addrs) == 0 {
				continue
			}

			_, err := BatchFetchTokenMeta(client, addrs)
			if err != nil {
				log.Printf("refresh token meta batch failed: %v", err)
			}
		}
	}()
}

// GetTokenPriceInETH 获取 Token 的 ETH 价格
func GetTokenPriceInETH(tokenAddr string) *big.Float {
	// 稳定币 → 直接返回 1/ETH (即 USD/ETH 的倒数)
	if strings.Contains(strings.ToLower(tokenAddr), "usdt") ||
		strings.Contains(strings.ToLower(tokenAddr), "usdc") ||
		strings.Contains(strings.ToLower(tokenAddr), "dai") {
		// 假设有 ETH/USD 价格，可以从 Redis 或 TokenGraph 拿
		ethPriceUSD := 1800.0 // TODO: 替换成真实 ETH/USD 价格来源
		return big.NewFloat(1.0 / ethPriceUSD)
	}

	// 其他 Token 走 TokenGraph
	// 注意：由于循环依赖，我们不能直接引用graph包
	// 这里应该通过其他方式获取汇率信息
	return big.NewFloat(0) // 没有价格
}

// -------------------- 池子元数据解析 --------------------

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

// ResolvePoolMetadata 使用 Multicall 一次性获取池子元数据
func ResolvePoolMetadata(client *ethclient.Client, poolAddr string, token0Addr string, token1Addr string) *PoolMetadata {
	ctx := context.Background()
	pool := common.HexToAddress(poolAddr)

	// 1. 如果没传 token0/token1 地址，从池子里取
	if token0Addr == "" || token1Addr == "" {
		token0Addr, token1Addr = fetchPoolTokens(ctx, client, pool)
		if token0Addr == "" || token1Addr == "" {
			log.Printf("ResolvePoolMetadata: fetchPoolTokens failed for %s", poolAddr)
			return nil
		}
	}

	// 2. 批量获取 token0/token1 的 meta
	tokens := []common.Address{common.HexToAddress(token0Addr), common.HexToAddress(token1Addr)}
	metas, err := BatchFetchTokenMeta(client, tokens)
	if err != nil || len(metas) != 2 {
		log.Printf("ResolvePoolMetadata: BatchFetchTokenMeta failed for %s", poolAddr)
		return nil
	}
	meta0, meta1 := metas[0], metas[1]

	// 3. 获取 reserves
	res0, res1 := fetchPoolReserves(ctx, client, pool)

	// 4. 计算池子大小 (USD 占位，需要结合你已有的价格模块)
	sizeETH := calcPoolSizeETH(meta0, meta1, res0, res1)

	// 5. 返回 PoolMetadata
	return &PoolMetadata{
		Address:      poolAddr,
		Token0:       token0Addr,
		Token1:       token1Addr,
		Token0Symbol: meta0.Symbol,
		Token1Symbol: meta1.Symbol,
		Decimals0:    meta0.Decimals,
		Decimals1:    meta1.Decimals,
		SizeETH:      sizeETH,
		Approved:     false,
	}
}

// fetchPoolTokens 调用池子 token0()/token1()
func fetchPoolTokens(ctx context.Context, client *ethclient.Client, pool common.Address) (string, string) {
	poolABIJson := `[{"inputs":[],"name":"token0","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},
	{"inputs":[],"name":"token1","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"}]`
	poolABI, _ := abi.JSON(strings.NewReader(poolABIJson))

	// token0()
	data0, _ := poolABI.Pack("token0")
	out0, err := client.CallContract(ctx, ethereum.CallMsg{To: &pool, Data: data0}, nil)
	if err != nil || len(out0) == 0 {
		return "", ""
	}
	token0, _ := poolABI.Methods["token0"].Outputs.Unpack(out0)

	// token1()
	data1, _ := poolABI.Pack("token1")
	out1, err := client.CallContract(ctx, ethereum.CallMsg{To: &pool, Data: data1}, nil)
	if err != nil || len(out1) == 0 {
		return "", ""
	}
	token1, _ := poolABI.Methods["token1"].Outputs.Unpack(out1)

	return token0[0].(common.Address).Hex(), token1[0].(common.Address).Hex()
}

// fetchPoolReserves 调用 getReserves()
func fetchPoolReserves(ctx context.Context, client *ethclient.Client, pool common.Address) (*big.Int, *big.Int) {
	poolABIJson := `[{"inputs":[],"name":"getReserves","outputs":[{"internalType":"uint112","name":"reserve0","type":"uint112"},
	{"internalType":"uint112","name":"reserve1","type":"uint112"},
	{"internalType":"uint32","name":"blockTimestampLast","type":"uint32"}],"stateMutability":"view","type":"function"}]`
	poolABI, _ := abi.JSON(strings.NewReader(poolABIJson))

	data, _ := poolABI.Pack("getReserves")
	out, err := client.CallContract(ctx, ethereum.CallMsg{To: &pool, Data: data}, nil)
	if err != nil || len(out) == 0 {
		return big.NewInt(0), big.NewInt(0)
	}
	res, _ := poolABI.Methods["getReserves"].Outputs.Unpack(out)
	return res[0].(*big.Int), res[1].(*big.Int)
}

// calcPoolSizeETH 计算池子规模 (以 ETH 计价)
func calcPoolSizeETH(meta0 *TokenMeta, meta1 *TokenMeta, res0 *big.Int, res1 *big.Int) float64 {
	// 转换成 float
	f0 := new(big.Float).SetInt(res0)
	f1 := new(big.Float).SetInt(res1)

	// 考虑 decimals
	dec0 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(meta0.Decimals)), nil))
	dec1 := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(meta1.Decimals)), nil))

	normalized0 := new(big.Float).Quo(f0, dec0)
	normalized1 := new(big.Float).Quo(f1, dec1)

	// 获取 Token 的 ETH 价格
	price0ETH := GetTokenPriceInETH(meta0.Address)
	price1ETH := GetTokenPriceInETH(meta1.Address)

	// 计算池子价值 (ETH)
	v0 := new(big.Float).Mul(normalized0, price0ETH)
	v1 := new(big.Float).Mul(normalized1, price1ETH)

	total := new(big.Float).Add(v0, v1)
	val, _ := total.Float64()
	return val
}
