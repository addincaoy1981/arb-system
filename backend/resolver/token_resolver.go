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
	if meta := redis.GetTokenMeta(addr); meta != nil {
		cacheMu.Lock()
		tokenCache[addr] = meta
		cacheMu.Unlock()
		return meta
	}

	// 链上读取
	meta, err := fetchFromChain(client, addr)
	if err != nil {
		log.Printf("fetch token meta failed for %s: %v", addr, err)
		return nil
	}

	// 存缓存 & Redis
	cacheMu.Lock()
	tokenCache[addr] = meta
	cacheMu.Unlock()
	redis.SetTokenMeta(addr, meta)

	return meta
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
		redis.SetTokenMeta(meta.Address, meta)

		metas = append(metas, meta)
	}
	return metas, nil
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
