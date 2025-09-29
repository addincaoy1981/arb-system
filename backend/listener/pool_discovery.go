package listener

import (
	"context"
	"log"
	"strings"

	"arb-system/backend/redis"
	"arb-system/backend/resolver"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Factory ABI 示例，只包含 PairCreated
const factoryABIJson = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"}]`

var factoryABI2 abi.ABI

func init() {
	var err error
	factoryABI2, err = abi.JSON(strings.NewReader(factoryABIJson))
	if err != nil {
		log.Fatalf("Load Factory ABI failed: %v", err)
	}
}

// SubscribeFactory 监听 Factory 合约新建 Pool
func SubscribeFactory2(client *ethclient.Client, factoryAddr common.Address) {
	query := ethereum.FilterQuery{
		Addresses: []common.Address{factoryAddr},
		Topics:    [][]common.Hash{{factoryABI2.Events["PairCreated"].ID}},
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
				handlePairCreated2(client, vLog)
			}
		}
	}()
}

// handlePairCreated 解析 PairCreated 日志并进入 Redis 待审核
func handlePairCreated2(client *ethclient.Client, vLog types.Log) {
	token0 := common.BytesToAddress(vLog.Topics[1].Bytes())
	token1 := common.BytesToAddress(vLog.Topics[2].Bytes())

	var data struct {
		Pair common.Address
	}
	err := factoryABI2.UnpackIntoInterface(&data, "PairCreated", vLog.Data)
	if err != nil {
		log.Println("Unpack PairCreated failed:", err)
		return
	}

	poolAddr := data.Pair.Hex()
	log.Printf("[NewPool] discovered via Factory: %s %s-%s", poolAddr, token0.Hex(), token1.Hex())

	// 如果已经在待审核列表，就跳过
	if _, err := getPendingPool(poolAddr); err == nil {
		log.Printf("[PendingPool] %s already exists, skip", poolAddr)
		return
	}

	// 获取池子元数据 (symbol/decimals/reserves/sizeUSD)
	// 使用 multicall 批量获取代币信息
	meta := resolver.ResolvePoolMetadata(client, poolAddr, token0.Hex(), token1.Hex())
	if meta == nil {
		log.Printf("[PendingPool] failed to resolve metadata for pool %s", poolAddr)
		return
	}

	// 写入 Redis → pending_pools
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

	if err := setPendingPool(redisMeta); err != nil {
		log.Printf("[PendingPool] %s save failed: %v", poolAddr, err)
		return
	}

	log.Printf("[PendingPool] %s [%s-%s] size=%.2f ETH",
		meta.Address, meta.Token0Symbol, meta.Token1Symbol, meta.SizeETH)
}

// setPendingPool 保存待审核池子（不会覆盖已存在）
func setPendingPool(meta *redis.PoolMetadata) error {
	return redis.SetPendingPool(meta)
}

// getPendingPool 获取单个待审核池
func getPendingPool(address string) (*redis.PoolMetadata, error) {
	return redis.GetPendingPool(address)
}
