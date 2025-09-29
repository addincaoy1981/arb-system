package listener

import (
	"context"
	"log"
	"strings"

	"arb-system/backend/graph"
	"arb-system/backend/redis"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Factory ABI 示例，只包含 PairCreated
const factoryABIJson = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"token0","type":"address"},{"indexed":true,"internalType":"address","name":"token1","type":"address"},{"indexed":false,"internalType":"address","name":"pair","type":"address"},{"indexed":false,"internalType":"uint256","name":"","type":"uint256"}],"name":"PairCreated","type":"event"}]`

var factoryABI abi.ABI

func init() {
	var err error
	factoryABI, err = abi.JSON(strings.NewReader(factoryABIJson))
	if err != nil {
		log.Fatalf("Load Factory ABI failed: %v", err)
	}
}

// SubscribeFactory 监听 Factory 合约新建 Pool
func SubscribeFactory(client *ethclient.Client, factoryAddr common.Address, tg *graph.TokenGraph) {
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
				handlePairCreated(vLog, tg)
			}
		}
	}()
}

// handlePairCreated 解析 PairCreated 日志并加入 TokenGraph + 自动订阅 Swap
func handlePairCreated(vLog types.Log, tg *graph.TokenGraph) {
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
	log.Printf("New Pool discovered: %s %s-%s", poolAddr, token0.Hex(), token1.Hex())

	// 默认 V3/fee，可根据实际 ABI 调整
	isV3 := true
	fee := 0.003

	// 添加到 TokenGraph
	tg.AddPool(poolAddr, token0.Hex(), token1.Hex(), fee, isV3)

	// 初始化 Redis 快照
	redis.SetPoolSnapshot(poolAddr, "0", "0")

	// 自动订阅新 Pool 的 Swap
	if tg.Client != nil {
		pool := tg.Pools[poolAddr]
		if pool != nil {
			go SubscribeSwap(tg.Client, pool, tg)
		}
	}
}
