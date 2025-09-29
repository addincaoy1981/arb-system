package service

import (
	"context"
	"log"

	"arb-system/backend/graph"
	"arb-system/backend/redis"
	"arb-system/backend/resolver"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ApproveAndActivatePool 审核通过池子 → 加入 TokenGraph → 开启监听
func ApproveAndActivatePool(client *ethclient.Client, poolAddr string, tg *graph.TokenGraph) error {
	meta, err := redis.ApprovePool(poolAddr)
	if err != nil {
		return err
	}

	// 审核时重新获取最新 reserves
	r0, r1 := resolver.FetchPoolReserves(context.Background(), client, common.HexToAddress(meta.Address))
	if r0 == nil || r1 == nil {
		log.Printf("[ApprovePool] GetReserves failed for pool %s", poolAddr)
		return nil
	}

	// 加入 TokenGraph
	tg.AddPool(&graph.PoolInfo{
		PoolAddress: meta.Address,
		Token0:      meta.Token0,
		Token1:      meta.Token1,
		IsV3:        false,
		Reserve0:    r0,
		Reserve1:    r1,
		Decimals0:   int(meta.Decimals0),
		Decimals1:   int(meta.Decimals1),
		FeeNumer:    997,
		FeeDenom:    1000,
	})

	log.Printf("[ApprovePool] Pool %s (%s-%s) 已加入 TokenGraph, 最新储备 = %s / %s",
		meta.Address, meta.Token0Symbol, meta.Token1Symbol, r0.String(), r1.String())

	return nil
}

// ApproveAllLargePools 批量审核池子（过滤小池子）
func ApproveAllLargePools(client *ethclient.Client, tg *graph.TokenGraph, minETH float64) {
	pools, err := redis.GetAllPendingPools()
	if err != nil {
		log.Printf("[ApproveAllLargePools] 获取待审核池失败: %v", err)
		return
	}

	for _, p := range pools {
		if p.Approved {
			continue
		}
		if p.SizeETH < minETH {
			log.Printf("[ApproveAllLargePools] 跳过小池 %s (%s-%s), size=%.4f ETH",
				p.Address, p.Token0Symbol, p.Token1Symbol, p.SizeETH)
			continue
		}

		// 审核并激活
		err := ApproveAndActivatePool(client, p.Address, tg)
		if err != nil {
			log.Printf("[ApproveAllLargePools] 审核池 %s 失败: %v", p.Address, err)
		}
	}
}
