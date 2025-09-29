package tools

import (
	"log"
	"math/big"

	"arb-system/backend/graph"
	"arb-system/backend/redis"

	"github.com/ethereum/go-ethereum/ethclient"
)

// PoolRaw 包含链上池子地址及类型
type PoolRaw struct {
	PoolAddress string
	Token0      string
	Token1      string
	Fee         float64
	IsV3        bool
}

// InitPools 初始化池子数据到 Redis 和内存
func InitPools(client *ethclient.Client, pools []PoolRaw) []*graph.PoolInfo {
	var result []*graph.PoolInfo

	for _, p := range pools {
		poolInfo := &graph.PoolInfo{
			PoolAddress: p.PoolAddress,
			Token0:      p.Token0,
			Token1:      p.Token1,
			Fee:         p.Fee,
			IsV3:        p.IsV3,
		}

		if p.IsV3 {
			// 调用 V3 Quoter 获取精确储备 / sqrtPriceX96
			amount0, amount1 := queryV3Reserves(client, p.PoolAddress, p.Token0, p.Token1)
			poolInfo.Reserve0 = amount0
			poolInfo.Reserve1 = amount1
		} else {
			// V2: 调用 getReserves()
			amount0, amount1 := queryV2Reserves(client, p.PoolAddress)
			poolInfo.Reserve0 = amount0
			poolInfo.Reserve1 = amount1
		}

		// Redis 缓存
		redis.SetPoolSnapshot(p.PoolAddress, poolInfo.Reserve0.String(), poolInfo.Reserve1.String())

		result = append(result, poolInfo)
		log.Printf("Initialized pool %s: %s/%s -> %s/%s", p.PoolAddress,
			poolInfo.Reserve0.String(), poolInfo.Reserve1.String(),
			poolInfo.Token0, poolInfo.Token1)
	}

	return result
}

// queryV2Reserves 查询 V2 池子储备
func queryV2Reserves(client *ethclient.Client, poolAddr string) (*big.Int, *big.Int) {
	// TODO: 使用 go-ethereum ABI 调用 getReserves()
	// 返回 reserve0, reserve1
	return big.NewInt(1e18), big.NewInt(5e17) // 占位示例
}

// queryV3Reserves 查询 V3 池子储备
func queryV3Reserves(client *ethclient.Client, poolAddr, token0, token1 string) (*big.Int, *big.Int) {
	// TODO: 调用 V3 Quoter 或 slot0 查询 sqrtPriceX96
	// 并计算精确储备
	return big.NewInt(1e18), big.NewInt(5e17) // 占位示例
}
