package pool

import (
	"context"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"

	"arb-system/backend/redis"
	"arb-system/backend/storage"

	"arb-system/backend/contracts/uniswapv2factory" // abigen 生成的 Factory Go binding
)

type PoolSyncer struct {
	Client      *ethclient.Client
	Factory     *uniswapv2factory.Uniswapv2factory
	DB          *storage.Postgres
	Redis       *redis.Redis
	FactoryAddr common.Address
}

// NewPoolSyncer
func NewPoolSyncer(client *ethclient.Client, factoryAddr common.Address, db *storage.Postgres, r *redis.Redis) (*PoolSyncer, error) {
	factory, err := uniswapv2factory.NewUniswapv2factory(factoryAddr, client)
	if err != nil {
		return nil, err
	}
	return &PoolSyncer{
		Client:      client,
		Factory:     factory,
		DB:          db,
		Redis:       r,
		FactoryAddr: factoryAddr,
	}, nil
}

// 全量同步 Factory 池子
func (ps *PoolSyncer) SyncAllPools(ctx context.Context) error {
	length, err := ps.Factory.AllPairsLength(&bind.CallOpts{Context: ctx})
	if err != nil {
		return err
	}
	log.Printf("Factory has %d pools\n", length.Int64())

	for i := int64(0); i < length.Int64(); i++ {
		poolAddr, err := ps.Factory.AllPairs(&bind.CallOpts{Context: ctx}, big.NewInt(i))
		if err != nil {
			log.Printf("Fetch pool %d error: %v", i, err)
			continue
		}

		// 检查 DB 是否已存在
		exists, _ := ps.DB.PoolExists(ctx, poolAddr.Hex())
		if !exists {
			log.Printf("New pool discovered: %s", poolAddr.Hex())
			ps.DB.InsertPool(ctx, poolAddr.Hex(), "", "", ps.FactoryAddr.Hex()) // token0/token1 可后续更新
			ps.Redis.AddActivePool(ctx, poolAddr.Hex())
		}
	}

	return nil
}

// 定期 Resync
func (ps *PoolSyncer) PeriodicResync(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			log.Println("Running periodic pool resync...")
			if err := ps.SyncAllPools(ctx); err != nil {
				log.Printf("Pool resync error: %v", err)
			}
		case <-ctx.Done():
			return
		}
	}
}
