package executor

import (
	"context"
	"log"
	"math/big"
	"sync"

	"arb-system/backend/arb"
	"arb-system/backend/db"
	"arb-system/backend/redis"

	"arb-system/backend/contracts/atomic" // 前面生成的原子套利智能合约绑定

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Executor 配置
type Executor struct {
	Client        *ethclient.Client
	PrivateKey    string
	Contract      *atomic.AtomicArb // 原子套利合约绑定
	ChainID       *big.Int
	WorkerNum     int
	PendingSwapCh chan *arb.ArbitrageOpportunity
}

// 启动 Worker 池
func (e *Executor) Run() {
	var wg sync.WaitGroup
	for i := 0; i < e.WorkerNum; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for op := range e.PendingSwapCh {
				e.ExecuteArbitrage(op)
			}
		}(i)
	}
	wg.Wait()
}

// ExecuteArbitrage 执行整个套利路径原子交易
func (e *Executor) ExecuteArbitrage(op *arb.ArbitrageOpportunity) {
	ctx := context.Background()
	auth, err := e.newTransactOpts()
	if err != nil {
		log.Println("Failed to create auth:", err)
		return
	}

	// 构建 SwapStep 数组
	poolMap := make(map[string]*arb.PoolInfo) // 从内存图中获取
	steps, err := arb.BuildSwapSteps(op, poolMap)
	if err != nil {
		log.Println("Build SwapSteps error:", err)
		return
	}

	// 调用原子套利智能合约 executeArb
	tx, err := e.Contract.ExecuteArb(auth, steps)
	if err != nil {
		log.Println("Arbitrage execution failed:", err)
		return
	}

	// 成功后更新 DB
	if err := db.UpdateArbitrageExecution(tx.Hash().Hex(), true); err != nil {
		log.Println("DB update failed:", err)
	}

	// 更新 Redis 快照
	for _, s := range steps {
		redis.SetPoolSnapshot(s.PoolAddress, s.AmountIn.String(), s.AmountOut.String())
	}

	log.Printf("Arbitrage executed: path=%v, profit=%s, tx=%s\n", op.Path, op.Profit.String(), tx.Hash().Hex())
}

// newTransactOpts 构造 auth
func (e *Executor) newTransactOpts() (*bind.TransactOpts, error) {
	privateKey, err := crypto.HexToECDSA(e.PrivateKey)
	if err != nil {
		return nil, err
	}
	auth, err := bind.NewKeyedTransactorWithChainID(privateKey, e.ChainID)
	if err != nil {
		return nil, err
	}
	auth.GasLimit = uint64(500000) // 可动态估算
	auth.Context = context.Background()
	return auth, nil
}
