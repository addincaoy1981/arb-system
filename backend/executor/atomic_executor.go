package executor

import (
	"context"
	"crypto/ecdsa"
	"log"
	"math/big"
	"sync"

	"arb-system/backend/db"
	"arb-system/backend/graph"
	"arb-system/backend/redis"

	"arb-system/backend/contracts/atomic" // 前面生成的原子套利智能合约绑定

	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

// Executor 配置
type Executor struct {
	Client          *ethclient.Client
	PrivateKeys     []string          // 支持多个私钥
	Contract        *atomic.AtomicArb // 原子套利合约绑定
	ChainID         *big.Int
	WorkerNum       int
	PendingSwapCh   chan *graph.ArbitrageOpportunity
	AllowedTokens   []string // 允许套利的代币地址
	privateKeyIndex int      // 当前使用的私钥索引
	mutex           sync.Mutex
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

// getNextPrivateKey 获取下一个私钥（轮询）
func (e *Executor) getNextPrivateKey() string {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	privateKey := e.PrivateKeys[e.privateKeyIndex]
	e.privateKeyIndex = (e.privateKeyIndex + 1) % len(e.PrivateKeys)
	return privateKey
}

// ExecuteArbitrage 执行整个套利路径原子交易
func (e *Executor) ExecuteArbitrage(op *graph.ArbitrageOpportunity) {
	// 获取下一个私钥
	privateKeyStr := e.getNextPrivateKey()

	auth, err := e.newTransactOpts(privateKeyStr)
	if err != nil {
		log.Println("Failed to create auth:", err)
		return
	}

	// 构建路径和金额
	var path []common.Address
	for _, pool := range op.Path {
		path = append(path, common.HexToAddress(pool.PoolAddress))
	}

	// 调用原子套利智能合约 executeArb
	tx, err := e.Contract.ExecuteArb(auth, path, op.AmountIn)
	if err != nil {
		log.Println("Arbitrage execution failed:", err)
		return
	}

	// 成功后更新 DB
	if err := db.UpdateArbitrageExecution(tx.Hash().Hex(), true); err != nil {
		log.Println("DB update failed:", err)
	}

	// 更新 Redis 快照
	for _, pool := range op.Path {
		redis.SetPoolSnapshot(pool.PoolAddress, pool.Reserve0.String(), pool.Reserve1.String())
	}

	log.Printf("Arbitrage executed: path=%v, profit=%s, tx=%s\n", path, op.Profit.String(), tx.Hash().Hex())

	// 套利完成后，更新执行套利账户的余额
	// 从私钥获取地址
	privateKey, err := crypto.HexToECDSA(privateKeyStr)
	if err != nil {
		log.Printf("Failed to parse private key: %v", err)
		return
	}
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Println("Error casting public key to ECDSA")
		return
	}
	address := crypto.PubkeyToAddress(*publicKeyECDSA)

	// 获取账户新余额
	balance, err := e.Client.BalanceAt(context.Background(), address, nil)
	if err != nil {
		log.Printf("Failed to get balance for account %s: %v", address.Hex(), err)
		return
	}

	// 更新 Redis 中的余额
	err = redis.SetAccountBalance(address.Hex(), balance.String())
	if err != nil {
		log.Printf("Failed to update balance for account %s: %v", address.Hex(), err)
	} else {
		log.Printf("Updated balance for account %s: %s", address.Hex(), balance.String())
	}
}

// newTransactOpts 构造 auth
func (e *Executor) newTransactOpts(privateKeyStr string) (*bind.TransactOpts, error) {
	privateKey, err := crypto.HexToECDSA(privateKeyStr)
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

// GetCurrentAccountAddress 获取当前使用的账户地址
func (e *Executor) GetCurrentAccountAddress() string {
	e.mutex.Lock()
	defer e.mutex.Unlock()

	// 获取当前私钥对应的地址
	privateKeyStr := e.PrivateKeys[e.privateKeyIndex]
	privateKey, err := crypto.HexToECDSA(privateKeyStr)
	if err != nil {
		log.Printf("Failed to parse private key: %v", err)
		return ""
	}
	publicKey := privateKey.Public()
	publicKeyECDSA, ok := publicKey.(*ecdsa.PublicKey)
	if !ok {
		log.Println("Error casting public key to ECDSA")
		return ""
	}
	address := crypto.PubkeyToAddress(*publicKeyECDSA)
	return address.Hex()
}
