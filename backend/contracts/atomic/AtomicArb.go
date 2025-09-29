package atomic

import (
	"context"
	"log"
	"math/big"

	"strings"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
)

// AtomicArb 智能合约封装
type AtomicArb struct {
	address     common.Address
	client      *ethclient.Client
	contractABI abi.ABI
}

// NewAtomicArb 创建 AtomicArb 对象
func NewAtomicArb(addr common.Address, client *ethclient.Client) (*AtomicArb, error) {
	// 合约 ABI 字符串（需要与 Solidity 合约一致）
	const atomicABI = `[{"inputs":[{"internalType":"address[]","name":"path","type":"address[]"},{"internalType":"uint256","name":"amountIn","type":"uint256"}],"name":"executeArb","outputs":[],"stateMutability":"nonpayable","type":"function"}]`

	parsedABI, err := abi.JSON(strings.NewReader(atomicABI))
	if err != nil {
		return nil, err
	}

	return &AtomicArb{
		address:     addr,
		client:      client,
		contractABI: parsedABI,
	}, nil
}

// ExecuteArb 调用合约 executeArb 方法
func (a *AtomicArb) ExecuteArb(auth *bind.TransactOpts, path []common.Address, amount *big.Int) (*types.Transaction, error) {
	// 打包参数
	input, err := a.contractABI.Pack("executeArb", path, amount)
	if err != nil {
		return nil, err
	}

	// 构造交易
	tx := types.NewTransaction(
		auth.Nonce.Uint64(),
		a.address,
		big.NewInt(0), // value 0
		auth.GasLimit,
		auth.GasPrice,
		input,
	)

	// 签名并发送交易
	signedTx, err := auth.Signer(auth.From, tx)
	if err != nil {
		return nil, err
	}

	err = a.client.SendTransaction(context.Background(), signedTx)
	if err != nil {
		return nil, err
	}

	log.Printf("executeArb tx sent: %s", signedTx.Hash().Hex())
	return signedTx, nil
}

// QueryArbResult 查询套利交易状态（可选）
func (a *AtomicArb) QueryArbResult(ctx context.Context, txHash common.Hash) (*types.Receipt, error) {
	receipt, err := a.client.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, err
	}
	return receipt, nil
}
