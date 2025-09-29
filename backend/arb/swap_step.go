package arb

import (
	"fmt"
	"math/big"

	"arb-system/backend/graph"
)

// SwapType 表示是 V2 或 V3
type SwapType int

const (
	V2 SwapType = iota
	V3
)

// SwapStep 表示单步交换
type SwapStep struct {
	PoolAddress string
	TokenIn     string
	TokenOut    string
	AmountIn    *big.Int
	AmountOut   *big.Int
	SwapType    SwapType
	Fee         float64 // 仅对 V3 有用
}

// BuildSwapSteps 将 ArbitrageOpportunity 转换为 SwapStep 数组
func BuildSwapSteps(op *graph.ArbitrageOpportunity, poolMap map[string]*graph.PoolInfo) ([]SwapStep, error) {
	path := op.Path
	if len(path) < 2 {
		return nil, fmt.Errorf("path too short")
	}

	steps := make([]SwapStep, 0, len(path)-1)
	currentAmount := big.NewInt(1e18) // 假设初始量为 1 token（可改为实际起始量）

	for i := 0; i < len(path)-1; i++ {
		tokenIn := path[i]
		tokenOut := path[i+1]

		// 根据 token pair 获取池子
		var pool *graph.PoolInfo
		poolKey1 := tokenIn + "_" + tokenOut
		poolKey2 := tokenOut + "_" + tokenIn
		if p, ok := poolMap[poolKey1]; ok {
			pool = p
		} else if p, ok := poolMap[poolKey2]; ok {
			pool = p
		} else {
			return nil, fmt.Errorf("pool not found for %s-%s", tokenIn, tokenOut)
		}

		var amountOut *big.Int
		if pool.IsV3 {
			// V3精确计算，调用 Quoter 合约
			out, err := pool.QueryV3AmountOut(tokenIn, tokenOut, currentAmount)
			if err != nil {
				return nil, fmt.Errorf("V3 Quoter error: %v", err)
			}
			amountOut = out
		} else {
			// V2精确恒定乘积公式
			if tokenIn == pool.Token0 {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve0, pool.Reserve1, pool.Fee)
			} else {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve1, pool.Reserve0, pool.Fee)
			}
		}

		stepType := V2
		if pool.IsV3 {
			stepType = V3
		}

		step := SwapStep{
			PoolAddress: pool.PairAddress,
			TokenIn:     tokenIn,
			TokenOut:    tokenOut,
			AmountIn:    new(big.Int).Set(currentAmount),
			AmountOut:   new(big.Int).Set(amountOut),
			SwapType:    stepType,
			Fee:         pool.Fee,
		}

		steps = append(steps, step)
		currentAmount = amountOut
	}

	return steps, nil
}

// CalcV2AmountOut 精确恒定乘积公式（考虑手续费）
func CalcV2AmountOut(amountIn, reserveIn, reserveOut *big.Int, fee float64) *big.Int {
	// fee 例如 0.003
	amountInFloat := new(big.Float).SetInt(amountIn)
	feeMul := new(big.Float).SetFloat64(1 - fee)
	amountInAfterFee := new(big.Float).Mul(amountInFloat, feeMul)

	reserveInFloat := new(big.Float).SetInt(reserveIn)
	reserveOutFloat := new(big.Float).SetInt(reserveOut)

	numerator := new(big.Float).Mul(amountInAfterFee, reserveOutFloat)
	denominator := new(big.Float).Add(reserveInFloat, amountInAfterFee)
	outFloat := new(big.Float).Quo(numerator, denominator)

	out := new(big.Int)
	outFloat.Int(out)
	return out
}
