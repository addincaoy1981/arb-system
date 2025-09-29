package arb

import (
	"arb-system/backend/graph"
	"math/big"
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
func BuildSwapSteps(op *graph.ArbitrageOpportunity) []SwapStep {
	path := op.Path
	if len(path) < 1 {
		return nil
	}

	steps := make([]SwapStep, 0, len(path))
	currentAmount := op.AmountIn // 使用实际的起始量

	for i := 0; i < len(path); i++ {
		pool := path[i]

		var amountOut *big.Int
		if pool.IsV3 {
			// V3精确计算，调用 Quoter 合约
			// 实际实现中应该调用 Uniswap V3 Quoter 合约来获取精确的交换结果
			// 这里使用简化的计算方法作为示例
			if pool.Token0 == pool.PoolAddress {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve0, pool.Reserve1,
					float64(pool.FeeNumer)/float64(pool.FeeDenom))
			} else {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve1, pool.Reserve0,
					float64(pool.FeeNumer)/float64(pool.FeeDenom))
			}
		} else {
			// V2池子使用恒定乘积公式计算
			if pool.Token0 == pool.PoolAddress {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve0, pool.Reserve1,
					float64(pool.FeeNumer)/float64(pool.FeeDenom))
			} else {
				amountOut = CalcV2AmountOut(currentAmount, pool.Reserve1, pool.Reserve0,
					float64(pool.FeeNumer)/float64(pool.FeeDenom))
			}
		}

		stepType := V2
		if pool.IsV3 {
			stepType = V3
		}

		step := SwapStep{
			PoolAddress: pool.PoolAddress,
			TokenIn:     pool.Token0,
			TokenOut:    pool.Token1,
			AmountIn:    new(big.Int).Set(currentAmount),
			AmountOut:   new(big.Int).Set(amountOut),
			SwapType:    stepType,
			Fee:         float64(pool.FeeNumer) / float64(pool.FeeDenom),
		}

		steps = append(steps, step)
		currentAmount = amountOut
	}

	return steps
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
