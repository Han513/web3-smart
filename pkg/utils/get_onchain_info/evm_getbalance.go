package smartmoney

import (
	"context"
	"fmt"
	"math/big"
	"sync"

	"web3-smart/pkg/utils"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/quotecoin"
)

// 获取smart money需要的balance

// GetWalletBalances 查询钱包余额
func GetWalletBalances(
	ctx context.Context,
	client *ethclient.Client,
	walletAddress common.Address, // 要查询的钱包地址
	erc20Tokens []common.Address, // 要查询的ERC20代币合约地址列表
) (nativeBalance *big.Int, tokenBalances map[common.Address]*big.Int, err error) {
	// 查询原生代币余额
	nativeBalance, err = client.BalanceAt(ctx, walletAddress, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get native balance: %w", err)
	}

	// 准备并发查询ERC20代币余额
	tokenBalances = make(map[common.Address]*big.Int)
	var wg sync.WaitGroup
	var mu sync.Mutex
	errCh := make(chan error, len(erc20Tokens))

	// 查询每个ERC20代币的余额
	for _, tokenAddr := range erc20Tokens {
		wg.Add(1)
		go func(token common.Address) {
			defer wg.Done()

			// 构建balanceOf函数调用数据
			callData, err := BalanceOfCallData(walletAddress)
			if err != nil {
				errCh <- fmt.Errorf("failed to encode call data for %s: %w", token.Hex(), err)
				return
			}

			// 调用合约
			result, err := client.CallContract(ctx, ethereum.CallMsg{
				To:   &token,
				Data: callData,
			}, nil)
			if err != nil {
				errCh <- fmt.Errorf("call contract failed for %s: %w", token.Hex(), err)
				return
			}

			// 解析返回结果
			balance, err := ParseBalanceResult(result)
			if err != nil {
				errCh <- fmt.Errorf("failed to parse balance for %s: %w", token.Hex(), err)
				return
			}

			// 安全写入结果
			mu.Lock()
			tokenBalances[token] = balance
			mu.Unlock()
		}(tokenAddr)
	}

	// 等待所有查询完成
	wg.Wait()
	close(errCh)

	// 检查错误
	var errors []error
	for e := range errCh {
		errors = append(errors, e)
	}

	if len(errors) > 0 {
		// 返回部分成功的结果和错误集合
		return nativeBalance, tokenBalances, fmt.Errorf("%d token queries failed, first error: %w", len(errors), errors[0])
	}

	return nativeBalance, tokenBalances, nil
}

// BalanceOfCallData 构建balanceOf函数调用数据
func BalanceOfCallData(walletAddress common.Address) ([]byte, error) {
	// ERC20的balanceOf函数签名
	methodID := []byte{0x70, 0xa0, 0x82, 0x31}

	// 填充地址参数(32字节)
	paddedAddr := common.LeftPadBytes(walletAddress.Bytes(), 32)

	// 拼接调用数据
	callData := append(methodID, paddedAddr...)
	return callData, nil
}

// ParseBalanceResult 解析合约调用的余额结果
func ParseBalanceResult(data []byte) (*big.Int, error) {
	if len(data) < 32 {
		return nil, fmt.Errorf("invalid balance data length: %d", len(data))
	}

	// 取最后32字节作为余额值
	balance := new(big.Int).SetBytes(data[len(data)-32:])
	return balance, nil
}

// GetBscBalance 获取带精度的持有代币余额
func GetBscBalance(ctx context.Context, client *ethclient.Client, address string) (decimal.Decimal, map[string]decimal.Decimal, error) {
	wallet := common.HexToAddress(address)

	// 要查询的ERC20代币列表
	tokens := []common.Address{
		common.HexToAddress(quotecoin.ID9006_WBNB_ADDRESS), // WBNB
		//common.HexToAddress(quotecoin.ID9006_USDT_ADDRESS), // USDT
		//common.HexToAddress(quotecoin.ID9006_USDC_ADDRESS), // USDC
		//common.HexToAddress(quotecoin.ID9006_USD1_ADDRESS), // USD1
	}

	// 查询余额
	nativeBal, tokenBals, err := GetWalletBalances(ctx, client, wallet, tokens)

	// 原生代币余额，带精度
	adjNativeBal := decimal.NewFromFloat(0)
	if nativeBal != nil {
		adjNativeBal = utils.AdjustDecimals(nativeBal, 18)
	}

	// 转换ERC20代币带精度余额
	m := make(map[string]decimal.Decimal)
	for tokenAddr, balance := range tokenBals {
		//fmt.Printf("%s Balance: %s AdjustDecimals: %s \n", tokenAddr.Hex(), balance.String(), utils.AdjustDecimals(balance, 18))
		m[tokenAddr.String()] = utils.AdjustDecimals(balance, 18)
	}

	return adjNativeBal, m, err
}
