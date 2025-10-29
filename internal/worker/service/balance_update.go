package service

import (
	"context"
	"errors"
	"fmt"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/monitor"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/wallet"
	"web3-smart/internal/worker/writer"
	"web3-smart/internal/worker/writer/balance"
	"web3-smart/pkg/moralis"

	"github.com/gagliardetto/solana-go"
	"go.uber.org/zap"
)

type BalanceUpdate struct {
	tl                    *zap.Logger
	mooxWallets           wallet.MooxWallet
	balanceDbWriter       *writer.AsyncBatchWriter[model.Balance]
	balanceSelectDbWriter *writer.AsyncBatchWriter[model.Balance]
	moralisClient         *moralis.MoralisClient
}

func NewBalanceUpdate(cfg config.Config, logger *zap.Logger, repo repository.Repository) *BalanceUpdate {
	balanceDbWriter := writer.NewAsyncBatchWriter(logger, balance.NewDbBalanceWriter(repo.GetDB(), logger), 1000, 300*time.Millisecond, "balance_db_writer", 1)
	balanceSelectDbWriter := writer.NewAsyncBatchWriter(logger, balance.NewSelectDBBalanceWriter(repo.GetSelectDBHttp(), logger), 5000, 1000*time.Millisecond, "balance_select_db_writer", 4)
	balanceDbWriter.Start(context.Background())
	balanceSelectDbWriter.Start(context.Background())
	wallet.MooxWalletInit(repo)
	return &BalanceUpdate{
		tl:                    logger,
		mooxWallets:           wallet.GetMooxWallet(),
		balanceDbWriter:       balanceDbWriter,
		balanceSelectDbWriter: balanceSelectDbWriter,
		moralisClient:         moralis.NewMoralisClient(cfg.Moralis, logger),
	}
}

func (b *BalanceUpdate) UpdateBalance(blockBalance model.BlockBalance) {
	netWork := blockBalance.Network
	for i, bl := range blockBalance.Balances {
		b.balanceSelectDbWriter.MustSubmit(model.Balance{
			Wallet:       bl.Wallet,
			TokenAddress: bl.TokenAddress,
			TokenAccount: bl.TokenAccount,
			Amount:       bl.Amount,
			Decimal:      int(bl.Decimal),
			BlockNumber:  int64(blockBalance.Number),
			Version:      int64(blockBalance.Number),
			UpdatedAt:    time.Now().Format("2006-01-02 15:04:05"),
			Network:      netWork,
		}, fmt.Sprintf("%s_%s_%s", netWork, bl.TokenAccount, bl.TokenAddress))
		if b.mooxWallets.CheckAddress(netWork, bl.Wallet) || b.mooxWallets.CheckAddress(netWork, bl.TokenAccount) {
			b.balanceDbWriter.Submit(model.Balance{
				Wallet:       bl.Wallet,
				TokenAddress: bl.TokenAddress,
				TokenAccount: bl.TokenAccount,
				Amount:       bl.Amount,
				Decimal:      int(bl.Decimal),
				BlockNumber:  int64(blockBalance.Number),
				Version:      int64(blockBalance.Number),
				UpdatedAt:    time.Now().Format("2006-01-02 15:04:05"),
				Network:      netWork,
			}, fmt.Sprintf("%s_%d", blockBalance.Hash, i))
		}
	}
	monitor.BalanceDelay.WithLabelValues(blockBalance.Network).Set(float64(uint64(time.Now().Unix()) - blockBalance.EventTime))
}

func (b *BalanceUpdate) LoadBalanceFromExternal(ctx context.Context, token model.HotToken) error {
	submitFunc := func(wallet, tokenAccount, Amount string) {
		b.balanceDbWriter.Submit(model.Balance{
			Wallet:       wallet,
			TokenAddress: token.Address,
			TokenAccount: tokenAccount,
			Amount:       Amount,
			Decimal:      token.Decimals,
			BlockNumber:  0,
			Version:      0,
			UpdatedAt:    time.Now().Format("2006-01-02 15:04:05"),
			Network:      token.Network,
		}, fmt.Sprintf("%s_%s_%s", token.Network, token.Address, wallet))
		if b.mooxWallets.CheckAddress(token.Network, wallet) || b.mooxWallets.CheckAddress(token.Network, tokenAccount) {
			b.balanceDbWriter.Submit(model.Balance{
				Wallet:       wallet,
				TokenAddress: token.Address,
				TokenAccount: tokenAccount,
				Amount:       Amount,
				Decimal:      token.Decimals,
				BlockNumber:  0,
				Version:      0,
				UpdatedAt:    time.Now().Format("2006-01-02 15:04:05"),
				Network:      token.Network,
			}, fmt.Sprintf("%s_%s_%s", token.Network, token.Address, wallet))
		}
	}
	switch token.Network {
	case "BSC":
		holders, err := b.moralisClient.GetEvmTokenHolders(ctx, token.Network, token.Address)
		if err != nil {
			return err
		}
		for _, holder := range holders {
			submitFunc(holder.OwnerAddress, holder.OwnerAddress, holder.Balance)
		}
	case "SOLANA":
		holders, err := b.moralisClient.GetSolanaTokenAllHolders(ctx, token.Network, token.Address)
		if err != nil {
			return err
		}
		for _, holder := range holders {
			owner, err := solana.PublicKeyFromBase58(holder.OwnerAddress)
			if err != nil {
				b.tl.Error("public key from base58 failed", zap.Error(err))
				continue
			}
			tokenMint, err := solana.PublicKeyFromBase58(token.Address)
			if err != nil {
				b.tl.Error("public key from base58 failed", zap.Error(err))
				continue
			}
			tokenAccount, _, err := solana.FindAssociatedTokenAddress(owner, tokenMint)
			if err != nil {
				b.tl.Error("find associated token address failed", zap.Error(err))
				continue
			}
			submitFunc(holder.OwnerAddress, tokenAccount.String(), holder.Balance)
		}
	default:
		return errors.New("not support network")
	}
	return nil
}

func (b *BalanceUpdate) Stop() error {
	return nil
}
