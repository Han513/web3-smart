package wallet

import (
	"strings"
	"time"
	"web3-smart/internal/worker/repository"
)

var wallet = &mooxWalletImpl{}

type mooxWalletImpl struct {
	Wallets map[string]struct{}
	repo    repository.Repository
	lastId  int
}

type MooxWallet interface {
	CheckAddress(network, address string) bool
}

func GetMooxWallet() MooxWallet {
	return wallet
}

type walletModel struct {
	Id      int    `json:"id"`
	Network string `json:"network"`
	Address string `json:"address"`
}

func (w walletModel) genId() string {
	return w.Network + "_" + w.Address
}

func MooxWalletInit(repo repository.Repository) {
	if wallet != nil && wallet.repo != nil {
		return
	}
	wl := &mooxWalletImpl{}
	wl.repo = repo
	wl.Wallets = make(map[string]struct{})

	const initSql = "SELECT id,network,address FROM moonx.t_web3_wallet_address ORDER BY id DESC"

	c, err := repo.GetDB().Raw(initSql).Rows()
	if err != nil {
		return
	}

	for c.Next() {
		var w walletModel
		c.Scan(&w)
		if wl.lastId == 0 {
			wl.lastId = w.Id
		}
		if len(w.Address) > 2 && w.Address[0] == '0' && w.Address[1] == 'x' {
			w.Address = strings.ToLower(w.Address)
		}
		wl.Wallets[w.genId()] = struct{}{}
	}
	wallet = wl
	go wl.autoLoadWallet()
}

func (mw *mooxWalletImpl) CheckAddress(network, address string) bool {
	_, ok := mw.Wallets[network+"_"+address]
	return ok
}

func (mw *mooxWalletImpl) autoLoadWallet() {
	const updateSql = "SELECT id,network,address FROM moonx.t_web3_wallet_address WHERE id > ? ORDER BY id DESC"
	for {
		time.Sleep(5 * time.Second)

		c, err := mw.repo.GetDB().Raw(updateSql, mw.lastId).Rows()
		if err != nil {
			continue
		}

		for c.Next() {
			var w walletModel
			c.Scan(&w)
			if w.Id > mw.lastId {
				mw.lastId = w.Id
				mw.Wallets[w.Address] = struct{}{}
			}
		}
	}
}
