package moralis

import (
	"context"
	"fmt"
	"strings"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/pkg/httpclient"

	"go.uber.org/zap"
)

type MoralisClient struct {
	baseURL    string
	gatewayURL string
	apiKey     string
	httpClient *httpclient.HTTPClient
	logger     *zap.Logger
}

func NewMoralisClient(cfg config.MoralisConfig, logger *zap.Logger) *MoralisClient {
	// 创建HTTP客户端配置
	httpCfg := httpclient.HTTPClientConfig{
		Timeout:    time.Duration(cfg.Timeout) * time.Second,
		RateLimit:  cfg.RateLimit,
		MaxRetries: 3,
		XApiKey:    cfg.APIKey,
	}

	// 创建HTTP客户端
	httpClient := httpclient.NewHTTPClient(httpCfg, logger)

	return &MoralisClient{
		baseURL:    cfg.BaseURL,
		gatewayURL: cfg.GatewayURL,
		apiKey:     cfg.APIKey,
		httpClient: httpClient,
		logger:     logger,
	}
}

func (m *MoralisClient) GetEvmTokenHolders(ctx context.Context, network string, tokenAddr string) ([]TokenHold, error) {
	var err error
	resp := []TokenHold{}
	url := fmt.Sprintf("%s/api/v2.2/erc20/%s/owners?chain=%s&limit=100&order=DESC", m.baseURL, tokenAddr, strings.ToLower(network))
	cursor := ""
	for {
		var tokenHolders TokenHoldersResp
		urlCopy := url
		if cursor != "" {
			urlCopy = fmt.Sprintf("%s&cursor=%s", url, cursor)
		}
		for range 5 {
			err = m.httpClient.Get(ctx, urlCopy, nil, nil, &tokenHolders)
			if err == nil {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
		if err != nil {
			return nil, fmt.Errorf("fetch evm token holders failed, url: %s, error: %v", url, err)
		}
		cursor = tokenHolders.Cursor
		resp = append(resp, tokenHolders.Result...)
		if cursor == "" || (tokenHolders.Page != 0 && tokenHolders.PageSize == 0) || tokenHolders.PageSize < 100 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return resp, nil
}

func (m *MoralisClient) GetSolanaTokenAllHolders(ctx context.Context, network string, tokenAddr string) ([]SolanaTokenHolder, error) {
	var err error
	resp := []SolanaTokenHolder{}
	url := fmt.Sprintf("%s/token/mainnet/%s/top-holders?limit=100", m.gatewayURL, tokenAddr)
	cursor := ""
	for {
		var tokenHolders SolanaHoldersResp
		urlCopy := url
		if cursor != "" {
			urlCopy = fmt.Sprintf("%s&cursor=%s", url, cursor)
		}
		for range 5 {
			err = m.httpClient.Get(ctx, urlCopy, nil, nil, &tokenHolders)
			if err == nil {
				break
			}
		}
		if err != nil {
			return nil, fmt.Errorf("fetch solana token holders failed, url: %s, error: %v", url, err)
		}
		cursor = tokenHolders.Cursor
		resp = append(resp, tokenHolders.Result...)
		if cursor == "" || (tokenHolders.Page != 0 && tokenHolders.PageSize == 0) || tokenHolders.PageSize < 100 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	return resp, nil
}
