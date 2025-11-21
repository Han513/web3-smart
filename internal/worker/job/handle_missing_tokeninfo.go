package job

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"
	"web3-smart/internal/worker/config"
	"web3-smart/internal/worker/model"
	"web3-smart/internal/worker/repository"
	"web3-smart/internal/worker/writer"
	"web3-smart/internal/worker/writer/wallet"
	"web3-smart/pkg/utils"

	"github.com/redis/go-redis/v9"
	"github.com/shopspring/decimal"
	"gitlab.codetech.pro/web3/chain_data/chain/dex_data_broker/common/bip0044"
	"go.uber.org/zap"
	"gorm.io/gorm"
)

type HandleMissingTokenInfo struct {
	cfg            config.Config
	repo           repository.Repository
	tl             *zap.Logger
	walletEsWriter writer.BatchWriter[model.WalletSummary]
}

func NewHandleMissingTokenInfo(cfg config.Config, repo repository.Repository, logger *zap.Logger) *HandleMissingTokenInfo {
	return &HandleMissingTokenInfo{
		repo:           repo,
		tl:             logger,
		cfg:            cfg,
		walletEsWriter: wallet.NewESWalletWriter(repo.GetElasticsearchClient(), logger, cfg.Elasticsearch.WalletsIndexName),
	}
}

func (j *HandleMissingTokenInfo) Run(ctx context.Context) error {
	startTime := time.Now()

	// 1. 从 Redis 查询待处理的数据
	rdb := j.repo.GetMainRDB()
	key := utils.MissingTokenInfoKey()
	members, err := j.queryPendingMembers(ctx, rdb, key)
	if err != nil {
		return err
	}

	if len(members) == 0 {
		j.tl.Debug("No missing token info to process")
		return nil
	}

	j.tl.Debug("Processing missing token info", zap.Int("count", len(members)))

	// 2. 解析并构建查询列表
	tokenMap, walletTokenList := j.parseMembers(members)
	if len(tokenMap) == 0 {
		j.tl.Debug("No valid tokens to query")
		j.removeProcessedMembers(ctx, rdb, key, members)
		return nil
	}

	// 3. 从 ES 批量查询 token 信息
	tokenInfoMap, err := j.fetchTokensFromES(ctx, tokenMap)
	if err != nil {
		return err
	}

	if len(tokenInfoMap) == 0 {
		j.tl.Debug("No valid token info found from ES")
		j.removeProcessedMembers(ctx, rdb, key, members)
		return nil
	}

	// 4. 更新数据库
	holdingUpdatedCount, transactionUpdatedCount := j.updateDatabase(ctx, walletTokenList, tokenInfoMap)

	// 5. 更新token info缓存
	for tokenKey, tokenInfo := range tokenInfoMap {
		parts := strings.Split(tokenKey, "_")
		chainID := parts[0]
		tokenAddress := parts[1]
		chainIDUint64, err := strconv.ParseUint(chainID, 10, 64)
		if err != nil {
			j.tl.Warn("Failed to parse chainID", zap.String("chainID", chainID))
			continue
		}
		cacheKey := utils.TokenInfoKey(chainIDUint64, tokenAddress)
		j.repo.GetDAOManager().TokenDAO.UpdateTokenInfoCache(ctx, cacheKey, tokenInfo)
	}

	// 6. 更新wallet最近交易过的token列表，更新缺失的token logo
	walletTokenListUpdatedCount := j.updateWalletTokenList(ctx, walletTokenList, tokenInfoMap)

	// 7. 清理 Redis 已处理数据
	j.removeProcessedMembers(ctx, rdb, key, members)

	j.tl.Info("Successfully processed missing token info",
		zap.Int("total_members", len(members)),
		zap.Int("holding_updated_count", holdingUpdatedCount),
		zap.Int("transaction_updated_count", transactionUpdatedCount),
		zap.Int("wallet_token_list_updated_count", walletTokenListUpdatedCount),
		zap.Int("unique_tokens", len(tokenInfoMap)),
		zap.Float64("elapsed_seconds", time.Since(startTime).Seconds()))

	return nil
}

// TokenKey 用于存储 token 的基本信息
type TokenKey struct {
	Network      string
	ChainID      uint64
	TokenAddress string
}

// WalletTokenKey 用于存储 wallet-token 组合信息
type WalletTokenKey struct {
	ChainID       uint64
	WalletAddress string
	TokenAddress  string
}

// queryPendingMembers 从 Redis 查询待处理的数据
func (j *HandleMissingTokenInfo) queryPendingMembers(ctx context.Context, rdb *redis.Client, key string) ([]redis.Z, error) {
	now := time.Now().Unix()
	maxScore := now - 20 // 20 秒前

	members, err := rdb.ZRangeByScoreWithScores(ctx, key, &redis.ZRangeBy{
		Min:    "0",
		Max:    fmt.Sprintf("%d", maxScore),
		Offset: 0,
		Count:  100,
	}).Result()

	if err != nil {
		j.tl.Warn("Failed to query missing token info from Redis", zap.Error(err))
		return nil, err
	}

	return members, nil
}

// parseMembers 解析 Redis members 并构建查询列表
func (j *HandleMissingTokenInfo) parseMembers(members []redis.Z) (map[string]TokenKey, []WalletTokenKey) {
	tokenMap := make(map[string]TokenKey)                      // docID -> TokenKey
	tokenAddressSet := make(map[string]bool)                   // 用于 token 去重
	walletTokenList := make([]WalletTokenKey, 0, len(members)) // 保存所有 wallet-token 组合

	for _, member := range members {
		memberStr, ok := member.Member.(string)
		if !ok {
			j.tl.Warn("Invalid member type", zap.Any("member", member.Member))
			continue
		}

		// 解析 member: network_walletAddress_tokenAddress
		parts := strings.Split(memberStr, "_")
		if len(parts) != 3 {
			j.tl.Warn("Invalid member format", zap.String("member", memberStr))
			continue
		}

		network := parts[0]
		walletAddress := parts[1]
		tokenAddress := parts[2]

		// 根据 network 获取 chain_id
		chainID := bip0044.NetworkNameToChainId(network)
		if chainID == 0 {
			j.tl.Warn("Unknown network", zap.String("network", network))
			continue
		}

		// 保存 wallet-token 组合
		walletTokenList = append(walletTokenList, WalletTokenKey{
			ChainID:       chainID,
			WalletAddress: walletAddress,
			TokenAddress:  tokenAddress,
		})

		// 构造 ES 文档 ID: {network}_{address}
		docID := fmt.Sprintf("%s_%s", network, tokenAddress)

		// 去重：同一个 token 只查询一次
		if !tokenAddressSet[docID] {
			tokenMap[docID] = TokenKey{
				Network:      network,
				ChainID:      chainID,
				TokenAddress: tokenAddress,
			}
			tokenAddressSet[docID] = true
		}
	}

	return tokenMap, walletTokenList
}

// fetchTokensFromES 从 ES 批量查询 token 信息
func (j *HandleMissingTokenInfo) fetchTokensFromES(ctx context.Context, tokenMap map[string]TokenKey) (map[string]*model.SmTokenRet, error) {
	// 构建文档 ID 列表
	docIDs := make([]string, 0, len(tokenMap))
	for docID := range tokenMap {
		docIDs = append(docIDs, docID)
	}

	// 执行 mget 查询
	esClient := j.repo.GetElasticsearchClient()
	indexName := j.cfg.Elasticsearch.Web3TokensIndexName

	mgetResult, err := esClient.Mget(ctx, indexName, docIDs)
	if err != nil {
		j.tl.Warn("Failed to mget tokens from ES", zap.Error(err))
		return nil, err
	}

	// 解析结果
	tokenInfoMap := make(map[string]*model.SmTokenRet) // chainID_tokenAddress -> SmTokenRet

	for _, doc := range mgetResult.Docs {
		if !doc.Found || doc.Source == nil {
			j.tl.Debug("Token not found in ES", zap.String("docID", doc.ID))
			continue
		}

		tokenKey, ok := tokenMap[doc.ID]
		if !ok {
			continue
		}

		// 解析 token 信息
		tokenInfo := j.parseTokenInfo(doc.Source, tokenKey.TokenAddress)

		// 存储到映射中
		key := fmt.Sprintf("%d_%s", tokenKey.ChainID, tokenKey.TokenAddress)
		tokenInfoMap[key] = tokenInfo
	}

	return tokenInfoMap, nil
}

// parseTokenInfo 解析 ES 返回的 token 信息
func (j *HandleMissingTokenInfo) parseTokenInfo(source map[string]interface{}, tokenAddress string) *model.SmTokenRet {
	tokenInfo := &model.SmTokenRet{
		Address: tokenAddress,
	}

	if name, ok := source["name"].(string); ok {
		tokenInfo.Name = name
	}

	if symbol, ok := source["symbol"].(string); ok {
		tokenInfo.Symbol = symbol
	}

	if logo, ok := source["logo"].(string); ok {
		tokenInfo.Logo = logo
	}

	if totalSupply, ok := source["total_supply"].(float64); ok {
		supply := decimal.NewFromFloat(totalSupply)
		tokenInfo.Supply = &supply
	}

	if contractInfo, ok := source["contract_info"].(map[string]interface{}); ok {
		if creator, ok := contractInfo["creator"].(string); ok && creator != "" {
			tokenInfo.Creater = &creator
		}
	}

	return tokenInfo
}

// UpdateItem 批量更新项
type UpdateItem struct {
	ChainID       uint64
	WalletAddress string
	TokenAddress  string
	TokenName     string
	TokenIcon     string
}

// updateDatabase 更新数据库中的 token 信息
func (j *HandleMissingTokenInfo) updateDatabase(ctx context.Context, walletTokenList []WalletTokenKey, tokenInfoMap map[string]*model.SmTokenRet) (int, int) {
	db := j.repo.GetDB()

	// 构建更新项列表
	updateItems := make([]UpdateItem, 0, len(walletTokenList))
	for _, wtKey := range walletTokenList {
		tokenKey := fmt.Sprintf("%d_%s", wtKey.ChainID, wtKey.TokenAddress)
		tokenInfo, ok := tokenInfoMap[tokenKey]
		if !ok {
			continue
		}

		updateItems = append(updateItems, UpdateItem{
			ChainID:       wtKey.ChainID,
			WalletAddress: wtKey.WalletAddress,
			TokenAddress:  wtKey.TokenAddress,
			TokenName:     tokenInfo.Symbol,
			TokenIcon:     tokenInfo.Logo,
		})
	}

	if len(updateItems) == 0 {
		return 0, 0
	}

	// 批量更新 WalletHolding（一条 SQL）
	holdingUpdatedCount := j.batchUpdateWalletHolding(ctx, db, updateItems)

	// 批量更新 WalletTransaction（一条 SQL）
	transactionUpdatedCount := j.batchUpdateWalletTransaction(ctx, db, updateItems)

	return holdingUpdatedCount, transactionUpdatedCount
}

// batchUpdateWalletHolding 使用一条 SQL 语句批量更新 WalletHolding 表
func (j *HandleMissingTokenInfo) batchUpdateWalletHolding(ctx context.Context, db *gorm.DB, items []UpdateItem) int {
	if len(items) == 0 {
		return 0
	}

	// 构建 CASE WHEN 语句
	var tokenNameCases strings.Builder
	var tokenIconCases strings.Builder
	var whereConditions []string
	var args []interface{}

	tokenNameCases.WriteString("CASE ")
	tokenIconCases.WriteString("CASE ")

	// 第一步：添加 token_name CASE 的所有参数
	for _, item := range items {
		tokenNameCases.WriteString("WHEN chain_id = ? AND wallet_address = ? AND token_address = ? THEN ? ")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress, item.TokenName)
	}
	tokenNameCases.WriteString("ELSE token_name END")

	// 第二步：添加 token_icon CASE 的所有参数
	for _, item := range items {
		tokenIconCases.WriteString("WHEN chain_id = ? AND wallet_address = ? AND token_address = ? THEN ? ")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress, item.TokenIcon)
	}
	tokenIconCases.WriteString("ELSE token_icon END")

	// 第三步：构建 WHERE 条件并添加参数
	for _, item := range items {
		whereConditions = append(whereConditions, "(chain_id = ? AND wallet_address = ? AND token_address = ?)")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress)
	}

	// 构建完整的 SQL
	sql := fmt.Sprintf(`
		UPDATE dex_query_v1.t_smart_holding 
		SET token_name = %s, token_icon = %s
		WHERE %s
	`, tokenNameCases.String(), tokenIconCases.String(), strings.Join(whereConditions, " OR "))

	// 执行更新
	result := db.WithContext(ctx).Exec(sql, args...)
	if result.Error != nil {
		j.tl.Warn("Failed to batch update WalletHolding",
			zap.Int("items_count", len(items)),
			zap.Error(result.Error))
		return 0
	}

	return int(result.RowsAffected)
}

// batchUpdateWalletTransaction 使用一条 SQL 语句批量更新 WalletTransaction 表
func (j *HandleMissingTokenInfo) batchUpdateWalletTransaction(ctx context.Context, db *gorm.DB, items []UpdateItem) int {
	if len(items) == 0 {
		return 0
	}

	// 构建 CASE WHEN 语句
	var tokenNameCases strings.Builder
	var tokenIconCases strings.Builder
	var whereConditions []string
	var args []interface{}

	tokenNameCases.WriteString("CASE ")
	tokenIconCases.WriteString("CASE ")

	// 第一步：添加 token_name CASE 的所有参数
	for _, item := range items {
		tokenNameCases.WriteString("WHEN chain_id = ? AND wallet_address = ? AND token_address = ? THEN ? ")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress, item.TokenName)
	}
	tokenNameCases.WriteString("ELSE token_name END")

	// 第二步：添加 token_icon CASE 的所有参数
	for _, item := range items {
		tokenIconCases.WriteString("WHEN chain_id = ? AND wallet_address = ? AND token_address = ? THEN ? ")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress, item.TokenIcon)
	}
	tokenIconCases.WriteString("ELSE token_icon END")

	// 第三步：构建 WHERE 条件并添加参数
	for _, item := range items {
		whereConditions = append(whereConditions, "(chain_id = ? AND wallet_address = ? AND token_address = ?)")
		args = append(args, item.ChainID, item.WalletAddress, item.TokenAddress)
	}

	// 构建完整的 SQL
	sql := fmt.Sprintf(`
		UPDATE dex_query_v1.t_smart_transaction 
		SET token_name = %s, token_icon = %s
		WHERE %s
	`, tokenNameCases.String(), tokenIconCases.String(), strings.Join(whereConditions, " OR "))

	// 执行更新
	result := db.WithContext(ctx).Exec(sql, args...)
	if result.Error != nil {
		j.tl.Warn("Failed to batch update WalletTransaction",
			zap.Int("items_count", len(items)),
			zap.Error(result.Error))
		return 0
	}

	return int(result.RowsAffected)
}

// updateWalletTokenList 更新 wallet 最近交易过的 token 列表中缺失的 token 信息
func (j *HandleMissingTokenInfo) updateWalletTokenList(ctx context.Context, walletTokenList []WalletTokenKey, tokenInfoMap map[string]*model.SmTokenRet) int {
	if len(walletTokenList) == 0 {
		return 0
	}

	// 按 chainID + walletAddress 分组
	type WalletKey struct {
		ChainID       uint64
		WalletAddress string
	}

	walletTokenMap := make(map[WalletKey][]WalletTokenKey)
	for _, wtKey := range walletTokenList {
		key := WalletKey{
			ChainID:       wtKey.ChainID,
			WalletAddress: wtKey.WalletAddress,
		}
		walletTokenMap[key] = append(walletTokenMap[key], wtKey)
	}

	db := j.repo.GetDB()
	walletDAO := j.repo.GetDAOManager().WalletDAO
	updatedCount := 0

	needUpdateWalletEs := make([]model.WalletSummary, 0, len(walletTokenMap))

	// 遍历每个 wallet
	for wKey := range walletTokenMap {
		// 获取 wallet 信息
		wallet, err := walletDAO.GetByWalletAddress(ctx, wKey.ChainID, wKey.WalletAddress)
		if err != nil || wallet == nil {
			j.tl.Debug("Wallet not found",
				zap.Uint64("chain_id", wKey.ChainID),
				zap.String("wallet_address", wKey.WalletAddress))
			continue
		}

		// 检查并更新 token_list 中的 token 信息
		updated := false
		for i := range wallet.TokenList {
			tokenKey := fmt.Sprintf("%d_%s", wKey.ChainID, wallet.TokenList[i].TokenAddress)
			if tokenInfo, ok := tokenInfoMap[tokenKey]; ok {
				// 找到了这个 token，检查是否需要更新
				if wallet.TokenList[i].TokenName == "" || wallet.TokenList[i].TokenName != tokenInfo.Symbol {
					wallet.TokenList[i].TokenName = tokenInfo.Symbol
					updated = true
				}

				if wallet.TokenList[i].TokenIcon == "" || wallet.TokenList[i].TokenIcon != tokenInfo.Logo {
					wallet.TokenList[i].TokenIcon = tokenInfo.Logo
					updated = true
				}
			}
		}

		// 如果有更新，保存到数据库和缓存
		if updated {
			needUpdateWalletEs = append(needUpdateWalletEs, *wallet)

			// 更新数据库
			err := db.WithContext(ctx).
				Table("dex_query_v1.t_smart_wallet").
				Where("chain_id = ? AND wallet_address = ?", wKey.ChainID, wKey.WalletAddress).
				Update("token_list", wallet.TokenList).Error

			if err != nil {
				j.tl.Warn("Failed to update wallet token_list",
					zap.Uint64("chain_id", wKey.ChainID),
					zap.String("wallet_address", wKey.WalletAddress),
					zap.Error(err))
				continue
			}

			// 更新缓存
			cacheKey := utils.WalletSummaryKey(wKey.ChainID, wKey.WalletAddress)
			walletDAO.UpdateWalletCache(ctx, cacheKey, wallet)

			updatedCount++
			j.tl.Debug("Updated wallet token_list",
				zap.Uint64("chain_id", wKey.ChainID),
				zap.String("wallet_address", wKey.WalletAddress),
				zap.Int("token_count", len(wallet.TokenList)))
		}
	}

	if updatedCount > 0 {
		j.walletEsWriter.BWrite(ctx, needUpdateWalletEs)
		j.tl.Debug("Updated wallet token_list", zap.Int("wallet_count", updatedCount))
	}

	return updatedCount
}

// removeProcessedMembers 删除已处理的 Redis 成员
func (j *HandleMissingTokenInfo) removeProcessedMembers(ctx context.Context, rdb *redis.Client, key string, members []redis.Z) {
	if len(members) == 0 {
		return
	}

	membersToRemove := make([]interface{}, len(members))
	for i, member := range members {
		membersToRemove[i] = member.Member
	}

	err := rdb.ZRem(ctx, key, membersToRemove...).Err()
	if err != nil {
		j.tl.Warn("Failed to remove processed members from Redis", zap.Error(err))
	} else {
		j.tl.Debug("Removed processed members from Redis", zap.Int("count", len(members)))
	}
}
