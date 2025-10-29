package elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"go.uber.org/zap"
)

type Client struct {
	es     *elasticsearch.Client
	logger *zap.Logger
}

type Config struct {
	Addresses []string                          `yaml:"addresses"`
	Username  string                            `yaml:"username"`
	Password  string                            `yaml:"password"`
	Indexs    map[string]map[string]interface{} `yaml:"indexs"` // indexName -> mapping
}

// BulkOperation 批量操作的结构
type BulkOperation struct {
	Action   string                 `json:"action"`   // index, create, update, delete
	Index    string                 `json:"index"`    // 索引名
	ID       string                 `json:"id"`       // 文档ID
	Routing  string                 `json:"routing"`  // 路由值
	Document map[string]interface{} `json:"document"` // 文档内容
}

func NewClient(cfg Config, log *zap.Logger) (*Client, error) {
	esCfg := elasticsearch.Config{
		Addresses: cfg.Addresses,
		Username:  cfg.Username,
		Password:  cfg.Password,
	}

	es, err := elasticsearch.NewClient(esCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create elasticsearch client: %w", err)
	}

	client := &Client{
		es:     es,
		logger: log,
	}

	// 初始化索引
	for indexName, mapping := range cfg.Indexs {
		if err := client.initializeIndex(context.Background(), indexName, mapping); err != nil {
			log.Error("Failed to initialize ES index", zap.Error(err))
		}
	}

	return client, nil
}

// initializeIndex 初始化索引
func (c *Client) initializeIndex(ctx context.Context, indexName string, mapping map[string]interface{}) error {
	return c.CreateIndex(ctx, indexName, mapping)
}

// BulkWrite 批量操作 - 简化接口，只负责执行
func (c *Client) BulkWrite(ctx context.Context, operations []BulkOperation) error {
	if len(operations) == 0 {
		return nil
	}

	var buf bytes.Buffer
	for _, op := range operations {
		// 构建操作行
		actionLine := map[string]interface{}{
			op.Action: map[string]interface{}{
				"_index": op.Index,
				"_id":    op.ID,
			},
		}

		// 暂时移除routing支持，因为ES版本不支持_routing参数
		// if op.Routing != "" {
		//	actionLine[op.Action].(map[string]interface{})["_routing"] = op.Routing
		// }

		// 写入操作行
		actionBytes, _ := json.Marshal(actionLine)
		buf.Write(actionBytes)
		buf.WriteByte('\n')

		// 如果是index或create操作，需要写入文档内容
		if op.Action == "index" || op.Action == "create" || op.Action == "update" {
			if op.Document != nil {
				docBytes, _ := json.Marshal(op.Document)
				buf.Write(docBytes)
				buf.WriteByte('\n')
			}
		}
	}

	req := esapi.BulkRequest{
		Body: &buf,
	}

	res, err := req.Do(ctx, c.es)
	if err != nil {
		return fmt.Errorf("bulk operation failed: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return fmt.Errorf("bulk operation error: %s", res.String())
	}

	c.logger.Debug("Bulk write operation completed",
		zap.Int("operations", len(operations)))

	return nil
}

// CreateIndex 创建索引
func (c *Client) CreateIndex(ctx context.Context, indexName string, mapping map[string]interface{}) error {
	mappingJSON, err := json.Marshal(mapping)
	if err != nil {
		return fmt.Errorf("failed to marshal mapping: %w", err)
	}

	req := esapi.IndicesCreateRequest{
		Index: indexName,
		Body:  strings.NewReader(string(mappingJSON)),
	}

	res, err := req.Do(ctx, c.es)
	if err != nil {
		return fmt.Errorf("failed to create index: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() && !strings.Contains(res.String(), "resource_already_exists_exception") {
		return fmt.Errorf("failed to create index: %s", res.String())
	}

	c.logger.Info("Index created or already exists", zap.String("index", indexName))
	return nil
}

// SearchWithRouting 带routing的搜索
func (c *Client) SearchWithRouting(ctx context.Context, indexName, routing string, query map[string]interface{}) (*SearchResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	req := esapi.SearchRequest{
		Index:   []string{indexName},
		Body:    strings.NewReader(string(queryJSON)),
		Routing: []string{routing},
	}

	res, err := req.Do(ctx, c.es)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search with routing: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search with routing error: %s", res.String())
	}

	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode search result: %w", err)
	}

	return &result, nil
}

// Search 普通搜索（不指定 routing）
func (c *Client) Search(ctx context.Context, indexName string, query map[string]interface{}) (*SearchResult, error) {
	queryJSON, err := json.Marshal(query)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query: %w", err)
	}

	req := esapi.SearchRequest{
		Index: []string{indexName},
		Body:  strings.NewReader(string(queryJSON)),
	}

	res, err := req.Do(ctx, c.es)
	if err != nil {
		return nil, fmt.Errorf("failed to execute search: %w", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		return nil, fmt.Errorf("search error: %s", res.String())
	}

	var result SearchResult
	if err := json.NewDecoder(res.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode search result: %w", err)
	}

	return &result, nil
}

type SearchResult struct {
	Took     int  `json:"took"`
	TimedOut bool `json:"timed_out"`
	Hits     struct {
		Total struct {
			Value    int    `json:"value"`
			Relation string `json:"relation"`
		} `json:"total"`
		MaxScore float64 `json:"max_score"`
		Hits     []Hit   `json:"hits"`
	} `json:"hits"`
	Aggregations map[string]interface{} `json:"aggregations,omitempty"`
}

type Hit struct {
	Index  string                 `json:"_index"`
	ID     string                 `json:"_id"`
	Score  float64                `json:"_score"`
	Source map[string]interface{} `json:"_source"`
}
