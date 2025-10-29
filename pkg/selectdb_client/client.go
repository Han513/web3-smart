package selectdbclient

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/bytedance/sonic"
)

// Client 配置 SelectDB HTTP Client
type Client struct {
	BaseURL    string // 例如 http://localhost:8123
	Database   string
	Username   string
	Password   string
	HTTPClient *http.Client
}

// NewClient 创建 Client
func NewClient(baseURL, database, username, password string) *Client {
	return &Client{
		BaseURL:  baseURL,
		Database: database,
		Username: username,
		Password: password,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// StreamLoadOptions 流式上传选项
type StreamLoadOptions struct {
	Table   string
	Format  string // json, tsv, csv 等
	Timeout time.Duration
}

// StreamLoadBalance 通过 HTTP 流式上传数据
func (c *Client) StreamLoadBalance(ctx context.Context, data io.Reader, opts StreamLoadOptions) error {
	url := fmt.Sprintf("%s/api/%s/%s/_stream_load?strip_outer_array=true", c.BaseURL, c.Database, opts.Table)

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, data)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}

	if c.Username != "" && c.Password != "" {
		req.SetBasicAuth(c.Username, c.Password)
	}

	// 必要的 Stream Load Header
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("label", fmt.Sprintf("load_%d", time.Now().UnixNano()))
	req.Header.Set("jsonpaths", `["$.network","$.token_address","$.wallet","$.token_account","$.amount","$.decimal","$.block_number","$.version","$.updated_at"]`)
	req.Header.Set("columns", "network,token_address,wallet,token_account,amount,decimal,block_number,version,updated_at")
	req.Header.Set("format", opts.Format) // 指明上传数据格式
	req.Header.Set("strip_outer_array", "true")

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return fmt.Errorf("http request failed: %w", err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var st statusResponse

	sonic.Unmarshal(body, &st)

	if (resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted) || st.Status != "Success" {
		return fmt.Errorf("stream load failed: status=%d, body=%s", resp.StatusCode, string(body))
	}

	return nil
}

type statusResponse struct {
	Status string `json:"Status"`
}
