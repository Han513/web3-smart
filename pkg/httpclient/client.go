package httpclient

import (
	"context"
	"fmt"
	"time"

	"resty.dev/v3"

	"go.uber.org/zap"
	"golang.org/x/time/rate"
)

// HTTPClientConfig 配置参数
type HTTPClientConfig struct {
	Timeout    time.Duration // 请求超时时间
	RateLimit  int           // 每分钟请求次数
	MaxRetries int           // 最大重试次数
	UserAgent  string        // 可选 User-Agent
	XApiKey    string
}

// HTTPClient 是一个通用的 HTTP 客户端
type HTTPClient struct {
	client    *resty.Client
	logger    *zap.Logger
	limiter   *rate.Limiter
	userAgent string
}

// NewHTTPClient 创建一个新的 HTTP 客户端
func NewHTTPClient(cfg HTTPClientConfig, logger *zap.Logger) *HTTPClient {
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 3
	}
	ratePerSecond := float64(cfg.RateLimit) / 60
	limiter := rate.NewLimiter(rate.Limit(ratePerSecond), 1)

	// 创建 Resty 客户端
	restyClient := resty.New().
		SetTimeout(cfg.Timeout).
		SetRetryCount(cfg.MaxRetries).
		AddRequestMiddleware(func(c *resty.Client, r *resty.Request) error {
			// 为限流器等待创建带超时的上下文
			limiterCtx, cancel := context.WithTimeout(r.Context(), cfg.Timeout)
			defer cancel()

			if err := limiter.Wait(limiterCtx); err != nil {
				logger.Warn("Rate limiter wait failed", zap.Error(err))
				return err
			}
			if cfg.UserAgent != "" {
				r.SetHeader("User-Agent", cfg.UserAgent)
			}
			if cfg.XApiKey != "" {
				r.SetHeader("X-API-Key", cfg.XApiKey)
			}
			logger.Debug("Outgoing request", zap.String("url", r.URL))
			return nil
		}).
		AddResponseMiddleware(func(c *resty.Client, resp *resty.Response) error {
			if resp.StatusCode() >= 400 {
				logger.Warn("HTTP request failed",
					zap.Int("status", resp.StatusCode()),
					zap.String("url", resp.Request.URL),
				)
			}
			return nil
		})

	return &HTTPClient{
		client:    restyClient,
		logger:    logger,
		limiter:   limiter,
		userAgent: cfg.UserAgent,
	}
}

// Get 发起 GET 请求并返回响应体
func (c *HTTPClient) Get(ctx context.Context, url string, queryParams map[string]string, headers map[string]string, out interface{}) error {
	req := c.client.R().
		SetContext(ctx).
		SetQueryParams(queryParams).
		SetResult(out)

	// 添加自定义 headers
	if headers != nil {
		req.SetHeaders(headers)
	}

	resp, err := req.Get(url)

	if err != nil {
		c.logger.Error("HTTP GET request failed", zap.String("url", url), zap.Error(err))
		return err
	}

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("non-2xx status code: %d", resp.StatusCode())
	}

	return nil
}

// Post 发起 POST 请求并返回响应体
func (c *HTTPClient) Post(ctx context.Context, url string, body, headers map[string]string, out interface{}) error {
	req := c.client.R().
		SetContext(ctx).
		SetFormData(body).
		SetResult(out)

	// 添加自定义 headers
	if headers != nil {
		req.SetHeaders(headers)
	}

	resp, err := req.Post(url)

	if err != nil {
		c.logger.Error("HTTP POST request failed", zap.String("url", url), zap.Error(err))
		return err
	}

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("non-2xx status code: %d", resp.StatusCode())
	}

	return nil
}

func (c *HTTPClient) PostJSON(ctx context.Context, url string, body interface{}, headers map[string]string, out interface{}) error {
	req := c.client.R().
		SetContext(ctx).
		SetBody(body).
		SetResult(out)

	// 添加自定义 headers
	if headers != nil {
		req.SetHeaders(headers)
	}

	// 强制设置 Content-Type 为 application/json（可选）
	req.SetHeader("Content-Type", "application/json")

	resp, err := req.Post(url)

	if err != nil {
		c.logger.Error("HTTP POST JSON request failed", zap.String("url", url), zap.Error(err))
		return err
	}

	if resp.StatusCode() >= 400 {
		return fmt.Errorf("non-2xx status code: %d", resp.StatusCode())
	}

	return nil
}

// HTTPError 自定义错误结构体
type HTTPError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP error %d: %s", e.Code, e.Message)
}
