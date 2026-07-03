package rdap

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"czdomains/internal/ratelimit"
)

const DefaultBaseURL = "https://rdap.nic.cz"

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Client struct {
	httpClient HTTPClient
	baseURL    string
	userAgent  string
	limiter    *ratelimit.Limiter
}

type Config struct {
	BaseURL   string
	UserAgent string
	Delay     time.Duration
}

func New(client HTTPClient, config Config) *Client {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if config.BaseURL == "" {
		config.BaseURL = DefaultBaseURL
	}
	if config.UserAgent == "" {
		config.UserAgent = "czdomains/1.0"
	}
	return &Client{
		httpClient: client,
		baseURL:    strings.TrimRight(config.BaseURL, "/"),
		userAgent:  config.UserAgent,
		limiter:    ratelimit.New(config.Delay),
	}
}

func (c *Client) Domain(ctx context.Context, domain string) (*Domain, int, error) {
	var record Domain
	status, err := c.getJSON(ctx, c.baseURL+"/domain/"+url.PathEscape(domain), &record)
	if err != nil {
		return nil, status, err
	}
	return &record, status, nil
}

func (c *Client) EntityByURL(ctx context.Context, rawURL string) (*Entity, int, error) {
	var record Entity
	status, err := c.getJSON(ctx, rawURL, &record)
	if err != nil {
		return nil, status, err
	}
	return &record, status, nil
}

func (c *Client) getJSON(ctx context.Context, rawURL string, target any) (int, error) {
	if err := c.limiter.Wait(ctx); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Accept", "application/rdap+json, application/json")
	req.Header.Set("User-Agent", c.userAgent)

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return resp.StatusCode, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		return resp.StatusCode, err
	}
	return resp.StatusCode, nil
}
