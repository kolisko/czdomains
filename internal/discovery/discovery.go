package discovery

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"czdomains/internal/domainutil"
)

const (
	DefaultCollInfoURL = "https://index.commoncrawl.org/collinfo.json"
	DefaultCRTShURL    = "https://crt.sh/"
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	Limit       int
	Sources     []string
	CCIndex     string
	CollInfoURL string
	CRTShURL    string
	UserAgent   string
	Delay       time.Duration
}

type Result struct {
	Domain string
	Source string
}

type Discoverer struct {
	client HTTPClient
	config Config
}

func New(client HTTPClient, config Config) *Discoverer {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if config.CollInfoURL == "" {
		config.CollInfoURL = DefaultCollInfoURL
	}
	if config.CRTShURL == "" {
		config.CRTShURL = DefaultCRTShURL
	}
	if config.UserAgent == "" {
		config.UserAgent = "czdomains/1.0"
	}
	if len(config.Sources) == 0 {
		config.Sources = []string{"commoncrawl"}
	}
	return &Discoverer{client: client, config: config}
}

func (d *Discoverer) Discover(ctx context.Context) ([]Result, error) {
	var all []Result
	var errs []error
	seen := make(map[string]struct{})

	for _, source := range d.config.Sources {
		source = strings.ToLower(strings.TrimSpace(source))
		var results []Result
		var err error
		switch source {
		case "commoncrawl", "cc":
			results, err = d.discoverCommonCrawl(ctx)
		case "crtsh":
			results, err = d.discoverCRTSh(ctx)
		default:
			err = fmt.Errorf("unknown source %q", source)
		}
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, result := range results {
			if _, ok := seen[result.Domain]; ok {
				continue
			}
			seen[result.Domain] = struct{}{}
			all = append(all, result)
			if d.config.Limit > 0 && len(all) >= d.config.Limit {
				return all, errors.Join(errs...)
			}
		}
	}

	return all, errors.Join(errs...)
}

type ccIndex struct {
	ID     string `json:"id"`
	CDXAPI string `json:"cdx-api"`
}

func (d *Discoverer) discoverCommonCrawl(ctx context.Context) ([]Result, error) {
	indexURL, err := d.commonCrawlIndexURL(ctx)
	if err != nil {
		return nil, err
	}

	pageCount, err := d.commonCrawlPageCount(ctx, indexURL)
	if err != nil {
		pageCount = 0
	}

	seen := map[string]struct{}{}
	results := make([]Result, 0)
	var errs []error
	if pageCount > 0 {
		for page := 0; page < pageCount; page++ {
			err := d.scanCommonCrawl(ctx, commonCrawlQuery(indexURL, page, 0), seen, &results)
			if err != nil {
				errs = append(errs, err)
				continue
			}
			if d.config.Limit > 0 && len(results) >= d.config.Limit {
				return results, errors.Join(errs...)
			}
		}
		return results, errors.Join(errs...)
	}

	err = d.scanCommonCrawl(ctx, commonCrawlQuery(indexURL, -1, commonCrawlRawLimit(d.config.Limit)), seen, &results)
	return results, err
}

func commonCrawlQuery(indexURL string, page int, limit int) string {
	query, err := url.Parse(indexURL)
	if err != nil {
		return indexURL
	}
	values := query.Query()
	values.Set("url", "*.cz/")
	values.Set("output", "json")
	values.Set("fl", "url")
	if page >= 0 {
		values.Set("page", fmt.Sprintf("%d", page))
	}
	if limit > 0 {
		values.Set("limit", fmt.Sprintf("%d", limit))
	}
	query.RawQuery = values.Encode()
	return query.String()
}

type ccPageInfo struct {
	Pages int `json:"pages"`
}

func (d *Discoverer) commonCrawlPageCount(ctx context.Context, indexURL string) (int, error) {
	query, err := url.Parse(indexURL)
	if err != nil {
		return 0, err
	}
	values := query.Query()
	values.Set("url", "*.cz/")
	values.Set("showNumPages", "true")
	values.Set("pageSize", "1000")
	query.RawQuery = values.Encode()

	req, err := d.newRequest(ctx, query.String())
	if err != nil {
		return 0, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return 0, fmt.Errorf("commoncrawl page count returned HTTP %d", resp.StatusCode)
	}

	var info ccPageInfo
	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return 0, err
	}
	return info.Pages, nil
}

func (d *Discoverer) scanCommonCrawl(ctx context.Context, rawURL string, seen map[string]struct{}, results *[]Result) error {
	req, err := d.newRequest(ctx, rawURL)
	if err != nil {
		return err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("commoncrawl returned HTTP %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for scanner.Scan() {
		var row struct {
			URL string `json:"url"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &row); err != nil || row.URL == "" {
			continue
		}
		domain, err := domainutil.FromURL(row.URL)
		if err != nil {
			continue
		}
		if _, ok := seen[domain]; ok {
			continue
		}
		seen[domain] = struct{}{}
		*results = append(*results, Result{Domain: domain, Source: "commoncrawl"})
		if d.config.Limit > 0 && len(*results) >= d.config.Limit {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return nil
}

func commonCrawlRawLimit(uniqueLimit int) int {
	if uniqueLimit <= 0 {
		return 0
	}
	rawLimit := uniqueLimit * 50
	if rawLimit < 1000 {
		rawLimit = 1000
	}
	if rawLimit > 200000 {
		rawLimit = 200000
	}
	return rawLimit
}

func (d *Discoverer) commonCrawlIndexURL(ctx context.Context) (string, error) {
	if d.config.CCIndex != "" && d.config.CCIndex != "latest" {
		if strings.HasPrefix(d.config.CCIndex, "http://") || strings.HasPrefix(d.config.CCIndex, "https://") {
			return d.config.CCIndex, nil
		}
		return fmt.Sprintf("https://index.commoncrawl.org/%s-index", d.config.CCIndex), nil
	}

	req, err := d.newRequest(ctx, d.config.CollInfoURL)
	if err != nil {
		return "", err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("commoncrawl collinfo returned HTTP %d", resp.StatusCode)
	}

	var indexes []ccIndex
	if err := json.NewDecoder(resp.Body).Decode(&indexes); err != nil {
		return "", err
	}
	for _, index := range indexes {
		if index.CDXAPI != "" {
			return index.CDXAPI, nil
		}
	}
	return "", errors.New("commoncrawl collinfo did not contain a cdx-api endpoint")
}

func (d *Discoverer) discoverCRTSh(ctx context.Context) ([]Result, error) {
	base, err := url.Parse(d.config.CRTShURL)
	if err != nil {
		return nil, err
	}
	values := base.Query()
	values.Set("q", "%.cz")
	values.Set("output", "json")
	base.RawQuery = values.Encode()

	req, err := d.newRequest(ctx, base.String())
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("crtsh returned HTTP %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024*1024))
	if err != nil {
		return nil, err
	}
	var rows []struct {
		NameValue string `json:"name_value"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	results := make([]Result, 0)
	for _, row := range rows {
		for _, name := range strings.Split(row.NameValue, "\n") {
			name = strings.TrimPrefix(strings.TrimSpace(name), "*.")
			domain, err := domainutil.FromHost(name)
			if err != nil {
				continue
			}
			if _, ok := seen[domain]; ok {
				continue
			}
			seen[domain] = struct{}{}
			results = append(results, Result{Domain: domain, Source: "crtsh"})
			if d.config.Limit > 0 && len(results) >= d.config.Limit {
				return results, nil
			}
		}
	}
	return results, nil
}

func (d *Discoverer) newRequest(ctx context.Context, rawURL string) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", d.config.UserAgent)
	return req, nil
}
