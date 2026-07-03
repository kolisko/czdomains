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

	httpAttempts        = 3
	commonCrawlPageSize = 5
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type Config struct {
	Limit        int
	Sources      []string
	CCIndex      string
	CollInfoURL  string
	CRTShURL     string
	UserAgent    string
	Delay        time.Duration
	CCIndexCount int
	PageTracker  PageTracker
	Progress     func(format string, args ...any)
}

type Result struct {
	Domain string
	Source string
}

type FoundDomain struct {
	Domain   string
	Source   string
	IndexURL string
	Page     int
}

type CrawlPage struct {
	Source   string
	IndexURL string
	Page     int
}

type Sink interface {
	AddDomain(ctx context.Context, domain FoundDomain) (bool, error)
	Count(ctx context.Context) (int, error)
}

type PageTracker interface {
	PageComplete(ctx context.Context, page CrawlPage) (bool, error)
	MarkPageStarted(ctx context.Context, page CrawlPage) error
	MarkPageCompleted(ctx context.Context, page CrawlPage) error
	MarkPageFailed(ctx context.Context, page CrawlPage, err error) error
}

type Discoverer struct {
	client HTTPClient
	config Config
}

var errLimitReached = errors.New("discovery limit reached")

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
	sink := newMemorySink()
	err := d.DiscoverTo(ctx, sink)
	return sink.Results(), err
}

func (d *Discoverer) DiscoverTo(ctx context.Context, sink Sink) error {
	var errs []error

	for _, source := range d.config.Sources {
		if d.config.Limit > 0 {
			count, err := sink.Count(ctx)
			if err != nil {
				errs = append(errs, err)
				break
			}
			if count >= d.config.Limit {
				break
			}
		}

		source = strings.ToLower(strings.TrimSpace(source))
		var err error
		switch source {
		case "commoncrawl", "cc":
			err = d.discoverCommonCrawl(ctx, sink)
		case "crtsh":
			err = d.discoverCRTSh(ctx, sink)
		default:
			err = fmt.Errorf("unknown source %q", source)
		}
		if err != nil {
			errs = append(errs, err)
		}
	}

	return errors.Join(errs...)
}

type ccIndex struct {
	ID     string `json:"id"`
	CDXAPI string `json:"cdx-api"`
}

func (d *Discoverer) discoverCommonCrawl(ctx context.Context, sink Sink) error {
	indexURLs, err := d.commonCrawlIndexURLs(ctx)
	if err != nil {
		return err
	}

	total, err := sink.Count(ctx)
	if err != nil {
		return err
	}
	if d.config.Limit > 0 && total >= d.config.Limit {
		return nil
	}
	var errs []error
	for _, indexURL := range indexURLs {
		d.progress("commoncrawl: scanning %s (%d unique domains so far)\n", indexURL, total)
		pageCount, err := d.commonCrawlPageCount(ctx, indexURL)
		if err != nil {
			d.progress("commoncrawl: page count failed for %s: %v\n", indexURL, err)
			errs = append(errs, err)
			pageCount = 0
		}

		beforeIndex := total
		if pageCount == 0 {
			page := CrawlPage{Source: "commoncrawl", IndexURL: indexURL, Page: -1}
			err := d.scanCommonCrawlPage(ctx, page, commonCrawlQuery(indexURL, -1, commonCrawlRawLimit(d.config.Limit)), sink, &total)
			if errors.Is(err, errLimitReached) {
				return errors.Join(errs...)
			}
			if err != nil {
				d.progress("commoncrawl: scan failed for %s: %v\n", indexURL, err)
				errs = append(errs, err)
			}
			d.progress("commoncrawl: index added %d unique domains\n", total-beforeIndex)
			continue
		}

		for page := 0; page < pageCount; page++ {
			crawlPage := CrawlPage{Source: "commoncrawl", IndexURL: indexURL, Page: page}
			err := d.scanCommonCrawlPage(ctx, crawlPage, commonCrawlQuery(indexURL, page, 0), sink, &total)
			if errors.Is(err, errLimitReached) {
				return errors.Join(errs...)
			}
			if err != nil {
				d.progress("commoncrawl: page %d/%d failed for %s: %v\n", page+1, pageCount, indexURL, err)
				errs = append(errs, err)
				continue
			}
			d.progress("commoncrawl: page %d/%d, %d unique domains\n", page+1, pageCount, total)
		}
		d.progress("commoncrawl: index added %d unique domains\n", total-beforeIndex)
	}

	return errors.Join(errs...)
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
		values.Set("pageSize", fmt.Sprintf("%d", commonCrawlPageSize))
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
	values.Set("pageSize", fmt.Sprintf("%d", commonCrawlPageSize))
	query.RawQuery = values.Encode()

	var lastErr error
	for attempt := 1; attempt <= httpAttempts; attempt++ {
		resp, err := d.getOK(ctx, query.String(), "commoncrawl page count")
		if err != nil {
			return 0, err
		}
		var info ccPageInfo
		err = json.NewDecoder(resp.Body).Decode(&info)
		_ = resp.Body.Close()
		if err == nil {
			return info.Pages, nil
		}
		lastErr = err
		if attempt < httpAttempts {
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return 0, err
			}
		}
	}
	return 0, lastErr
}

func (d *Discoverer) scanCommonCrawlPage(ctx context.Context, page CrawlPage, rawURL string, sink Sink, total *int) error {
	if d.config.PageTracker != nil {
		complete, err := d.config.PageTracker.PageComplete(ctx, page)
		if err != nil {
			return err
		}
		if complete {
			d.progress("commoncrawl: skipping completed page %d for %s\n", page.Page, page.IndexURL)
			return nil
		}
		if err := d.config.PageTracker.MarkPageStarted(ctx, page); err != nil {
			return err
		}
	}

	var lastErr error
	for attempt := 1; attempt <= httpAttempts; attempt++ {
		resp, err := d.getOK(ctx, rawURL, "commoncrawl")
		if err != nil {
			lastErr = err
			break
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
			inserted, err := sink.AddDomain(ctx, FoundDomain{
				Domain:   domain,
				Source:   "commoncrawl",
				IndexURL: page.IndexURL,
				Page:     page.Page,
			})
			if err != nil {
				_ = resp.Body.Close()
				return err
			}
			if inserted {
				(*total)++
				if d.config.Limit > 0 && *total >= d.config.Limit {
					_ = resp.Body.Close()
					return errLimitReached
				}
			}
		}
		err = scanner.Err()
		_ = resp.Body.Close()
		if err == nil {
			if d.config.PageTracker != nil {
				if err := d.config.PageTracker.MarkPageCompleted(ctx, page); err != nil {
					return err
				}
			}
			return nil
		}
		lastErr = err
		if attempt < httpAttempts {
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return err
			}
		}
	}
	if d.config.PageTracker != nil && lastErr != nil {
		if err := d.config.PageTracker.MarkPageFailed(ctx, page, lastErr); err != nil {
			return err
		}
	}
	return lastErr
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

func (d *Discoverer) commonCrawlIndexURLs(ctx context.Context) ([]string, error) {
	if d.config.CCIndex != "" && d.config.CCIndex != "latest" {
		if strings.HasPrefix(d.config.CCIndex, "http://") || strings.HasPrefix(d.config.CCIndex, "https://") {
			return []string{d.config.CCIndex}, nil
		}
		return []string{fmt.Sprintf("https://index.commoncrawl.org/%s-index", d.config.CCIndex)}, nil
	}

	resp, err := d.getOK(ctx, d.config.CollInfoURL, "commoncrawl collinfo")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var indexes []ccIndex
	if err := json.NewDecoder(resp.Body).Decode(&indexes); err != nil {
		return nil, err
	}
	indexLimit := d.config.CCIndexCount
	if indexLimit < 0 {
		indexLimit = 0
	}
	if indexLimit == 0 {
		indexLimit = len(indexes)
	}
	indexURLs := make([]string, 0, indexLimit)
	for _, index := range indexes {
		if index.CDXAPI != "" {
			indexURLs = append(indexURLs, index.CDXAPI)
			if len(indexURLs) >= indexLimit {
				break
			}
		}
	}
	if len(indexURLs) == 0 {
		return nil, errors.New("commoncrawl collinfo did not contain a cdx-api endpoint")
	}
	return indexURLs, nil
}

func (d *Discoverer) discoverCRTSh(ctx context.Context, sink Sink) error {
	base, err := url.Parse(d.config.CRTShURL)
	if err != nil {
		return err
	}
	values := base.Query()
	values.Set("q", "%.cz")
	values.Set("output", "json")
	base.RawQuery = values.Encode()

	resp, err := d.getOK(ctx, base.String(), "crtsh")
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 128*1024*1024))
	if err != nil {
		return err
	}
	var rows []struct {
		NameValue string `json:"name_value"`
	}
	if err := json.Unmarshal(body, &rows); err != nil {
		return err
	}

	total, err := sink.Count(ctx)
	if err != nil {
		return err
	}
	if d.config.Limit > 0 && total >= d.config.Limit {
		return nil
	}
	for _, row := range rows {
		for _, name := range strings.Split(row.NameValue, "\n") {
			name = strings.TrimPrefix(strings.TrimSpace(name), "*.")
			domain, err := domainutil.FromHost(name)
			if err != nil {
				continue
			}
			inserted, err := sink.AddDomain(ctx, FoundDomain{Domain: domain, Source: "crtsh", Page: -1})
			if err != nil {
				return err
			}
			if inserted {
				total++
				if d.config.Limit > 0 && total >= d.config.Limit {
					return nil
				}
			}
		}
	}
	return nil
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

func (d *Discoverer) getOK(ctx context.Context, rawURL string, label string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= httpAttempts; attempt++ {
		req, err := d.newRequest(ctx, rawURL)
		if err != nil {
			return nil, err
		}
		resp, err := d.client.Do(req)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode <= 299 {
			return resp, nil
		}
		if err != nil {
			lastErr = err
		} else {
			lastErr = fmt.Errorf("%s returned HTTP %d", label, resp.StatusCode)
			if resp.Body != nil {
				_ = resp.Body.Close()
			}
			if !isRetriableStatus(resp.StatusCode) {
				return nil, lastErr
			}
		}
		if attempt < httpAttempts {
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func isRetriableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func sleepBeforeRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(attempt) * 300 * time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (d *Discoverer) progress(format string, args ...any) {
	if d.config.Progress != nil {
		d.config.Progress(format, args...)
	}
}
