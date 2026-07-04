package discovery

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"czdomains/internal/domainutil"
)

const (
	DefaultCollInfoURL       = "https://index.commoncrawl.org/collinfo.json"
	DefaultCCCollectionsURL  = "https://data.commoncrawl.org/cc-index/collections/index.html"
	DefaultCCDataBaseURL     = "https://data.commoncrawl.org/"
	DefaultCRTShURL          = "https://crt.sh/"
	httpAttempts             = 3
	DefaultCCFailThreshold   = 3
	DefaultCCCooldown        = 15 * time.Minute
	DefaultCCWaitProgress    = time.Second
	maxCommonCrawlLineLength = 8 * 1024 * 1024
)

type HTTPClient interface {
	Do(req *http.Request) (*http.Response, error)
}

type CooldownWaitFunc func(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error

type Config struct {
	Limit            int
	Sources          []string
	CCIndex          string
	CCIndexCount     int
	CollInfoURL      string
	CCCollectionsURL string
	CCDataBaseURL    string
	CRTShURL         string
	UserAgent        string
	Delay            time.Duration
	BlockTracker     BlockTracker
	Progress         func(format string, args ...any)

	CCFailThreshold int
	CCCooldown      time.Duration
	CCWaitProgress  time.Duration
	CCMaxCooldowns  int
	CooldownWait    CooldownWaitFunc
}

type Result struct {
	Domain string
	Source string
}

type FoundDomain struct {
	Domain    string
	Source    string
	IndexFile string
	Block     int64
}

type CrawlBlock struct {
	Source    string
	Crawl     string
	IndexFile string
	Block     int64
}

type Sink interface {
	AddDomain(ctx context.Context, domain FoundDomain) (bool, error)
	Count(ctx context.Context) (int, error)
}

type BlockTracker interface {
	BlockComplete(ctx context.Context, block CrawlBlock) (bool, error)
	MarkBlockStarted(ctx context.Context, block CrawlBlock) error
	MarkBlockCompleted(ctx context.Context, block CrawlBlock) error
	MarkBlockFailed(ctx context.Context, block CrawlBlock, err error) error
}

type Discoverer struct {
	client HTTPClient
	config Config
}

type ccCrawl struct {
	ID string
}

type ccManifest struct {
	CDXPaths     []string
	ClusterPath string
	MetadataPath string
}

type ccClusterBlock struct {
	Key       string
	IndexPath string
	Offset    int64
	Length    int64
}

var (
	errLimitReached      = errors.New("discovery limit reached")
	crawlIDPattern       = regexp.MustCompile(`CC-MAIN-(?:\d{4}-\d{4}|\d{4}-\d{2}|\d{4})`)
	crawlIDSortKeyRegexp = regexp.MustCompile(`^CC-MAIN-(\d{4})(?:-(\d{2,4}))?$`)
)

func New(client HTTPClient, config Config) *Discoverer {
	if client == nil {
		client = &http.Client{Timeout: 30 * time.Second}
	}
	if config.CollInfoURL == "" {
		config.CollInfoURL = DefaultCollInfoURL
	}
	if config.CCCollectionsURL == "" {
		config.CCCollectionsURL = DefaultCCCollectionsURL
	}
	if config.CCDataBaseURL == "" {
		config.CCDataBaseURL = DefaultCCDataBaseURL
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
	if config.Progress == nil {
		config.Progress = func(string, ...any) {}
	}
	if config.CCFailThreshold <= 0 {
		config.CCFailThreshold = DefaultCCFailThreshold
	}
	if config.CCCooldown <= 0 {
		config.CCCooldown = DefaultCCCooldown
	}
	if config.CCWaitProgress <= 0 {
		config.CCWaitProgress = DefaultCCWaitProgress
	}
	if config.CooldownWait == nil {
		config.CooldownWait = waitWithProgress
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

func (d *Discoverer) discoverCommonCrawl(ctx context.Context, sink Sink) error {
	crawls, err := d.commonCrawlCrawls(ctx)
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

	d.progress("commoncrawl: selected %d crawl(s): %s\n", len(crawls), crawlIDs(crawls))
	var errs []error
	for i, crawl := range crawls {
		before := total
		d.progress("commoncrawl: crawl %d/%d %s starting (%d domains in database)\n", i+1, len(crawls), crawl.ID, total)
		err := d.scanCommonCrawlCrawl(ctx, crawl, sink, &total)
		if errors.Is(err, errLimitReached) {
			d.progress("commoncrawl: limit reached after %s; crawl added %d unique domains\n", crawl.ID, total-before)
			return errors.Join(errs...)
		}
		if err != nil {
			d.progress("commoncrawl: crawl %s completed with warning: %v\n", crawl.ID, err)
			errs = append(errs, err)
		}
		d.progress("commoncrawl: crawl %s added %d unique domains (%d total)\n", crawl.ID, total-before, total)
	}

	return errors.Join(errs...)
}

func (d *Discoverer) scanCommonCrawlCrawl(ctx context.Context, crawl ccCrawl, sink Sink, total *int) error {
	manifestURL := d.commonCrawlDataURL(fmt.Sprintf("crawl-data/%s/cc-index.paths.gz", crawl.ID))
	d.progress("commoncrawl: downloading manifest %s\n", manifestURL)
	manifest, err := d.fetchCommonCrawlManifest(ctx, manifestURL)
	if err != nil {
		return err
	}
	d.progress("commoncrawl: manifest has %d CDX files, cluster.idx=%t, metadata.yaml=%t\n", len(manifest.CDXPaths), manifest.ClusterPath != "", manifest.MetadataPath != "")

	if manifest.ClusterPath != "" {
		d.progress("commoncrawl: scan mode cluster range scan\n")
		err := d.scanCommonCrawlWithCluster(ctx, crawl, manifest, sink, total)
		if err == nil || errors.Is(err, errLimitReached) {
			return err
		}
		d.progress("commoncrawl: cluster range scan failed, switching to sequential fallback: %v\n", err)
	}

	d.progress("commoncrawl: scan mode sequential fallback\n")
	return d.scanCommonCrawlSequential(ctx, crawl, manifest.CDXPaths, sink, total)
}

func (d *Discoverer) commonCrawlCrawls(ctx context.Context) ([]ccCrawl, error) {
	index := strings.TrimSpace(d.config.CCIndex)
	if index == "" || strings.EqualFold(index, "latest") {
		return d.latestCommonCrawlCrawls(ctx)
	}
	if strings.HasPrefix(index, "http://") || strings.HasPrefix(index, "https://") || strings.HasSuffix(index, "-index") {
		return nil, fmt.Errorf("commoncrawl: --cc-index must be latest or a crawl id like CC-MAIN-2026-25, not %q", index)
	}
	if !crawlIDPattern.MatchString(index) || crawlIDPattern.FindString(index) != index {
		return nil, fmt.Errorf("commoncrawl: invalid crawl id %q", index)
	}
	manifestURL := d.commonCrawlDataURL(fmt.Sprintf("crawl-data/%s/cc-index.paths.gz", index))
	d.progress("commoncrawl: verifying explicit crawl %s via %s\n", index, manifestURL)
	resp, err := d.getOKWithCooldown(ctx, manifestURL, "commoncrawl manifest verification", fmt.Sprintf("manifest %s", index))
	if err != nil {
		return nil, err
	}
	_ = resp.Body.Close()
	return []ccCrawl{{ID: index}}, nil
}

func (d *Discoverer) latestCommonCrawlCrawls(ctx context.Context) ([]ccCrawl, error) {
	d.progress("commoncrawl: looking up crawls via collinfo.json %s\n", d.config.CollInfoURL)
	crawls, err := d.commonCrawlCrawlsFromCollInfo(ctx)
	if err != nil {
		d.progress("commoncrawl: collinfo.json lookup failed: %v\n", err)
		d.progress("commoncrawl: falling back to data HTML index %s\n", d.config.CCCollectionsURL)
		crawls, err = d.commonCrawlCrawlsFromHTML(ctx)
		if err != nil {
			return nil, err
		}
	} else {
		d.progress("commoncrawl: collinfo.json returned %d crawl(s)\n", len(crawls))
	}
	sortCrawls(crawls)
	limit := d.config.CCIndexCount
	if limit < 0 {
		limit = 0
	}
	if limit == 0 {
		limit = 1
	}
	if limit < len(crawls) {
		crawls = crawls[:limit]
	}
	if len(crawls) == 0 {
		return nil, errors.New("commoncrawl: no crawls found")
	}
	return crawls, nil
}

func (d *Discoverer) commonCrawlCrawlsFromCollInfo(ctx context.Context) ([]ccCrawl, error) {
	resp, err := d.getOKLimited(ctx, d.config.CollInfoURL, "commoncrawl collinfo", "Common Crawl crawl list")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var rows []struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&rows); err != nil {
		return nil, err
	}
	crawls := make([]ccCrawl, 0, len(rows))
	for _, row := range rows {
		if row.ID != "" && crawlIDPattern.FindString(row.ID) == row.ID {
			crawls = append(crawls, ccCrawl{ID: row.ID})
		}
	}
	if len(crawls) == 0 {
		return nil, errors.New("commoncrawl: collinfo.json did not contain crawl ids")
	}
	return dedupeCrawls(crawls), nil
}

func (d *Discoverer) commonCrawlCrawlsFromHTML(ctx context.Context) ([]ccCrawl, error) {
	resp, err := d.getOKLimited(ctx, d.config.CCCollectionsURL, "commoncrawl collections", "Common Crawl HTML crawl list")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8*1024*1024))
	if err != nil {
		return nil, err
	}
	matches := crawlIDPattern.FindAllString(string(body), -1)
	crawls := make([]ccCrawl, 0, len(matches))
	for _, match := range matches {
		crawls = append(crawls, ccCrawl{ID: match})
	}
	if len(crawls) == 0 {
		return nil, errors.New("commoncrawl: HTML crawl list did not contain crawl ids")
	}
	crawls = dedupeCrawls(crawls)
	d.progress("commoncrawl: HTML crawl list returned %d crawl(s)\n", len(crawls))
	return crawls, nil
}

func (d *Discoverer) fetchCommonCrawlManifest(ctx context.Context, manifestURL string) (ccManifest, error) {
	resp, err := d.getOKWithCooldown(ctx, manifestURL, "commoncrawl manifest", path.Base(manifestURL))
	if err != nil {
		return ccManifest{}, err
	}
	defer resp.Body.Close()
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return ccManifest{}, err
	}
	defer gz.Close()
	return parseCommonCrawlManifest(gz)
}

func parseCommonCrawlManifest(r io.Reader) (ccManifest, error) {
	var manifest ccManifest
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxCommonCrawlLineLength)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		switch {
		case strings.Contains(line, "indexes/cdx-") && strings.HasSuffix(line, ".gz"):
			manifest.CDXPaths = append(manifest.CDXPaths, line)
		case strings.HasSuffix(line, "/indexes/cluster.idx"):
			manifest.ClusterPath = line
		case strings.HasSuffix(line, "/metadata.yaml"):
			manifest.MetadataPath = line
		}
	}
	if err := scanner.Err(); err != nil {
		return ccManifest{}, err
	}
	if len(manifest.CDXPaths) == 0 {
		return ccManifest{}, errors.New("commoncrawl: manifest did not contain any cdx-*.gz files")
	}
	return manifest, nil
}

func (d *Discoverer) scanCommonCrawlWithCluster(ctx context.Context, crawl ccCrawl, manifest ccManifest, sink Sink, total *int) error {
	clusterURL := d.commonCrawlDataURL(manifest.ClusterPath)
	d.progress("commoncrawl: downloading cluster map %s\n", clusterURL)
	resp, err := d.getOKWithCooldown(ctx, clusterURL, "commoncrawl cluster", fmt.Sprintf("cluster.idx %s", crawl.ID))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	blocks, err := parseCommonCrawlCluster(resp.Body)
	if err != nil {
		return err
	}
	if len(blocks) == 0 {
		return errors.New("commoncrawl: cluster.idx did not contain cz blocks")
	}
	d.progress("commoncrawl: cluster selected %d CZ candidate block(s)\n", len(blocks))
	before := *total
	for i, block := range blocks {
		d.progress("commoncrawl: block %d/%d %s offset=%d length=%d (%d new, %d total)\n", i+1, len(blocks), path.Base(block.IndexPath), block.Offset, block.Length, *total-before, *total)
		err := d.scanCommonCrawlRangeBlock(ctx, crawl, block, sink, total)
		if errors.Is(err, errLimitReached) {
			return err
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func parseCommonCrawlCluster(r io.Reader) ([]ccClusterBlock, error) {
	var blocks []ccClusterBlock
	var previous *ccClusterBlock
	inCZRange := false
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), maxCommonCrawlLineLength)
	for scanner.Scan() {
		block, ok := parseClusterLine(scanner.Text())
		if !ok {
			continue
		}
		if block.Key >= "cz," && block.Key < "cz-" {
			if !inCZRange && previous != nil {
				blocks = appendClusterBlock(blocks, *previous)
			}
			blocks = appendClusterBlock(blocks, block)
			inCZRange = true
		} else if inCZRange && block.Key >= "cz-" {
			break
		}
		previous = &block
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return blocks, nil
}

func parseClusterLine(line string) (ccClusterBlock, bool) {
	fields := strings.Fields(line)
	if len(fields) < 5 {
		return ccClusterBlock{}, false
	}
	offset, err := strconv.ParseInt(fields[len(fields)-3], 10, 64)
	if err != nil {
		return ccClusterBlock{}, false
	}
	length, err := strconv.ParseInt(fields[len(fields)-2], 10, 64)
	if err != nil || length <= 0 {
		return ccClusterBlock{}, false
	}
	return ccClusterBlock{
		Key:       fields[0],
		IndexPath: fields[len(fields)-4],
		Offset:    offset,
		Length:    length,
	}, true
}

func appendClusterBlock(blocks []ccClusterBlock, block ccClusterBlock) []ccClusterBlock {
	for _, existing := range blocks {
		if existing.IndexPath == block.IndexPath && existing.Offset == block.Offset {
			return blocks
		}
	}
	return append(blocks, block)
}

func (d *Discoverer) scanCommonCrawlRangeBlock(ctx context.Context, crawl ccCrawl, block ccClusterBlock, sink Sink, total *int) error {
	crawlBlock := CrawlBlock{Source: "commoncrawl", Crawl: crawl.ID, IndexFile: block.IndexPath, Block: block.Offset}
	if d.config.BlockTracker != nil {
		complete, err := d.config.BlockTracker.BlockComplete(ctx, crawlBlock)
		if err != nil {
			return err
		}
		if complete {
			d.progress("commoncrawl: skipping completed block %s offset=%d\n", path.Base(block.IndexPath), block.Offset)
			return nil
		}
		if err := d.config.BlockTracker.MarkBlockStarted(ctx, crawlBlock); err != nil {
			return err
		}
	}

	rawURL := d.commonCrawlDataURL(block.IndexPath)
	resp, err := d.getRangeOKWithCooldown(ctx, rawURL, block.Offset, block.Length, "commoncrawl cdx range", fmt.Sprintf("%s:%d", path.Base(block.IndexPath), block.Offset))
	if err != nil {
		if d.config.BlockTracker != nil {
			_ = d.config.BlockTracker.MarkBlockFailed(ctx, crawlBlock, err)
		}
		return err
	}
	defer resp.Body.Close()
	err = d.scanCommonCrawlCDXGzip(ctx, resp.Body, crawlBlock, sink, total, true)
	if err != nil {
		if d.config.BlockTracker != nil && !errors.Is(err, errLimitReached) {
			_ = d.config.BlockTracker.MarkBlockFailed(ctx, crawlBlock, err)
		}
		return err
	}
	if d.config.BlockTracker != nil {
		if err := d.config.BlockTracker.MarkBlockCompleted(ctx, crawlBlock); err != nil {
			return err
		}
	}
	return nil
}

func (d *Discoverer) scanCommonCrawlSequential(ctx context.Context, crawl ccCrawl, cdxPaths []string, sink Sink, total *int) error {
	before := *total
	seenCZ := false
	for i, cdxPath := range cdxPaths {
		crawlBlock := CrawlBlock{Source: "commoncrawl", Crawl: crawl.ID, IndexFile: cdxPath, Block: 0}
		if d.config.BlockTracker != nil {
			complete, err := d.config.BlockTracker.BlockComplete(ctx, crawlBlock)
			if err != nil {
				return err
			}
			if complete {
				d.progress("commoncrawl: skipping completed file %d/%d %s\n", i+1, len(cdxPaths), path.Base(cdxPath))
				continue
			}
			if err := d.config.BlockTracker.MarkBlockStarted(ctx, crawlBlock); err != nil {
				return err
			}
		}

		d.progress("commoncrawl: file %d/%d %s (%d new, %d total)\n", i+1, len(cdxPaths), path.Base(cdxPath), *total-before, *total)
		resp, err := d.getOKWithCooldown(ctx, d.commonCrawlDataURL(cdxPath), "commoncrawl cdx", path.Base(cdxPath))
		if err != nil {
			if d.config.BlockTracker != nil {
				_ = d.config.BlockTracker.MarkBlockFailed(ctx, crawlBlock, err)
			}
			return err
		}
		hitCZ, passedCZ, scanErr := d.scanCommonCrawlSequentialGzip(ctx, resp.Body, crawlBlock, sink, total)
		_ = resp.Body.Close()
		if scanErr != nil {
			if d.config.BlockTracker != nil && !errors.Is(scanErr, errLimitReached) {
				_ = d.config.BlockTracker.MarkBlockFailed(ctx, crawlBlock, scanErr)
			}
			return scanErr
		}
		if d.config.BlockTracker != nil {
			if err := d.config.BlockTracker.MarkBlockCompleted(ctx, crawlBlock); err != nil {
				return err
			}
		}
		if hitCZ {
			seenCZ = true
		}
		if seenCZ && passedCZ {
			d.progress("commoncrawl: sequential scan passed cz, prefix; stopping after %s\n", path.Base(cdxPath))
			return nil
		}
	}
	return nil
}

func (d *Discoverer) scanCommonCrawlSequentialGzip(ctx context.Context, r io.Reader, block CrawlBlock, sink Sink, total *int) (bool, bool, error) {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return false, false, err
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 64*1024), maxCommonCrawlLineLength)
	hitCZ := false
	passedCZ := false
	for scanner.Scan() {
		key := cdxLineKey(scanner.Text())
		if key >= "cz," && key < "cz-" {
			hitCZ = true
			inserted, err := d.addCDXDomain(ctx, scanner.Text(), block, sink, total)
			if err != nil {
				return hitCZ, passedCZ, err
			}
			if inserted && d.config.Limit > 0 && *total >= d.config.Limit {
				return hitCZ, passedCZ, errLimitReached
			}
			continue
		}
		if hitCZ && key >= "cz-" {
			passedCZ = true
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return hitCZ, passedCZ, err
	}
	return hitCZ, passedCZ, nil
}

func (d *Discoverer) scanCommonCrawlCDXGzip(ctx context.Context, r io.Reader, block CrawlBlock, sink Sink, total *int, onlyCZ bool) error {
	gz, err := gzip.NewReader(r)
	if err != nil {
		return err
	}
	defer gz.Close()
	scanner := bufio.NewScanner(gz)
	scanner.Buffer(make([]byte, 0, 64*1024), maxCommonCrawlLineLength)
	for scanner.Scan() {
		line := scanner.Text()
		if onlyCZ && !strings.HasPrefix(line, "cz,") {
			continue
		}
		inserted, err := d.addCDXDomain(ctx, line, block, sink, total)
		if err != nil {
			return err
		}
		if inserted && d.config.Limit > 0 && *total >= d.config.Limit {
			return errLimitReached
		}
	}
	return scanner.Err()
}

func (d *Discoverer) addCDXDomain(ctx context.Context, line string, block CrawlBlock, sink Sink, total *int) (bool, error) {
	domain, err := domainFromCDXLine(line)
	if err != nil {
		return false, nil
	}
	inserted, err := sink.AddDomain(ctx, FoundDomain{
		Domain:    domain,
		Source:    "commoncrawl",
		IndexFile: block.IndexFile,
		Block:     block.Block,
	})
	if err != nil {
		return false, err
	}
	if inserted {
		(*total)++
	}
	return inserted, nil
}

func domainFromCDXLine(line string) (string, error) {
	return domainFromCDXKey(cdxLineKey(line))
}

func cdxLineKey(line string) string {
	if idx := strings.IndexByte(line, ' '); idx >= 0 {
		return line[:idx]
	}
	return line
}

func domainFromCDXKey(key string) (string, error) {
	if !strings.HasPrefix(key, "cz,") {
		return "", domainutil.ErrNotCZDomain
	}
	end := strings.Index(key, ")/")
	if end < 0 {
		end = strings.IndexByte(key, ')')
	}
	if end < 0 {
		return "", domainutil.ErrNotCZDomain
	}
	hostKey := key[:end]
	parts := strings.Split(hostKey, ",")
	if len(parts) < 2 || parts[0] != "cz" {
		return "", domainutil.ErrNotCZDomain
	}
	for _, label := range parts[1:] {
		if label == "" || strings.Contains(label, "_") {
			return "", domainutil.ErrNotCZDomain
		}
	}
	return domainutil.FromHost(parts[1] + ".cz")
}

func (d *Discoverer) commonCrawlDataURL(objectPath string) string {
	base := strings.TrimRight(d.config.CCDataBaseURL, "/")
	return base + "/" + strings.TrimLeft(objectPath, "/")
}

func sortCrawls(crawls []ccCrawl) {
	sort.Slice(crawls, func(i, j int) bool {
		iy, iw := crawlSortParts(crawls[i].ID)
		jy, jw := crawlSortParts(crawls[j].ID)
		if iy != jy {
			return iy > jy
		}
		return iw > jw
	})
}

func crawlSortParts(id string) (int, int) {
	match := crawlIDSortKeyRegexp.FindStringSubmatch(id)
	if match == nil {
		return 0, 0
	}
	year, _ := strconv.Atoi(match[1])
	week := 0
	if match[2] != "" {
		week, _ = strconv.Atoi(match[2])
	}
	return year, week
}

func dedupeCrawls(crawls []ccCrawl) []ccCrawl {
	seen := map[string]struct{}{}
	out := make([]ccCrawl, 0, len(crawls))
	for _, crawl := range crawls {
		if _, ok := seen[crawl.ID]; ok {
			continue
		}
		seen[crawl.ID] = struct{}{}
		out = append(out, crawl)
	}
	return out
}

func crawlIDs(crawls []ccCrawl) string {
	ids := make([]string, 0, len(crawls))
	for _, crawl := range crawls {
		ids = append(ids, crawl.ID)
	}
	return strings.Join(ids, ", ")
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
			inserted, err := sink.AddDomain(ctx, FoundDomain{Domain: domain, Source: "crtsh", Block: -1})
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
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", d.config.UserAgent)
	return req, nil
}

func (d *Discoverer) getOK(ctx context.Context, rawURL string, label string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= httpAttempts; attempt++ {
		resp, err := d.doRequest(ctx, rawURL, label)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isTransientError(err) {
			return nil, lastErr
		}
		if attempt < httpAttempts {
			d.progress("commoncrawl: retrying %s after transient error: %v\n", label, err)
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func (d *Discoverer) getOKLimited(ctx context.Context, rawURL string, label string, retryLabel string) (*http.Response, error) {
	var lastErr error
	for attempt := 1; attempt <= httpAttempts; attempt++ {
		resp, err := d.doRequest(ctx, rawURL, label)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isTransientError(err) {
			return nil, lastErr
		}
		if attempt < httpAttempts {
			d.progress("commoncrawl: retrying %s after transient error: %v\n", retryLabel, err)
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return nil, err
			}
		}
	}
	return nil, lastErr
}

func (d *Discoverer) getOKWithCooldown(ctx context.Context, rawURL string, label string, retryLabel string) (*http.Response, error) {
	var lastErr error
	transientFailures := 0
	cooldowns := 0
	for attempt := 1; ; attempt++ {
		resp, err := d.doRequest(ctx, rawURL, label)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isTransientError(err) {
			return nil, lastErr
		}
		cooledDown, cooldownErr := d.cooldownAfterTransient(ctx, retryLabel, err, &transientFailures, &cooldowns)
		if cooldownErr != nil {
			return nil, cooldownErr
		}
		if cooledDown {
			attempt = 0
			continue
		}
		if attempt < httpAttempts {
			d.progress("commoncrawl: retrying %s after transient error: %v\n", retryLabel, err)
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return nil, err
			}
			continue
		}
		attempt = 0
	}
}

func (d *Discoverer) getRangeOKWithCooldown(ctx context.Context, rawURL string, offset int64, length int64, label string, retryLabel string) (*http.Response, error) {
	var lastErr error
	transientFailures := 0
	cooldowns := 0
	for attempt := 1; ; attempt++ {
		resp, err := d.doRangeRequest(ctx, rawURL, offset, length, label)
		if err == nil {
			return resp, nil
		}
		lastErr = err
		if !isTransientError(err) {
			return nil, lastErr
		}
		cooledDown, cooldownErr := d.cooldownAfterTransient(ctx, retryLabel, err, &transientFailures, &cooldowns)
		if cooldownErr != nil {
			return nil, cooldownErr
		}
		if cooledDown {
			attempt = 0
			continue
		}
		if attempt < httpAttempts {
			d.progress("commoncrawl: retrying %s after transient error: %v\n", retryLabel, err)
			if err := sleepBeforeRetry(ctx, attempt); err != nil {
				return nil, err
			}
			continue
		}
		attempt = 0
	}
}

func (d *Discoverer) doRequest(ctx context.Context, rawURL string, label string) (*http.Response, error) {
	req, err := d.newRequest(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 200 && resp.StatusCode <= 299 {
		return resp, nil
	}
	statusErr := httpStatusError{
		label:      label,
		status:     resp.StatusCode,
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	return nil, statusErr
}

func (d *Discoverer) doRangeRequest(ctx context.Context, rawURL string, offset int64, length int64, label string) (*http.Response, error) {
	req, err := d.newRequest(ctx, rawURL)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusPartialContent {
		return resp, nil
	}
	statusErr := httpStatusError{
		label:      label,
		status:     resp.StatusCode,
		retryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
	}
	if resp.Body != nil {
		_ = resp.Body.Close()
	}
	return nil, statusErr
}

type httpStatusError struct {
	label      string
	status     int
	retryAfter time.Duration
}

func (e httpStatusError) Error() string {
	return fmt.Sprintf("%s returned HTTP %d", e.label, e.status)
}

func isRetriableStatus(status int) bool {
	return status == http.StatusTooManyRequests || status >= 500
}

func isTransientError(err error) bool {
	if err == nil {
		return false
	}
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
		return isRetriableStatus(statusErr.status)
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) && (netErr.Timeout() || netErr.Temporary()) {
		return true
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "connection refused") ||
		strings.Contains(message, "unexpected eof") ||
		strings.Contains(message, "eof")
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil {
		if seconds <= 0 {
			return 0
		}
		return time.Duration(seconds) * time.Second
	}
	when, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	if !when.After(now) {
		return 0
	}
	return when.Sub(now)
}

func retryAfter(err error) time.Duration {
	var statusErr httpStatusError
	if errors.As(err, &statusErr) {
		return statusErr.retryAfter
	}
	return 0
}

func (d *Discoverer) cooldownAfterTransient(ctx context.Context, retryLabel string, err error, transientFailures *int, cooldowns *int) (bool, error) {
	(*transientFailures)++
	if *transientFailures < d.config.CCFailThreshold {
		return false, nil
	}
	*transientFailures = 0
	if d.config.CCMaxCooldowns > 0 && *cooldowns >= d.config.CCMaxCooldowns {
		return false, err
	}
	*cooldowns++
	duration := retryAfter(err)
	if duration <= 0 {
		duration = d.config.CCCooldown
	}
	d.progress("commoncrawl: transient failures %d/%d, cooling down for %s before retrying %s: %v\n", d.config.CCFailThreshold, d.config.CCFailThreshold, duration.Round(time.Second), retryLabel, err)
	waitErr := d.config.CooldownWait(ctx, duration, d.config.CCWaitProgress, func(remaining time.Duration) {
		if remaining < 0 {
			remaining = 0
		}
		d.progress("\rcommoncrawl: waiting %s before retrying %s   ", remaining.Round(time.Second), retryLabel)
	})
	if waitErr != nil {
		d.progress("\n")
		return true, waitErr
	}
	d.progress("\rcommoncrawl: retrying %s after cooldown\n", retryLabel)
	return true, nil
}

func waitWithProgress(ctx context.Context, duration time.Duration, progressEvery time.Duration, onProgress func(time.Duration)) error {
	if duration <= 0 {
		onProgress(0)
		return nil
	}
	if progressEvery <= 0 {
		progressEvery = duration
	}
	deadline := time.Now().Add(duration)
	onProgress(duration)
	timer := time.NewTimer(duration)
	ticker := time.NewTicker(progressEvery)
	defer timer.Stop()
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			onProgress(time.Until(deadline))
		case <-timer.C:
			onProgress(0)
			return nil
		}
	}
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
