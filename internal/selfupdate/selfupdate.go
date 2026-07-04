package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const (
	DefaultAPIURL = "https://api.github.com/repos/kolisko/czdomains/releases/latest"
)

type Config struct {
	CurrentVersion string
	APIURL         string
	ExecutablePath string
	GOOS           string
	GOARCH         string
	Client         *http.Client
}

type Release struct {
	TagName string  `json:"tag_name"`
	HTMLURL string  `json:"html_url"`
	Assets  []Asset `json:"assets"`
}

type Asset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Digest             string `json:"digest"`
	Size               int64  `json:"size"`
}

type CheckResult struct {
	CurrentVersion string
	LatestVersion  string
	LatestURL      string
	Outdated       bool
}

func Check(ctx context.Context, config Config) (CheckResult, error) {
	config = withDefaults(config)
	release, err := Latest(ctx, config)
	if err != nil {
		return CheckResult{}, err
	}
	result := CheckResult{
		CurrentVersion: config.CurrentVersion,
		LatestVersion:  release.TagName,
		LatestURL:      release.HTMLURL,
	}
	result.Outdated = IsReleaseVersion(config.CurrentVersion) && CompareVersions(config.CurrentVersion, release.TagName) < 0
	return result, nil
}

func Latest(ctx context.Context, config Config) (Release, error) {
	config = withDefaults(config)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, config.APIURL, nil)
	if err != nil {
		return Release{}, err
	}
	req.Header.Set("Accept", "application/vnd.github+json")
	req.Header.Set("User-Agent", "czdomains-self-update")
	resp, err := config.Client.Do(req)
	if err != nil {
		return Release{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return Release{}, fmt.Errorf("latest release returned HTTP %d", resp.StatusCode)
	}
	var release Release
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return Release{}, err
	}
	if release.TagName == "" {
		return Release{}, errors.New("latest release did not contain tag_name")
	}
	return release, nil
}

func Update(ctx context.Context, config Config, out io.Writer) error {
	config = withDefaults(config)
	release, err := Latest(ctx, config)
	if err != nil {
		return err
	}
	if IsReleaseVersion(config.CurrentVersion) && CompareVersions(config.CurrentVersion, release.TagName) >= 0 {
		_, _ = fmt.Fprintf(out, "czdomains is already up to date (%s)\n", config.CurrentVersion)
		return nil
	}
	assetName := AssetName(config.GOOS, config.GOARCH)
	asset, ok := findAsset(release.Assets, assetName)
	if !ok {
		return fmt.Errorf("latest release %s does not contain asset %s", release.TagName, assetName)
	}
	exePath, err := executablePath(config.ExecutablePath)
	if err != nil {
		return err
	}
	if err := cleanupStaleFiles(exePath); err != nil {
		return err
	}
	tmpPath, err := downloadAsset(ctx, config, asset, exePath)
	if err != nil {
		return err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpPath)
		}
	}()
	if runtime.GOOS == "windows" || config.GOOS == "windows" {
		if err := scheduleWindowsReplace(tmpPath, exePath); err != nil {
			return err
		}
		cleanup = false
		_, _ = fmt.Fprintf(out, "czdomains update to %s has been scheduled; run czdomains again after this process exits\n", release.TagName)
		return nil
	}
	if err := replaceExecutable(tmpPath, exePath); err != nil {
		return err
	}
	cleanup = false
	_, _ = fmt.Fprintf(out, "czdomains updated to %s\n", release.TagName)
	return nil
}

func IsReleaseVersion(version string) bool {
	version = strings.TrimSpace(version)
	return strings.HasPrefix(version, "v") && len(version) > 1 && version[1] >= '0' && version[1] <= '9'
}

func CompareVersions(a string, b string) int {
	ap := versionParts(a)
	bp := versionParts(b)
	max := len(ap)
	if len(bp) > max {
		max = len(bp)
	}
	for i := 0; i < max; i++ {
		var av, bv int
		if i < len(ap) {
			av = ap[i]
		}
		if i < len(bp) {
			bv = bp[i]
		}
		if av < bv {
			return -1
		}
		if av > bv {
			return 1
		}
	}
	return 0
}

func AssetName(goos string, goarch string) string {
	name := fmt.Sprintf("czdomains-%s-%s", goos, goarch)
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

func withDefaults(config Config) Config {
	if config.APIURL == "" {
		config.APIURL = DefaultAPIURL
	}
	if config.GOOS == "" {
		config.GOOS = runtime.GOOS
	}
	if config.GOARCH == "" {
		config.GOARCH = runtime.GOARCH
	}
	if config.Client == nil {
		config.Client = &http.Client{Timeout: 60 * time.Second}
	}
	if config.CurrentVersion == "" {
		config.CurrentVersion = "dev"
	}
	return config
}

func versionParts(version string) []int {
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	version = strings.SplitN(version, "-", 2)[0]
	fields := strings.Split(version, ".")
	parts := make([]int, 0, len(fields))
	for _, field := range fields {
		if field == "" {
			parts = append(parts, 0)
			continue
		}
		value, err := strconv.Atoi(field)
		if err != nil {
			parts = append(parts, 0)
			continue
		}
		parts = append(parts, value)
	}
	return parts
}

func findAsset(assets []Asset, name string) (Asset, bool) {
	for _, asset := range assets {
		if asset.Name == name {
			return asset, true
		}
	}
	return Asset{}, false
}

func executablePath(path string) (string, error) {
	if path == "" {
		exe, err := os.Executable()
		if err != nil {
			return "", err
		}
		path = exe
	}
	if resolved, err := filepath.EvalSymlinks(path); err == nil {
		path = resolved
	}
	return filepath.Abs(path)
}

func tempDownloadPath(exePath string) string {
	dir := filepath.Dir(exePath)
	base := filepath.Base(exePath)
	return filepath.Join(dir, fmt.Sprintf(".%s.update.%d.tmp", base, os.Getpid()))
}

func tempScriptPath(exePath string) string {
	dir := filepath.Dir(exePath)
	base := filepath.Base(exePath)
	return filepath.Join(dir, fmt.Sprintf(".%s.update.%d.cmd", base, os.Getpid()))
}

func downloadAsset(ctx context.Context, config Config, asset Asset, exePath string) (string, error) {
	if asset.BrowserDownloadURL == "" {
		return "", fmt.Errorf("asset %s does not contain browser_download_url", asset.Name)
	}
	tmpPath := tempDownloadPath(exePath)
	_ = os.Remove(tmpPath)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, asset.BrowserDownloadURL, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "czdomains-self-update")
	resp, err := config.Client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return "", fmt.Errorf("download %s returned HTTP %d", asset.Name, resp.StatusCode)
	}
	file, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return "", err
	}
	hash := sha256.New()
	written, copyErr := io.Copy(io.MultiWriter(file, hash), resp.Body)
	closeErr := file.Close()
	if copyErr != nil {
		_ = os.Remove(tmpPath)
		return "", copyErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return "", closeErr
	}
	if asset.Size > 0 && written != asset.Size {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("downloaded %d bytes for %s, expected %d", written, asset.Name, asset.Size)
	}
	if err := verifyDigest(asset, hash.Sum(nil)); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	mode := os.FileMode(0o755)
	if current, err := os.Stat(exePath); err == nil {
		mode = current.Mode() | 0o700
	}
	if err := os.Chmod(tmpPath, mode); err != nil {
		_ = os.Remove(tmpPath)
		return "", err
	}
	return tmpPath, nil
}

func verifyDigest(asset Asset, got []byte) error {
	if asset.Digest == "" {
		return nil
	}
	want, ok := strings.CutPrefix(asset.Digest, "sha256:")
	if !ok {
		return nil
	}
	gotHex := hex.EncodeToString(got)
	if !strings.EqualFold(want, gotHex) {
		return fmt.Errorf("sha256 mismatch for %s", asset.Name)
	}
	return nil
}

func replaceExecutable(tmpPath string, exePath string) error {
	if err := os.Rename(tmpPath, exePath); err != nil {
		return fmt.Errorf("replace executable: %w", err)
	}
	return nil
}

func scheduleWindowsReplace(tmpPath string, exePath string) error {
	scriptPath := tempScriptPath(exePath)
	_ = os.Remove(scriptPath)
	script := fmt.Sprintf(`@echo off
setlocal
:wait
tasklist /FI "PID eq %d" 2>NUL | find "%d" >NUL
if not errorlevel 1 (
  timeout /T 1 /NOBREAK >NUL
  goto wait
)
move /Y %s %s >NUL
if errorlevel 1 (
  del %s >NUL 2>NUL
  del "%%~f0" >NUL 2>NUL
  exit /B 1
)
del "%%~f0" >NUL 2>NUL
`, os.Getpid(), os.Getpid(), quoteWindows(tmpPath), quoteWindows(exePath), quoteWindows(tmpPath))
	if err := os.WriteFile(scriptPath, []byte(script), 0o600); err != nil {
		return err
	}
	cmd := exec.Command("cmd", "/C", scriptPath)
	if err := cmd.Start(); err != nil {
		_ = os.Remove(scriptPath)
		return err
	}
	return nil
}

func quoteWindows(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}

func cleanupStaleFiles(exePath string) error {
	dir := filepath.Dir(exePath)
	base := filepath.Base(exePath)
	patterns := []string{
		filepath.Join(dir, "."+base+".update.*.tmp"),
		filepath.Join(dir, "."+base+".update.*.cmd"),
	}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(pattern)
		if err != nil {
			return err
		}
		for _, match := range matches {
			_ = os.Remove(match)
		}
	}
	return nil
}
