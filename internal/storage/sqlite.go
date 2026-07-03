package storage

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"time"

	"czdomains/internal/discovery"

	_ "modernc.org/sqlite"
)

type Store struct {
	db         *sql.DB
	mu         sync.Mutex
	tx         *sql.Tx
	insertStmt *sql.Stmt
	pending    int
}

type Options struct {
	Fresh bool
}

func Open(path string, options Options) (*Store, error) {
	if path == "" {
		return nil, errors.New("sqlite path is required")
	}
	if options.Fresh {
		if err := removeSQLiteFiles(path); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	store := &Store{db: db}
	if err := store.init(context.Background()); err != nil {
		_ = db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	if err := s.Flush(); err != nil {
		_ = s.db.Close()
		return err
	}
	return s.db.Close()
}

func (s *Store) init(ctx context.Context) error {
	statements := []string{
		`PRAGMA journal_mode=WAL`,
		`PRAGMA synchronous=NORMAL`,
		`PRAGMA temp_store=MEMORY`,
		`PRAGMA busy_timeout=5000`,
		`CREATE TABLE IF NOT EXISTS domains (
			domain TEXT PRIMARY KEY,
			first_seen_source TEXT NOT NULL,
			first_seen_index TEXT,
			first_seen_page INTEGER,
			first_seen_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS crawl_pages (
			source TEXT NOT NULL,
			index_url TEXT NOT NULL,
			page INTEGER NOT NULL,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT,
			started_at TEXT,
			completed_at TEXT,
			PRIMARY KEY (source, index_url, page)
		)`,
		`CREATE INDEX IF NOT EXISTS idx_domains_seen ON domains(first_seen_source, first_seen_index)`,
		`CREATE INDEX IF NOT EXISTS idx_crawl_pages_status ON crawl_pages(status)`,
	}
	for _, statement := range statements {
		if _, err := s.db.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) AddDomain(ctx context.Context, domain discovery.FoundDomain) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if err := s.ensureInsertTx(ctx); err != nil {
		return false, err
	}
	result, err := s.insertStmt.ExecContext(ctx,
		domain.Domain,
		domain.Source,
		nullableString(domain.IndexURL),
		nullableInt(domain.Page),
		time.Now().UTC().Format(time.RFC3339),
	)
	if err != nil {
		return false, err
	}
	s.pending++
	if s.pending >= 2000 {
		if err := s.flushLocked(); err != nil {
			return false, err
		}
	}
	affected, err := result.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func (s *Store) ensureInsertTx(ctx context.Context) error {
	if s.tx != nil {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, `INSERT OR IGNORE INTO domains
		(domain, first_seen_source, first_seen_index, first_seen_page, first_seen_at)
		VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	s.tx = tx
	s.insertStmt = stmt
	s.pending = 0
	return nil
}

func (s *Store) Flush() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.flushLocked()
}

func (s *Store) flushLocked() error {
	if s.tx == nil {
		return nil
	}
	if err := s.insertStmt.Close(); err != nil {
		_ = s.tx.Rollback()
		s.tx = nil
		s.insertStmt = nil
		return err
	}
	if err := s.tx.Commit(); err != nil {
		s.tx = nil
		s.insertStmt = nil
		return err
	}
	s.tx = nil
	s.insertStmt = nil
	s.pending = 0
	return nil
}

func (s *Store) Count(ctx context.Context) (int, error) {
	if err := s.Flush(); err != nil {
		return 0, err
	}
	var count int
	if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM domains`).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

func (s *Store) PageComplete(ctx context.Context, page discovery.CrawlPage) (bool, error) {
	if err := s.Flush(); err != nil {
		return false, err
	}
	var status string
	err := s.db.QueryRowContext(ctx, `SELECT status FROM crawl_pages
		WHERE source = ? AND index_url = ? AND page = ?`,
		page.Source, page.IndexURL, page.Page,
	).Scan(&status)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return status == "completed", nil
}

func (s *Store) MarkPageStarted(ctx context.Context, page discovery.CrawlPage) error {
	if err := s.Flush(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO crawl_pages
		(source, index_url, page, status, attempts, started_at)
		VALUES (?, ?, ?, 'running', 1, ?)
		ON CONFLICT(source, index_url, page) DO UPDATE SET
			status = 'running',
			attempts = attempts + 1,
			started_at = excluded.started_at,
			last_error = NULL`,
		page.Source, page.IndexURL, page.Page, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Store) MarkPageCompleted(ctx context.Context, page discovery.CrawlPage) error {
	if err := s.Flush(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO crawl_pages
		(source, index_url, page, status, attempts, completed_at)
		VALUES (?, ?, ?, 'completed', 1, ?)
		ON CONFLICT(source, index_url, page) DO UPDATE SET
			status = 'completed',
			completed_at = excluded.completed_at,
			last_error = NULL`,
		page.Source, page.IndexURL, page.Page, time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Store) MarkPageFailed(ctx context.Context, page discovery.CrawlPage, pageErr error) error {
	if err := s.Flush(); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `INSERT INTO crawl_pages
		(source, index_url, page, status, attempts, last_error, completed_at)
		VALUES (?, ?, ?, 'failed', 1, ?, ?)
		ON CONFLICT(source, index_url, page) DO UPDATE SET
			status = 'failed',
			last_error = excluded.last_error,
			completed_at = excluded.completed_at`,
		page.Source, page.IndexURL, page.Page, fmt.Sprint(pageErr), time.Now().UTC().Format(time.RFC3339),
	)
	return err
}

func (s *Store) ForEachDomain(ctx context.Context, fn func(domain string) error) error {
	if err := s.Flush(); err != nil {
		return err
	}
	rows, err := s.db.QueryContext(ctx, `SELECT domain FROM domains ORDER BY domain`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var domain string
		if err := rows.Scan(&domain); err != nil {
			return err
		}
		if err := fn(domain); err != nil {
			return err
		}
	}
	return rows.Err()
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func nullableInt(value int) any {
	if value < 0 {
		return nil
	}
	return value
}
