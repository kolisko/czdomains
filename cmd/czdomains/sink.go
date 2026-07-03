package main

import (
	"bufio"
	"context"
	"os"

	"czdomains/internal/discovery"
)

type optionalTXTSink struct {
	primary discovery.Sink
	file    *os.File
	writer  *bufio.Writer
}

func newOptionalTXTSink(primary discovery.Sink, outPath string, fresh bool) (*optionalTXTSink, error) {
	sink := &optionalTXTSink{primary: primary}
	if outPath == "" {
		return sink, nil
	}
	flag := os.O_CREATE | os.O_WRONLY | os.O_APPEND
	if fresh {
		flag = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}
	file, err := os.OpenFile(outPath, flag, 0o644)
	if err != nil {
		return nil, err
	}
	sink.file = file
	sink.writer = bufio.NewWriter(file)
	return sink, nil
}

func (s *optionalTXTSink) AddDomain(ctx context.Context, domain discovery.FoundDomain) (bool, error) {
	inserted, err := s.primary.AddDomain(ctx, domain)
	if err != nil || !inserted || s.writer == nil {
		return inserted, err
	}
	if _, err := s.writer.WriteString(domain.Domain + "\n"); err != nil {
		return inserted, err
	}
	return inserted, nil
}

func (s *optionalTXTSink) Count(ctx context.Context) (int, error) {
	return s.primary.Count(ctx)
}

func (s *optionalTXTSink) Flush() error {
	if s.writer != nil {
		if err := s.writer.Flush(); err != nil {
			return err
		}
	}
	if flusher, ok := s.primary.(interface{ Flush() error }); ok {
		return flusher.Flush()
	}
	return nil
}

func (s *optionalTXTSink) Close() error {
	if err := s.Flush(); err != nil {
		if s.file != nil {
			_ = s.file.Close()
		}
		return err
	}
	if s.file != nil {
		return s.file.Close()
	}
	return nil
}
