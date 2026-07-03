package ratelimit

import (
	"context"
	"time"
)

type Limiter struct {
	delay time.Duration
	next  time.Time
}

func New(delay time.Duration) *Limiter {
	return &Limiter{delay: delay}
}

func (l *Limiter) Wait(ctx context.Context) error {
	if l == nil || l.delay <= 0 {
		return nil
	}

	now := time.Now()
	if l.next.IsZero() || now.After(l.next) {
		l.next = now.Add(l.delay)
		return nil
	}

	timer := time.NewTimer(time.Until(l.next))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		l.next = time.Now().Add(l.delay)
		return nil
	}
}
