package discovery

import "context"

type memorySink struct {
	seen    map[string]struct{}
	results []Result
}

func newMemorySink() *memorySink {
	return &memorySink{seen: map[string]struct{}{}}
}

func (s *memorySink) AddDomain(_ context.Context, domain FoundDomain) (bool, error) {
	if _, ok := s.seen[domain.Domain]; ok {
		return false, nil
	}
	s.seen[domain.Domain] = struct{}{}
	s.results = append(s.results, Result{Domain: domain.Domain, Source: domain.Source})
	return true, nil
}

func (s *memorySink) Count(context.Context) (int, error) {
	return len(s.results), nil
}

func (s *memorySink) Results() []Result {
	return s.results
}
