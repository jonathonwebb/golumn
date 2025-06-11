package golumn

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
)

type Loader interface {
	Load(context.Context) ([]*Migration, error)
}

type GlobLoader struct {
	Pattern string
}

func (l GlobLoader) Load(ctx context.Context) ([]*Migration, error) {
	matches, err := filepath.Glob(l.Pattern)
	if err != nil {
		return nil, err
	}

	migrations := make([]*Migration, len(matches))
	for i, p := range matches {
		f, err := os.Open(p)
		if err != nil {
			return nil, err
		}
		defer f.Close()

		m, err := Parse(ctx, bufio.NewReader(f), filepath.Base(p))
		if err != nil {
			return nil, err
		}

		migrations[i] = m
	}
	return migrations, nil
}
