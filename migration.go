package golumn

import (
	"context"
	"database/sql"
	"fmt"
)

type Migration struct {
	Version  int64
	UpFunc   func(context.Context, *sql.DB) error
	DownFunc func(context.Context, *sql.DB) error
}

func (m *Migration) Up(ctx context.Context, db *sql.DB) error {
	if m.UpFunc == nil {
		return fmt.Errorf("migration %d: missing up func", m.Version)
	}
	return m.UpFunc(ctx, db)
}

func (m *Migration) Down(ctx context.Context, db *sql.DB) error {
	if m.DownFunc == nil {
		return fmt.Errorf("migration %d: missing down func", m.Version)
	}
	return m.DownFunc(ctx, db)
}
