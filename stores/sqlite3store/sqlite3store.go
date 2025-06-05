package sqlite3store

import (
	"context"
	"database/sql"
	"errors"

	"github.com/jonathonwebb/golumn"
	"github.com/mattn/go-sqlite3"
)

type Sqlite3Store struct {
	instance *sql.DB
}

var _ golumn.Store = (*Sqlite3Store)(nil)

func NewSqlite3Store(db *sql.DB) *Sqlite3Store {
	return &Sqlite3Store{db}
}

func (s *Sqlite3Store) DB() *sql.DB {
	return s.instance
}

func (s *Sqlite3Store) Init(ctx context.Context) error {
	if err := s.withTx(ctx, func(tCtx context.Context, tx *sql.Tx) error {
		if _, err := s.instance.ExecContext(tCtx, "CREATE TABLE IF NOT EXISTS schema_lock (id INTEGER PRIMARY KEY)"); err != nil {
			return err
		}

		if _, err := s.instance.ExecContext(tCtx, "CREATE TABLE IF NOT EXISTS schema_migrations (id INTEGER PRIMARY KEY, version_id INTEGER UNIQUE NOT NULL, applied_at DATETIME NOT NULL DEFAULT (datetime('now')))"); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (s *Sqlite3Store) Lock(ctx context.Context) error {
	_, err := s.instance.ExecContext(ctx, "INSERT INTO schema_lock (id) VALUES (1)")
	if err == nil {
		return nil
	}

	var sqliteErr sqlite3.Error
	if errors.As(err, &sqliteErr) && sqliteErr.Code == sqlite3.ErrConstraint {
		return golumn.ErrLocked
	}
	return err
}

func (s *Sqlite3Store) Release(ctx context.Context) error {
	_, err := s.instance.ExecContext(ctx, "DELETE FROM schema_lock WHERE id = 1;")
	if err != nil {
		return err
	}
	return nil
}

func (s *Sqlite3Store) Version(ctx context.Context) (int64, error) {
	row := s.instance.QueryRowContext(ctx, `SELECT version_id FROM schema_migrations ORDER BY version_id DESC LIMIT 1`)
	var version int64
	err := row.Scan(&version)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, golumn.ErrInitialVersion
		}
		return 0, err
	}
	return version, err
}

func (s *Sqlite3Store) Insert(ctx context.Context, v int64) error {
	if _, err := s.instance.ExecContext(ctx, "INSERT INTO schema_migrations (version_id) VALUES (?)", v); err != nil {
		return err
	}
	return nil
}

func (s *Sqlite3Store) Remove(ctx context.Context, v int64) error {
	if _, err := s.instance.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version_id = ?", v); err != nil {
		return err
	}
	return nil
}

func (s *Sqlite3Store) withTx(ctx context.Context, fn func(context.Context, *sql.Tx) error) (err error) {
	tx, err := s.instance.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, tx.Rollback())
	}()

	err = fn(ctx, tx)
	if err != nil {
		return err
	}

	if err = tx.Commit(); err != nil {
		return err
	}

	return nil
}
