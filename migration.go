package luamigrate

import (
	"context"
	"database/sql"
)

type Migration interface {
	Id() int
	Label() string
	Up() func(context.Context, *sql.DB, ...any) error
	Down() func(context.Context, *sql.DB, ...any) error
}

type LuaMigration struct {
	id    int
	label string
	path  string
}

func (lm *LuaMigration) Id() int {
	return lm.id
}

func (lm *LuaMigration) Label() string {
	return lm.label
}

func (lm *LuaMigration) Up(ctx context.Context, db *sql.DB, a ...any) error {
	return nil
}

func (lm *LuaMigration) Down(ctx context.Context, db *sql.DB, a ...any) error {
	return nil
}
