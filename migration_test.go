package golumn_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/jonathonwebb/golumn"
)

func TestMigration_Up(t *testing.T) {
	tests := []struct {
		name      string
		migration *golumn.Migration
		wantErr   bool
		errMsg    string
	}{
		{
			name: "successful up migration",
			migration: &golumn.Migration{
				Version: 1,
				UpFunc: func(ctx context.Context, db *sql.DB) error {
					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "up migration with error",
			migration: &golumn.Migration{
				Version: 2,
				UpFunc: func(ctx context.Context, db *sql.DB) error {
					return errors.New("migration failed")
				},
			},
			wantErr: true,
			errMsg:  "migration failed",
		},
		{
			name: "missing up function",
			migration: &golumn.Migration{
				Version: 3,
				UpFunc:  nil,
			},
			wantErr: true,
			errMsg:  "migration 3: missing up func",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := tt.migration.Up(ctx, nil) // Using nil as we control the UpFunc implementation

			if tt.wantErr {
				if err == nil {
					t.Errorf("Migration.Up() expected error but got nil")
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("Migration.Up() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Migration.Up() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestMigration_Down(t *testing.T) {
	tests := []struct {
		name      string
		migration *golumn.Migration
		wantErr   bool
		errMsg    string
	}{
		{
			name: "successful down migration",
			migration: &golumn.Migration{
				Version: 1,
				DownFunc: func(ctx context.Context, db *sql.DB) error {
					return nil
				},
			},
			wantErr: false,
		},
		{
			name: "down migration with error",
			migration: &golumn.Migration{
				Version: 2,
				DownFunc: func(ctx context.Context, db *sql.DB) error {
					return errors.New("rollback failed")
				},
			},
			wantErr: true,
			errMsg:  "rollback failed",
		},
		{
			name: "missing down function",
			migration: &golumn.Migration{
				Version:  4,
				DownFunc: nil,
			},
			wantErr: true,
			errMsg:  "migration 4: missing down func",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			err := tt.migration.Down(ctx, nil) // Using nil as we control the DownFunc implementation

			if tt.wantErr {
				if err == nil {
					t.Errorf("Migration.Down() expected error but got nil")
					return
				}
				if tt.errMsg != "" && err.Error() != tt.errMsg {
					t.Errorf("Migration.Down() error = %v, want %v", err.Error(), tt.errMsg)
				}
			} else {
				if err != nil {
					t.Errorf("Migration.Down() unexpected error = %v", err)
				}
			}
		})
	}
}

func TestMigration_ContextPropagation(t *testing.T) {
	t.Run("up function receives context", func(t *testing.T) {
		type contextKey string
		key := contextKey("test")
		expectedValue := "test-value"

		ctx := context.WithValue(context.Background(), key, expectedValue)

		migration := &golumn.Migration{
			Version: 1,
			UpFunc: func(ctx context.Context, db *sql.DB) error {
				if value := ctx.Value(key); value != expectedValue {
					t.Errorf("Context value = %v, want %v", value, expectedValue)
				}
				return nil
			},
		}

		err := migration.Up(ctx, nil)
		if err != nil {
			t.Errorf("Migration.Up() unexpected error = %v", err)
		}
	})

	t.Run("down function receives context", func(t *testing.T) {
		type contextKey string
		key := contextKey("test")
		expectedValue := "test-value"

		ctx := context.WithValue(context.Background(), key, expectedValue)

		migration := &golumn.Migration{
			Version: 1,
			DownFunc: func(ctx context.Context, db *sql.DB) error {
				if value := ctx.Value(key); value != expectedValue {
					t.Errorf("Context value = %v, want %v", value, expectedValue)
				}
				return nil
			},
		}

		err := migration.Down(ctx, nil)
		if err != nil {
			t.Errorf("Migration.Down() unexpected error = %v", err)
		}
	})
}
