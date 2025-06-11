package sqlite3store_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jonathonwebb/golumn"
	"github.com/jonathonwebb/golumn/stores/sqlite3store"
	_ "github.com/mattn/go-sqlite3"
)

func TestSqlite3Store_New(t *testing.T) {
	db := createTestDB(t)
	defer closeTestDB(t, db)

	store := sqlite3store.New(db)

	if store == nil {
		t.Fatal("NewSqlite3Store returned nil")
	}

	if store.DB() != db {
		t.Error("DB() should return the same database instance")
	}
}

func TestSqlite3Store_Init(t *testing.T) {
	tests := []struct {
		name    string
		setup   func(*sql.DB) error
		wantErr bool
	}{
		{
			name:    "fresh_database",
			setup:   nil,
			wantErr: false,
		},
		{
			name: "tables_already_exist",
			setup: func(db *sql.DB) error {
				store := sqlite3store.New(db)
				return store.Init(context.Background())
			},
			wantErr: false,
		},
		{
			name: "partial_tables_exist",
			setup: func(db *sql.DB) error {
				_, err := db.Exec("CREATE TABLE schema_lock (id INTEGER PRIMARY KEY)")
				return err
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			if tt.setup != nil {
				if err := tt.setup(db); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			store := sqlite3store.New(db)
			err := store.Init(context.Background())

			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.wantErr {
				tables := []string{"schema_lock", "schema_migrations"}
				for _, table := range tables {
					var count int
					query := "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?"
					err := db.QueryRow(query, table).Scan(&count)
					if err != nil {
						t.Errorf("failed to check table %s: %v", table, err)
					}
					if count != 1 {
						t.Errorf("table %s not found", table)
					}
				}
			}
		})
	}
}

func TestSqlite3Store_Lock(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*sqlite3store.Sqlite3Store) error
		wantErr   error
	}{
		{
			name:      "acquire_lock_success",
			setupFunc: nil,
			wantErr:   nil,
		},
		{
			name: "lock_already_held",
			setupFunc: func(store *sqlite3store.Sqlite3Store) error {
				return store.Lock(context.Background())
			},
			wantErr: golumn.ErrLocked,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			store := sqlite3store.New(db)
			if err := store.Init(context.Background()); err != nil {
				t.Fatalf("failed to init store: %v", err)
			}

			if tt.setupFunc != nil {
				if err := tt.setupFunc(store); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			err := store.Lock(context.Background())

			if tt.wantErr != nil {
				if err == nil {
					t.Error("expected error but got none")
				} else if err != tt.wantErr {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if tt.wantErr == nil {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM schema_lock WHERE id = 1").Scan(&count)
				if err != nil {
					t.Errorf("failed to check lock state: %v", err)
				}
				if count != 1 {
					t.Error("lock record not found in database")
				}
			}
		})
	}
}

func TestSqlite3Store_Release(t *testing.T) {
	tests := []struct {
		name      string
		setupFunc func(*sqlite3store.Sqlite3Store) error
		wantErr   bool
	}{
		{
			name:      "release_without_lock",
			setupFunc: nil,
			wantErr:   false,
		},
		{
			name: "release_existing_lock",
			setupFunc: func(store *sqlite3store.Sqlite3Store) error {
				return store.Lock(context.Background())
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			store := sqlite3store.New(db)
			if err := store.Init(context.Background()); err != nil {
				t.Fatalf("failed to init store: %v", err)
			}

			if tt.setupFunc != nil {
				if err := tt.setupFunc(store); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			err := store.Release(context.Background())

			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.wantErr {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM schema_lock WHERE id = 1").Scan(&count)
				if err != nil {
					t.Errorf("failed to check lock state: %v", err)
				}
				if count != 0 {
					t.Error("lock record still exists in database")
				}
			}
		})
	}
}

func TestSqlite3Store_Version(t *testing.T) {
	tests := []struct {
		name        string
		setupFunc   func(*sqlite3store.Sqlite3Store) error
		wantVersion int64
		wantErr     error
	}{
		{
			name:        "no_migrations",
			setupFunc:   nil,
			wantVersion: 0,
			wantErr:     golumn.ErrInitialVersion,
		},
		{
			name: "single_migration",
			setupFunc: func(store *sqlite3store.Sqlite3Store) error {
				return store.Insert(context.Background(), 1)
			},
			wantVersion: 1,
			wantErr:     nil,
		},
		{
			name: "multiple_migrations",
			setupFunc: func(store *sqlite3store.Sqlite3Store) error {
				versions := []int64{1, 3, 2, 5}
				for _, v := range versions {
					if err := store.Insert(context.Background(), v); err != nil {
						return err
					}
				}
				return nil
			},
			wantVersion: 5,
			wantErr:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			store := sqlite3store.New(db)
			if err := store.Init(context.Background()); err != nil {
				t.Fatalf("failed to init store: %v", err)
			}

			if tt.setupFunc != nil {
				if err := tt.setupFunc(store); err != nil {
					t.Fatalf("setup failed: %v", err)
				}
			}

			version, err := store.Version(context.Background())

			if tt.wantErr != nil {
				if err == nil {
					t.Error("expected error but got none")
				} else if err != tt.wantErr {
					t.Errorf("expected error %v, got %v", tt.wantErr, err)
				}
			} else if err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if version != tt.wantVersion {
				t.Errorf("expected version %d, got %d", tt.wantVersion, version)
			}
		})
	}
}

func TestSqlite3Store_Insert(t *testing.T) {
	tests := []struct {
		name      string
		versions  []int64
		insertVer int64
		wantErr   bool
	}{
		{
			name:      "insert_first_migration",
			versions:  []int64{},
			insertVer: 1,
			wantErr:   false,
		},
		{
			name:      "insert_additional_migration",
			versions:  []int64{1, 2},
			insertVer: 3,
			wantErr:   false,
		},
		{
			name:      "insert_duplicate_version",
			versions:  []int64{1, 2},
			insertVer: 2,
			wantErr:   true,
		},
		{
			name:      "insert_zero_version",
			versions:  []int64{},
			insertVer: 0,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			store := sqlite3store.New(db)
			if err := store.Init(context.Background()); err != nil {
				t.Fatalf("failed to init store: %v", err)
			}

			for _, v := range tt.versions {
				if err := store.Insert(context.Background(), v); err != nil {
					t.Fatalf("failed to insert version %d: %v", v, err)
				}
			}

			err := store.Insert(context.Background(), tt.insertVer)

			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.wantErr {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version_id = ?", tt.insertVer).Scan(&count)
				if err != nil {
					t.Errorf("failed to verify insertion: %v", err)
				}
				if count != 1 {
					t.Errorf("expected 1 record for version %d, got %d", tt.insertVer, count)
				}

				var appliedAt string
				err = db.QueryRow("SELECT applied_at FROM schema_migrations WHERE version_id = ?", tt.insertVer).Scan(&appliedAt)
				if err != nil {
					t.Errorf("failed to get applied_at: %v", err)
				}
				if appliedAt == "" {
					t.Error("applied_at should not be empty")
				}
			}
		})
	}
}

func TestSqlite3Store_Remove(t *testing.T) {
	tests := []struct {
		name      string
		versions  []int64
		removeVer int64
		wantErr   bool
	}{
		{
			name:      "remove_existing_version",
			versions:  []int64{1, 2, 3},
			removeVer: 2,
			wantErr:   false,
		},
		{
			name:      "remove_nonexistent_version",
			versions:  []int64{1, 2, 3},
			removeVer: 5,
			wantErr:   false,
		},
		{
			name:      "remove_from_empty_table",
			versions:  []int64{},
			removeVer: 1,
			wantErr:   false,
		},
		{
			name:      "remove_last_version",
			versions:  []int64{1},
			removeVer: 1,
			wantErr:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db := createTestDB(t)
			defer closeTestDB(t, db)

			store := sqlite3store.New(db)
			if err := store.Init(context.Background()); err != nil {
				t.Fatalf("failed to init store: %v", err)
			}

			for _, v := range tt.versions {
				if err := store.Insert(context.Background(), v); err != nil {
					t.Fatalf("failed to insert version %d: %v", v, err)
				}
			}

			err := store.Remove(context.Background(), tt.removeVer)

			if tt.wantErr && err == nil {
				t.Error("expected error but got none")
			}
			if !tt.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}

			if !tt.wantErr {
				var count int
				err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version_id = ?", tt.removeVer).Scan(&count)
				if err != nil {
					t.Errorf("failed to verify removal: %v", err)
				}
				if count != 0 {
					t.Errorf("expected 0 records for version %d after removal, got %d", tt.removeVer, count)
				}

				for _, v := range tt.versions {
					if v != tt.removeVer {
						err := db.QueryRow("SELECT COUNT(*) FROM schema_migrations WHERE version_id = ?", v).Scan(&count)
						if err != nil {
							t.Errorf("failed to check version %d: %v", v, err)
						}
						if count != 1 {
							t.Errorf("version %d should still exist", v)
						}
					}
				}
			}
		})
	}
}

func TestSqlite3Store_Workflow(t *testing.T) {
	db := createTestDB(t)
	defer closeTestDB(t, db)

	store := sqlite3store.New(db)

	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("failed to init: %v", err)
	}

	_, err := store.Version(context.Background())
	if err != golumn.ErrInitialVersion {
		t.Errorf("expected ErrInitialVersion, got %v", err)
	}

	if err := store.Lock(context.Background()); err != nil {
		t.Fatalf("failed to acquire lock: %v", err)
	}

	if err := store.Lock(context.Background()); err != golumn.ErrLocked {
		t.Errorf("expected ErrLocked, got %v", err)
	}

	versions := []int64{1, 2, 3}
	for _, v := range versions {
		if err := store.Insert(context.Background(), v); err != nil {
			t.Fatalf("failed to insert version %d: %v", v, err)
		}

		currentVersion, err := store.Version(context.Background())
		if err != nil {
			t.Errorf("failed to get version after inserting %d: %v", v, err)
		}
		if currentVersion != v {
			t.Errorf("expected current version %d, got %d", v, currentVersion)
		}
	}

	for i := len(versions) - 1; i >= 0; i-- {
		v := versions[i]
		if err := store.Remove(context.Background(), v); err != nil {
			t.Fatalf("failed to remove version %d: %v", v, err)
		}

		var expectedVersion int64
		var expectedErr error
		if i > 0 {
			expectedVersion = versions[i-1]
		} else {
			expectedErr = golumn.ErrInitialVersion
		}

		currentVersion, err := store.Version(context.Background())
		if expectedErr != nil {
			if err != expectedErr {
				t.Errorf("expected error %v after removing %d, got %v", expectedErr, v, err)
			}
		} else {
			if err != nil {
				t.Errorf("unexpected error after removing %d: %v", v, err)
			}
			if currentVersion != expectedVersion {
				t.Errorf("expected version %d after removing %d, got %d", expectedVersion, v, currentVersion)
			}
		}
	}

	// Release lock
	if err := store.Release(context.Background()); err != nil {
		t.Fatalf("failed to release lock: %v", err)
	}

	// Verify lock is released by acquiring it again
	if err := store.Lock(context.Background()); err != nil {
		t.Errorf("should be able to acquire lock after release: %v", err)
	}

	if err := store.Release(context.Background()); err != nil {
		t.Errorf("failed to release lock again: %v", err)
	}
}

func TestSqlite3Store_ContextCancellation(t *testing.T) {
	db := createTestDB(t)
	defer closeTestDB(t, db)

	store := sqlite3store.New(db)
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("failed to init: %v", err)
	}

	tests := []struct {
		name string
		op   func(context.Context, *sqlite3store.Sqlite3Store) error
	}{
		{"lock", func(ctx context.Context, s *sqlite3store.Sqlite3Store) error { return s.Lock(ctx) }},
		{"release", func(ctx context.Context, s *sqlite3store.Sqlite3Store) error { return s.Release(ctx) }},
		{"version", func(ctx context.Context, s *sqlite3store.Sqlite3Store) error { _, err := s.Version(ctx); return err }},
		{"insert", func(ctx context.Context, s *sqlite3store.Sqlite3Store) error { return s.Insert(ctx, 1) }},
		{"remove", func(ctx context.Context, s *sqlite3store.Sqlite3Store) error { return s.Remove(ctx, 1) }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			err := tt.op(ctx, store)
			if err == nil {
				t.Error("expected error due to cancelled context")
			}
		})
	}
}

func TestSqlite3Store_SchemaValidation(t *testing.T) {
	db := createTestDB(t)
	defer closeTestDB(t, db)

	store := sqlite3store.New(db)
	if err := store.Init(context.Background()); err != nil {
		t.Fatalf("failed to init: %v", err)
	}

	// Test schema_migrations table structure
	rows, err := db.Query("PRAGMA table_info(schema_migrations)")
	if err != nil {
		t.Fatalf("failed to get table info: %v", err)
	}
	defer rows.Close()

	columns := make(map[string]string)
	for rows.Next() {
		var cid int
		var name, dataType string
		var notNull, pk int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &pk); err != nil {
			t.Fatalf("failed to scan column info: %v", err)
		}
		columns[name] = dataType
	}

	expectedColumns := map[string]string{
		"id":         "INTEGER",
		"version_id": "INTEGER",
		"applied_at": "DATETIME",
	}

	for name, expectedType := range expectedColumns {
		if actualType, exists := columns[name]; !exists {
			t.Errorf("column %s not found", name)
		} else if actualType != expectedType {
			t.Errorf("column %s has type %s, expected %s", name, actualType, expectedType)
		}
	}

	if err := store.Insert(context.Background(), 1); err != nil {
		t.Fatalf("first insert failed: %v", err)
	}

	if err := store.Insert(context.Background(), 1); err == nil {
		t.Error("duplicate insert should have failed due to UNIQUE constraint")
	}
}

func createTestDB(t *testing.T) *sql.DB {
	t.Helper()

	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("failed to open test database: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Fatalf("failed to ping test database: %v", err)
	}

	return db
}

func closeTestDB(t *testing.T, db *sql.DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Errorf("failed to close test database: %v", err)
	}
}
