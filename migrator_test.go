package golumn_test

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"sync"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/jonathonwebb/golumn"
)

type fakeStore struct {
	versions []int64
	applied  []int64
	reverted []int64
	locked   bool
	mu       sync.Mutex

	initCalls    int
	lockCalls    int
	releaseCalls int
	versionCalls int
	insertCalls  int
	removeCalls  int

	initFunc    func(context.Context, *fakeStore) error
	lockFunc    func(context.Context, *fakeStore) error
	releaseFunc func(context.Context, *fakeStore) error
	versionFunc func(context.Context, *fakeStore) (int64, error)
	insertFunc  func(context.Context, int64, *fakeStore) error
	removeFunc  func(context.Context, int64, *fakeStore) error
}

func defaultInitFunc(_ context.Context, _ *fakeStore) error {
	return nil
}

func defaultLockFunc(_ context.Context, s *fakeStore) error {
	if s.locked {
		return golumn.ErrLocked
	}
	s.mu.Lock()
	s.locked = true
	s.mu.Unlock()
	return nil
}

func defaultReleaseFunc(_ context.Context, s *fakeStore) error {
	s.mu.Lock()
	s.locked = false
	s.mu.Unlock()
	return nil
}

func defaultVersionFunc(_ context.Context, s *fakeStore) (int64, error) {
	if len(s.versions) == 0 {
		return 0, golumn.ErrInitialVersion
	}
	return s.versions[len(s.versions)-1], nil
}

func defaultInsertFunc(_ context.Context, v int64, s *fakeStore) error {
	s.mu.Lock()
	s.versions = append(s.versions, v)
	s.applied = append(s.applied, v)
	s.mu.Unlock()
	return nil
}

func defaultRemoveFunc(_ context.Context, v int64, s *fakeStore) error {
	if len(s.versions) > 0 {
		s.mu.Lock()
		s.versions = s.versions[:len(s.versions)-1]
		s.reverted = append(s.reverted, v)
		s.mu.Unlock()
	}
	return nil
}

func (s *fakeStore) DB() *sql.DB { return nil }

func (s *fakeStore) Init(ctx context.Context) error {
	s.initCalls += 1
	if s.initFunc != nil {
		return s.initFunc(ctx, s)
	}
	return defaultInitFunc(ctx, s)
}

func (s *fakeStore) Lock(ctx context.Context) error {
	s.lockCalls += 1
	if s.lockFunc != nil {
		return s.lockFunc(ctx, s)
	}
	return defaultLockFunc(ctx, s)
}

func (s *fakeStore) Release(ctx context.Context) error {
	s.releaseCalls += 1
	if s.releaseFunc != nil {
		return s.releaseFunc(ctx, s)
	}
	return defaultReleaseFunc(ctx, s)
}

func (s *fakeStore) Version(ctx context.Context) (int64, error) {
	s.versionCalls += 1
	if s.versionFunc != nil {
		return s.versionFunc(ctx, s)
	}
	return defaultVersionFunc(ctx, s)
}

func (s *fakeStore) Insert(ctx context.Context, v int64) error {
	s.insertCalls += 1
	if s.insertFunc != nil {
		return s.insertFunc(ctx, v, s)
	}
	return defaultInsertFunc(ctx, v, s)
}

func (s *fakeStore) Remove(ctx context.Context, v int64) error {
	s.removeCalls += 1
	if s.removeFunc != nil {
		return s.removeFunc(ctx, v, s)
	}
	return defaultRemoveFunc(ctx, v, s)
}

func noopMigration(ctx context.Context, db *sql.DB) error { return nil }

func TestMigrator_Up(t *testing.T) {
	cases := []struct {
		name              string
		store             *fakeStore
		migrations        []*golumn.Migration
		to                int64
		holdLockOnFailure bool

		wantErr      bool
		wantVersions []int64
		wantApplied  []int64
		wantLocked   bool
	}{
		{
			name: "none",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{},
			to:         0,

			wantVersions: []int64{},
			wantApplied:  []int64{},
		},
		{
			name: "none_applied",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{1, 2, 3},
		},
		{
			name: "some_applied",
			store: &fakeStore{
				versions: []int64{1},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{2, 3},
		},
		{
			name: "all_applied",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{},
		},
		{
			name:  "negative_version",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: -1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name:  "misordered",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name:  "duplicate",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name: "init_err",
			store: &fakeStore{
				initFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test init error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name: "lock_err",
			store: &fakeStore{
				lockFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test lock error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name: "release_err",
			store: &fakeStore{
				releaseFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test release error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{1, 2, 3},
			wantLocked:   true,
			wantErr:      true,
		},
		{
			name: "version_err",
			store: &fakeStore{
				versionFunc: func(ctx context.Context, s *fakeStore) (int64, error) { return 0, fmt.Errorf("test version error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name: "version_err",
			store: &fakeStore{
				versionFunc: func(ctx context.Context, s *fakeStore) (int64, error) { return 0, fmt.Errorf("test version error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
		},
		{
			name: "insert_err",
			store: &fakeStore{
				insertFunc: func(ctx context.Context, v int64, s *fakeStore) error {
					if s.insertCalls == 2 {
						return fmt.Errorf("test insert error")
					}
					return defaultInsertFunc(ctx, v, s)
				},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1},
			wantApplied:  []int64{1},
			wantErr:      true,
		},
		{
			name:  "up_err",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: func(_ context.Context, _ *sql.DB) error { return fmt.Errorf("test up migration error") }, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1},
			wantApplied:  []int64{1},
			wantErr:      true,
		},
		{
			name:  "up_err_hold_lock",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: func(_ context.Context, _ *sql.DB) error { return fmt.Errorf("test up migration error") }, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			holdLockOnFailure: true,
			to:                3,

			wantVersions: []int64{1},
			wantApplied:  []int64{1},
			wantErr:      true,
			wantLocked:   true,
		},
		{
			name: "skip_to_specific_version",
			store: &fakeStore{
				versions: []int64{1},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 4, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3, // Skip version 4

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{2, 3},
		},
		{
			name: "target_version_zero",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 0,

			wantVersions: []int64{},
			wantApplied:  []int64{},
		},
		{
			name: "target_below_current_version",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 2,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{},
		},
		{
			name: "initial_version_error_handling",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 2,

			wantVersions: []int64{1, 2},
			wantApplied:  []int64{1, 2},
		},
		{
			name: "gap_in_migration_versions",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 5, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 10, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 10,

			wantVersions: []int64{1, 5, 10},
			wantApplied:  []int64{1, 5, 10},
		},
		{
			name: "target_version_not_in_migrations",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 4, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2},
			wantApplied:  []int64{1, 2},
		},
		{
			name: "single_migration_application",
			store: &fakeStore{
				versions: []int64{1, 2},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 4, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 3,

			wantVersions: []int64{1, 2, 3},
			wantApplied:  []int64{3},
		},
		{
			name: "empty_migrations_with_target",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{},
			to:         5,

			wantVersions: []int64{},
			wantApplied:  []int64{},
		},
		{
			name: "locked",
			store: &fakeStore{
				locked: true,
				lockFunc: func(ctx context.Context, s *fakeStore) error {
					return golumn.ErrLocked
				},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{},
			wantApplied:  []int64{},
			wantErr:      true,
			wantLocked:   true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &golumn.Migrator{
				Store:             tc.store,
				Sources:           tc.migrations,
				HoldLockOnFailure: tc.holdLockOnFailure,
			}
			err := m.Up(t.Context(), tc.to)

			if tc.wantErr && err == nil {
				t.Errorf("%s: wanted err != nil, but got nil", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("%s: wanted err = nil, but got %v", tc.name, err)
			}

			if !slices.Equal(tc.wantVersions, tc.store.versions) {
				diff := cmp.Diff(tc.wantVersions, tc.store.versions)
				t.Errorf("%s: store.versions mismatch (-want, +got):\n%s", tc.name, diff)
			}

			if !slices.Equal(tc.wantApplied, tc.store.applied) {
				diff := cmp.Diff(tc.wantApplied, tc.store.applied)
				t.Errorf("%s: store.applied mismatch (-want, +got):\n%s", tc.name, diff)
			}

			if tc.wantLocked != tc.store.locked {
				t.Errorf("%s: wanted store.locked = %v, but got %v", tc.name, tc.wantLocked, tc.store.locked)
			}
		})
	}

	migrations := []*golumn.Migration{
		{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
		{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
		{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
	}

	migrator := &golumn.Migrator{
		Store:   &fakeStore{},
		Sources: migrations,
	}

	if err := migrator.Up(t.Context(), 2); err != nil {
		t.Error(err)
	}
}

func TestMigrator_Down(t *testing.T) {
	cases := []struct {
		name              string
		store             *fakeStore
		migrations        []*golumn.Migration
		to                int64
		holdLockOnFailure bool

		wantErr      bool
		wantVersions []int64
		wantReverted []int64
		wantLocked   bool
	}{
		{
			name: "none_to_revert",
			store: &fakeStore{
				versions: []int64{1},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1},
			wantReverted: []int64{},
		},
		{
			name: "revert_all",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: -1,

			wantVersions: []int64{},
			wantReverted: []int64{3, 2, 1},
		},
		{
			name: "revert_partial",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1},
			wantReverted: []int64{3, 2},
		},
		{
			name: "already_at_target",
			store: &fakeStore{
				versions: []int64{1, 2},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 2,

			wantVersions: []int64{1, 2},
			wantReverted: []int64{},
		},
		{
			name: "target_below_current",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 0,

			wantErr:      true,
			wantVersions: []int64{1, 2, 3},
			wantReverted: []int64{},
		},
		{
			name:  "negative_version",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: -1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 0,

			wantVersions: []int64{},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name:  "misordered",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 0,

			wantVersions: []int64{},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name:  "duplicate",
			store: &fakeStore{},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 0,

			wantVersions: []int64{},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "missing_target_version",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 5,

			wantVersions: []int64{1, 2, 3},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "missing_remote_version_migration",
			store: &fakeStore{
				versions: []int64{1, 2, 5},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2, 5},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "init_err",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
				initFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test init error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2, 3},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "lock_err",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
				lockFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test lock error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2, 3},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "release_err",
			store: &fakeStore{
				versions:    []int64{1, 2, 3},
				releaseFunc: func(ctx context.Context, _ *fakeStore) error { return fmt.Errorf("test release error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1},
			wantReverted: []int64{3, 2},
			wantLocked:   true,
			wantErr:      true,
		},
		{
			name: "version_err_first_call",
			store: &fakeStore{
				versions:    []int64{1, 2, 3},
				versionFunc: func(ctx context.Context, s *fakeStore) (int64, error) { return 0, fmt.Errorf("test version error") },
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2, 3},
			wantReverted: []int64{},
			wantErr:      true,
		},
		{
			name: "version_err_subsequent_call",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
				versionFunc: func(ctx context.Context, s *fakeStore) (int64, error) {
					if s.versionCalls > 1 {
						return 0, fmt.Errorf("test version error")
					}
					return defaultVersionFunc(ctx, s)
				},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2},
			wantReverted: []int64{3},
			wantErr:      true,
		},
		{
			name: "remove_err",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
				removeFunc: func(ctx context.Context, v int64, s *fakeStore) error {
					if s.removeCalls == 2 {
						return fmt.Errorf("test remove error")
					}
					return defaultRemoveFunc(ctx, v, s)
				},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2},
			wantReverted: []int64{3},
			wantErr:      true,
		},
		{
			name: "down_err",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: func(_ context.Context, _ *sql.DB) error { return fmt.Errorf("test down migration error") }},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{1, 2},
			wantReverted: []int64{3},
			wantErr:      true,
		},
		{
			name: "down_err_hold_lock",
			store: &fakeStore{
				versions: []int64{1, 2, 3},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: func(_ context.Context, _ *sql.DB) error { return fmt.Errorf("test down migration error") }},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			holdLockOnFailure: true,
			to:                1,

			wantVersions: []int64{1, 2},
			wantReverted: []int64{3},
			wantErr:      true,
			wantLocked:   true,
		},
		{
			name: "initial_version",
			store: &fakeStore{
				versions: []int64{},
			},
			migrations: []*golumn.Migration{
				{Version: 1, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 2, UpFunc: noopMigration, DownFunc: noopMigration},
				{Version: 3, UpFunc: noopMigration, DownFunc: noopMigration},
			},
			to: 1,

			wantVersions: []int64{},
			wantReverted: []int64{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &golumn.Migrator{
				Store:             tc.store,
				Sources:           tc.migrations,
				HoldLockOnFailure: tc.holdLockOnFailure,
			}
			err := m.Down(context.Background(), tc.to)

			if tc.wantErr && err == nil {
				t.Errorf("%s: wanted err != nil, but got nil", tc.name)
			}
			if !tc.wantErr && err != nil {
				t.Errorf("%s: wanted err = nil, but got %v", tc.name, err)
			}

			if !slices.Equal(tc.wantVersions, tc.store.versions) {
				diff := cmp.Diff(tc.wantVersions, tc.store.versions)
				t.Errorf("%s: store.versions mismatch (-want, +got):\n%s", tc.name, diff)
			}

			if !slices.Equal(tc.wantReverted, tc.store.reverted) {
				diff := cmp.Diff(tc.wantReverted, tc.store.reverted)
				t.Errorf("%s: store.reverted mismatch (-want, +got):\n%s", tc.name, diff)
			}

			if tc.wantLocked != tc.store.locked {
				t.Errorf("%s: wanted store.locked = %v, but got %v", tc.name, tc.wantLocked, tc.store.locked)
			}
		})
	}
}
