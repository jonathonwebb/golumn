package golumn

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
)

type Migrator struct {
	Store   Store
	Sources []*Migration
	LogW    io.Writer
	DebugW  io.Writer

	HoldLockOnFailure bool
}

func (m *Migrator) log(f string, a ...any) {
	if m.LogW != nil {
		fmt.Fprintf(m.LogW, f, a...)
	}
}

func (m *Migrator) debug(f string, a ...any) {
	if m.DebugW != nil {
		fmt.Fprintf(m.DebugW, f, a...)
	}
}

func (m *Migrator) check() error {
	var prev int64 = -1
	seen := map[int64]bool{}

	for _, migration := range m.Sources {
		if migration.Version < 0 {
			return fmt.Errorf("negative migration version: %d", migration.Version)
		}
		if migration.Version < prev {
			return fmt.Errorf("migration order: %d found after %d", migration.Version, prev)
		}
		if _, ok := seen[migration.Version]; ok {
			return fmt.Errorf("duplicate migration version: %d", migration.Version)
		} else {
			seen[migration.Version] = true
		}
		prev = migration.Version
	}

	return nil
}

func (m *Migrator) Up(ctx context.Context, to int64) (err error) {
	defer func() {
		if err == nil {
			m.log("done")
		}
	}()

	if err := m.check(); err != nil {
		return fmt.Errorf("invalid sources: %w", err)
	}

	if err := m.Store.Init(ctx); err != nil {
		return fmt.Errorf("failed to init version store: %w", err)
	}
	if err := m.Store.Lock(ctx); err != nil {
		return fmt.Errorf("failed to get version store lock: %w", err)
	}
	shouldRelease := true
	defer func() {
		if shouldRelease {
			if rlErr := m.Store.Release(ctx); rlErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to release version store lock: %w", rlErr))
			}
		}
	}()

	var remoteVersion int64 = -1
	remoteVersion, err = m.Store.Version(ctx)
	if err != nil {
		if !errors.Is(err, ErrInitialVersion) {
			return fmt.Errorf("failed to get version store state: %w", err)
		}

	}
	m.log("remote version: %d", remoteVersion)

	var toApply []*Migration
	for _, migration := range m.Sources {
		if migration.Version > remoteVersion && migration.Version <= to {
			toApply = append(toApply, migration)
		}
	}

	if len(toApply) == 0 {
		return nil
	}

	if m.HoldLockOnFailure {
		shouldRelease = false
	}
	for _, migration := range m.Sources {
		if migration.Version > remoteVersion && migration.Version <= to {
			m.log("applying migration: %d", migration.Version)
			if err := migration.Up(ctx, m.Store.DB()); err != nil {
				return fmt.Errorf("failed to apply migration %d: %w", migration.Version, err)
			}
			if err := m.Store.Insert(ctx, migration.Version); err != nil {
				return fmt.Errorf("failed to insert migration %d in version store: %w", migration.Version, err)
			}
		}
	}

	shouldRelease = true
	return nil
}

func (m *Migrator) Down(ctx context.Context, to int64) (err error) {
	defer func() {
		if err == nil {
			m.log("done")
		}
	}()

	if err := m.check(); err != nil {
		return fmt.Errorf("invalid sources: %w", err)
	}

	migrationCmpFunc := func(s *Migration, t int64) int {
		if s.Version < t {
			return -1
		}
		if s.Version > t {
			return 1
		}
		return 0
	}

	_, ok := slices.BinarySearchFunc(m.Sources, to, migrationCmpFunc)
	if !ok {
		if to != -1 {
			return fmt.Errorf("missing target version migration: %d", to)
		}
	}

	if err := m.Store.Init(ctx); err != nil {
		return fmt.Errorf("failed to init version store: %w", err)
	}
	if err := m.Store.Lock(ctx); err != nil {
		return fmt.Errorf("failed to get version store lock: %w", err)
	}
	shouldRelease := true
	defer func() {
		if shouldRelease {
			if rlErr := m.Store.Release(ctx); rlErr != nil {
				err = errors.Join(err, fmt.Errorf("failed to release version store lock: %w", rlErr))
			}
		}
	}()

	var remoteVersion int64

	remoteVersion, err = m.Store.Version(ctx)
	if err != nil {
		if errors.Is(err, ErrInitialVersion) {
			return nil
		}
		return fmt.Errorf("failed to get version store state: %w", err)
	}
	m.log("remote version: %d", remoteVersion)

	if m.HoldLockOnFailure {
		shouldRelease = false
	}
	for {
		if remoteVersion <= to {
			break
		}

		idx, ok := slices.BinarySearchFunc(m.Sources, remoteVersion, migrationCmpFunc)
		if !ok {
			return fmt.Errorf("missing remote version migration: %d", remoteVersion)
		}

		migration := m.Sources[idx]
		m.log("reverting migration: %d", migration.Version)
		if err := migration.Down(ctx, m.Store.DB()); err != nil {
			return fmt.Errorf("failed to revert migration %d: %w", migration.Version, err)
		}
		if err := m.Store.Remove(ctx, migration.Version); err != nil {
			return fmt.Errorf("failed to delete migration %d from version store: %w", migration.Version, err)
		}

		remoteVersion, err = m.Store.Version(ctx)
		if err != nil {
			if errors.Is(err, ErrInitialVersion) {
				return nil
			}
			return fmt.Errorf("failed to get version store state: %w", err)
		}
	}

	shouldRelease = true
	return nil
}
