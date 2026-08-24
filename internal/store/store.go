// Package store owns the SQLite schema and every write the control plane
// makes. Callers never hand it raw SQL: each mutation is a method that
// takes a *sql.Tx so state transitions compose inside one transaction.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"time"

	_ "modernc.org/sqlite"
)

// ErrNotFound is returned by getters when no row matches.
var ErrNotFound = errors.New("store: not found")

// Clock returns the current time in Unix milliseconds. It is injectable so
// tests and the scheduler never depend on the wall clock.
type Clock func() int64

// WallClock is the default Clock.
func WallClock() int64 { return time.Now().UnixMilli() }

// Store wraps a single-connection SQLite database. SQLite serialises
// writers anyway; one connection makes that explicit, keeps the pragmas
// connection-wide, and rules out SQLITE_BUSY between our own transactions.
type Store struct {
	db  *sql.DB
	now Clock
}

// Option configures Open.
type Option func(*Store)

// WithClock replaces the timestamp source.
func WithClock(c Clock) Option { return func(s *Store) { s.now = c } }

// Open opens (creating if needed) the database at path, enables WAL,
// busy_timeout and foreign keys, and applies pending migrations.
func Open(ctx context.Context, path string, opts ...Option) (*Store, error) {
	q := url.Values{}
	q.Add("_pragma", "journal_mode(WAL)")
	q.Add("_pragma", "busy_timeout(5000)")
	q.Add("_pragma", "foreign_keys(ON)")
	q.Add("_pragma", "synchronous(NORMAL)")
	// Every Tx is a write: take the write lock up front (BEGIN IMMEDIATE)
	// so a transaction never has to upgrade a read lock mid-way.
	q.Set("_txlock", "immediate")
	db, err := sql.Open("sqlite", "file:"+path+"?"+q.Encode())
	if err != nil {
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	// One connection, never recycled: pragmas are per-connection.
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)
	db.SetConnMaxIdleTime(0)

	s := &Store{db: db, now: WallClock}
	for _, o := range opts {
		o(s)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: open %s: %w", path, err)
	}
	if err := s.migrate(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close releases the underlying connection.
func (s *Store) Close() error { return s.db.Close() }

// Now returns the store's current time in Unix milliseconds.
func (s *Store) Now() int64 { return s.now() }

// Tx runs fn inside a transaction, committing when it returns nil and
// rolling back otherwise (including on panic). Every state transition in
// the system happens inside exactly one Tx call.
func (s *Store) Tx(ctx context.Context, fn func(tx *sql.Tx) error) (err error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("store: begin: %w", err)
	}
	defer func() {
		if p := recover(); p != nil {
			_ = tx.Rollback()
			panic(p)
		}
		if err != nil {
			_ = tx.Rollback()
			return
		}
		if cerr := tx.Commit(); cerr != nil {
			err = fmt.Errorf("store: commit: %w", cerr)
		}
	}()
	return fn(tx)
}
