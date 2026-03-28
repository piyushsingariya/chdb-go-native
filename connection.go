package gockhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chdb "github.com/chdb-io/chdb-go/chdb"
	"github.com/huandu/go-sqlbuilder"
	"github.com/piyushsingariya/gockhouse/internal"
)

// Open creates a chdb-backed ClickHouse connection. dir is the path to a
// persistent session directory; pass an empty string for an in-memory session.
func Open(dir string) (clickhouse.Conn, error) {
	var session *chdb.Session
	var err error
	if dir == "" {
		session, err = chdb.NewSession()
	} else {
		session, err = chdb.NewSession(dir)
	}
	if err != nil {
		return nil, fmt.Errorf("gockhouse: open session: %w", err)
	}
	return &chdbConn{session: session}, nil
}

// clusterAllReplicasRe matches clusterAllReplicas('<cluster>', <table>) and captures
// the table expression so we can rewrite it for chdb's single-node context.
var clusterAllReplicasRe = regexp.MustCompile(`(?i)clusterAllReplicas\('[^']*',\s*([^)]+)\)`)

// rewriteClusterAllReplicas strips the clusterAllReplicas wrapper from a query,
// replacing it with a direct table reference. This lets single-node chdb sessions
// execute queries originally written for a multi-node ClickHouse cluster.
func rewriteClusterAllReplicas(query string) string {
	return clusterAllReplicasRe.ReplaceAllStringFunc(query, func(match string) string {
		sub := clusterAllReplicasRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		return strings.TrimSpace(sub[1])
	})
}

// interpolateArgs substitutes ? placeholders in query using the ClickHouse SQL flavor
// from go-sqlbuilder — the same mechanism used by chdb's own database/sql driver.
func interpolateArgs(query string, args []any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	return sqlbuilder.ClickHouse.Interpolate(query, args)
}

// chdbConn wraps a chdb Session and exposes it as a clickhouse.Conn.
// Exec, Select, Query, and QueryRow execute queries for real via chdb.
// The remaining interface methods are lightweight stubs sufficient for testing.
type chdbConn struct {
	session *chdb.Session
}

var _ clickhouse.Conn = (*chdbConn)(nil)

func (c *chdbConn) Contributors() []string { return nil }

func (c *chdbConn) ServerVersion() (*driver.ServerVersion, error) {
	return &driver.ServerVersion{DisplayName: "chdb"}, nil
}

func (c *chdbConn) Ping(_ context.Context) error { return nil }

func (c *chdbConn) Stats() driver.Stats { return driver.Stats{} }

func (c *chdbConn) Close() error {
	c.session.Close()
	return nil
}

func (c *chdbConn) AsyncInsert(ctx context.Context, query string, _ bool, args ...any) error {
	return c.Exec(ctx, query, args...)
}

func (c *chdbConn) PrepareBatch(ctx context.Context, query string, _ ...driver.PrepareBatchOption) (driver.Batch, error) {
	return newBatch(ctx, c, query)
}

// Exec executes a DDL or DML statement (CREATE TABLE, INSERT, DROP, …) via chdb.
// Any result set is discarded; only errors are surfaced.
func (c *chdbConn) Exec(_ context.Context, query string, args ...any) error {
	query = rewriteClusterAllReplicas(query)
	compiled, err := interpolateArgs(query, args)
	if err != nil {
		return fmt.Errorf("chdbConn: Exec: interpolate args: %w", err)
	}
	result, err := c.session.Query(compiled, "CSV")
	if err != nil {
		return fmt.Errorf("chdbConn: Exec: %w", err)
	}
	defer result.Free()
	return result.Error()
}

// Select executes query and scans all result rows into dest.
// dest must be a pointer to a slice of structs or maps.
//
// Struct fields are matched to ClickHouse columns using the following priority:
//  1. `ch:"<column>"` struct tag
//  2. `json:"<column>"` struct tag
//  3. Lowercased field name
func (c *chdbConn) Select(_ context.Context, dest any, query string, args ...any) error {
	query = rewriteClusterAllReplicas(query)
	compiled, err := interpolateArgs(query, args)
	if err != nil {
		return fmt.Errorf("chdbConn: Select: interpolate args: %w", err)
	}
	result, err := c.session.Query(compiled, "JSONCompact")
	if err != nil {
		return fmt.Errorf("chdbConn: Select: %w", err)
	}
	defer result.Free()
	if err := result.Error(); err != nil {
		return err
	}
	return internal.ScanJSONCompactIntoSlice(result.String(), dest)
}

// Query executes query and returns a Rows iterator.
func (c *chdbConn) Query(_ context.Context, query string, args ...any) (driver.Rows, error) {
	query = rewriteClusterAllReplicas(query)
	compiled, err := interpolateArgs(query, args)
	if err != nil {
		return nil, fmt.Errorf("chdbConn: Query: interpolate args: %w", err)
	}
	result, err := c.session.Query(compiled, "JSONCompact")
	if err != nil {
		return nil, fmt.Errorf("chdbConn: Query: %w", err)
	}
	if err := result.Error(); err != nil {
		result.Free()
		return nil, err
	}
	return internal.NewChdbRows(result)
}

// QueryRow executes query and returns a single Row.
func (c *chdbConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	rows, err := c.Query(ctx, query, args...)
	if err != nil {
		return (&internal.ChdbRow{}).SetError(err)
	}
	return (&internal.ChdbRow{}).SetRows(rows.(*internal.ChdbRows))
}
