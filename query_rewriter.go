package gockhouse

import (
	"regexp"
	"strings"
	"sync"

	"github.com/AfterShip/clickhouse-sql-parser/parser"
)

// QueryRewriter rewrites ClickHouse queries for the chdb single-node context.
// It tracks Distributed table mappings and rewrites references in subsequent queries.
//
// QueryRewriter implements parser.WalkFunc via Visit — pass r.Visit to parser.Walk.
// To add a new AST rewrite, add a case in Visit and a corresponding visitXxx method.
type QueryRewriter struct {
	mu        sync.RWMutex
	distByKey map[string]string // "db.table" or "table" → local table name
}

func NewQueryRewriter() *QueryRewriter {
	return &QueryRewriter{distByKey: make(map[string]string)}
}

// ProcessQuery applies all query rewrites for the chdb single-node context.
//
//   - CREATE TABLE ... Distributed(...): record mapping, skip=true
//   - DROP TABLE <dist>: remove mapping, skip=true
//   - RENAME TABLE <dist> TO <new>: update mapping; execute remaining non-dist pairs
//   - Structural DDL (ALTER TABLE schema changes, TRUNCATE) on a distributed name: skip=true
//   - ALTER TABLE UPDATE/DELETE on a distributed name: rewrite to local
//   - Everything else: rewrite distributed→local table refs + strip ON CLUSTER
func (r *QueryRewriter) ProcessQuery(query string) (string, bool) {
	query = rewriteClusterFunctions(query)

	stmts, err := parser.NewParser(query).ParseStmts()
	if err != nil || len(stmts) == 0 {
		return query, false
	}

	if distName, localName, ok := detectDistributedFromStmts(stmts); ok {
		r.record(distName, localName)
		return "", true
	}

	if key, ok := r.detectDrop(stmts); ok {
		r.remove(key)
		return "", true
	}

	if outSQL, skip, wasRename := r.handleRename(stmts); wasRename {
		return outSQL, skip
	}

	if r.isDDLSkip(stmts) {
		return "", true
	}

	return r.rewriteAndStrip(stmts, query), false
}

func (r *QueryRewriter) record(distName, localName string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.distByKey[distName] = localName
}

func (r *QueryRewriter) remove(key string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.distByKey, key)
}

// ResolveTable returns the local table name for a distributed table reference,
// or tableRef unchanged if it is not a tracked distributed table.
func (r *QueryRewriter) ResolveTable(tableRef string) string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if local, ok := r.distByKey[tableRef]; ok {
		return local
	}
	return tableRef
}

// detectDrop returns the distByKey key if stmts[0] is DROP TABLE targeting a
// tracked distributed table name.
func (r *QueryRewriter) detectDrop(stmts []parser.Expr) (key string, ok bool) {
	d, ok2 := stmts[0].(*parser.DropStmt)
	if !ok2 || d.DropTarget != "TABLE" || d.IsTemporary {
		return "", false
	}
	k := tableIdentKey(d.Name)
	r.mu.RLock()
	_, found := r.distByKey[k]
	r.mu.RUnlock()
	return k, found
}

// handleRename processes RENAME TABLE statements. For each pair where the old
// name is a tracked distributed table, it updates the mapping in distByKey.
// It returns the SQL for any non-distributed pairs (possibly ""), skip=true when
// all pairs were distributed, and wasRename=true when stmts[0] is RENAME TABLE.
func (r *QueryRewriter) handleRename(stmts []parser.Expr) (outSQL string, skip bool, wasRename bool) {
	rn, ok := stmts[0].(*parser.RenameStmt)
	if !ok || rn.RenameTarget != "TABLE" {
		return "", false, false
	}

	type mappingUpdate struct{ oldKey, newKey, localName string }
	var updates []mappingUpdate
	var localPairs []*parser.TargetPair

	for _, pair := range rn.TargetPairList {
		oldKey := tableIdentKey(pair.Old)
		r.mu.RLock()
		localName, found := r.distByKey[oldKey]
		r.mu.RUnlock()
		if found {
			updates = append(updates, mappingUpdate{oldKey, tableIdentKey(pair.New), localName})
		} else {
			localPairs = append(localPairs, pair)
		}
	}

	if len(updates) == 0 {
		// No distributed pairs — fall through to normal rewrite/strip path.
		return "", false, false
	}

	r.mu.Lock()
	for _, u := range updates {
		delete(r.distByKey, u.oldKey)
		r.distByKey[u.newKey] = u.localName
	}
	r.mu.Unlock()

	if len(localPairs) == 0 {
		return "", true, true // all pairs were distributed; skip execution
	}

	// Reconstruct a RENAME TABLE for the non-distributed pairs only.
	rn.TargetPairList = localPairs
	rn.OnCluster = nil
	return rn.String(), false, true
}

// isDDLSkip returns true for structural DDL statements that target a tracked
// distributed table. These are silently skipped — customers apply equivalent
// DDL directly to their local table.
//
// ALTER TABLE mutations (UPDATE/DELETE) are not skipped; they are rewritten.
func (r *QueryRewriter) isDDLSkip(stmts []parser.Expr) bool {
	switch s := stmts[0].(type) {
	case *parser.AlterTable:
		if isAlterMutation(s) {
			return false // data mutations fall through to rewriteAndStrip
		}
		r.mu.RLock()
		_, found := r.distByKey[tableIdentKey(s.TableIdentifier)]
		r.mu.RUnlock()
		return found
	case *parser.TruncateTable:
		r.mu.RLock()
		_, found := r.distByKey[tableIdentKey(s.Name)]
		r.mu.RUnlock()
		return found
	}
	return false
}

// isAlterMutation returns true if every alter expression in at is a data
// mutation (UPDATE or DELETE). Structural changes return false.
func isAlterMutation(at *parser.AlterTable) bool {
	if len(at.AlterExprs) == 0 {
		return false
	}
	for _, expr := range at.AlterExprs {
		switch expr.(type) {
		case *parser.AlterTableUpdate, *parser.AlterTableDelete:
			// data mutation — continue checking
		default:
			return false
		}
	}
	return true
}

// rewriteAndStrip rewrites distributed→local table references and strips any
// ON CLUSTER clause from all statements. Returns the original query unchanged
// when there is nothing to do (no mappings and no ON CLUSTER present).
func (r *QueryRewriter) rewriteAndStrip(stmts []parser.Expr, original string) string {
	r.mu.RLock()
	empty := len(r.distByKey) == 0
	r.mu.RUnlock()

	hasOnCluster := strings.Contains(strings.ToUpper(original), "ON CLUSTER")
	if empty && !hasOnCluster {
		return original
	}

	stripOnCluster(stmts)
	if !empty {
		for _, stmt := range stmts {
			parser.Walk(stmt, r.Visit)
		}
	}

	parts := make([]string, len(stmts))
	for i, s := range stmts {
		parts[i] = s.String()
	}
	return strings.Join(parts, ";\n")
}

// rewriteQueryAST parses query, rewrites distributed table refs, strips ON
// CLUSTER, and returns the serialised result. Returns the original query on
// parse failure.
func (r *QueryRewriter) rewriteQueryAST(query string) string {
	stmts, err := parser.NewParser(query).ParseStmts()
	if err != nil || len(stmts) == 0 {
		return query
	}
	return r.rewriteAndStrip(stmts, query)
}

// Visit is QueryRewriter's WalkFunc. Pass r.Visit directly to parser.Walk.
func (r *QueryRewriter) Visit(node parser.Expr) bool {
	switch n := node.(type) {
	case *parser.TableIdentifier:
		r.visitTableIdentifier(n)
	}
	return true
}

func (r *QueryRewriter) visitTableIdentifier(ti *parser.TableIdentifier) {
	var key string
	if ti.Database != nil {
		key = ti.Database.Name + "." + ti.Table.Name
	} else {
		key = ti.Table.Name
	}
	r.mu.RLock()
	local, found := r.distByKey[key]
	r.mu.RUnlock()
	if !found {
		return
	}
	if idx := strings.Index(local, "."); idx >= 0 {
		if ti.Database != nil {
			ti.Database.Name = local[:idx]
		}
		ti.Table.Name = local[idx+1:]
	} else {
		ti.Table.Name = local
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

// tableIdentKey builds the distByKey lookup key for a TableIdentifier.
func tableIdentKey(ti *parser.TableIdentifier) string {
	if ti.Database != nil {
		return ti.Database.Name + "." + ti.Table.Name
	}
	return ti.Table.Name
}

// stripOnCluster nils out the OnCluster field on every statement type that
// carries it. ChDB is a single-node in-process engine with no cluster support
// and errors on ON CLUSTER clauses.
func stripOnCluster(stmts []parser.Expr) {
	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *parser.CreateTable:
			s.OnCluster = nil
		case *parser.CreateMaterializedView:
			s.OnCluster = nil
		case *parser.CreateView:
			s.OnCluster = nil
		case *parser.CreateFunction:
			s.OnCluster = nil
		case *parser.CreateDatabase:
			s.OnCluster = nil
		case *parser.AlterTable:
			s.OnCluster = nil
		case *parser.DropStmt:
			s.OnCluster = nil
		case *parser.DropDatabase:
			s.OnCluster = nil
		case *parser.TruncateTable:
			s.OnCluster = nil
		case *parser.RenameStmt:
			s.OnCluster = nil
		case *parser.OptimizeStmt:
			s.OnCluster = nil
		case *parser.DeleteClause:
			s.OnCluster = nil
		}
	}
}

// clusterFunctionRe matches clusterAllReplicas() and cluster() table functions.
var clusterFunctionRe = regexp.MustCompile(`(?i)(?:clusterAllReplicas|cluster)\('[^']*',\s*([^)]+)\)`)

// rewriteClusterFunctions strips clusterAllReplicas('cluster', expr) and
// cluster('cluster', expr) wrappers, leaving only the inner table expression.
// This must run before AST parsing since these functions are not TableIdentifiers.
func rewriteClusterFunctions(query string) string {
	return clusterFunctionRe.ReplaceAllStringFunc(query, func(match string) string {
		sub := clusterFunctionRe.FindStringSubmatch(match)
		if len(sub) < 2 {
			return match
		}
		return strings.TrimSpace(sub[1])
	})
}

// detectDistributedFromStmts checks whether stmts[0] is a CREATE TABLE with a
// Distributed engine and extracts the distributed and local table names.
func detectDistributedFromStmts(stmts []parser.Expr) (distName, localName string, ok bool) {
	ct, ok2 := stmts[0].(*parser.CreateTable)
	if !ok2 || ct.Engine == nil || !strings.EqualFold(ct.Engine.Name, "Distributed") {
		return "", "", false
	}
	params := ct.Engine.Params
	if params == nil || params.Items == nil || len(params.Items.Items) < 3 {
		return "", "", false
	}

	tableName := ct.Name.Table.Name
	if ct.Name.Database != nil {
		distName = ct.Name.Database.Name + "." + tableName
	} else {
		distName = tableName
	}

	rawLocal, ok3 := identString(params.Items.Items[2])
	if !ok3 {
		return "", "", false
	}
	rawDB, _ := identString(params.Items.Items[1])

	// Only db-qualify localName when the dist table itself was db-qualified;
	// otherwise an unqualified table with engine param db="default" would produce
	// "default.local_events" instead of "local_events".
	if ct.Name.Database != nil && rawDB != "" {
		localName = rawDB + "." + rawLocal
	} else {
		localName = rawLocal
	}
	return distName, localName, true
}

// detectDistributed parses query and, if it is a CREATE TABLE ... ENGINE =
// Distributed(...) statement, returns the distributed table name (possibly
// db-qualified), the local table name, and ok=true.
func detectDistributed(query string) (distName, localName string, ok bool) {
	stmts, err := parser.NewParser(query).ParseStmts()
	if err != nil || len(stmts) == 0 {
		return "", "", false
	}
	return detectDistributedFromStmts(stmts)
}

func identString(expr parser.Expr) (string, bool) {
	switch v := expr.(type) {
	case *parser.Ident:
		return unquoteIdent(v.Name), true
	case *parser.StringLiteral:
		return unquoteIdent(v.Literal), true
	default:
		return "", false
	}
}

func unquoteIdent(s string) string {
	if len(s) >= 2 {
		f, l := s[0], s[len(s)-1]
		if (f == '`' && l == '`') || (f == '\'' && l == '\'') || (f == '"' && l == '"') {
			return s[1 : len(s)-1]
		}
	}
	return s
}
