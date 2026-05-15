package gockhouse

import (
	"sync"
	"testing"
)

// ── detectDistributed ────────────────────────────────────────────────────────

func TestDetectDistributed(t *testing.T) {
	cases := []struct {
		name          string
		query         string
		wantDistName  string
		wantLocalName string
		wantOK        bool
	}{
		{
			name:          "unqualified distributed table with identifier args",
			query:         "CREATE TABLE dist_events ENGINE = Distributed(cluster, default, local_events, rand())",
			wantDistName:  "dist_events",
			wantLocalName: "local_events",
			wantOK:        true,
		},
		{
			name:          "unqualified distributed table with string literal args",
			query:         "CREATE TABLE dist_events ENGINE = Distributed('cluster', 'default', 'local_events', rand())",
			wantDistName:  "dist_events",
			wantLocalName: "local_events",
			wantOK:        true,
		},
		{
			name:          "qualified distributed table includes db in both names",
			query:         "CREATE TABLE mydb.dist_events ENGINE = Distributed('cluster', 'mydb', 'local_events', rand())",
			wantDistName:  "mydb.dist_events",
			wantLocalName: "mydb.local_events",
			wantOK:        true,
		},
		{
			name:          "engine keyword is case-insensitive",
			query:         "CREATE TABLE dist_events engine = distributed('c', 'default', 'local_events', rand())",
			wantDistName:  "dist_events",
			wantLocalName: "local_events",
			wantOK:        true,
		},
		{
			name:   "non-distributed engine returns false",
			query:  "CREATE TABLE local_events (id UInt64) ENGINE = MergeTree() ORDER BY id",
			wantOK: false,
		},
		{
			name:   "select statement returns false",
			query:  "SELECT * FROM dist_events",
			wantOK: false,
		},
		{
			name:   "fewer than 3 engine params returns false",
			query:  "CREATE TABLE dist_events ENGINE = Distributed('cluster', 'default')",
			wantOK: false,
		},
		{
			name:   "empty query returns false",
			query:  "",
			wantOK: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dist, local, ok := detectDistributed(tc.query)
			if ok != tc.wantOK {
				t.Fatalf("ok: got %v, want %v", ok, tc.wantOK)
			}
			if !tc.wantOK {
				return
			}
			if dist != tc.wantDistName {
				t.Errorf("distName: got %q, want %q", dist, tc.wantDistName)
			}
			if local != tc.wantLocalName {
				t.Errorf("localName: got %q, want %q", local, tc.wantLocalName)
			}
		})
	}
}

// ── rewriteClusterFunctions ──────────────────────────────────────────────────

func TestRewriteClusterFunctions(t *testing.T) {
	cases := []struct {
		name  string
		input string
		want  string
	}{
		{
			name:  "clusterAllReplicas strips wrapper leaving bare table name",
			input: "SELECT * FROM clusterAllReplicas('mycluster', events)",
			want:  "SELECT * FROM events",
		},
		{
			name:  "clusterAllReplicas strips wrapper leaving qualified table name",
			input: "SELECT * FROM clusterAllReplicas('mycluster', mydb.events)",
			want:  "SELECT * FROM mydb.events",
		},
		{
			name:  "clusterAllReplicas case-insensitive match",
			input: "SELECT * FROM CLUSTERALLREPLICAS('c', events)",
			want:  "SELECT * FROM events",
		},
		{
			name:  "no wrapper is unchanged",
			input: "SELECT * FROM events",
			want:  "SELECT * FROM events",
		},
		{
			name:  "cluster() strips wrapper leaving bare table name",
			input: "SELECT * FROM cluster('mycluster', events)",
			want:  "SELECT * FROM events",
		},
		{
			name:  "cluster() strips wrapper leaving qualified table name",
			input: "SELECT * FROM cluster('mycluster', mydb.events)",
			want:  "SELECT * FROM mydb.events",
		},
		{
			name:  "cluster() case-insensitive match",
			input: "SELECT * FROM CLUSTER('c', events)",
			want:  "SELECT * FROM events",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := rewriteClusterFunctions(tc.input)
			if got != tc.want {
				t.Errorf("got %q, want %q", got, tc.want)
			}
		})
	}
}

// ── rewriteQueryAST ──────────────────────────────────────────────────────────

func TestRewriteQueryAST(t *testing.T) {
	cases := []struct {
		name      string
		query     string
		distByKey map[string]string
		want      string
	}{
		{
			name:      "replaces table in simple select",
			query:     "SELECT * FROM dist_events",
			distByKey: map[string]string{"dist_events": "local_events"},
			want:      "SELECT * FROM local_events",
		},
		{
			name:      "replaces qualified table",
			query:     "SELECT * FROM mydb.dist_events",
			distByKey: map[string]string{"mydb.dist_events": "mydb.local_events"},
			want:      "SELECT * FROM mydb.local_events",
		},
		{
			name:      "replaces table in insert",
			query:     "INSERT INTO dist_events (id) VALUES (1)",
			distByKey: map[string]string{"dist_events": "local_events"},
			want:      "INSERT INTO local_events (id) VALUES (1)",
		},
		{
			name:      "replaces table in where and join",
			query:     "SELECT a.id FROM dist_events AS a JOIN other_table AS b ON a.id = b.id",
			distByKey: map[string]string{"dist_events": "local_events"},
			want:      "SELECT a.id FROM local_events AS a JOIN other_table AS b ON a.id = b.id",
		},
		{
			name:      "no match returns original query unchanged",
			query:     "SELECT * FROM other_table",
			distByKey: map[string]string{"dist_events": "local_events"},
			want:      "SELECT * FROM other_table",
		},
		{
			name:      "unparseable query is returned unchanged",
			query:     "NOT VALID SQL $$$$",
			distByKey: map[string]string{"dist_events": "local_events"},
			want:      "NOT VALID SQL $$$$",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := &QueryRewriter{distByKey: tc.distByKey}
			got := rw.rewriteQueryAST(tc.query)
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// ── QueryRewriter.ProcessQuery ───────────────────────────────────────────────

func TestQueryRewriterProcessQuery(t *testing.T) {
	cases := []struct {
		name      string
		setup     []string // queries run first to register mappings
		query     string
		wantQuery string
		wantSkip  bool
	}{
		{
			name:      "distributed create table is intercepted and skipped",
			query:     "CREATE TABLE dist_events ENGINE = Distributed('cluster', 'default', 'local_events', rand())",
			wantQuery: "",
			wantSkip:  true,
		},
		{
			name:      "non-distributed create table passes through",
			query:     "CREATE TABLE local_events (id UInt64) ENGINE = MergeTree() ORDER BY id",
			wantQuery: "CREATE TABLE local_events (id UInt64) ENGINE = MergeTree() ORDER BY id",
			wantSkip:  false,
		},
		{
			name:      "select with no mappings is unchanged",
			query:     "SELECT * FROM events",
			wantQuery: "SELECT * FROM events",
			wantSkip:  false,
		},
		{
			name:      "registered distributed table is rewritten in select",
			setup:     []string{"CREATE TABLE dist_events ENGINE = Distributed('cluster', 'default', 'local_events', rand())"},
			query:     "SELECT * FROM dist_events",
			wantQuery: "SELECT * FROM local_events",
			wantSkip:  false,
		},
		{
			name:      "registered qualified distributed table is rewritten",
			setup:     []string{"CREATE TABLE mydb.dist_events ENGINE = Distributed('cluster', 'mydb', 'local_events', rand())"},
			query:     "SELECT * FROM mydb.dist_events",
			wantQuery: "SELECT * FROM mydb.local_events",
			wantSkip:  false,
		},
		{
			name:      "clusterAllReplicas wrapper is stripped",
			query:     "SELECT * FROM clusterAllReplicas('c', events)",
			wantQuery: "SELECT * FROM events",
			wantSkip:  false,
		},
		{
			name: "multiple mappings are all applied",
			setup: []string{
				"CREATE TABLE dist_a ENGINE = Distributed('c', 'default', 'local_a', rand())",
				"CREATE TABLE dist_b ENGINE = Distributed('c', 'default', 'local_b', rand())",
			},
			query:     "SELECT * FROM dist_a JOIN dist_b ON dist_a.id = dist_b.id",
			wantQuery: "SELECT * FROM local_a JOIN local_b ON dist_a.id = dist_b.id",
			wantSkip:  false,
		},
		{
			name:      "distributed create table detection is case-insensitive",
			query:     "CREATE TABLE DIST_EVENTS engine = DISTRIBUTED('cluster', 'default', 'local_events', rand())",
			wantQuery: "",
			wantSkip:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range tc.setup {
				rw.ProcessQuery(s)
			}
			gotQuery, gotSkip := rw.ProcessQuery(tc.query)
			if gotSkip != tc.wantSkip {
				t.Fatalf("skip: got %v, want %v", gotSkip, tc.wantSkip)
			}
			if gotQuery != tc.wantQuery {
				t.Errorf("query:\n got  %q\n want %q", gotQuery, tc.wantQuery)
			}
		})
	}
}

// ── DROP TABLE lifecycle ─────────────────────────────────────────────────────

func TestProcessQuery_DropTable(t *testing.T) {
	cases := []struct {
		name           string
		setup          []string
		query          string
		wantSkip       bool
		wantMappingKey string // if non-empty, assert this key is absent after the query
	}{
		{
			name:           "drop tracked distributed table removes mapping and skips",
			setup:          []string{"CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())"},
			query:          "DROP TABLE dist_events",
			wantSkip:       true,
			wantMappingKey: "dist_events",
		},
		{
			name:           "drop with IF EXISTS removes mapping and skips",
			setup:          []string{"CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())"},
			query:          "DROP TABLE IF EXISTS dist_events",
			wantSkip:       true,
			wantMappingKey: "dist_events",
		},
		{
			name:           "drop qualified tracked distributed table removes mapping and skips",
			setup:          []string{"CREATE TABLE mydb.dist_events ENGINE = Distributed('c', 'mydb', 'local_events', rand())"},
			query:          "DROP TABLE mydb.dist_events",
			wantSkip:       true,
			wantMappingKey: "mydb.dist_events",
		},
		{
			name:     "drop non-distributed table passes through",
			query:    "DROP TABLE some_other_table",
			wantSkip: false,
		},
		{
			name:     "drop table not in mapping passes through",
			query:    "DROP TABLE dist_events",
			wantSkip: false,
		},
		{
			name:     "drop view passes through",
			query:    "DROP VIEW some_view",
			wantSkip: false,
		},
		{
			name:     "drop temporary table passes through",
			query:    "DROP TEMPORARY TABLE tmp",
			wantSkip: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range tc.setup {
				rw.ProcessQuery(s)
			}
			_, gotSkip := rw.ProcessQuery(tc.query)
			if gotSkip != tc.wantSkip {
				t.Fatalf("skip: got %v, want %v", gotSkip, tc.wantSkip)
			}
			if tc.wantMappingKey != "" {
				rw.mu.RLock()
				_, stillPresent := rw.distByKey[tc.wantMappingKey]
				rw.mu.RUnlock()
				if stillPresent {
					t.Errorf("mapping key %q was not removed after DROP TABLE", tc.wantMappingKey)
				}
			}
		})
	}
}

func TestProcessQuery_DropAfterRename(t *testing.T) {
	rw := NewQueryRewriter()
	rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
	rw.ProcessQuery("RENAME TABLE dist_events TO dist_events_v2")
	_, skip := rw.ProcessQuery("DROP TABLE dist_events_v2")
	if !skip {
		t.Fatal("DROP TABLE dist_events_v2 after rename should be skipped")
	}
	rw.mu.RLock()
	_, oldPresent := rw.distByKey["dist_events"]
	_, newPresent := rw.distByKey["dist_events_v2"]
	rw.mu.RUnlock()
	if oldPresent || newPresent {
		t.Errorf("both mappings should be gone: old=%v new=%v", oldPresent, newPresent)
	}
}

// ── RENAME TABLE lifecycle ───────────────────────────────────────────────────

func TestProcessQuery_RenameTable(t *testing.T) {
	t.Run("rename single distributed table updates mapping and skips", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
		outSQL, skip := rw.ProcessQuery("RENAME TABLE dist_events TO dist_events_v2")
		if !skip {
			t.Fatalf("expected skip=true, got outSQL=%q skip=%v", outSQL, skip)
		}
		rw.mu.RLock()
		_, oldPresent := rw.distByKey["dist_events"]
		newLocal, newPresent := rw.distByKey["dist_events_v2"]
		rw.mu.RUnlock()
		if oldPresent {
			t.Error("old key dist_events should be removed")
		}
		if !newPresent || newLocal != "local_events" {
			t.Errorf("new key dist_events_v2 should map to local_events, got %q present=%v", newLocal, newPresent)
		}
	})

	t.Run("after rename select uses new distributed name", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
		rw.ProcessQuery("RENAME TABLE dist_events TO dist_v2")
		got, _ := rw.ProcessQuery("SELECT * FROM dist_v2")
		if got != "SELECT * FROM local_events" {
			t.Errorf("got %q, want SELECT * FROM local_events", got)
		}
	})

	t.Run("old distributed name no longer rewrites after rename", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
		rw.ProcessQuery("RENAME TABLE dist_events TO dist_v2")
		got, _ := rw.ProcessQuery("SELECT * FROM dist_events")
		if got != "SELECT * FROM dist_events" {
			t.Errorf("old name should not be rewritten anymore, got %q", got)
		}
	})

	t.Run("rename multiple distributed tables updates all mappings", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_a ENGINE = Distributed('c', 'default', 'local_a', rand())")
		rw.ProcessQuery("CREATE TABLE dist_b ENGINE = Distributed('c', 'default', 'local_b', rand())")
		_, skip := rw.ProcessQuery("RENAME TABLE dist_a TO dist_a_v2, dist_b TO dist_b_v2")
		if !skip {
			t.Fatal("expected skip=true for all-distributed rename")
		}
		rw.mu.RLock()
		aLocal := rw.distByKey["dist_a_v2"]
		bLocal := rw.distByKey["dist_b_v2"]
		rw.mu.RUnlock()
		if aLocal != "local_a" {
			t.Errorf("dist_a_v2 → got %q, want local_a", aLocal)
		}
		if bLocal != "local_b" {
			t.Errorf("dist_b_v2 → got %q, want local_b", bLocal)
		}
	})

	t.Run("mixed rename executes only non-distributed pairs", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
		outSQL, skip := rw.ProcessQuery("RENAME TABLE dist_events TO dist_v2, reg_table TO reg_v2")
		if skip {
			t.Fatal("expected skip=false for mixed rename")
		}
		if outSQL != "RENAME TABLE reg_table TO reg_v2" {
			t.Errorf("outSQL: got %q, want RENAME TABLE reg_table TO reg_v2", outSQL)
		}
	})

	t.Run("rename of non-distributed table falls through to normal processing", func(t *testing.T) {
		rw := NewQueryRewriter()
		_, skip := rw.ProcessQuery("RENAME TABLE a TO b")
		if skip {
			t.Fatal("expected skip=false for non-distributed rename")
		}
	})
}

// ── ON CLUSTER stripping ─────────────────────────────────────────────────────

func TestProcessQuery_OnClusterStripping(t *testing.T) {
	cases := []struct {
		name  string
		setup []string
		query string
		want  string
	}{
		{
			name:  "CREATE TABLE without ON CLUSTER unchanged",
			query: "CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id",
			want:  "CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id",
		},
		{
			name:  "CREATE TABLE ON CLUSTER stripped",
			query: "CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id ON CLUSTER 'mycluster'",
			want:  "CREATE TABLE t (id UInt64) ENGINE = MergeTree() ORDER BY id",
		},
		{
			name:  "DROP TABLE ON CLUSTER stripped",
			query: "DROP TABLE some_table ON CLUSTER '{cluster}'",
			want:  "DROP TABLE some_table",
		},
		{
			name: "ALTER TABLE ON CLUSTER stripped for non-distributed table",
			query: "ALTER TABLE local_events ON CLUSTER 'c' ADD COLUMN extra String",
			want:  "ALTER TABLE local_events ADD COLUMN extra String",
		},
		{
			name:  "RENAME TABLE ON CLUSTER stripped for non-distributed table",
			query: "RENAME TABLE a TO b ON CLUSTER 'c'",
			want:  "RENAME TABLE a TO b",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range tc.setup {
				rw.ProcessQuery(s)
			}
			got, skip := rw.ProcessQuery(tc.query)
			if skip {
				t.Fatalf("unexpected skip=true for query %q", tc.query)
			}
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}

// ── ALTER TABLE structural DDL → skip ────────────────────────────────────────

func TestProcessQuery_AlterTableStructuralSkipsDistributed(t *testing.T) {
	setup := []string{"CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())"}

	cases := []struct {
		name     string
		query    string
		wantSkip bool
	}{
		{
			name:     "ALTER TABLE ADD COLUMN on distributed table is skipped",
			query:    "ALTER TABLE dist_events ADD COLUMN extra String",
			wantSkip: true,
		},
		{
			name:     "ALTER TABLE MODIFY COLUMN on distributed table is skipped",
			query:    "ALTER TABLE dist_events MODIFY COLUMN id UInt64",
			wantSkip: true,
		},
		{
			name:     "ALTER TABLE ON CLUSTER on distributed table is skipped",
			query:    "ALTER TABLE dist_events ON CLUSTER 'c' ADD COLUMN extra String",
			wantSkip: true,
		},
		{
			name:     "ALTER TABLE ADD COLUMN on non-distributed table passes through",
			query:    "ALTER TABLE local_events ADD COLUMN extra String",
			wantSkip: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range setup {
				rw.ProcessQuery(s)
			}
			_, skip := rw.ProcessQuery(tc.query)
			if skip != tc.wantSkip {
				t.Errorf("skip: got %v, want %v", skip, tc.wantSkip)
			}
		})
	}
}

// ── ALTER TABLE mutations → rewrite ─────────────────────────────────────────

func TestProcessQuery_AlterTableMutationRewritesDistributed(t *testing.T) {
	setup := []string{"CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())"}

	cases := []struct {
		name      string
		query     string
		wantQuery string
		wantSkip  bool
	}{
		{
			name:      "ALTER TABLE UPDATE on distributed rewrites to local",
			query:     "ALTER TABLE dist_events UPDATE val = 0 WHERE id > 100",
			wantQuery: "ALTER TABLE local_events UPDATE val = 0 WHERE id > 100",
			wantSkip:  false,
		},
		{
			name:      "ALTER TABLE DELETE on distributed rewrites to local",
			query:     "ALTER TABLE dist_events DELETE WHERE id < 10",
			wantQuery: "ALTER TABLE local_events DELETE WHERE id < 10",
			wantSkip:  false,
		},
		{
			name:      "ALTER TABLE UPDATE on non-distributed passes through",
			query:     "ALTER TABLE other_table UPDATE val = 1 WHERE id = 5",
			wantQuery: "ALTER TABLE other_table UPDATE val = 1 WHERE id = 5",
			wantSkip:  false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range setup {
				rw.ProcessQuery(s)
			}
			got, skip := rw.ProcessQuery(tc.query)
			if skip != tc.wantSkip {
				t.Fatalf("skip: got %v, want %v", skip, tc.wantSkip)
			}
			if got != tc.wantQuery {
				t.Errorf("query:\n got  %q\n want %q", got, tc.wantQuery)
			}
		})
	}
}

// ── TRUNCATE TABLE → skip ────────────────────────────────────────────────────

func TestProcessQuery_TruncateSkipsDistributed(t *testing.T) {
	setup := []string{"CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())"}

	cases := []struct {
		name     string
		query    string
		wantSkip bool
	}{
		{
			name:     "TRUNCATE TABLE on distributed is skipped",
			query:    "TRUNCATE TABLE dist_events",
			wantSkip: true,
		},
		{
			name:     "TRUNCATE TABLE IF EXISTS on distributed is skipped",
			query:    "TRUNCATE TABLE IF EXISTS dist_events",
			wantSkip: true,
		},
		{
			name:     "TRUNCATE TABLE on non-distributed passes through",
			query:    "TRUNCATE TABLE local_events",
			wantSkip: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rw := NewQueryRewriter()
			for _, s := range setup {
				rw.ProcessQuery(s)
			}
			_, skip := rw.ProcessQuery(tc.query)
			if skip != tc.wantSkip {
				t.Errorf("skip: got %v, want %v for query %q", skip, tc.wantSkip, tc.query)
			}
		})
	}
}

// ── cluster() function stripping in ProcessQuery ─────────────────────────────

func TestProcessQuery_ClusterFunctionStripping(t *testing.T) {
	t.Run("cluster() stripped and then dist table rewritten", func(t *testing.T) {
		rw := NewQueryRewriter()
		rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")
		got, skip := rw.ProcessQuery("SELECT * FROM cluster('c', dist_events)")
		if skip {
			t.Fatal("unexpected skip=true")
		}
		if got != "SELECT * FROM local_events" {
			t.Errorf("got %q, want SELECT * FROM local_events", got)
		}
	})
}

// ── concurrent safety ────────────────────────────────────────────────────────

func TestQueryRewriter_ConcurrentDropAndSelect(t *testing.T) {
	rw := NewQueryRewriter()
	rw.ProcessQuery("CREATE TABLE dist_events ENGINE = Distributed('c', 'default', 'local_events', rand())")

	var wg sync.WaitGroup
	for range 50 {
		wg.Add(2)
		go func() {
			defer wg.Done()
			rw.ProcessQuery("DROP TABLE dist_events")
		}()
		go func() {
			defer wg.Done()
			rw.ProcessQuery("SELECT * FROM dist_events")
		}()
	}
	wg.Wait()
}
