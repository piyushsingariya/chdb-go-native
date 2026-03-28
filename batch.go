package gockhouse

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

var insertRe = regexp.MustCompile(`(?i)^\s*INSERT\s+INTO\s+(\S+)\s*(?:\(([^)]+)\))?\s*(?:VALUES\s*)?$`)

// chdbBatch buffers rows for a ClickHouse INSERT and executes them on Send.
// Columns are stored in a columnar layout: colData[colIdx][rowIdx].
type chdbBatch struct {
	conn    *chdbConn
	table   string
	cols    []string
	colData [][]any
	sent    bool
	aborted bool
}

func newBatch(ctx context.Context, conn *chdbConn, query string) (*chdbBatch, error) {
	query = strings.TrimSpace(query)
	m := insertRe.FindStringSubmatch(query)
	if m == nil {
		return nil, fmt.Errorf("chdbBatch: cannot parse INSERT query: %q", query)
	}
	table := m[1]

	var cols []string
	if m[2] != "" {
		for _, c := range strings.Split(m[2], ",") {
			cols = append(cols, strings.TrimSpace(c))
		}
	}

	// No column list in query — DESCRIBE the table to get ordered columns.
	if len(cols) == 0 {
		rows, err := conn.Query(ctx, fmt.Sprintf("DESCRIBE TABLE %s", table))
		if err != nil {
			return nil, fmt.Errorf("chdbBatch: DESCRIBE TABLE %s: %w", table, err)
		}
		defer rows.Close()
		for rows.Next() {
			var name, typ string
			if err := rows.Scan(&name, &typ); err != nil {
				return nil, fmt.Errorf("chdbBatch: scan DESCRIBE: %w", err)
			}
			cols = append(cols, name)
		}
	}

	return &chdbBatch{
		conn:    conn,
		table:   table,
		cols:    cols,
		colData: make([][]any, len(cols)),
	}, nil
}

// Append adds one row (one value per column) to the batch.
func (b *chdbBatch) Append(v ...any) error {
	if b.aborted || b.sent {
		return fmt.Errorf("chdbBatch: batch is closed")
	}
	if len(v) != len(b.cols) {
		return fmt.Errorf("chdbBatch: Append: got %d values for %d columns", len(v), len(b.cols))
	}
	for i, val := range v {
		b.colData[i] = append(b.colData[i], val)
	}
	return nil
}

// AppendStruct adds one row by reading struct fields in column order.
// Field lookup uses the same tag priority as Select: ch → json → lowercase name.
func (b *chdbBatch) AppendStruct(v any) error {
	if b.aborted || b.sent {
		return fmt.Errorf("chdbBatch: batch is closed")
	}
	rv := reflect.ValueOf(v)
	if rv.Kind() == reflect.Ptr {
		rv = rv.Elem()
	}
	if rv.Kind() != reflect.Struct {
		return fmt.Errorf("chdbBatch: AppendStruct: expected struct, got %T", v)
	}
	t := rv.Type()

	for i, col := range b.cols {
		field := findBatchField(t, rv, col)
		if !field.IsValid() {
			b.colData[i] = append(b.colData[i], nil)
			continue
		}
		b.colData[i] = append(b.colData[i], field.Interface())
	}
	return nil
}

// Column returns a BatchColumn that appends values into column i.
func (b *chdbBatch) Column(i int) driver.BatchColumn {
	return &chdbBatchColumn{batch: b, idx: i}
}

// Flush sends the current buffer to chdb and clears it, allowing the batch to
// be reused. This mirrors the clickhouse-go behaviour when WithCloseOnFlush is
// NOT set.
func (b *chdbBatch) Flush() error {
	if b.aborted {
		return fmt.Errorf("chdbBatch: batch was aborted")
	}
	if err := b.execute(); err != nil {
		return err
	}
	// clear buffers
	for i := range b.colData {
		b.colData[i] = nil
	}
	return nil
}

// Send executes the buffered INSERT and marks the batch as sent.
func (b *chdbBatch) Send() error {
	if b.aborted {
		return fmt.Errorf("chdbBatch: batch was aborted")
	}
	if b.sent {
		return fmt.Errorf("chdbBatch: batch already sent")
	}
	if err := b.execute(); err != nil {
		return err
	}
	b.sent = true
	return nil
}

func (b *chdbBatch) execute() error {
	rowCount := b.Rows()
	if rowCount == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("INSERT INTO ")
	sb.WriteString(b.table)
	sb.WriteString(" (")
	for i, c := range b.cols {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(c)
	}
	sb.WriteString(") VALUES ")

	for row := range rowCount {
		if row > 0 {
			sb.WriteString(", ")
		}
		sb.WriteByte('(')
		for col := range len(b.cols) {
			if col > 0 {
				sb.WriteString(", ")
			}
			var val any
			if row < len(b.colData[col]) {
				val = b.colData[col][row]
			}
			sb.WriteString(formatSQLValue(val))
		}
		sb.WriteByte(')')
	}

	return b.conn.Exec(context.Background(), sb.String())
}

func (b *chdbBatch) Abort() error {
	b.aborted = true
	return nil
}

func (b *chdbBatch) Close() error {
	b.aborted = true
	return nil
}

func (b *chdbBatch) IsSent() bool { return b.sent }

func (b *chdbBatch) Rows() int {
	if len(b.colData) == 0 {
		return 0
	}
	return len(b.colData[0])
}

// Columns returns nil — column.Interface requires heavy proto machinery that
// is unnecessary for a test double.
func (b *chdbBatch) Columns() []column.Interface { return nil }

// chdbBatchColumn implements driver.BatchColumn for a single column.
type chdbBatchColumn struct {
	batch *chdbBatch
	idx   int
}

// Append accepts a slice and appends each element as a separate row value for
// this column.
func (c *chdbBatchColumn) Append(v any) error {
	rv := reflect.ValueOf(v)
	if rv.Kind() != reflect.Slice {
		return fmt.Errorf("chdbBatchColumn: Append expects a slice, got %T", v)
	}
	for i := range rv.Len() {
		c.batch.colData[c.idx] = append(c.batch.colData[c.idx], rv.Index(i).Interface())
	}
	return nil
}

// AppendRow appends a single value for this column.
func (c *chdbBatchColumn) AppendRow(v any) error {
	c.batch.colData[c.idx] = append(c.batch.colData[c.idx], v)
	return nil
}

// findBatchField returns the Value of the struct field that maps to colName
// using the same tag priority as the scan side (ch → json → lowercase).
func findBatchField(t reflect.Type, v reflect.Value, colName string) reflect.Value {
	colLower := strings.ToLower(colName)
	for i := range t.NumField() {
		f := t.Field(i)
		if tag, _, _ := strings.Cut(f.Tag.Get("ch"), ","); tag == colName {
			return v.Field(i)
		}
		if tag, _, _ := strings.Cut(f.Tag.Get("json"), ","); tag == colName {
			return v.Field(i)
		}
		if strings.ToLower(f.Name) == colLower {
			return v.Field(i)
		}
	}
	return reflect.Value{}
}

// formatSQLValue converts a Go value into a ClickHouse SQL literal.
func formatSQLValue(v any) string {
	if v == nil {
		return "NULL"
	}
	// Dereference pointers (Nullable types).
	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Ptr {
		if rv.IsNil() {
			return "NULL"
		}
		rv = rv.Elem()
		v = rv.Interface()
	}

	switch val := v.(type) {
	case bool:
		if val {
			return "1"
		}
		return "0"
	case int:
		return strconv.FormatInt(int64(val), 10)
	case int8:
		return strconv.FormatInt(int64(val), 10)
	case int16:
		return strconv.FormatInt(int64(val), 10)
	case int32:
		return strconv.FormatInt(int64(val), 10)
	case int64:
		return strconv.FormatInt(val, 10)
	case uint:
		return strconv.FormatUint(uint64(val), 10)
	case uint8:
		return strconv.FormatUint(uint64(val), 10)
	case uint16:
		return strconv.FormatUint(uint64(val), 10)
	case uint32:
		return strconv.FormatUint(uint64(val), 10)
	case uint64:
		return strconv.FormatUint(val, 10)
	case float32:
		f := float64(val)
		if math.IsNaN(f) || math.IsInf(f, 0) {
			return "NULL"
		}
		return strconv.FormatFloat(f, 'f', -1, 32)
	case float64:
		if math.IsNaN(val) || math.IsInf(val, 0) {
			return "NULL"
		}
		return strconv.FormatFloat(val, 'f', -1, 64)
	case string:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(val, `\`, `\\`), `'`, `\'`) + "'"
	case []byte:
		return "'" + strings.ReplaceAll(strings.ReplaceAll(string(val), `\`, `\\`), `'`, `\'`) + "'"
	case time.Time:
		return "'" + val.UTC().Format("2006-01-02 15:04:05") + "'"
	}

	// Slices → ClickHouse array literal [v1, v2, ...]
	if rv.Kind() == reflect.Slice {
		var sb strings.Builder
		sb.WriteByte('[')
		for i := range rv.Len() {
			if i > 0 {
				sb.WriteString(", ")
			}
			sb.WriteString(formatSQLValue(rv.Index(i).Interface()))
		}
		sb.WriteByte(']')
		return sb.String()
	}

	return fmt.Sprintf("%v", v)
}
