package internal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	chdbpurego "github.com/chdb-io/chdb-go/chdb-purego"
)

// ChdbRows implements clickhouse/v2/lib/driver.Rows over a parsed JSONCompact response.
type ChdbRows struct {
	meta   []jsonMeta
	data   [][]json.RawMessage
	cursor int
	result chdbpurego.ChdbResult // held so we can Free() on Close
}

func NewChdbRows(result chdbpurego.ChdbResult) (*ChdbRows, error) {
	str := result.String()
	if strings.TrimSpace(str) == "" {
		return &ChdbRows{result: result, cursor: -1}, nil
	}
	var jr jsonCompactResult
	if err := json.Unmarshal([]byte(str), &jr); err != nil {
		return nil, fmt.Errorf("ChdbRows: parse response: %w", err)
	}
	return &ChdbRows{
		meta:   jr.Meta,
		data:   jr.Data,
		cursor: -1,
		result: result,
	}, nil
}

func (r *ChdbRows) Next() bool {
	r.cursor++
	return r.cursor < len(r.data)
}

// Scan copies the current row's columns into dest (positional pointer arguments).
func (r *ChdbRows) Scan(dest ...any) error {
	if r.cursor < 0 || r.cursor >= len(r.data) {
		return fmt.Errorf("ChdbRows: Scan called outside a valid row")
	}
	row := r.data[r.cursor]
	for i, d := range dest {
		if i >= len(row) {
			break
		}
		dv := reflect.ValueOf(d)
		if dv.Kind() != reflect.Ptr {
			return fmt.Errorf("ChdbRows: Scan dest[%d] must be a pointer", i)
		}
		if err := unmarshalIntoField(row[i], dv.Elem()); err != nil {
			return fmt.Errorf("ChdbRows: Scan col %d: %w", i, err)
		}
	}
	return nil
}

// ScanStruct fills a struct from the current row using the same tag-based field
// matching as Select.
func (r *ChdbRows) ScanStruct(dest any) error {
	if r.cursor < 0 || r.cursor >= len(r.data) {
		return fmt.Errorf("ChdbRows: ScanStruct called outside a valid row")
	}
	elem := reflect.ValueOf(dest)
	if elem.Kind() == reflect.Ptr {
		elem = elem.Elem()
	}
	return ScanRowIntoValue(r.meta, r.data[r.cursor], elem)
}

func (r *ChdbRows) ColumnTypes() []driver.ColumnType {
	types := make([]driver.ColumnType, len(r.meta))
	for i, m := range r.meta {
		types[i] = &chdbColumnType{name: m.Name, dbType: m.Type}
	}
	return types
}

func (r *ChdbRows) Totals(_ ...any) error { return nil }

func (r *ChdbRows) Columns() []string {
	cols := make([]string, len(r.meta))
	for i, m := range r.meta {
		cols[i] = m.Name
	}
	return cols
}

func (r *ChdbRows) Close() error {
	if r.result != nil {
		r.result.Free()
		r.result = nil
	}
	return nil
}

func (r *ChdbRows) Err() error { return nil }

// chdbRow wraps ChdbRows and exposes the first row as clickhouse/v2/lib/driver.Row.
type ChdbRow struct {
	err  error
	rows *ChdbRows
}

func (r *ChdbRow) SetError(err error) *ChdbRow {
	r.err = err
	return r
}

func (r *ChdbRow) SetRows(rows *ChdbRows) *ChdbRow {
	r.rows = rows
	return r
}

func (r *ChdbRow) Err() error { return r.err }

func (r *ChdbRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		return fmt.Errorf("chdb: no rows in result set")
	}
	return r.rows.Scan(dest...)
}

func (r *ChdbRow) ScanStruct(dest any) error {
	if r.err != nil {
		return r.err
	}
	if !r.rows.Next() {
		return fmt.Errorf("chdb: no rows in result set")
	}
	return r.rows.ScanStruct(dest)
}

// chdbColumnType implements driver.ColumnType for chdb result metadata.
type chdbColumnType struct {
	name   string
	dbType string
}

func (c *chdbColumnType) Name() string             { return c.name }
func (c *chdbColumnType) Nullable() bool           { return strings.HasPrefix(c.dbType, "Nullable") }
func (c *chdbColumnType) ScanType() reflect.Type   { return chScanType(c.dbType) }
func (c *chdbColumnType) DatabaseTypeName() string { return c.dbType }

// chScanType maps a ClickHouse type name to the Go reflect.Type used when
// scanning. Nullable(T) maps to the pointer equivalent of T's scan type.
func chScanType(chType string) reflect.Type {
	// Unwrap Nullable(T).
	inner := chType
	nullable := strings.HasPrefix(chType, "Nullable(") && strings.HasSuffix(chType, ")")
	if nullable {
		inner = chType[len("Nullable(") : len(chType)-1]
	}

	// Strip any LowCardinality wrapper.
	if strings.HasPrefix(inner, "LowCardinality(") && strings.HasSuffix(inner, ")") {
		inner = inner[len("LowCardinality(") : len(inner)-1]
	}

	var base reflect.Type
	switch {
	case inner == "Bool":
		base = reflect.TypeOf(false)
	case inner == "Int8":
		base = reflect.TypeOf(int8(0))
	case inner == "Int16":
		base = reflect.TypeOf(int16(0))
	case inner == "Int32":
		base = reflect.TypeOf(int32(0))
	case inner == "Int64":
		base = reflect.TypeOf(int64(0))
	case inner == "UInt8":
		base = reflect.TypeOf(uint8(0))
	case inner == "UInt16":
		base = reflect.TypeOf(uint16(0))
	case inner == "UInt32":
		base = reflect.TypeOf(uint32(0))
	case inner == "UInt64":
		base = reflect.TypeOf(uint64(0))
	case inner == "Float32":
		base = reflect.TypeOf(float32(0))
	case inner == "Float64":
		base = reflect.TypeOf(float64(0))
	case inner == "String", strings.HasPrefix(inner, "FixedString("), inner == "UUID":
		base = reflect.TypeOf("")
	case inner == "Date", inner == "Date32",
		inner == "DateTime", strings.HasPrefix(inner, "DateTime("),
		strings.HasPrefix(inner, "DateTime64("):
		base = reflect.TypeOf("")
	default:
		base = reflect.TypeOf("")
	}

	if nullable {
		return reflect.PointerTo(base)
	}
	return base
}
