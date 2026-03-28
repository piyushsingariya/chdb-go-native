package internal

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

// jsonCompactResult is the top-level structure of ClickHouse's JSONCompact output format.
type jsonCompactResult struct {
	Meta []jsonMeta          `json:"meta"`
	Data [][]json.RawMessage `json:"data"`
}

type jsonMeta struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// scanJSONCompactIntoSlice parses a JSONCompact response and appends rows into dest
// (must be a pointer to a slice of structs or maps).
func ScanJSONCompactIntoSlice(jsonStr string, dest any) error {
	if strings.TrimSpace(jsonStr) == "" {
		return nil
	}
	var jr jsonCompactResult
	if err := json.Unmarshal([]byte(jsonStr), &jr); err != nil {
		return fmt.Errorf("chdbConn: Select: parse response: %w", err)
	}

	destVal := reflect.ValueOf(dest)
	if destVal.Kind() != reflect.Ptr || destVal.Elem().Kind() != reflect.Slice {
		return fmt.Errorf("chdbConn: Select: dest must be a pointer to a slice, got %T", dest)
	}
	sliceVal := destVal.Elem()
	elemType := sliceVal.Type().Elem()

	for _, row := range jr.Data {
		elem := reflect.New(elemType).Elem()
		if err := ScanRowIntoValue(jr.Meta, row, elem); err != nil {
			return err
		}
		sliceVal.Set(reflect.Append(sliceVal, elem))
	}
	return nil
}

// ScanRowIntoValue fills a struct or map Value from a single JSONCompact data row.
func ScanRowIntoValue(meta []jsonMeta, row []json.RawMessage, elem reflect.Value) error {
	switch elem.Kind() {
	case reflect.Struct:
		for i, m := range meta {
			if i >= len(row) {
				break
			}
			field := findStructField(elem, m.Name)
			if !field.IsValid() {
				continue
			}
			if err := unmarshalIntoField(row[i], field); err != nil {
				return fmt.Errorf("column %q: %w", m.Name, err)
			}
		}
	case reflect.Map:
		if elem.IsNil() {
			elem.Set(reflect.MakeMap(elem.Type()))
		}
		for i, m := range meta {
			if i >= len(row) {
				break
			}
			var v any
			if err := json.Unmarshal(row[i], &v); err != nil {
				return err
			}
			elem.SetMapIndex(reflect.ValueOf(m.Name), reflect.ValueOf(v))
		}
	default:
		return fmt.Errorf("chdbConn: Select: unsupported element kind %s", elem.Kind())
	}
	return nil
}

// findStructField returns the reflect.Value of the struct field corresponding to colName.
// Priority: `ch` tag → `json` tag → lowercased field name.
func findStructField(structVal reflect.Value, colName string) reflect.Value {
	t := structVal.Type()
	colLower := strings.ToLower(colName)
	for i := range t.NumField() {
		f := t.Field(i)
		if tag, _, _ := strings.Cut(f.Tag.Get("ch"), ","); tag == colName {
			return structVal.Field(i)
		}
		if tag, _, _ := strings.Cut(f.Tag.Get("json"), ","); tag == colName {
			return structVal.Field(i)
		}
		if strings.ToLower(f.Name) == colLower {
			return structVal.Field(i)
		}
	}
	return reflect.Value{}
}

// unmarshalIntoField deserializes raw JSON into field, performing numeric conversions
// needed for ClickHouse integer types (UInt64, Int64, …).
func unmarshalIntoField(raw json.RawMessage, field reflect.Value) error {
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return err
	}
	return assignToField(field, v)
}

// assignToField converts src (from json.Decoder with UseNumber) and assigns it to field.
func assignToField(field reflect.Value, src any) error {
	if src == nil {
		// For pointer types (Nullable), set to typed nil; otherwise zero value.
		field.Set(reflect.Zero(field.Type()))
		return nil
	}

	// Handle pointer types (Nullable(T)): allocate and fill the pointed-to value.
	if field.Kind() == reflect.Ptr {
		ptr := reflect.New(field.Type().Elem())
		if err := assignToField(ptr.Elem(), src); err != nil {
			return err
		}
		field.Set(ptr)
		return nil
	}

	if num, ok := src.(json.Number); ok {
		switch field.Kind() {
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			// Use ParseUint so values > math.MaxInt64 (valid UInt64) are not rejected.
			n, err := strconv.ParseUint(num.String(), 10, 64)
			if err != nil {
				return err
			}
			field.SetUint(n)
			return nil
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			n, err := num.Int64()
			if err != nil {
				return err
			}
			field.SetInt(n)
			return nil
		case reflect.Float32, reflect.Float64:
			n, err := num.Float64()
			if err != nil {
				return err
			}
			field.SetFloat(n)
			return nil
		case reflect.String:
			field.SetString(num.String())
			return nil
		}
	}

	// Handle []interface{} → []T conversions (ClickHouse arrays decoded from JSON).
	if srcSlice, ok := src.([]interface{}); ok && field.Kind() == reflect.Slice {
		result := reflect.MakeSlice(field.Type(), len(srcSlice), len(srcSlice))
		for i, item := range srcSlice {
			if err := assignToField(result.Index(i), item); err != nil {
				return fmt.Errorf("slice element %d: %w", i, err)
			}
		}
		field.Set(result)
		return nil
	}

	srcVal := reflect.ValueOf(src)
	if srcVal.Type().AssignableTo(field.Type()) {
		field.Set(srcVal)
		return nil
	}
	if srcVal.Type().ConvertibleTo(field.Type()) {
		field.Set(srcVal.Convert(field.Type()))
		return nil
	}
	return fmt.Errorf("cannot assign %T to %s", src, field.Type())
}
