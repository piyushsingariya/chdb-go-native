package gockhouse

import (
	"testing"
)

func TestArgsHandlerProcessArgs(t *testing.T) {
	h := NewArgsHandler()

	cases := []struct {
		name    string
		query   string
		args    []any
		want    string
		wantErr bool
	}{
		{
			name:  "no args returns query unchanged",
			query: "SELECT * FROM events",
			args:  nil,
			want:  "SELECT * FROM events",
		},
		{
			name:  "empty args returns query unchanged",
			query: "SELECT * FROM events",
			args:  []any{},
			want:  "SELECT * FROM events",
		},
		{
			name:  "single string arg is interpolated",
			query: "SELECT * FROM events WHERE name = ?",
			args:  []any{"alice"},
			want:  "SELECT * FROM events WHERE name = 'alice'",
		},
		{
			name:  "single integer arg is interpolated",
			query: "SELECT * FROM events WHERE id = ?",
			args:  []any{42},
			want:  "SELECT * FROM events WHERE id = 42",
		},
		{
			name:  "multiple args are interpolated in order",
			query: "SELECT * FROM events WHERE name = ? AND id = ?",
			args:  []any{"bob", 7},
			want:  "SELECT * FROM events WHERE name = 'bob' AND id = 7",
		},
		{
			name:  "string arg with single quotes is escaped",
			query: "SELECT * FROM events WHERE name = ?",
			args:  []any{"o'brien"},
			want:  `SELECT * FROM events WHERE name = 'o\'brien'`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := h.ProcessArgs(tc.query, tc.args)
			if (err != nil) != tc.wantErr {
				t.Fatalf("err: got %v, wantErr %v", err, tc.wantErr)
			}
			if got != tc.want {
				t.Errorf("got  %q\nwant %q", got, tc.want)
			}
		})
	}
}
