package gockhouse

import "github.com/huandu/go-sqlbuilder"

// ArgsHandler substitutes query placeholders with their argument values.
type ArgsHandler struct{}

func NewArgsHandler() *ArgsHandler { return &ArgsHandler{} }

// ProcessArgs interpolates args into the ? placeholders of query using the
// ClickHouse SQL flavor from go-sqlbuilder.
func (a *ArgsHandler) ProcessArgs(query string, args []any) (string, error) {
	if len(args) == 0 {
		return query, nil
	}
	return sqlbuilder.ClickHouse.Interpolate(query, args)
}
