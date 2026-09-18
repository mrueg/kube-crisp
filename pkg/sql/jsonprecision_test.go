package sql

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// TestJSONAggregationKeepsEveryDigit is the loss at its source. The aggregate
// used to be decoded with json.Unmarshal, which makes every number a float64,
// and a float64 keeps fifty-three bits of an integer: 9007199254740993 came
// out of the decoder as 9007199254740992 before anything downstream could see
// it. CockroachDB's unique_rowid() never produces an id below that range.
func TestJSONAggregationKeepsEveryDigit(t *testing.T) {
	pool := newTestPool(t, true)
	ctx := context.Background()

	stmt, err := pool.Prepare(`SELECT json_array(json_object('id', 9007199254740993, 'qty', 9223372036854775807, 'ratio', 0.5)) AS rows`, time.Second, 10)
	if err != nil {
		t.Fatalf("Prepare() returned error: %v", err)
	}
	stmt.Format = FormatJSONArray

	rows, err := pool.Query(ctx, stmt, nil)
	if err != nil {
		t.Fatalf("Query() returned error: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("decoded %d rows, want 1", len(rows))
	}
	for column, want := range map[string]string{
		"id":    "9007199254740993",
		"qty":   "9223372036854775807",
		"ratio": "0.5",
	} {
		got, ok := rows[0][column].(json.Number)
		if !ok {
			t.Errorf("%s arrived as %T, want json.Number: the digits are already gone", column, rows[0][column])
			continue
		}
		if got.String() != want {
			t.Errorf("%s = %s, want %s", column, got, want)
		}
	}
}

// TestDecodeJSONRefusesTrailingContent: a Decoder stops at the end of the
// first value where json.Unmarshal reads to the end of the input, and the
// switch between them must not start accepting a column that holds two
// documents as the first of them.
func TestDecodeJSONRefusesTrailingContent(t *testing.T) {
	var decoded any
	if err := DecodeJSON([]byte(`{"a":1} {"b":2}`), &decoded); err == nil {
		t.Error("DecodeJSON() accepted a second document after the first")
	}
	if err := DecodeJSON([]byte(`{"a":1}   `), &decoded); err != nil {
		t.Errorf("DecodeJSON() refused trailing whitespace: %v", err)
	}
}
