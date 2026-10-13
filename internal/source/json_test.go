package source

import (
	"encoding/json"
	"testing"
)

func TestJSON_Read(t *testing.T) {
	recs, err := collect(t, &JSON{Path: "testdata/prices.json"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 3 {
		t.Fatalf("got %d records, want 3", len(recs))
	}
	if recs[0].Fields["coin"] != "bitcoin" {
		t.Errorf("first coin = %v, want bitcoin", recs[0].Fields["coin"])
	}
	if n, ok := recs[0].Fields["price_usd"].(json.Number); !ok || n.String() != "67000.5" {
		t.Errorf("price_usd = %#v, want json.Number(67000.5)", recs[0].Fields["price_usd"])
	}
}

func TestJSON_NotAnArray(t *testing.T) {
	recs, err := collect(t, &JSON{Path: "testdata/not_array.json"})
	if err == nil {
		t.Fatal("expected an error when the file is not a JSON array")
	}
	if len(recs) != 0 {
		t.Errorf("got %d records, want 0", len(recs))
	}
}
