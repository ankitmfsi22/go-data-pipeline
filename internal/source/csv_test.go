package source

import "testing"

func TestCSV_Read(t *testing.T) {
	recs, err := collect(t, &CSV{Path: "testdata/covid.csv"})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if len(recs) != 5 {
		t.Fatalf("got %d records, want 5", len(recs))
	}

	first := recs[0]
	if first.Source != "covid.csv" || first.Line != 2 {
		t.Errorf("first record Source=%q Line=%d, want covid.csv / 2", first.Source, first.Line)
	}
	if first.Fields["country"] != "India" || first.Fields["cases"] != "500" {
		t.Errorf("first record fields = %v", first.Fields)
	}
	last := recs[4]
	if v, ok := last.Fields["deaths"]; !ok || v != "" {
		t.Errorf("missing column: deaths = %v (present=%v), want empty string", v, ok)
	}
}

func TestCSV_Errors(t *testing.T) {
	tests := []struct {
		name string
		path string
		want int
	}{
		{"file not found", "testdata/nope.csv", 0},
		{"header only", "testdata/header_only.csv", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			recs, err := collect(t, &CSV{Path: tt.path})
			if len(recs) != tt.want {
				t.Errorf("got %d records, want %d", len(recs), tt.want)
			}
			if tt.name == "file not found" && err == nil {
				t.Error("expected an error for a missing file")
			}
			if tt.name == "header only" && err != nil {
				t.Errorf("header-only file should give 0 records and no error, got %v", err)
			}
		})
	}
}
