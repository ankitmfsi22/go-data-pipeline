package source

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func fakeAPI(t *testing.T, status int, body string, delay time.Duration) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if delay > 0 {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestHTTP_Read(t *testing.T) {
	tests := []struct {
		name    string
		status  int
		body    string
		want    int
		wantErr bool
	}{
		{"array of objects", 200, `[{"id":1},{"id":2},{"id":3}]`, 3, false},
		{"single object", 200, `{"temperature":28.5}`, 1, false},
		{"server error", 500, `{"error":"boom"}`, 0, true},
		{"invalid json", 200, `not json`, 0, true},
		{"array of non-objects", 200, `[1,2,3]`, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := fakeAPI(t, tt.status, tt.body, 0)
			recs, err := collect(t, &HTTP{URL: srv.URL, Client: srv.Client()})

			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if len(recs) != tt.want {
				t.Errorf("got %d records, want %d", len(recs), tt.want)
			}
		})
	}
}

func TestHTTP_Timeout(t *testing.T) {
	srv := fakeAPI(t, 200, `[]`, 2*time.Second)

	start := time.Now()
	_, err := collect(t, &HTTP{URL: srv.URL, Client: srv.Client(), Timeout: 100 * time.Millisecond})

	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("took %v, timeout should stop the call after ~100ms", elapsed)
	}
}
