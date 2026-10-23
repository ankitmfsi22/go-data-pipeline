package api

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

var okHandler = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })

func send(t *testing.T, h http.Handler, method, path, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequestWithContext(t.Context(), method, path, nil)
	if key != "" {
		req.Header.Set("X-API-Key", key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestAPIKey(t *testing.T) {
	h := APIKey("secret")(okHandler)

	tests := []struct {
		name, path, key string
		want            int
	}{
		{"no key", "/api/v1/pipelines", "", http.StatusUnauthorized},
		{"wrong key", "/api/v1/pipelines", "nope", http.StatusUnauthorized},
		{"right key", "/api/v1/pipelines", "secret", http.StatusOK},
		{"health stays open", "/healthz", "", http.StatusOK},
		{"metrics stay open", "/metrics", "", http.StatusOK},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := send(t, h, http.MethodGet, tt.path, tt.key).Code; got != tt.want {
				t.Errorf("status = %d, want %d", got, tt.want)
			}
		})
	}
	if got := send(t, APIKey("")(okHandler), http.MethodGet, "/api/v1/pipelines", "").Code; got != http.StatusOK {
		t.Errorf("auth off: status = %d, want 200", got)
	}
}

func TestSecureHeaders(t *testing.T) {
	rec := send(t, SecureHeaders(okHandler), http.MethodGet, "/", "")
	if rec.Header().Get("X-Content-Type-Options") != "nosniff" || rec.Header().Get("X-Frame-Options") != "DENY" {
		t.Errorf("headers = %v", rec.Header())
	}
}

func TestRateLimit(t *testing.T) {
	h := RateLimit(2)(okHandler)

	for i, want := range []int{http.StatusOK, http.StatusOK, http.StatusTooManyRequests} {
		rec := send(t, h, http.MethodPost, "/api/v1/pipelines", "")
		if rec.Code != want {
			t.Errorf("POST #%d = %d, want %d", i+1, rec.Code, want)
		}
		if want == http.StatusTooManyRequests && rec.Header().Get("Retry-After") == "" {
			t.Error("429 without Retry-After")
		}
	}

	if got := send(t, h, http.MethodGet, "/api/v1/pipelines", "").Code; got != http.StatusOK {
		t.Errorf("GET = %d, want 200", got)
	}
}

func TestLimiter_NewWindow(t *testing.T) {
	l := &limiter{perMinute: 1, clients: map[string]*window{}}
	now := time.Now()
	if _, ok := l.allow("1.2.3.4", now); !ok {
		t.Fatal("first request should pass")
	}
	if _, ok := l.allow("1.2.3.4", now.Add(time.Second)); ok {
		t.Error("second request in the same minute should be blocked")
	}
	if _, ok := l.allow("5.6.7.8", now.Add(time.Second)); !ok {
		t.Error("another client has its own limit")
	}
	if _, ok := l.allow("1.2.3.4", now.Add(61*time.Second)); !ok {
		t.Error("a new minute should reset the count")
	}
}
