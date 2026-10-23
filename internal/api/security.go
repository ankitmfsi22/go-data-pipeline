package api

import (
	"crypto/subtle"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

func APIKey(key string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if key == "" || !strings.HasPrefix(r.URL.Path, "/api/") {
				next.ServeHTTP(w, r)
				return
			}
			if subtle.ConstantTimeCompare([]byte(r.Header.Get("X-API-Key")), []byte(key)) != 1 {
				writeError(w, http.StatusUnauthorized, "missing or wrong X-API-Key header")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
func SecureHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h := w.Header()
		h.Set("X-Content-Type-Options", "nosniff")
		h.Set("X-Frame-Options", "DENY")
		h.Set("Referrer-Policy", "no-referrer")
		h.Set("Content-Security-Policy", "default-src 'none'")
		h.Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r)
	})
}
func RateLimit(perMinute int) func(http.Handler) http.Handler {
	l := &limiter{perMinute: perMinute, clients: map[string]*window{}}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if perMinute <= 0 || r.Method != http.MethodPost {
				next.ServeHTTP(w, r)
				return
			}
			if wait, ok := l.allow(clientIP(r), time.Now()); !ok {
				w.Header().Set("Retry-After", strconv.Itoa(int(wait.Seconds())+1))
				writeError(w, http.StatusTooManyRequests, "too many requests, try again later")
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}

type limiter struct {
	perMinute int
	mu        sync.Mutex
	clients   map[string]*window
}

type window struct {
	start time.Time
	count int
}

func (l *limiter) allow(client string, now time.Time) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.clients) > 10000 {
		for c, win := range l.clients {
			if now.Sub(win.start) > time.Minute {
				delete(l.clients, c)
			}
		}
	}

	win, ok := l.clients[client]
	if !ok || now.Sub(win.start) >= time.Minute {
		l.clients[client] = &window{start: now, count: 1}
		return 0, true
	}
	if win.count >= l.perMinute {
		return time.Minute - now.Sub(win.start), false
	}
	win.count++
	return 0, true
}

func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
