package api

import (
	"bytes"
	"encoding/json"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

func uploadRequest(t *testing.T, field, filename, content string) *http.Request {
	t.Helper()
	var body bytes.Buffer
	mw := multipart.NewWriter(&body)
	fw, err := mw.CreateFormFile(field, filename)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = fw.Write([]byte(content))
	_ = mw.Close()

	req := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/api/v1/files", &body)
	req.Header.Set("Content-Type", mw.FormDataContentType())
	return req
}

func TestUpload(t *testing.T) {
	h := &Handler{UploadDir: t.TempDir()}

	rec := httptest.NewRecorder()
	h.upload(rec, uploadRequest(t, "file", "sales.csv", "country,amount\nIndia,100\n"))
	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}

	var res struct {
		Type     string `json:"type"`
		Location string `json:"location"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &res); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(res.Location)
	if err != nil || res.Type != "csv" || !strings.HasPrefix(string(data), "country,amount") {
		t.Errorf("saved %q as %q (err %v)", data, res.Type, err)
	}
}

func TestUpload_Rejects(t *testing.T) {
	h := &Handler{UploadDir: t.TempDir()}

	tests := []struct{ name, field, filename string }{
		{"wrong field name", "upload", "a.csv"},
		{"not csv or json", "file", "run.sh"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			h.upload(rec, uploadRequest(t, tt.field, tt.filename, "x"))
			if rec.Code != http.StatusBadRequest {
				t.Errorf("status = %d, want 400", rec.Code)
			}
		})
	}
}

func TestUpload_PathTraversal(t *testing.T) {
	dir := t.TempDir()
	h := &Handler{UploadDir: dir}

	rec := httptest.NewRecorder()
	h.upload(rec, uploadRequest(t, "file", "../../evil.csv", "a\n1\n"))

	var res struct {
		Location string `json:"location"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &res)
	if !strings.HasPrefix(res.Location, dir) {
		t.Errorf("file saved outside the upload folder: %s", res.Location)
	}
}
