package api

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const maxUploadBytes = 50 << 20

func (h *Handler) upload(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadBytes)

	file, header, err := r.FormFile("file")
	if err != nil {
		writeError(w, http.StatusBadRequest, `send the file as form-data with the key "file" (max 50 MB)`)
		return
	}
	defer func() { _ = file.Close() }()
	name := filepath.Base(header.Filename)
	ext := strings.ToLower(filepath.Ext(name))
	if ext != ".csv" && ext != ".json" {
		writeError(w, http.StatusBadRequest, "only .csv and .json files are allowed")
		return
	}

	if err := os.MkdirAll(h.UploadDir, 0o750); err != nil {
		h.fail(w, err)
		return
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	path := filepath.Join(h.UploadDir, hex.EncodeToString(b)+"_"+name)

	dst, err := os.Create(path) //nolint:gosec // path is our upload folder + a cleaned base name
	if err != nil {
		h.fail(w, err)
		return
	}
	size, err := io.Copy(dst, file)
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		h.fail(w, fmt.Errorf("save upload: %w", err))
		return
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"name":     name,
		"type":     strings.TrimPrefix(ext, "."),
		"location": path,
		"size":     size,
		"use_it":   map[string]string{"type": strings.TrimPrefix(ext, "."), "location": path},
	})
}
