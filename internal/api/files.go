package api

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"errors"
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
	head := make([]byte, 512)
	n, err := io.ReadFull(file, head)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "could not read the file")
		return
	}
	head = head[:n]
	if n == 0 || !strings.HasPrefix(http.DetectContentType(head), "text/plain") {
		writeError(w, http.StatusBadRequest, "file content is not a text CSV or JSON file")
		return
	}

	if err := os.MkdirAll(h.UploadDir, 0o750); err != nil {
		h.fail(w, err)
		return
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	stored := hex.EncodeToString(b) + "_" + name
	root, err := os.OpenRoot(h.UploadDir)
	if err != nil {
		h.fail(w, err)
		return
	}
	defer func() { _ = root.Close() }()

	dst, err := root.Create(stored)
	if err != nil {
		h.fail(w, err)
		return
	}
	size, err := io.Copy(dst, io.MultiReader(bytes.NewReader(head), file))
	if cerr := dst.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = root.Remove(stored)
		h.fail(w, fmt.Errorf("save upload: %w", err))
		return
	}

	path := filepath.Join(h.UploadDir, stored)

	writeJSON(w, http.StatusCreated, map[string]any{
		"name":     name,
		"type":     strings.TrimPrefix(ext, "."),
		"location": path,
		"size":     size,
		"use_it":   map[string]string{"type": strings.TrimPrefix(ext, "."), "location": path},
	})
}
