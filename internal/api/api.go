package api

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/ankitmfsi22/go-data-pipeline/internal/domain"
	"github.com/ankitmfsi22/go-data-pipeline/internal/job"
)

const maxBodyBytes = 1 << 20

type Handler struct {
	jobs      *job.Manager
	log       *slog.Logger
	UploadDir string
}

func New(jobs *job.Manager, log *slog.Logger) *Handler {
	return &Handler{jobs: jobs, log: log, UploadDir: "data/uploads"}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/pipelines", h.create)
	mux.HandleFunc("GET /api/v1/pipelines", h.list)
	mux.HandleFunc("GET /api/v1/pipelines/{id}", h.get)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/progress", h.progress)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/results", h.results)
	mux.HandleFunc("GET /api/v1/pipelines/{id}/errors", h.errors)
	mux.HandleFunc("PATCH /api/v1/pipelines/{id}/cancel", h.cancel)
	mux.HandleFunc("DELETE /api/v1/pipelines/{id}", h.remove)
	mux.HandleFunc("POST /api/v1/files", h.upload)
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()

	var spec domain.Spec
	if err := dec.Decode(&spec); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON: "+err.Error())
		return
	}

	j, err := h.jobs.Start(r.Context(), spec)
	if err != nil {
		h.fail(w, err)
		return
	}
	w.Header().Set("Location", "/api/v1/pipelines/"+j.ID)
	writeJSON(w, http.StatusAccepted, j)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	jobs, err := h.jobs.List(r.Context())
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"count": len(jobs), "jobs": jobs})
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request) {
	j, err := h.jobs.Get(r.Context(), r.PathValue("id"))
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, j)
}

func (h *Handler) progress(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	j, err := h.jobs.Get(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	p, _ := h.jobs.Progress(id)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": j.Status, "progress": p})
}

func (h *Handler) results(w http.ResponseWriter, r *http.Request) {
	limit, err := queryInt(r, "limit", 100, 1, 1000)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	offset, err := queryInt(r, "offset", 0, 0, 1<<31-1)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	res, err := h.jobs.Results(r.Context(), r.PathValue("id"), limit, offset)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, res)
}
func (h *Handler) errors(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	errs, err := h.jobs.Errors(r.Context(), id)
	if err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "count": len(errs), "errors": errs})
}
func (h *Handler) cancel(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if err := h.jobs.Cancel(r.Context(), id); err != nil {
		h.fail(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]string{"id": id, "message": "cancel requested"})
}
func (h *Handler) remove(w http.ResponseWriter, r *http.Request) {
	if err := h.jobs.Delete(r.Context(), r.PathValue("id")); err != nil {
		h.fail(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
func (h *Handler) fail(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, job.ErrNotFound):
		writeError(w, http.StatusNotFound, "job not found")
	case errors.Is(err, job.ErrInvalidSpec):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, job.ErrFinished), errors.Is(err, job.ErrNotFinished):
		writeError(w, http.StatusConflict, err.Error())
	default:
		h.log.Error("request failed", "err", err)
		writeError(w, http.StatusInternalServerError, "internal error")
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
func queryInt(r *http.Request, name string, def, lo, hi int) (int, error) {
	s := r.URL.Query().Get(name)
	if s == "" {
		return def, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < lo || n > hi {
		return 0, errors.New(name + " must be a number between " + strconv.Itoa(lo) + " and " + strconv.Itoa(hi))
	}
	return n, nil
}
