package results

import (
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/httpx"
	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/uuid"
)

// Server — HTTP-сервер сервиса результатов.
type Server struct {
	storage Storage
	// now подменяется в тестах для детерминированного времени.
	now func() time.Time
}

// NewServer создаёт сервер поверх хранилища.
func NewServer(storage Storage) *Server {
	return &Server{storage: storage, now: time.Now}
}

// Handler возвращает маршрутизатор со всеми ручками сервиса.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	// Публичные (админские) ручки.
	mux.HandleFunc("POST /admin/polls", s.handleCreatePoll)
	mux.HandleFunc("GET /admin/polls/{id}/results", s.handleAdminResults)
	// Внутренние ручки для API-воркера и consumer'а.
	mux.HandleFunc("GET /internal/polls/{id}", s.handleInternalGetPoll)
	mux.HandleFunc("POST /internal/polls/{id}/results", s.handleFlushResults)
	// Служебная ручка.
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

// flushResponse — тело ответа идемпотентного flush.
type flushResponse struct {
	Written bool `json:"written"`
}

// handleCreatePoll — POST /admin/polls.
func (s *Server) handleCreatePoll(w http.ResponseWriter, r *http.Request) {
	var req model.CreatePollRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	if msg := validateCreate(&req); msg != "" {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, msg)
		return
	}

	now := s.now()
	endsAt, ok := resolveEndsAt(&req, now)
	if !ok {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest,
			"either duration_seconds (> 0) or a future ends_at is required")
		return
	}

	poll := model.Poll{
		ID:        uuid.New(),
		Question:  req.Question,
		Options:   buildOptions(req.Options),
		Status:    model.PollStatusActive,
		CreatedAt: now,
		EndsAt:    endsAt,
	}

	ctx := r.Context()
	if err := s.storage.CreatePoll(ctx, poll); err != nil {
		httpx.WriteError(w, http.StatusInternalServerError, httpx.ErrInternal, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusCreated, poll)
}

// handleAdminResults — GET /admin/polls/{id}/results.
func (s *Server) handleAdminResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	poll, err := s.storage.GetPoll(r.Context(), id)
	if err != nil {
		s.writeStorageError(w, err)
		return
	}
	httpx.WriteJSON(w, http.StatusOK, poll)
}

// handleInternalGetPoll — GET /internal/polls/{id}.
// Возвращает метаданные без результатов — потребителям нужен ends_at и список
// опций, а не итоги.
func (s *Server) handleInternalGetPoll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	poll, err := s.storage.GetPoll(r.Context(), id)
	if err != nil {
		s.writeStorageError(w, err)
		return
	}
	poll.Results = nil
	httpx.WriteJSON(w, http.StatusOK, poll)
}

// handleFlushResults — POST /internal/polls/{id}/results.
// Идемпотентен: повторный flush уже завершённого опроса даёт 409, а не ошибку.
func (s *Server) handleFlushResults(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	var req model.FlushRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	written, err := s.storage.FlushResults(r.Context(), id, req.Results)
	if err != nil {
		s.writeStorageError(w, err)
		return
	}
	if written {
		httpx.WriteJSON(w, http.StatusOK, flushResponse{Written: true})
		return
	}
	httpx.WriteJSON(w, http.StatusConflict, flushResponse{Written: false})
}

// handleHealthz — GET /healthz.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if err := s.storage.Ping(r.Context()); err != nil {
		httpx.WriteError(w, http.StatusServiceUnavailable, httpx.ErrUnavailable, err.Error())
		return
	}
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// writeStorageError переводит ошибку хранилища в HTTP-ответ.
func (s *Server) writeStorageError(w http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) {
		httpx.WriteError(w, http.StatusNotFound, httpx.ErrNotFound, "poll not found")
		return
	}
	httpx.WriteError(w, http.StatusInternalServerError, httpx.ErrInternal, err.Error())
}

// validateCreate проверяет тело запроса на создание опроса.
// Возвращает текст ошибки или пустую строку, если всё в порядке.
func validateCreate(req *model.CreatePollRequest) string {
	if req.Question == "" {
		return "question must not be empty"
	}
	if len(req.Options) < 2 {
		return "options must contain at least 2 items"
	}
	for i, o := range req.Options {
		if o == "" {
			return "option at index " + strconv.Itoa(i) + " must not be empty"
		}
	}
	return ""
}

// resolveEndsAt вычисляет время окончания опроса.
// Приоритет: ends_at (если задан) > duration_seconds.
func resolveEndsAt(req *model.CreatePollRequest, now time.Time) (time.Time, bool) {
	if req.EndsAt != nil {
		if req.EndsAt.After(now) {
			return *req.EndsAt, true
		}
		return time.Time{}, false
	}
	if req.DurationSeconds > 0 {
		return now.Add(time.Duration(req.DurationSeconds) * time.Second), true
	}
	return time.Time{}, false
}

// buildOptions превращает список текстов в опции с id, начиная с 1.
func buildOptions(texts []string) []model.Option {
	opts := make([]model.Option, len(texts))
	for i, t := range texts {
		opts[i] = model.Option{ID: i + 1, Text: t}
	}
	return opts
}
