// Package api реализует API-воркер: приём голосов, выдачу cookie, буферизацию
// и асинхронную отправку батчей в Kafka.
//
// См. specs/api.md — источник истины по контрактам.
package api

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/fingerprint"
	"github.com/KotovBoris/highload-poll-backend/internal/httpx"
	"github.com/KotovBoris/highload-poll-backend/internal/model"
	"github.com/KotovBoris/highload-poll-backend/internal/resultsclient"
	"github.com/KotovBoris/highload-poll-backend/internal/uuid"
)

// cookieMaxAge — время жизни cookie voter_id.
const cookieMaxAge = 24 * time.Hour

// Server — HTTP-сервер API-воркера.
type Server struct {
	cache    *pollCache
	buf      *voteBuffer
	sender   *Sender
	workerID string
	logger   *slog.Logger
	now      func() time.Time
	closeTTL time.Duration
}

// Config — параметры сервера.
type Config struct {
	WorkerID  string
	BatchSize int
	CacheTTL  time.Duration
	// CloseTTL — через сколько после закрытия удалять буфер опроса.
	CloseTTL time.Duration
}

// NewServer создаёт сервер.
func NewServer(client resultsclient.Client, sender *Sender, cfg Config, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 10000
	}
	if cfg.CacheTTL <= 0 {
		cfg.CacheTTL = time.Minute
	}
	if cfg.CloseTTL <= 0 {
		cfg.CloseTTL = cfg.CacheTTL
	}
	now := time.Now
	return &Server{
		cache:    newPollCache(client, cfg.CacheTTL, now),
		buf:      newVoteBuffer(cfg.BatchSize),
		sender:   sender,
		workerID: cfg.WorkerID,
		logger:   logger,
		now:      now,
		closeTTL: cfg.CloseTTL,
	}
}

// SetNow подменяет источник времени (для тестов).
func (s *Server) SetNow(now func() time.Time) {
	s.now = now
	s.cache.now = now
}

// Handler возвращает маршрутизатор.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /polls/{id}", s.handleGetPoll)
	mux.HandleFunc("POST /polls/{id}/vote", s.handleVote)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

// handleGetPoll — GET /polls/{id}: метаданные опроса + cookie voter_id.
func (s *Server) handleGetPoll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	poll, err := s.cache.get(r.Context(), id)
	if err != nil {
		s.writePollError(w, err)
		return
	}

	// Выдаём cookie только если её ещё нет — не перезаписываем существующую.
	if _, err := r.Cookie(fingerprint.CookieName); err != nil {
		http.SetCookie(w, &http.Cookie{
			Name:     fingerprint.CookieName,
			Value:    uuid.New(),
			Path:     "/",
			MaxAge:   int(cookieMaxAge.Seconds()),
			SameSite: http.SameSiteLaxMode,
			HttpOnly: true,
		})
	}

	poll.Results = nil // результаты не публичны
	httpx.WriteJSON(w, http.StatusOK, poll)
}

// handleVote — POST /polls/{id}/vote.
func (s *Server) handleVote(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")

	var req model.VoteRequest
	if err := httpx.ReadJSON(r, &req); err != nil {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	poll, err := s.cache.get(r.Context(), id)
	if err != nil {
		s.writePollError(w, err)
		return
	}

	if !validOption(poll, req.OptionID) {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, "unknown option_id")
		return
	}

	now := s.now()
	if !now.Before(poll.EndsAt) {
		// Опрос закрыт: инициируем закрытие (досылка остатка + done) и
		// отклоняем голос. Закрытие идемпотентно.
		go s.closePoll(id)
		httpx.WriteError(w, http.StatusForbidden, httpx.ErrClosed, "poll closed")
		return
	}

	fp := fingerprint.FromRequest(r)
	flush, closed := s.buf.Add(id, model.Vote{Fingerprint: fp, OptionID: req.OptionID})
	if closed {
		httpx.WriteError(w, http.StatusForbidden, httpx.ErrClosed, "poll closed")
		return
	}
	if len(flush) > 0 {
		s.sender.Enqueue(model.VoteBatch{PollID: id, WorkerID: s.workerID, Votes: flush})
	}

	httpx.WriteJSON(w, http.StatusAccepted, model.VoteResponse{Status: "accepted"})
}

// handleHealthz — GET /healthz.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// closePoll закрывает опрос: досылает остаток буфера и "done"-сообщение.
// Идемпотентно: повторный вызов не отправит "done" дважды.
func (s *Server) closePoll(pollID string) {
	flush, alreadyClosed := s.buf.Close(pollID, s.now())
	if alreadyClosed {
		return
	}
	if len(flush) > 0 {
		s.sender.Enqueue(model.VoteBatch{PollID: pollID, WorkerID: s.workerID, Votes: flush})
	}
	s.sender.Enqueue(model.VoteBatch{PollID: pollID, WorkerID: s.workerID, Done: true})
	s.logger.Info("poll closed by api worker", "poll_id", pollID, "worker_id", s.workerID)
}

// StartWatcher запускает фоновый обход: закрывает опросы, у которых наступил
// ends_at, и чистит буферы старых закрытых опросов.
func (s *Server) StartWatcher(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.sweep()
		}
	}
}

// sweep — один проход watcher'а.
func (s *Server) sweep() {
	now := s.now()
	for _, id := range s.buf.PollIDs() {
		poll, err := s.cache.get(context.Background(), id)
		if err != nil {
			continue
		}
		if !now.Before(poll.EndsAt) {
			s.closePoll(id)
		}
	}
	s.buf.Cleanup(s.closeTTL, now)
}

// validOption проверяет, что option_id входит в список опций опроса.
func validOption(poll model.Poll, optionID int) bool {
	for _, o := range poll.Options {
		if o.ID == optionID {
			return true
		}
	}
	return false
}

// writePollError переводит ошибку получения метаданных в HTTP-ответ.
func (s *Server) writePollError(w http.ResponseWriter, err error) {
	var nf resultsclient.ErrNotFound
	if errors.As(err, &nf) {
		httpx.WriteError(w, http.StatusNotFound, httpx.ErrNotFound, "poll not found")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		httpx.WriteError(w, http.StatusServiceUnavailable, httpx.ErrUnavailable, "results service timeout")
		return
	}
	httpx.WriteError(w, http.StatusServiceUnavailable, httpx.ErrUnavailable, "results service unavailable")
}
