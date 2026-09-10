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
)

// cookieMaxAge — время жизни cookie voter_id.
const cookieMaxAge = 24 * time.Hour

// issueTTLGrace — на сколько после ends_at опроса помнить факт выдачи cookie.
// После этого запись лимитера удаляется.
const issueTTLGrace = 10 * time.Minute

// Server — HTTP-сервер API-воркера.
type Server struct {
	cache    *pollCache
	buf      *voteBuffer
	sender   *Sender
	signer   *fingerprint.Signer
	limiter  *issueLimiter
	workerID string
	logger   *slog.Logger
	now      func() time.Time
	closeTTL time.Duration
	// limitIssuing выключает лимит выдачи (для нагрузочных тестов).
	limitIssuing bool
	// maxCookiesPerClient — предел выдач cookie на один IP в рамках опроса.
	maxCookiesPerClient int
}

// Config — параметры сервера.
type Config struct {
	WorkerID  string
	BatchSize int
	CacheTTL  time.Duration
	// CloseTTL — через сколько после закрытия удалять буфер опроса.
	CloseTTL time.Duration
	// CookieSecret — секрет HMAC-подписи cookie. Пустой — случайный (dev).
	CookieSecret string
	// MaxIssuedEntries — предел числа записей лимитера выдачи. 0 — без предела.
	MaxIssuedEntries int
	// MaxCookiesPerClient — сколько cookie можно выдать одному IP в рамках
	// опроса. 0 — без ограничения.
	MaxCookiesPerClient int
	// DisableIssueLimit отключает лимит выдачи (используется нагрузочным тестом).
	DisableIssueLimit bool
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
	var signer *fingerprint.Signer
	if cfg.CookieSecret != "" {
		signer = fingerprint.NewSigner(cfg.CookieSecret)
	} else {
		signer = fingerprint.NewRandomSigner()
		logger.Warn("COOKIE_SECRET is empty: using random secret, cookies will not survive restart")
	}
	now := time.Now
	srv := &Server{
		cache:               newPollCache(client, cfg.CacheTTL, now),
		sender:              sender,
		signer:              signer,
		limiter:             newIssueLimiter(cfg.MaxIssuedEntries, now),
		maxCookiesPerClient: cfg.MaxCookiesPerClient,
		workerID:            cfg.WorkerID,
		logger:              logger,
		now:                 now,
		closeTTL:            cfg.CloseTTL,
		limitIssuing:        !cfg.DisableIssueLimit,
	}
	// Буфер сам ставит элементы в очередь отправки под своим мьютексом —
	// это и даёт гарантию, что "done" не обгонит батч (см. buffer.go).
	srv.buf = newVoteBuffer(cfg.BatchSize, srv.sendFromBuffer)
	return srv
}

// sendFromBuffer — колбэк буфера: превращает исходящий элемент опроса в
// Kafka-сообщение. Вызывается drainer'ом опроса строго в порядке добавления.
func (s *Server) sendFromBuffer(pollID string, votes []model.Vote, done bool) {
	s.sender.Enqueue(model.VoteBatch{
		PollID:   pollID,
		WorkerID: s.workerID,
		Votes:    votes,
		Done:     done,
	})
}

// SetNow подменяет источник времени (для тестов).
func (s *Server) SetNow(now func() time.Time) {
	s.now = now
	s.cache.now = now
	s.limiter.now = now
}

// Handler возвращает маршрутизатор.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /polls/{id}", s.handleGetPoll)
	mux.HandleFunc("POST /polls/{id}/vote", s.handleVote)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	return mux
}

// handleGetPoll — GET /polls/{id}: метаданные опроса + подписанная cookie.
//
// Ручка НИКОГДА не отвечает ошибкой из-за лимита: зритель обязан увидеть
// страницу опроса. Если у клиента уже есть cookie или исчерпан лимит выдач
// для его IP — страница отдаётся без новой cookie, и его голос уйдёт в
// fallback-дедупликацию.
func (s *Server) handleGetPoll(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	poll, err := s.cache.get(r.Context(), id)
	if err != nil {
		s.writePollError(w, err)
		return
	}

	// Если у клиента уже есть cookie — повторно не выдаём (страница
	// перезагружается, но новых cookie не появляется).
	if _, has := fingerprint.CookieValue(r); !has {
		if !s.limitIssuing || s.allowIssue(id, r, poll.Poll) {
			_, cookieValue := s.signer.Issue(id)
			http.SetCookie(w, &http.Cookie{
				Name:     fingerprint.CookieName,
				Value:    cookieValue,
				Path:     "/",
				MaxAge:   int(cookieMaxAge.Seconds()),
				SameSite: http.SameSiteLaxMode,
				HttpOnly: true,
			})
		}
	}

	// Результаты не публичны. Значение из кэша неизменяемо, поэтому отдаём
	// копию без поля Results (копия дешевле, чем мутация разделяемого значения).
	out := *poll.Poll
	out.Results = nil
	httpx.WriteJSON(w, http.StatusOK, &out)
}

// allowIssue регистрирует выдачу cookie по ключу (poll_id, IP).
// Возвращает false, если для этого IP исчерпан лимит выдач по опросу.
// UA в ключ не входит: его ротация бесплатна и обесценила бы лимит.
func (s *Server) allowIssue(pollID string, r *http.Request, poll *model.Poll) bool {
	key := issueKey(pollID + "|" + fingerprint.HashIP(fingerprint.ClientIP(r)))
	// Помним запись до ends_at + запас: после закрытия опроса лимит не нужен.
	expiresAt := poll.EndsAt.Add(issueTTLGrace)
	return s.limiter.Allow(key, s.maxCookiesPerClient, expiresAt)
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

	if _, ok := poll.OptionSet[req.OptionID]; !ok {
		httpx.WriteError(w, http.StatusBadRequest, httpx.ErrBadRequest, "unknown option_id")
		return
	}

	now := s.now()
	if !now.Before(poll.Poll.EndsAt) {
		// Опрос закрыт: инициируем закрытие (досылка остатка + done) и
		// отклоняем голос. Закрытие идемпотентно.
		go s.closePoll(id)
		httpx.WriteError(w, http.StatusForbidden, httpx.ErrClosed, "poll closed")
		return
	}

	// Подпись проверяется относительно конкретного опроса: cookie,
	// выданная для другого опроса, отбрасывается и уходит в fallback.
	fp := fingerprint.FromRequest(r, id, s.signer)
	// Add сам поставит в очередь отправки как полный батч, так и (если опрос
	// закрылся конкурентно) отказ — под тем же мьютексом, что и Close.
	if !s.buf.Add(id, model.Vote{Fingerprint: fp, OptionID: req.OptionID}) {
		httpx.WriteError(w, http.StatusForbidden, httpx.ErrClosed, "poll closed")
		return
	}

	// Ответ на успешный голос постоянен — отдаём предвычисленный JSON,
	// не тратя CPU на сериализацию на каждом запросе (hot path).
	httpx.WriteJSONBytes(w, http.StatusAccepted, acceptedResponse)
}

// acceptedResponse — постоянный ответ на принятый голос.
var acceptedResponse = []byte(`{"status":"accepted"}` + "\n")

// handleHealthz — GET /healthz.
func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// closePoll закрывает опрос: ставит в очередь остаток буфера и "done".
// Идемпотентно: повторный вызов не отправит "done" дважды. Всё делается под
// мьютексом буфера, поэтому "done" гарантированно идёт после любого батча,
// сформированного конкурентным Add().
func (s *Server) closePoll(pollID string) {
	if s.buf.Close(pollID, s.now()) {
		return // уже закрыт ранее — "done" уже отправлен
	}
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
		if !now.Before(poll.Poll.EndsAt) {
			s.closePoll(id)
		}
	}
	s.buf.Cleanup(s.closeTTL, now)
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
