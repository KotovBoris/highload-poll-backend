// Package httpx — небольшие хелперы для JSON HTTP-ручек: единообразная
// сериализация ответов, разбор тел запросов и единый формат ошибок.
package httpx

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// Коды ошибок (поле "error" в ErrorResponse).
const (
	ErrBadRequest  = "bad_request"
	ErrNotFound    = "not_found"
	ErrConflict    = "conflict"
	ErrClosed      = "poll_closed"
	ErrInternal    = "internal_error"
	ErrUnavailable = "unavailable"
	ErrTooMany     = "too_many_requests"
)

// maxBodyBytes ограничивает размер тела запроса (защита от злоупотребления).
const maxBodyBytes = 1 << 20 // 1 MiB

// WriteJSON сериализует v как JSON и отправляет с указанным статусом.
func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if v == nil {
		return
	}
	_ = json.NewEncoder(w).Encode(v)
}

// WriteError отправляет ошибку в едином формате.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, model.ErrorResponse{Error: code, Message: message})
}

// ReadJSON читает тело запроса в v, ограничивая размер и запрещая
// неизвестные поля (DisallowUnknownFields — раннее обнаружение опечаток).
func ReadJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	// Убеждаемся, что в теле нет второго JSON-документа.
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}
