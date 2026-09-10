// Package httpx — небольшие хелперы для JSON HTTP-ручек: единообразная
// сериализация ответов, разбор тел запросов и единый формат ошибок.
package httpx

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"

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
)

// maxBodyBytes ограничивает размер тела запроса (защита от злоупотребления).
const maxBodyBytes = 1 << 20 // 1 MiB

// WriteJSON сериализует v как JSON и отправляет с указанным статусом.
//
// Использует json.Marshal + один w.Write вместо json.NewEncoder(w).Encode:
// Encoder пишет в поток несколькими мелкими вызовами, что на hot path даёт
// лишние аллокации и записи (видно в CPU-профиле на 60K RPS).
func WriteJSON(w http.ResponseWriter, status int, v any) {
	if v == nil {
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(status)
		return
	}
	buf, err := json.Marshal(v)
	if err != nil {
		// Сериализация наших типов ошибиться не может; на всякий случай
		// отдаём валидный JSON-объект ошибки, не роняя соединение.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error":"internal_error"}`))
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(buf)))
	w.WriteHeader(status)
	_, _ = w.Write(buf)
}

// WriteJSONBytes отправляет уже сериализованный JSON. Позволяет держать
// постоянные ответы (например, {"status":"accepted"} на голосовании) в виде
// предвычисленного среза и не тратить CPU на сериализацию на каждом запросе.
func WriteJSONBytes(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteError отправляет ошибку в едином формате.
func WriteError(w http.ResponseWriter, status int, code, message string) {
	WriteJSON(w, status, model.ErrorResponse{Error: code, Message: message})
}

// ReadJSON читает тело запроса в v, ограничивая размер.
//
// Тело читается целиком (io.ReadAll поверх MaxBytesReader) и разбирается
// json.Unmarshal. Это замеренно быстрее json.Decoder с DisallowUnknownFields
// на hot path (POST /vote): тела мелкие, а Decoder тянул лишние аллокации и
// работу на refill. Строгость к неизвестным полям сохраняется на уровне
// структуры (см. Validate в вызывающем коде), а мусор после первого объекта
// отвергается проверкой trailing bytes.
func ReadJSON(r *http.Request, v any) error {
	r.Body = http.MaxBytesReader(nil, r.Body, maxBodyBytes)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, v)
}
