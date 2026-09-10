// Package fingerprint реализует стратегию вычисления fingerprint зрителя
// для базовой дедупликации голосов.
//
// Стратегия (см. docs/architecture/03-deduplication.md):
//
//	fingerprint = raw UUID из ПОДПИСАННОЙ cookie   — если подпись валидна
//	fingerprint = sha256(IP)                       — иначе (fallback)
//
// Подпись проверяется относительно конкретного poll_id (см. signature.go),
// поэтому cookie, выданная для одного опроса, не принимается на другом, а
// выдуманный вручную UUID отбрасывается и уходит в fallback.
//
// Fallback намеренно строится ТОЛЬКО по IP, без User-Agent. UA — клиентский
// заголовок, его ротация бесплатна, и он превращал fallback в «бесконечный
// источник новых fingerprint'ов»: атакующий слал UA: bot-1, bot-2, ... и каждый
// раз получал новый fingerprint. Без UA один IP без cookie даёт ровно один
// fingerprint, поэтому накрутка с одного IP ограничена (ценой схлопывания
// нескольких устройств за одним NAT без cookie — осознанный компромисс).
package fingerprint

import (
	"crypto/sha256"
	"encoding/hex"
	"net"
	"net/http"
	"strings"
)

// CookieName — имя cookie, в которой API-воркер выдаёт идентификатор зрителя.
const CookieName = "voter_id"

// Verifier проверяет подпись значения cookie для конкретного опроса.
type Verifier interface {
	Verify(pollID, cookieValue string) (raw string, ok bool)
}

// FromRequest вычисляет fingerprint для входящего HTTP-запроса с проверкой
// подписи cookie относительно poll_id.
//
// Если cookie есть и подпись валидна — используется сырой UUID из неё
// (уникален для устройства, не зависит от NAT). Во всех остальных случаях
// (cookie нет, подпись не сошлась, формат нарушен) — fallback sha256(IP+UA).
//
// Из-за fallback подделка cookie не даёт атакующему ничего сверх «нового
// IP+UA», а валидные подписанные cookie выдаются однократно на пару
// (poll_id, IP+UA) — см. internal/api/issued.go.
func FromRequest(r *http.Request, pollID string, v Verifier) string {
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		if raw, ok := v.Verify(pollID, c.Value); ok {
			return raw
		}
	}
	return HashIP(ClientIP(r))
}

// CookieValue возвращает сырое значение cookie voter_id, если она есть.
// Используется на выдаче, чтобы не перезаписывать уже имеющуюся cookie.
func CookieValue(r *http.Request) (string, bool) {
	c, err := r.Cookie(CookieName)
	if err != nil || c.Value == "" {
		return "", false
	}
	return c.Value, true
}

// HashIP возвращает hex-представление sha256 от IP.
// Фиксированная длина (64 символа) делает ключ компактным и единообразным.
// UA сознательно не участвует — см. комментарий к пакету.
func HashIP(ip string) string {
	sum := sha256.Sum256([]byte(ip))
	return hex.EncodeToString(sum[:])
}

// ClientIP извлекает IP клиента, учитывая заголовки реверс-прокси
// (X-Forwarded-For, X-Real-IP) и fallback на RemoteAddr.
func ClientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		// Берём первый (клиентский) адрес из цепочки.
		parts := strings.Split(xff, ",")
		return strings.TrimSpace(parts[0])
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
