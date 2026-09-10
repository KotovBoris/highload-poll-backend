// Package fingerprint реализует стратегию вычисления fingerprint зрителя
// для базовой дедупликации голосов.
//
// Стратегия (см. docs/architecture/03-deduplication.md):
//
//	fingerprint = cookie (voter_id, UUID, как есть)  — если cookie есть
//	fingerprint = sha256(IP + "|" + User-Agent)      — иначе (fallback)
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

// FromRequest вычисляет fingerprint для входящего HTTP-запроса.
//
// Основной источник — cookie voter_id (уникальна для устройства, не зависит
// от NAT). Если cookie нет, используется fallback sha256(IP + "|" + UA).
func FromRequest(r *http.Request) string {
	if c, err := r.Cookie(CookieName); err == nil && c.Value != "" {
		return c.Value
	}
	return HashIPUA(ClientIP(r), r.UserAgent())
}

// HashIPUA возвращает hex-представление sha256 от "IP|User-Agent".
// Фиксированная длина (64 символа) делает ключ компактным и единообразным.
func HashIPUA(ip, userAgent string) string {
	sum := sha256.Sum256([]byte(ip + "|" + userAgent))
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
