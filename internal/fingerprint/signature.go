package fingerprint

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"

	"github.com/KotovBoris/highload-poll-backend/internal/uuid"
)

// cookieSeparator разделяет сырой идентификатор и подпись в значении cookie.
// Не встречается в UUID, поэтому разделитель однозначен.
const cookieSeparator = "."

// Signer подписывает и проверяет значение cookie voter_id.
//
// Формат cookie: "<raw>.<hex HMAC-SHA256(secret, poll_id || 0x00 || raw)>".
//
// Зачем подпись:
//   - без неё зритель мог выдумать произвольный UUID и «стать новым
//     избирателем» на каждый запрос;
//   - подпись привязана к poll_id, поэтому cookie, выданная для одного опроса,
//     не принимается на другом;
//   - подделать подпись без знания секрета невозможно.
//
// Проверка выполняется за O(1) (один HMAC) и не требует обращений к хранилищу,
// поэтому cookie остаётся stateless.
type Signer struct {
	secret []byte
}

// NewSigner создаёт подписыватель с указанным секретом.
// Пустой секрет допустим только в тестах/локальном запуске (см. NewRandomSigner).
func NewSigner(secret string) *Signer {
	return &Signer{secret: []byte(secret)}
}

// NewRandomSigner создаёт подписыватель со случайным секретом.
// Используется, когда COOKIE_SECRET не задан: куки живут до рестарта процесса.
func NewRandomSigner() *Signer {
	return &Signer{secret: []byte(uuid.New() + uuid.New())}
}

// signature считает HMAC-SHA256 от poll_id || 0x00 || raw.
// Нулевой байт-разделитель исключает склейку разных (poll_id, raw) в одну
// строку с одинаковым MAC.
func (s *Signer) signature(pollID, raw string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(pollID))
	mac.Write([]byte{0})
	mac.Write([]byte(raw))
	return hex.EncodeToString(mac.Sum(nil))
}

// Sign возвращает подписанное значение cookie для пары (pollID, raw).
func (s *Signer) Sign(pollID, raw string) string {
	return raw + cookieSeparator + s.signature(pollID, raw)
}

// Issue генерирует новый идентификатор и возвращает его сырое значение и
// готовое значение cookie.
func (s *Signer) Issue(pollID string) (raw, cookieValue string) {
	raw = uuid.New()
	return raw, s.Sign(pollID, raw)
}

// Verify проверяет подпись cookie и возвращает сырой идентификатор.
// ok=false, если формат нарушен или подпись не совпадает.
func (s *Signer) Verify(pollID, cookieValue string) (raw string, ok bool) {
	idx := strings.LastIndex(cookieValue, cookieSeparator)
	if idx <= 0 || idx == len(cookieValue)-1 {
		return "", false
	}
	raw = cookieValue[:idx]
	got := cookieValue[idx+1:]

	expected := s.signature(pollID, raw)
	// Сравнение в постоянном времени — защита от timing-атак.
	if !hmac.Equal([]byte(got), []byte(expected)) {
		return "", false
	}
	return raw, true
}
