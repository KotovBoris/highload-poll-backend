package fingerprint

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"hash"
	"strings"
	"sync"

	"github.com/KotovBoris/highload-poll-backend/internal/uuid"
)

// cookieSeparator разделяет сырой идентификатор и подпись в значении cookie.
// Не встречается в UUID, поэтому разделитель однозначен.
const cookieSeparator = "."
const cookieSeparatorByte = byte('.')

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
	pool   sync.Pool
}

func newSigner(secret []byte) *Signer {
	s := &Signer{secret: secret}
	s.pool.New = func() any { return hmac.New(sha256.New, s.secret) }
	return s
}

// NewSigner создаёт подписыватель с указанным секретом.
// Пустой секрет допустим только в тестах/локальном запуске (см. NewRandomSigner).
func NewSigner(secret string) *Signer {
	return newSigner([]byte(secret))
}

// NewRandomSigner создаёт подписыватель со случайным секретом.
// Используется, когда COOKIE_SECRET не задан: куки живут до рестарта процесса.
func NewRandomSigner() *Signer {
	return newSigner([]byte(uuid.New() + uuid.New()))
}

// signInto считает HMAC-SHA256 от poll_id || 0x00 || raw в переданный буфер.
// Нулевой байт-разделитель исключает склейку разных (poll_id, raw) в одну
// строку с одинаковым MAC.
//
// Хешер берётся из пула и переиспользуется: на пути голоса Verify вызывается
// на каждый запрос, а hmac.New(sha256.New, ...) заметно аллоцировал (видно в
// профиле). Буфер dst позволяет складывать MAC без промежуточных срезов.
func (s *Signer) signInto(dst []byte, pollID, raw string) []byte {
	h := s.pool.Get().(hash.Hash)
	defer func() {
		h.Reset()
		s.pool.Put(h)
	}()
	// Собираем сообщение в стековом буфере, чтобы не конвертировать строки
	// в []byte (каждая конверсия — аллокация на hot path).
	var msg [128]byte
	n := copy(msg[:], pollID)
	msg[n] = 0
	n++
	n += copy(msg[n:], raw)
	h.Write(msg[:n])
	return h.Sum(dst)
}

// Sign возвращает подписанное значение cookie для пары (pollID, raw).
func (s *Signer) Sign(pollID, raw string) string {
	var sumBuf [sha256.Size]byte
	mac := s.signInto(sumBuf[:0], pollID, raw)

	var hexBuf [sha256.Size * 2]byte
	hex.Encode(hexBuf[:], mac)

	out := make([]byte, 0, len(raw)+1+len(hexBuf))
	out = append(out, raw...)
	out = append(out, cookieSeparatorByte)
	out = append(out, hexBuf[:]...)
	return string(out)
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

	// Подпись в cookie — hex. Декодируем присланный hex в стековый буфер и
	// сравниваем байты MAC: без конверсии hex-строки и лишних аллокаций.
	if len(got) != sha256.Size*2 {
		return "", false
	}
	var gotBin [sha256.Size]byte
	if _, err := hex.Decode(gotBin[:], []byte(got)); err != nil {
		return "", false
	}
	var sumBuf [sha256.Size]byte
	mac := s.signInto(sumBuf[:0], pollID, raw)
	// Сравнение в постоянном времени — защита от timing-атак.
	if subtle.ConstantTimeCompare(mac, gotBin[:]) != 1 {
		return "", false
	}
	return raw, true
}
