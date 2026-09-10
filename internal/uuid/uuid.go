// Package uuid — минимальный генератор UUIDv4 на crypto/rand.
//
// Реализован локально, чтобы не тянуть внешнюю зависимость ради одной функции.
// Криптостойкость здесь излишня, но rand.Read из crypto/rand даёт хорошее
// распределение, а стоимость генерации ничтожна для наших целей.
package uuid

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
)

// New возвращает случайный UUIDv4 в каноническом строковом виде.
func New() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand в Go 1.24+ не возвращает ошибок; ветка на всякий случай.
		panic(fmt.Sprintf("uuid: rand read failed: %v", err))
	}
	// version 4
	b[6] = (b[6] & 0x0f) | 0x40
	// variant RFC 4122
	b[8] = (b[8] & 0x3f) | 0x80

	var out [36]byte
	hex.Encode(out[0:8], b[0:4])
	out[8] = '-'
	hex.Encode(out[9:13], b[4:6])
	out[13] = '-'
	hex.Encode(out[14:18], b[6:8])
	out[18] = '-'
	hex.Encode(out[19:23], b[8:10])
	out[23] = '-'
	hex.Encode(out[24:36], b[10:16])
	return string(out[:])
}
