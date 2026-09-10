// Package model содержит общие доменные структуры, разделяемые всеми
// сервисами (API-воркер, consumer, сервис результатов) и форматами обмена
// (HTTP-контракты и Kafka-сообщения).
package model

import "time"

// PollStatus — статус жизненного цикла опроса.
type PollStatus string

const (
	// PollStatusActive — опрос создан и принимает голоса (до ends_at).
	PollStatusActive PollStatus = "active"
	// PollStatusCompleted — результаты посчитаны и сохранены (flush сделан).
	PollStatusCompleted PollStatus = "completed"
)

// Option — вариант ответа в опросе.
type Option struct {
	ID   int    `json:"id"`
	Text string `json:"text"`
}

// Poll — опрос: метаданные + (опционально) итоговые результаты.
//
// В БД хранится как одна строка таблицы polls; Options и Results — JSONB.
type Poll struct {
	ID        string     `json:"id"`
	Question  string     `json:"question"`
	Options   []Option   `json:"options"`
	Status    PollStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	EndsAt    time.Time  `json:"ends_at"`
	// Results заполняется только после завершения опроса.
	Results *Results `json:"results,omitempty"`
}

// OptionResult — число голосов за конкретный вариант.
type OptionResult struct {
	OptionID int   `json:"option_id"`
	Count    int64 `json:"count"`
}

// Results — итоговые (обезличенные) результаты опроса. Именно эта структура
// сохраняется consumer'ом через идемпотентный flush и отдаётся в админку.
type Results struct {
	Total   int64          `json:"total"`
	Options []OptionResult `json:"options"`
	// WorkersSeen / WorkersDone — диагностика механизма завершения опроса
	// (сколько API-воркеров прислало голоса и сколько подтвердило "done").
	WorkersSeen int `json:"workers_seen,omitempty"`
	WorkersDone int `json:"workers_done,omitempty"`
}

// Vote — один голос: fingerprint зрителя и выбранный вариант.
type Vote struct {
	Fingerprint string `json:"fingerprint"`
	OptionID    int    `json:"option_id"`
}

// VoteBatch — единица обмена через Kafka. Одно сообщение = батч голосов,
// собранных одним API-воркером по одному опросу. key = poll_id.
//
// При Done=true сообщение является маркером завершения: воркер сообщает, что
// дослал весь остаток буфера по данному опросу и больше голосов не будет.
type VoteBatch struct {
	PollID   string `json:"poll_id"`
	WorkerID string `json:"worker_id"`
	Done     bool   `json:"done"`
	Votes    []Vote `json:"votes,omitempty"`
}

// CreatePollRequest — тело запроса админки на создание опроса.
// Указывается либо DurationSeconds (относительно now), либо EndsAt (абсолютно).
type CreatePollRequest struct {
	Question        string     `json:"question"`
	Options         []string   `json:"options"`
	DurationSeconds int        `json:"duration_seconds,omitempty"`
	EndsAt          *time.Time `json:"ends_at,omitempty"`
}

// VoteRequest — тело запроса на голосование.
type VoteRequest struct {
	OptionID int `json:"option_id"`
}

// VoteResponse — ответ API-воркера на принятый голос.
type VoteResponse struct {
	Status string `json:"status"`
}

// ErrorResponse — единый формат ошибки всех HTTP-ручек.
type ErrorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message,omitempty"`
}

// Результаты flush, отправляемого consumer'ом в сервис результатов.
type FlushRequest struct {
	Results Results `json:"results"`
}
