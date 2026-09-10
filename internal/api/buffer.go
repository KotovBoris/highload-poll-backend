package api

import (
	"sync"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// voteBuffer — потокобезопасный in-memory буфер голосов, разбитый по опросам.
//
// При достижении batchSize голосов по опросу буфер «срабатывает» и отдаёт
// накопленный срез вызывающей стороне для отправки в Kafka. Буфер также умеет
// закрывать опрос: после закрытия новые голоса не принимаются, а остаток
// выдаётся один раз (для досылки перед "done").
type voteBuffer struct {
	mu        sync.Mutex
	polls     map[string]*pollBuffer
	batchSize int
}

type pollBuffer struct {
	votes    []model.Vote
	closed   bool
	closedAt time.Time
}

func newVoteBuffer(batchSize int) *voteBuffer {
	if batchSize <= 0 {
		batchSize = 10000
	}
	return &voteBuffer{
		polls:     make(map[string]*pollBuffer),
		batchSize: batchSize,
	}
}

// Add добавляет голос в буфер опроса.
// Возвращает:
//   - flush — готовый к отправке батч (если достигнут batchSize), иначе nil;
//   - closed — true, если опрос уже закрыт (голос не принят).
func (b *voteBuffer) Add(pollID string, v model.Vote) (flush []model.Vote, closed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pb := b.polls[pollID]
	if pb == nil {
		pb = &pollBuffer{}
		b.polls[pollID] = pb
	}
	if pb.closed {
		return nil, true
	}
	pb.votes = append(pb.votes, v)
	if len(pb.votes) >= b.batchSize {
		flush = pb.votes
		pb.votes = nil
	}
	return flush, false
}

// Close закрывает опрос. Возвращает остаток голосов (для досылки) и признак
// alreadyClosed — true, если опрос был закрыт ранее (идемпотентность: "done"
// должен уйти ровно один раз).
func (b *voteBuffer) Close(pollID string, now time.Time) (flush []model.Vote, alreadyClosed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pb := b.polls[pollID]
	if pb == nil {
		pb = &pollBuffer{}
		b.polls[pollID] = pb
	}
	if pb.closed {
		return nil, true
	}
	pb.closed = true
	pb.closedAt = now
	flush = pb.votes
	pb.votes = nil
	return flush, false
}

// PollIDs возвращает список известных буферу опросов (для фонового watcher'а).
func (b *voteBuffer) PollIDs() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	ids := make([]string, 0, len(b.polls))
	for id := range b.polls {
		ids = append(ids, id)
	}
	return ids
}

// Cleanup удаляет буферы закрытых опросов, которые закрыты более ttl назад.
func (b *voteBuffer) Cleanup(ttl time.Duration, now time.Time) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for id, pb := range b.polls {
		if pb.closed && now.Sub(pb.closedAt) > ttl {
			delete(b.polls, id)
		}
	}
}
