package api

import (
	"sync"
	"time"

	"github.com/KotovBoris/highload-poll-backend/internal/model"
)

// sendFunc отправляет один исходящий элемент опроса во внешний путь (Kafka).
// votes непуст — обычный батч; done=true — маркер завершения.
type sendFunc func(pollID string, votes []model.Vote, done bool)

// outMsg — элемент исходящей очереди опроса.
type outMsg struct {
	votes []model.Vote
	done  bool
}

// voteBuffer — потокобезопасный in-memory буфер голосов, разбитый по опросам.
//
// Ключевая гарантия порядка: и «срабатывание батча» (набор полного батча), и
// закрытие опроса (остаток + "done") формируют исходящую очередь pb.out СТРОГО
// под мьютексом буфера. Отправкой занимается отдельный drainer-горутина на
// опрос, которая разбирает очередь в порядке добавления. Поэтому "done" не может
// обогнать батч, порождённый конкурентным Add() — а именно эта гонка приводила
// к потере голосов.
//
// Раньше решение об отправке принималось вне мьютекса (Add возвращал батч, а
// вызывающий его отправлял), из-за чего closePoll мог вклиниться между
// формированием батча и его отправкой и отправить "done" раньше батча.
type voteBuffer struct {
	mu        sync.Mutex
	polls     map[string]*pollBuffer
	batchSize int
	send      sendFunc
}

type pollBuffer struct {
	votes    []model.Vote
	out      []outMsg
	closed   bool
	closedAt time.Time
	draining bool
	wake     chan struct{}
}

func newVoteBuffer(batchSize int, send sendFunc) *voteBuffer {
	if batchSize <= 0 {
		batchSize = 10000
	}
	return &voteBuffer{
		polls:     make(map[string]*pollBuffer),
		batchSize: batchSize,
		send:      send,
	}
}

// pollLocked возвращает буфер опроса, создавая его при необходимости.
// Вызывается под удерживаемым мьютексом.
func (b *voteBuffer) pollLocked(pollID string) *pollBuffer {
	pb := b.polls[pollID]
	if pb == nil {
		pb = &pollBuffer{}
		b.polls[pollID] = pb
	}
	return pb
}

// Add добавляет голос в буфер опроса.
// Возвращает false, если опрос закрыт (голос не принят).
func (b *voteBuffer) Add(pollID string, v model.Vote) (accepted bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pb := b.pollLocked(pollID)
	if pb.closed {
		return false
	}
	pb.votes = append(pb.votes, v)
	if len(pb.votes) >= b.batchSize {
		pb.out = append(pb.out, outMsg{votes: pb.votes})
		pb.votes = nil
	}
	b.ensureDrainerLocked(pollID, pb)
	return true
}

// Close закрывает опрос: ставит в очередь остаток голосов и "done".
// Возвращает alreadyClosed=true, если опрос был закрыт ранее (идемпотентность:
// "done" уходит ровно один раз).
func (b *voteBuffer) Close(pollID string, now time.Time) (alreadyClosed bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	pb := b.pollLocked(pollID)
	if pb.closed {
		return true
	}
	pb.closed = true
	pb.closedAt = now
	if len(pb.votes) > 0 {
		pb.out = append(pb.out, outMsg{votes: pb.votes})
		pb.votes = nil
	}
	pb.out = append(pb.out, outMsg{done: true})
	b.ensureDrainerLocked(pollID, pb)
	return false
}

// ensureDrainerLocked запускает drainer опроса (один раз) и будит его, если он
// уже работает. Вызывается под удерживаемым мьютексом.
func (b *voteBuffer) ensureDrainerLocked(pollID string, pb *pollBuffer) {
	if pb.draining {
		select {
		case pb.wake <- struct{}{}:
		default:
		}
		return
	}
	pb.draining = true
	pb.wake = make(chan struct{}, 1)
	go b.drain(pollID, pb)
}

// drain последовательно отправляет исходящую очередь опроса. Гарантирует, что
// вызывающий send() получает элементы в порядке добавления. Завершается, когда
// опрос закрыт и очередь пуста.
func (b *voteBuffer) drain(pollID string, pb *pollBuffer) {
	for {
		b.mu.Lock()
		if len(pb.out) == 0 {
			closed := pb.closed
			b.mu.Unlock()
			if closed {
				return
			}
			<-pb.wake
			continue
		}
		msg := pb.out[0]
		pb.out = pb.out[1:]
		b.mu.Unlock()

		if b.send != nil {
			b.send(pollID, msg.votes, msg.done)
		}
	}
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
		// Удаляем только когда очередь уже разобрана drainer'ом, иначе
		// потеряем неотправленные элементы.
		if pb.closed && len(pb.out) == 0 && now.Sub(pb.closedAt) > ttl {
			delete(b.polls, id)
		}
	}
}
