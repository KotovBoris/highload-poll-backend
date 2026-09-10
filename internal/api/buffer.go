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

// Размер шарда: сколько независимых сегментов буфера создаём. Каждый шард
// обслуживает свой набор poll_id под собственным мьютексом.
const bufferShardCount = 64

// voteBuffer — потокобезопасный in-memory буфер голосов, разбитый по опросам.
//
// Ключевая гарантия порядка: и «срабатывание батча» (набор полного батча), и
// закрытие опроса (остаток + "done") формируют исходящую очередь pb.out СТРОГО
// под мьютексом шарда. Отправкой занимается отдельный drainer-горутина на
// опрос, которая разбирает очередь в порядке добавления. Поэтому "done" не может
// обогнать батч, порождённый конкурентным Add() — а именно эта гонка приводила
// к потере голосов.
//
// Шардирование по poll_id: один глобальный мьютекс стал бы точкой contention
// при десятках тысяч RPS на многoядерной машине (pprof показывал заметную долю
// в futex/lock). Шард выбирается хешем poll_id, поэтому все операции одного
// опроса идут под одним мьютексом (гарантия порядка сохраняется), а разные
// опросы не блокируют друг друга.
type voteBuffer struct {
	shards    [bufferShardCount]bufferShard
	batchSize int
	send      sendFunc
}

// bufferShard — независимый сегмент буфера со своим мьютексом.
type bufferShard struct {
	mu    sync.Mutex
	polls map[string]*pollBuffer
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
		batchSize: batchSize,
		send:      send,
	}
}

// shardFor выбирает шард по poll_id (FNV-1a). Один и тот же опрос всегда
// попадает в один шард — это и обеспечивает порядок внутри опроса.
func (b *voteBuffer) shardFor(pollID string) *bufferShard {
	var h uint32 = 2166136261
	for i := 0; i < len(pollID); i++ {
		h ^= uint32(pollID[i])
		h *= 16777619
	}
	return &b.shards[h%bufferShardCount]
}

// pollLocked возвращает буфер опроса, создавая его при необходимости.
// Вызывается под удерживаемым мьютексом шарда.
func (s *bufferShard) pollLocked(pollID string) *pollBuffer {
	if s.polls == nil {
		s.polls = make(map[string]*pollBuffer)
	}
	pb := s.polls[pollID]
	if pb == nil {
		pb = &pollBuffer{}
		s.polls[pollID] = pb
	}
	return pb
}

// Add добавляет голос в буфер опроса.
// Возвращает false, если опрос закрыт (голос не принят).
func (b *voteBuffer) Add(pollID string, v model.Vote) (accepted bool) {
	sh := b.shardFor(pollID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	pb := sh.pollLocked(pollID)
	if pb.closed {
		return false
	}
	pb.votes = append(pb.votes, v)
	if len(pb.votes) >= b.batchSize {
		pb.out = append(pb.out, outMsg{votes: pb.votes})
		pb.votes = nil
	}
	ensureDrainerLocked(sh, pollID, pb, b.send)
	return true
}

// Close закрывает опрос: ставит в очередь остаток голосов и "done".
// Возвращает alreadyClosed=true, если опрос был закрыт ранее (идемпотентность:
// "done" уходит ровно один раз).
func (b *voteBuffer) Close(pollID string, now time.Time) (alreadyClosed bool) {
	sh := b.shardFor(pollID)
	sh.mu.Lock()
	defer sh.mu.Unlock()

	pb := sh.pollLocked(pollID)
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
	ensureDrainerLocked(sh, pollID, pb, b.send)
	return false
}

// ensureDrainerLocked запускает drainer опроса (один раз) и будит его, если он
// уже работает. Вызывается под удерживаемым мьютексом шарда.
func ensureDrainerLocked(sh *bufferShard, pollID string, pb *pollBuffer, send sendFunc) {
	if pb.draining {
		select {
		case pb.wake <- struct{}{}:
		default:
		}
		return
	}
	pb.draining = true
	pb.wake = make(chan struct{}, 1)
	go drainPoll(sh, pollID, pb, send)
}

// drainPoll последовательно отправляет исходящую очередь опроса. Гарантирует,
// что send() получает элементы в порядке добавления. Завершается, когда опрос
// закрыт и очередь пуста. Мьютекс шарда — тот же, под которым наполняется out,
// поэтому порядок и отсутствие гонок сохранены.
func drainPoll(sh *bufferShard, pollID string, pb *pollBuffer, send sendFunc) {
	for {
		sh.mu.Lock()
		if len(pb.out) == 0 {
			closed := pb.closed
			sh.mu.Unlock()
			if closed {
				return
			}
			<-pb.wake
			continue
		}
		msg := pb.out[0]
		pb.out = pb.out[1:]
		sh.mu.Unlock()

		if send != nil {
			send(pollID, msg.votes, msg.done)
		}
	}
}

// PollIDs возвращает список известных буферу опросов (для фонового watcher'а).
// Обходит все шарды, каждый — под своим мьютексом (блокировка не общая).
func (b *voteBuffer) PollIDs() []string {
	var ids []string
	for i := range b.shards {
		sh := &b.shards[i]
		sh.mu.Lock()
		for id := range sh.polls {
			ids = append(ids, id)
		}
		sh.mu.Unlock()
	}
	return ids
}

// Cleanup удаляет буферы закрытых опросов, которые закрыты более ttl назад.
func (b *voteBuffer) Cleanup(ttl time.Duration, now time.Time) {
	for i := range b.shards {
		sh := &b.shards[i]
		sh.mu.Lock()
		for id, pb := range sh.polls {
			// Удаляем только когда очередь уже разобрана drainer'ом, иначе
			// потеряем неотправленные элементы.
			if pb.closed && len(pb.out) == 0 && now.Sub(pb.closedAt) > ttl {
				delete(sh.polls, id)
			}
		}
		sh.mu.Unlock()
	}
}
