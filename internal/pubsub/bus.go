// Package pubsub 将撮合事件向多个订阅者分发；慢订阅者被断开而非阻塞撮合。
//
// 语义：Publish 永不阻塞。某订阅者缓冲满时，该订阅者立即收到 Overflow 信号并被断开，
// 撮合方与其他订阅者不受影响，事件也不会从权威事件日志（event.Store）丢失；
// 订阅者可重新 Subscribe 并通过 Engine/Store 的 From(lastSeenSeq) 追赶缺失事件。
package pubsub

import (
	"sync"

	"github.com/lisama569/order-matching-engine-go/internal/event"
)

type sub struct {
	id       string
	ch       chan event.Event
	overflow chan struct{}
	dropped  bool
}

// Subscriber 是一个事件订阅句柄。
type Subscriber struct {
	// Events 接收撮合事件；被判定为慢订阅者后该通道会被关闭。
	Events <-chan event.Event
	// Overflow 在该订阅者因缓冲满而被断开时关闭（恰好一次信号）。
	Overflow <-chan struct{}

	id  string
	raw *sub
}

// ID 返回订阅者标识。
func (s *Subscriber) ID() string { return s.id }

// Bus 为一对多事件分发器，线程安全。
type Bus struct {
	mu   sync.Mutex
	subs map[string]*sub
}

// NewBus 创建分发器。
func NewBus() *Bus {
	return &Bus{subs: make(map[string]*sub)}
}

// Subscribe 创建带缓冲订阅；缓冲必须 > 0。相同 ID 重复订阅会先断开旧订阅。
func (b *Bus) Subscribe(id string, buffer int) *Subscriber {
	if buffer <= 0 {
		buffer = 1
	}
	s := &sub{
		id:       id,
		ch:       make(chan event.Event, buffer),
		overflow: make(chan struct{}),
	}
	b.mu.Lock()
	if old, ok := b.subs[id]; ok {
		b.dropLocked(old)
	}
	b.subs[id] = s
	b.mu.Unlock()
	return &Subscriber{Events: s.ch, Overflow: s.overflow, id: id, raw: s}
}

func (b *Bus) dropLocked(s *sub) {
	if s.dropped {
		return
	}
	s.dropped = true
	close(s.overflow)
	close(s.ch)
	delete(b.subs, s.id)
}

// Publish 非阻塞发布事件：任何缓冲满的订阅者都会被立即断开，绝不等待。
func (b *Bus) Publish(e event.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		select {
		case s.ch <- e:
		default:
			b.dropLocked(s) // 慢订阅者：断开而非阻塞或丢弃
		}
	}
}

// Unsubscribe 主动移除订阅。
func (b *Bus) Unsubscribe(s *Subscriber) {
	if s == nil {
		return
	}
	b.mu.Lock()
	if cur, ok := b.subs[s.id]; ok && cur == s.raw {
		b.dropLocked(cur)
	}
	b.mu.Unlock()
}

// Close 断开并关闭全部订阅。
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		b.dropLocked(s)
	}
}
