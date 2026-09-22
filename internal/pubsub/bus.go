package pubsub

import (
	"sync"
	"sync/atomic"

	"github.com/lisama569/order-matching-engine-go/internal/event"
)

// Dropped 通知订阅者：因消费过慢丢弃的事件数量。
type Dropped struct {
	SubscriptionID int
	DroppedCount   int64 // 该订阅累计丢弃数
	LastSeq        int64 // 本次丢弃发生时最新事件序号
}

type subscriber struct {
	id     int
	events chan event.Event
	drops  chan Dropped
	lost   int64 // 累计丢弃数
}

// Bus 撮合结果多订阅者分发总线。
// Publish 绝不阻塞：订阅者缓冲满时丢弃该订阅最旧的事件并累加计数，
// 通过 drops 通道异步通知（通知本身同样非阻塞）。
type Bus struct {
	mu      sync.RWMutex
	subs    map[int]*subscriber
	bufSize int
	nextID  int
	closed  atomic.Bool
}

// New 创建总线，subscriberBuffer 为每个订阅者的缓冲事件数（最小为 1）。
func New(subscriberBuffer int) *Bus {
	if subscriberBuffer < 1 {
		subscriberBuffer = 1
	}
	return &Bus{subs: make(map[int]*subscriber), bufSize: subscriberBuffer}
}

// Subscribe 注册订阅者，返回订阅ID、事件通道与丢弃通知通道。
func (b *Bus) Subscribe() (int, <-chan event.Event, <-chan Dropped) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextID++
	s := &subscriber{
		id:     b.nextID,
		events: make(chan event.Event, b.bufSize),
		drops:  make(chan Dropped, 1),
	}
	b.subs[s.id] = s
	return s.id, s.events, s.drops
}

// Publish 非阻塞发布到所有订阅者。
func (b *Bus) Publish(ev event.Event) {
	if b.closed.Load() {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	for _, s := range b.subs {
		// 先尝试直接入队。
		select {
		case s.events <- ev:
			continue
		default:
		}
		// 缓冲已满：弹出最旧事件腾位，保证最新事件可达且撮合不被阻塞。
		select {
		case <-s.events:
			s.lost++
		default:
		}
		s.events <- ev // 腾出一位后立即可入队
		notice := Dropped{SubscriptionID: s.id, DroppedCount: s.lost, LastSeq: ev.Seq}
		select {
		case s.drops <- notice:
		default:
			// 通知通道有待处理通知时，仅更新计数后丢弃本次通知。
		}
	}
}

// Close 关闭总线与全部订阅通道。
func (b *Bus) Close() {
	if !b.closed.CompareAndSwap(false, true) {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, s := range b.subs {
		close(s.events)
		close(s.drops)
	}
	b.subs = make(map[int]*subscriber)
}
