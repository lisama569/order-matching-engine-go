package pubsub

import (
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/event"
)

func TestFanoutAndSlowSubscriber(t *testing.T) {
	b := New(2)
	defer b.Close()
	id1, ch1, drop1 := b.Subscribe()
	id2, ch2, _ := b.Subscribe()
	_ = id2

	// 快速订阅者正常接收。
	b.Publish(event.Event{Seq: 1})
	b.Publish(event.Event{Seq: 2})
	if got := <-ch1; got.Seq != 1 {
		t.Fatalf("ch1 seq = %d", got.Seq)
	}
	if got := <-ch1; got.Seq != 2 {
		t.Fatalf("ch1 seq = %d", got.Seq)
	}
	// ch2 不读取，继续发布触发丢弃最旧事件，发布本身不得阻塞。
	done := make(chan struct{})
	go func() {
		for i := int64(3); i <= 20; i++ {
			b.Publish(event.Event{Seq: i})
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("publish blocked by slow subscriber")
	}
	select {
	case d := <-drop1:
		if d.SubscriptionID != id1 || d.DroppedCount == 0 || d.LastSeq < 3 || d.LastSeq > 20 {
			t.Fatalf("drop notice = %+v", d)
		}
	default:
		t.Fatal("slow subscriber should receive drop notice")
	}
	// 最新事件必须仍可达：排空缓冲后最后一个应为 20。
	var last int64
drain:
	for {
		select {
		case ev := <-ch2:
			last = ev.Seq
		default:
			break drain
		}
	}
	if last != 20 {
		t.Fatalf("slow subscriber latest seq = %d, want 20", last)
	}
}
