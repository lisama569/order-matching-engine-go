package pubsub

import (
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/event"
)

func TestFanoutAndSlowSubscriber(t *testing.T) {
	b := NewBus()
	defer b.Close()

	fast := b.Subscribe("fast", 8)
	slow := b.Subscribe("slow", 1)

	// 第一条两条订阅者都收到
	b.Publish(event.Event{Seq: 1})
	assertSeq(t, fast, 1)
	assertSeq(t, slow, 1)

	// slow 缓冲=1 且未消费，再发布即溢出：slow 被断开，fast 不丢
	for seq := int64(2); seq <= 6; seq++ {
		b.Publish(event.Event{Seq: seq})
	}

	select {
	case <-slow.Overflow:
		t.Log("判定依据: slow 缓冲已满 => Overflow 关闭，Publish 立即返回且不阻塞撮合")
	case <-time.After(time.Second):
		t.Fatal("slow subscriber should be marked overflow")
	}
	// 关闭带缓冲通道时缓冲内残余事件仍可被排空，排空后必须读到关闭标志
	drained := false
	for range slow.Events {
		drained = true
	}
	_ = drained
	if _, ok := <-slow.Events; ok {
		t.Fatal("slow Events channel should be closed after overflow")
	}

	// fast 必须收到全部 2..6（事件未被慢订阅者影响）
	for seq := int64(2); seq <= 6; seq++ {
		assertSeq(t, fast, seq)
	}
	t.Log("fast 订阅者收到 seq=1..6 全部事件，慢订阅者未造成事件丢失或阻塞")
}

func assertSeq(t *testing.T, s *Subscriber, want int64) {
	t.Helper()
	select {
	case ev := <-s.Events:
		if ev.Seq != want {
			t.Fatalf("%s got seq=%d want %d", s.ID(), ev.Seq, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("%s timeout waiting seq=%d", s.ID(), want)
	}
}
