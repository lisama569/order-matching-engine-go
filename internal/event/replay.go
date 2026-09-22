package event

import "fmt"

// Replay 将事件流按严格顺序投递给 apply。
// 校验规则：必须以序号 1 开始，且每个事件序号恰为前一个 +1。
// 重复 => DUPLICATE；回退 => OUT_OF_ORDER；跳跃 => GAP；任一违规立即终止，不继续推进。
func Replay(events []Event, apply func(Event) error) error {
	seen := make(map[int64]struct{}, len(events))
	var expected int64 = 1
	for _, e := range events {
		var kind string
		switch {
		case e.Seq == expected:
			// 正常推进
		case e.Seq < expected:
			if _, dup := seen[e.Seq]; dup {
				kind = SeqDuplicate
			} else {
				kind = SeqOutOfOrder
			}
		default: // e.Seq > expected
			kind = SeqGap
		}
		if kind != "" {
			return &SequenceError{Kind: kind, Got: e.Seq, Expected: expected}
		}
		seen[e.Seq] = struct{}{}
		if err := apply(e); err != nil {
			return fmt.Errorf("replay apply event %d: %w", e.Seq, err)
		}
		expected++
	}
	return nil
}
