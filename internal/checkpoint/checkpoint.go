// Package checkpoint 提供检查点的完整性封装与文件存取。
//
// 检查点内容本身由引擎定义（本包不依赖引擎），本包只负责：
//   - Encode/Decode：JSON 序列化 + SHA-256 校验和封装；任何字节损坏、
//     截断或篡改都会在 Decode 时被识别为 ErrCorrupt，绝不静默通过；
//   - FileStore：按序号落盘与读取；文件缺失返回 ErrNotFound，
//     调用方据此安全退回全量重建。
package checkpoint

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// ErrCorrupt 表示检查点数据损坏（校验和不匹配或无法解析）。
var ErrCorrupt = errors.New("checkpoint corrupt")

// ErrNotFound 表示检查点不存在。
var ErrNotFound = errors.New("checkpoint not found")

// envelope 是落盘/传输的封装格式：负载 + 负载校验和。
type envelope struct {
	Payload  json.RawMessage `json:"payload"`
	Checksum string          `json:"checksum"` // hex(sha256(Payload))
}

// Encode 将任意可 JSON 序列化的检查点内容封装为带校验和的字节流。
func Encode(v any) ([]byte, error) {
	payload, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("checkpoint encode payload: %w", err)
	}
	sum := sha256.Sum256(payload)
	env := envelope{Payload: payload, Checksum: hex.EncodeToString(sum[:])}
	out, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("checkpoint encode envelope: %w", err)
	}
	return out, nil
}

// Decode 解开封装并校验完整性；校验和不匹配、JSON 畸形或负载类型不符
// 都返回 ErrCorrupt（可用 errors.Is 判定）。
func Decode(data []byte, v any) error {
	var env envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("%w: envelope parse: %v", ErrCorrupt, err)
	}
	sum := sha256.Sum256(env.Payload)
	if hex.EncodeToString(sum[:]) != env.Checksum {
		return fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	}
	if err := json.Unmarshal(env.Payload, v); err != nil {
		return fmt.Errorf("%w: payload parse: %v", ErrCorrupt, err)
	}
	return nil
}

// FileStore 以 "cp-<seq>.json" 命名规则在目录中存取检查点。
type FileStore struct {
	Dir string
}

func (fs FileStore) path(seq int64) string {
	return filepath.Join(fs.Dir, fmt.Sprintf("cp-%d.json", seq))
}

// Save 将已封装的检查点写入 Dir（原子替换同序号文件）。
func (fs FileStore) Save(seq int64, data []byte) error {
	if err := os.MkdirAll(fs.Dir, 0o755); err != nil {
		return err
	}
	tmp := fs.path(seq) + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, fs.path(seq))
}

// Load 读取指定序号的检查点；不存在返回 ErrNotFound。
func (fs FileStore) Load(seq int64) ([]byte, error) {
	data, err := os.ReadFile(fs.path(seq))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

// Latest 返回目录中序号最大的检查点；没有任何检查点时返回 ErrNotFound。
// 文件名无法解析的条目被忽略（视为无关文件）。
func (fs FileStore) Latest() (int64, []byte, error) {
	entries, err := os.ReadDir(fs.Dir)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil, ErrNotFound
	}
	if err != nil {
		return 0, nil, err
	}
	var seqs []int64
	for _, en := range entries {
		name := en.Name()
		if !strings.HasPrefix(name, "cp-") || !strings.HasSuffix(name, ".json") {
			continue
		}
		n, err := strconv.ParseInt(name[3:len(name)-5], 10, 64)
		if err != nil {
			continue
		}
		seqs = append(seqs, n)
	}
	if len(seqs) == 0 {
		return 0, nil, ErrNotFound
	}
	sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
	latest := seqs[len(seqs)-1]
	data, err := fs.Load(latest)
	if err != nil {
		return 0, nil, err
	}
	return latest, data, nil
}
