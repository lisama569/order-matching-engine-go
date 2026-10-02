//go:build !race

package engine

// raceEnabled 在 -race 构建下为 true（计时类断言需放宽）。
const raceEnabled = false
