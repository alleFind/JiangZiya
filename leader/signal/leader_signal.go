package signal

import "sync"

// 判断listener何时发送stop信号给leaders 这里的leaders与Leader shard的leader不一样
type StopSignal struct {
	stoplock      sync.Mutex
	stopGap       int // 记录来自leaders有多少个空txList
	stopThreshold int
}

func NewStopSignal(stopThreshold int) *StopSignal {
	return &StopSignal{
		stopGap:       0,
		stopThreshold: stopThreshold,
	}
}

// 当接收到一个空的txList时，增加stopGap
func (ss *StopSignal) StopGap_Increase() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap++
}

// 接收到已执行的交易的消息时，重置stopGap
func (ss *StopSignal) StopGap_Reset() {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	ss.stopGap = 0
}

// stopGap达到阈值时，发送stop消息给leaders
func (ss *StopSignal) GapEnough() bool {
	ss.stoplock.Lock()
	defer ss.stoplock.Unlock()
	return ss.stopGap >= ss.stopThreshold
}
