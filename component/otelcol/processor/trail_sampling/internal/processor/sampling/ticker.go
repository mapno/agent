package sampling

import "time"

type PolicyTicker interface {
	Start(d time.Duration)
	OnTick()
	Stop()
}

var _ PolicyTicker = (*Ticker)(nil)

type Ticker struct {
	Ticker     *time.Ticker
	OnTickFunc func()
	StopCh     chan struct{}
}

func (pt *Ticker) Start(d time.Duration) {
	pt.Ticker = time.NewTicker(d)
	pt.StopCh = make(chan struct{})
	go func() {
		for {
			select {
			case <-pt.Ticker.C:
				pt.OnTick()
			case <-pt.StopCh:
				return
			}
		}
	}()
}

func (pt *Ticker) OnTick() {
	pt.OnTickFunc()
}

func (pt *Ticker) Stop() {
	if pt.StopCh == nil {
		return
	}
	close(pt.StopCh)
	pt.Ticker.Stop()
}
