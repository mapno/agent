package sampling

import "time"

type PolicyTicker struct {
	Ticker     *time.Ticker
	OnTickFunc func()
	StopCh     chan struct{}
}

func (pt *PolicyTicker) Start(d time.Duration) {
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

func (pt *PolicyTicker) OnTick() {
	pt.OnTickFunc()
}

func (pt *PolicyTicker) Stop() {
	if pt.StopCh == nil {
		return
	}
	close(pt.StopCh)
	pt.Ticker.Stop()
}
