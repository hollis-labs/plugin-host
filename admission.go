package pluginhost

import "sync"

const reversePerConnection = 8
const reversePerHost = 64

// reversePool is shared by the connections belonging to one host. Admission is
// immediate; a canceled callback keeps its permit until execution really ends.
type reversePool struct {
	mu     sync.Mutex
	active int
}

type reverseAdmission struct {
	pool   *reversePool
	active int // protected by pool.mu
}

type reversePermit struct {
	owner *reverseAdmission
	once  sync.Once
}

func (a *reverseAdmission) acquire() *reversePermit {
	a.pool.mu.Lock()
	defer a.pool.mu.Unlock()
	if a.active >= reversePerConnection || a.pool.active >= reversePerHost {
		return nil
	}
	a.active++
	a.pool.active++
	return &reversePermit{owner: a}
}

// release belongs to execution completion, never logical cancellation.
func (p *reversePermit) release() {
	p.once.Do(func() {
		a := p.owner
		a.pool.mu.Lock()
		defer a.pool.mu.Unlock()
		a.active--
		a.pool.active--
	})
}
