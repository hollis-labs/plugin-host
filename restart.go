package pluginhost

import "time"

// RestartPolicy is a restart budget with exponential backoff. The zero value
// is the schedule Tangent and Nanite both ran: 3 restarts, 1s doubling to a
// 30s ceiling, and a budget that is never refilled.
type RestartPolicy struct {
	// MaxRestarts is the number of restarts allowed. Zero means 3; a
	// negative value disables restarting.
	MaxRestarts int
	// Initial is the delay before the first restart (default 1s), Max the
	// ceiling (default 30s) and Factor the multiplier per attempt (default
	// 2.0).
	Initial time.Duration
	Max     time.Duration
	Factor  float64
	// StableFor refills the budget: a process that ran at least this long
	// before exiting starts the count again. Zero means never (the budget is
	// cumulative over the supervisor's life).
	StableFor time.Duration
}

// Backoff returns the delay before restart number attempt (0-indexed: the
// count of restarts already made) and whether a restart is allowed at all.
func (p RestartPolicy) Backoff(attempt int) (time.Duration, bool) {
	limit := p.MaxRestarts
	if limit == 0 {
		limit = 3
	}
	if limit < 0 || attempt >= limit {
		return 0, false
	}
	initial, ceiling, factor := p.Initial, p.Max, p.Factor
	if initial <= 0 {
		initial = time.Second
	}
	if ceiling <= 0 {
		ceiling = 30 * time.Second
	}
	if factor <= 0 {
		factor = 2.0
	}
	delay := initial
	for i := 0; i < attempt; i++ {
		delay = time.Duration(float64(delay) * factor)
		if delay >= ceiling {
			return ceiling, true
		}
	}
	return min(delay, ceiling), true
}
