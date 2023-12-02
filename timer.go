package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

type Timer struct {
	lock   sync.RWMutex
	timers map[string][]time.Duration
	total  time.Duration
}

var timer *Timer

func getTimer() *Timer {
	if timer == nil {
		timer = NewTimer()
		return timer
	}
	return timer
}

func NewTimer() *Timer {
	return &Timer{
		timers: make(map[string][]time.Duration),
	}
}

func (t *Timer) timer(name string) func() {
	start := time.Now()
	return func() {
		t.lock.Lock()
		defer t.lock.Unlock()
		if t.timers[name] == nil {
			t.timers[name] = []time.Duration{}
		}
		t.timers[name] = append(t.timers[name], time.Since(start))
	}
}

func (t *Timer) avg(name string) time.Duration {
	l := int64(len(t.timers[name]))
	tot := time.Duration(0)
	for _, d := range t.timers[name] {
		tot += d
	}

	return time.Duration(int64(tot) / l)
}

func (t *Timer) min(name string) time.Duration {
	min, err := time.ParseDuration("1m")
	if err != nil {
		log.Fatal(err)
	}
	for _, d := range t.timers[name] {
		if d < min {
			min = d
		}
	}
	return min
}

func (t *Timer) max(name string) time.Duration {
	var max time.Duration
	for _, d := range t.timers[name] {
		if d > max {
			max = d
		}
	}
	return max
}

func (t *Timer) first(name string, count int) []time.Duration {
	return t.timers[name][:count]
}

func (t *Timer) last(name string, count int) []time.Duration {
	return t.timers[name][len(t.timers[name])-count:]
}

func (t *Timer) med(name string) time.Duration {
	return t.timers[name][len(t.timers[name])/2]
}

func (t *Timer) percentialAboveMedian(name string) float32 {
	median := t.med(name)
	count := float32(0)
	for i := 0; i < len(t.timers[name]); i++ {
		if t.timers[name][i] > median {
			count++
		}
	}
	return count / float32(len(t.timers[name])) * 100
}

func (t *Timer) percentail(name string, percentail float32) float32 {
	nth := float32(t.max(name)) * percentail
	count := float32(0)
	for i := 0; i < len(t.timers[name]); i++ {
		if float32(t.timers[name][i]) > nth {
			count++
		}
	}
	return count / float32(len(t.timers[name])) * 100

}

func (t *Timer) getStats() {
	for name := range t.timers {
		fmt.Printf(
			"%s::\n total: %d, Avg: %s, Min: %s, Max: %s, Med: %s, above median: %.1f%%, 90th: %.3f%%\n\n",
			name, len(t.timers[name]), t.avg(name), t.min(name), t.max(name), t.med(name), t.percentialAboveMedian(name), t.percentail(name, 0.9),
		)
	}
}
