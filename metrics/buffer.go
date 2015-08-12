package metrics

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	bufferMetrics = map[string]metric{
		"buffers_checkpoint":    metric{Name: "checkpoint_total", Help: "Number of buffers written during checkpoints"},
		"buffers_clean":         metric{Name: "clean_total", Help: "Number of buffers written by the background writer"},
		"maxwritten_clean":      metric{Name: "maxwritten_clean_total", Help: "Number of times the background writer stopped a cleaning scan because it had written too many buffers"},
		"buffers_backend":       metric{Name: "backend_total", Help: "Number of buffers written directly by a backend"},
		"buffers_backend_fsync": metric{Name: "backend_fsync_total", Help: "Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)"},
		"buffers_alloc":         metric{Name: "alloc_total", Help: "Number of buffers allocated"},
	}
)

type BufferMetrics struct {
	mutex   sync.Mutex
	metrics map[string]prometheus.Gauge
}

func NewBufferMetrics() *BufferMetrics {
	return &BufferMetrics{
		metrics: map[string]prometheus.Gauge{},
	}
}

func (b *BufferMetrics) Scrape(db *sql.DB) error {
	b.mutex.Lock()
	defer b.mutex.Unlock()

	result, err := getMetrics(db, bufferMetrics, "pg_stat_bgwriter", nil)
	if err != nil {
		return errors.New("error running table stats query on database: " + err.Error())
	}

	for key, val := range result {
		if _, ok := b.metrics[key]; !ok {
			b.metrics[key] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "buffers",
				Name:      bufferMetrics[key].Name,
				Help:      bufferMetrics[key].Help,
			})
		}

		b.metrics[key].Set(val)
	}

	return nil
}

func (b *BufferMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range b.metrics {
		m.Describe(ch)
	}
}

func (b *BufferMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, m := range b.metrics {
		m.Collect(ch)
	}
}

// check interface
var _ Collection = new(BufferMetrics)
