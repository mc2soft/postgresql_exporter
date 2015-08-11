package metrics

import (
	"database/sql"
	"errors"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	bufferMetrics = map[string]string{
		"buffers_checkpoint":    "Number of buffers written during checkpoints",
		"buffers_clean":         "Number of buffers written by the background writer",
		"maxwritten_clean":      "Number of times the background writer stopped a cleaning scan because it had written too many buffers",
		"buffers_backend":       "Number of buffers written directly by a backend",
		"buffers_backend_fsync": "Number of times a backend had to execute its own fsync call (normally the background writer handles those even when the backend does its own write)",
		"buffers_alloc":         "Number of buffers allocated",
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

	var keys []string
	for key := range bufferMetrics {
		keys = append(keys, key)
	}

	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = new(float64)
	}
	err := db.QueryRow("SELECT " + strings.Join(keys, ",") + " FROM pg_stat_bgwriter").Scan(vals...)
	if err != nil {
		return errors.New("error running buffers stats query on database: " + err.Error())
	}

	for i, val := range vals {
		key := keys[i]
		if _, ok := b.metrics[key]; !ok {
			b.metrics[key] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: "buffers",
				Name:      key,
				Help:      bufferMetrics[key],
			})
		}

		b.metrics[key].Set(*val.(*float64))
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
