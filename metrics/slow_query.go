package metrics

import (
	"database/sql"
	"errors"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type SlowQueryMetrics struct {
	mutex   sync.Mutex
	metrics map[string]prometheus.Gauge
	seconds int
}

func NewSlowQueryMetrics(secondsToConsiderSlow int) *SlowQueryMetrics {
	return &SlowQueryMetrics{
		seconds: secondsToConsiderSlow,
		metrics: map[string]prometheus.Gauge{
			"slow_queries": prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "slow_queries_total",
				Help:      "Number of slow queries",
			}),
			"slow_select_queries": prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "slow_select_queries_total",
				Help:      "Number of slow SELECT queries",
			}),
			"slow_dml_queries": prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Name:      "slow_dml_queries_total",
				Help:      "Number of slow data manipulation queries (INSERT, UPDATE, DELETE)",
			}),
		},
	}
}

func (s *SlowQueryMetrics) Scrape(db *sql.DB) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	count := new(float64)
	err := db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND NOW() - query_start > ($1 || ' sec')::interval", s.seconds).Scan(count)
	if err != nil {
		return errors.New("error counting slow queries: " + err.Error())
	}
	s.metrics["slow_queries"].Set(*count)

	err = db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND NOW() - query_start > ($1 || ' sec')::interval AND query ilike 'select%'", s.seconds).Scan(count)
	if err != nil {
		return errors.New("error counting slow select queries: " + err.Error())
	}
	s.metrics["slow_select_queries"].Set(*count)

	err = db.QueryRow("SELECT count(*) FROM pg_stat_activity WHERE state = 'active' AND NOW() - query_start > ($1 || ' sec')::interval AND (query ilike 'insert%' OR query ilike 'update%' OR query ilike 'delete%')", s.seconds).Scan(count)
	if err != nil {
		return errors.New("error counting slow dml queries: " + err.Error())
	}
	s.metrics["slow_dml_queries"].Set(*count)

	return nil
}

func (s *SlowQueryMetrics) Describe(ch chan<- *prometheus.Desc) {
	for _, m := range s.metrics {
		m.Describe(ch)
	}
}

func (s *SlowQueryMetrics) Collect(ch chan<- prometheus.Metric) {
	for _, m := range s.metrics {
		m.Collect(ch)
	}
}

// check interface
var _ Collection = new(SlowQueryMetrics)
