package metrics

import (
	"database/sql"
	"errors"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	namespace = "postgresql"
)

type Metric interface {
	Scrape(*sql.DB) error
	Collect(chan<- prometheus.Metric)
	Describe(chan<- *prometheus.Desc)
}

func getMetrics(db *sql.DB, metrics map[string]prometheus.Gauge, metricsDef map[string]string, subsystem, tail string, args []interface{}) error {
	var keys []string
	for key := range metricsDef {
		keys = append(keys, key)
	}

	vals := make([]interface{}, len(keys))
	for i := range keys {
		vals[i] = new(float64)
	}
	err := db.QueryRow("SELECT "+strings.Join(keys, ",")+" FROM "+tail, args...).Scan(vals...)
	if err != nil {
		return errors.New("error running buffers stats query on database: " + err.Error())
	}

	for i, val := range vals {
		key := keys[i]
		if _, ok := metrics[key]; !ok {
			metrics[key] = prometheus.NewGauge(prometheus.GaugeOpts{
				Namespace: namespace,
				Subsystem: subsystem,
				Name:      key,
				Help:      metricsDef[key],
			})
		}

		metrics[key].Set(*val.(*float64))
	}

	return nil
}
